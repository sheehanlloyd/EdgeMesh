// Package api implements EdgeMesh's admin HTTP API.
//
// # Leader semantics
//
// Every write is proposed through Raft and therefore must reach the leader. A
// follower does not proxy: it answers 503 with the leader's address in a
// `Location`-style hint, and the CLI follows it. Internal forwarding would hide
// which node actually served a write, which is exactly the thing an operator
// debugging a control-plane problem needs to see.
//
// Reads are served by the leader for the same reason: a follower's state
// machine may lag by an uncommitted entry, and EdgeMesh does not claim
// linearizable follower reads.
//
// # Validation before consensus
//
// Requests are validated before a Raft proposal is made. A malformed route must
// cost a 400, not a consensus round and a log entry that every replica has to
// store and replay forever.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/sheehanlloyd/edgemesh/internal/control/membership"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/raft/node"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
	"github.com/sheehanlloyd/edgemesh/internal/security"
)

// maxBodyBytes bounds an admin request body. Without it an unauthenticated
// caller could make the process buffer arbitrary memory before auth even runs,
// which is why the auth middleware also runs first.
const maxBodyBytes = 4 << 20

// Proposer submits a command to Raft and waits for it to commit.
type Proposer interface {
	Propose(ctx context.Context, cmd *statemachine.Command) (statemachine.Result, error)
	Status() node.Status
	IsLeader() bool
	LeaderID() string
}

// LeaderAddressResolver maps a leader ID to its advertised admin address.
type LeaderAddressResolver func(leaderID string) string

// Options configures the API.
type Options struct {
	Node         Proposer
	StateMachine *statemachine.StateMachine
	Membership   *membership.Tracker
	Auth         *security.TokenAuthenticator
	Logger       *slog.Logger
	// LeaderAddress resolves a leader hint for redirects.
	LeaderAddress LeaderAddressResolver
	// NodeID identifies this control node in responses.
	NodeID string
	// Version is reported by /v1/status.
	Version string
	// OnWrite is called after a successful write so the control process can
	// broadcast the change to edges.
	OnWrite func(statemachine.Result, *statemachine.Command)
	// ProposeTimeout bounds one admin write.
	ProposeTimeout time.Duration
	// Metrics records per-endpoint outcomes.
	RecordRequest func(endpoint, result string)
}

// API is the admin HTTP surface.
type API struct {
	opts Options
	log  *slog.Logger
	// marshaler renders protobuf configuration objects as idiomatic JSON so the
	// API's schema is exactly the .proto schema, with no hand-maintained DTOs
	// to drift from it.
	marshaler   protojson.MarshalOptions
	unmarshaler protojson.UnmarshalOptions
}

// New builds the API.
func New(o Options) (*API, error) {
	if o.Node == nil {
		return nil, errs.New(errs.ClassValidation, "admin api: a raft node is required")
	}
	if o.StateMachine == nil {
		return nil, errs.New(errs.ClassValidation, "admin api: a state machine is required")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.ProposeTimeout <= 0 {
		o.ProposeTimeout = 10 * time.Second
	}
	return &API{
		opts: o,
		log:  o.Logger,
		marshaler: protojson.MarshalOptions{
			// Zero values are emitted so a client can tell "disabled" from
			// "absent" without consulting the schema.
			EmitUnpopulated: true,
			UseProtoNames:   true,
		},
		unmarshaler: protojson.UnmarshalOptions{DiscardUnknown: false},
	}, nil
}

// Handler returns the routed admin handler.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/status", a.wrap("status", a.handleStatus))
	mux.HandleFunc("GET /v1/raft", a.wrap("raft", a.handleRaft))
	mux.HandleFunc("GET /v1/nodes", a.wrap("nodes", a.handleNodes))

	mux.HandleFunc("GET /v1/routes", a.wrap("routes.list", a.handleListRoutes))
	mux.HandleFunc("POST /v1/routes", a.wrap("routes.create", a.handleCreateRoute))
	mux.HandleFunc("GET /v1/routes/{id}", a.wrap("routes.get", a.handleGetRoute))
	mux.HandleFunc("PUT /v1/routes/{id}", a.wrap("routes.update", a.handleUpdateRoute))
	mux.HandleFunc("DELETE /v1/routes/{id}", a.wrap("routes.delete", a.handleDeleteRoute))

	mux.HandleFunc("GET /v1/origin-pools", a.wrap("pools.list", a.handleListPools))
	mux.HandleFunc("POST /v1/origin-pools", a.wrap("pools.create", a.handleCreatePool))
	mux.HandleFunc("GET /v1/origin-pools/{id}", a.wrap("pools.get", a.handleGetPool))
	mux.HandleFunc("PUT /v1/origin-pools/{id}", a.wrap("pools.update", a.handleUpdatePool))
	mux.HandleFunc("DELETE /v1/origin-pools/{id}", a.wrap("pools.delete", a.handleDeletePool))

	mux.HandleFunc("GET /v1/settings", a.wrap("settings.get", a.handleGetSettings))
	mux.HandleFunc("PUT /v1/settings", a.wrap("settings.update", a.handleSetSettings))

	mux.HandleFunc("POST /v1/cache/purge", a.wrap("cache.purge", a.handlePurge))

	var h http.Handler = mux
	if a.opts.Auth.Enabled() {
		// Authentication runs before routing and before any body is read, so an
		// unauthenticated caller cannot make this process do parsing work.
		h = a.opts.Auth.Middleware(h)
	}
	return h
}

// wrap adds body limits, error translation, and metric recording.
func (a *API) wrap(endpoint string, fn func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		err := fn(w, r)
		result := "ok"
		if err != nil {
			result = string(errs.ClassOf(err))
			a.writeError(w, r, err)
		}
		if a.opts.RecordRequest != nil {
			a.opts.RecordRequest(endpoint, result)
		}
	}
}
