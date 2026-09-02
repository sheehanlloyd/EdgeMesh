package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/origin"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
	"github.com/sheehanlloyd/edgemesh/internal/routing"
)

// StatusResponse is the payload of GET /v1/status.
type StatusResponse struct {
	NodeID        string `json:"node_id"`
	Version       string `json:"version"`
	Role          string `json:"role"`
	Term          uint64 `json:"term"`
	LeaderID      string `json:"leader_id"`
	IsLeader      bool   `json:"is_leader"`
	ConfigVersion uint64 `json:"config_version"`
	Routes        int    `json:"routes"`
	OriginPools   int    `json:"origin_pools"`
	EdgeNodes     int    `json:"edge_nodes"`
	CommitIndex   uint64 `json:"commit_index"`
	LastApplied   uint64 `json:"last_applied"`
}

func (a *API) handleStatus(w http.ResponseWriter, _ *http.Request) error {
	st := a.opts.Node.Status()
	resp := StatusResponse{
		NodeID:        a.opts.NodeID,
		Version:       a.opts.Version,
		Role:          string(st.Role),
		Term:          st.Term,
		LeaderID:      st.LeaderID,
		IsLeader:      a.opts.Node.IsLeader(),
		ConfigVersion: a.opts.StateMachine.ConfigVersion(),
		Routes:        len(a.opts.StateMachine.Routes()),
		OriginPools:   len(a.opts.StateMachine.OriginPools()),
		CommitIndex:   st.CommitIndex,
		LastApplied:   st.LastApplied,
	}
	if a.opts.Membership != nil {
		resp.EdgeNodes = len(a.opts.Membership.Snapshot().GetNodes())
	}
	return a.writeJSON(w, resp)
}

func (a *API) handleRaft(w http.ResponseWriter, _ *http.Request) error {
	return a.writeJSON(w, a.opts.Node.Status())
}

// NodesResponse is the payload of GET /v1/nodes.
type NodesResponse struct {
	Nodes             []json.RawMessage `json:"nodes"`
	MembershipVersion uint64            `json:"membership_version"`
	IsLeader          bool              `json:"is_leader"`
}

func (a *API) handleNodes(w http.ResponseWriter, _ *http.Request) error {
	if a.opts.Membership == nil {
		return a.writeJSON(w, NodesResponse{Nodes: []json.RawMessage{}})
	}
	// A follower does not own liveness, so its node list would be empty or
	// stale. Redirecting is more honest than returning an empty list that looks
	// like "no edges are running".
	if err := a.requireLeader(); err != nil {
		return err
	}
	all := a.opts.Membership.All()
	raw := make([]json.RawMessage, 0, len(all))
	for _, n := range all {
		b, err := a.marshaler.Marshal(n)
		if err != nil {
			return errs.Wrap(errs.ClassProtocol, err, "render edge node %q", n.GetId())
		}
		raw = append(raw, b)
	}
	return a.writeJSON(w, NodesResponse{
		Nodes:             raw,
		MembershipVersion: a.opts.Membership.Version(),
		IsLeader:          true,
	})
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

func (a *API) handleListRoutes(w http.ResponseWriter, _ *http.Request) error {
	routes := a.opts.StateMachine.Routes()
	raw := make([]json.RawMessage, 0, len(routes))
	for _, r := range routes {
		b, err := a.marshaler.Marshal(r)
		if err != nil {
			return errs.Wrap(errs.ClassProtocol, err, "render route %q", r.GetId())
		}
		raw = append(raw, b)
	}
	return a.writeJSON(w, map[string]any{
		"routes":         raw,
		"config_version": a.opts.StateMachine.ConfigVersion(),
	})
}

func (a *API) handleGetRoute(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	route, ok := a.opts.StateMachine.Route(id)
	if !ok {
		return errs.New(errs.ClassNotFound, "route %q does not exist", id)
	}
	b, err := a.marshaler.Marshal(route)
	if err != nil {
		return errs.Wrap(errs.ClassProtocol, err, "render route %q", id)
	}
	return a.writeRaw(w, http.StatusOK, b)
}

func (a *API) handleCreateRoute(w http.ResponseWriter, r *http.Request) error {
	return a.writeRoute(w, r, statemachine.CommandCreateRoute, "")
}

func (a *API) handleUpdateRoute(w http.ResponseWriter, r *http.Request) error {
	return a.writeRoute(w, r, statemachine.CommandUpdateRoute, r.PathValue("id"))
}

func (a *API) writeRoute(w http.ResponseWriter, r *http.Request, ct statemachine.CommandType, pathID string) error {
	if err := a.requireLeader(); err != nil {
		return err
	}
	var route edgemeshv1.Route
	if err := a.decode(r, &route); err != nil {
		return err
	}
	// The path is authoritative for an update: a body claiming a different ID
	// would silently modify a different route than the URL names.
	if pathID != "" {
		if route.GetId() != "" && route.GetId() != pathID {
			return errs.New(errs.ClassValidation,
				"route id in the body (%q) does not match the path (%q)", route.GetId(), pathID)
		}
		route.Id = pathID
	}
	// Validation runs before the proposal so a bad route costs a 400, not a
	// consensus round and a permanent log entry.
	if err := routing.Validate(&route); err != nil {
		return err
	}

	cmd := &statemachine.Command{
		Type: ct,
		// The leader stamps the timestamp so every replica applies the same
		// value; reading a clock inside Apply would make replicas diverge.
		TimestampUnixMs: time.Now().UnixMilli(),
		Route:           &route,
		ExpectedVersion: parseVersion(r),
	}
	res, err := a.propose(r.Context(), cmd)
	if err != nil {
		return err
	}
	b, err := a.marshaler.Marshal(res.Route)
	if err != nil {
		return errs.Wrap(errs.ClassProtocol, err, "render created route")
	}
	code := http.StatusOK
	if ct == statemachine.CommandCreateRoute {
		code = http.StatusCreated
	}
	w.Header().Set("X-EdgeMesh-Config-Version", formatUint(res.ConfigVersion))
	return a.writeRaw(w, code, b)
}

func (a *API) handleDeleteRoute(w http.ResponseWriter, r *http.Request) error {
	if err := a.requireLeader(); err != nil {
		return err
	}
	res, err := a.propose(r.Context(), &statemachine.Command{
		Type:            statemachine.CommandDeleteRoute,
		TimestampUnixMs: time.Now().UnixMilli(),
		ID:              r.PathValue("id"),
		ExpectedVersion: parseVersion(r),
	})
	if err != nil {
		return err
	}
	return a.writeJSON(w, map[string]any{
		"deleted":        r.PathValue("id"),
		"config_version": res.ConfigVersion,
	})
}

// ---------------------------------------------------------------------------
// Origin pools
// ---------------------------------------------------------------------------

func (a *API) handleListPools(w http.ResponseWriter, _ *http.Request) error {
	pools := a.opts.StateMachine.OriginPools()
	raw := make([]json.RawMessage, 0, len(pools))
	for _, p := range pools {
		b, err := a.marshaler.Marshal(p)
		if err != nil {
			return errs.Wrap(errs.ClassProtocol, err, "render origin pool %q", p.GetId())
		}
		raw = append(raw, b)
	}
	return a.writeJSON(w, map[string]any{
		"origin_pools":   raw,
		"config_version": a.opts.StateMachine.ConfigVersion(),
	})
}

func (a *API) handleGetPool(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	pool, ok := a.opts.StateMachine.OriginPool(id)
	if !ok {
		return errs.New(errs.ClassNotFound, "origin pool %q does not exist", id)
	}
	b, err := a.marshaler.Marshal(pool)
	if err != nil {
		return errs.Wrap(errs.ClassProtocol, err, "render origin pool %q", id)
	}
	return a.writeRaw(w, http.StatusOK, b)
}

func (a *API) handleCreatePool(w http.ResponseWriter, r *http.Request) error {
	return a.writePool(w, r, statemachine.CommandCreateOriginPool, "")
}

func (a *API) handleUpdatePool(w http.ResponseWriter, r *http.Request) error {
	return a.writePool(w, r, statemachine.CommandUpdateOriginPool, r.PathValue("id"))
}

func (a *API) writePool(w http.ResponseWriter, r *http.Request, ct statemachine.CommandType, pathID string) error {
	if err := a.requireLeader(); err != nil {
		return err
	}
	var pool edgemeshv1.OriginPool
	if err := a.decode(r, &pool); err != nil {
		return err
	}
	if pathID != "" {
		if pool.GetId() != "" && pool.GetId() != pathID {
			return errs.New(errs.ClassValidation,
				"origin pool id in the body (%q) does not match the path (%q)", pool.GetId(), pathID)
		}
		pool.Id = pathID
	}
	if err := origin.ValidatePool(&pool); err != nil {
		return err
	}

	res, err := a.propose(r.Context(), &statemachine.Command{
		Type:            ct,
		TimestampUnixMs: time.Now().UnixMilli(),
		OriginPool:      &pool,
		ExpectedVersion: parseVersion(r),
	})
	if err != nil {
		return err
	}
	b, err := a.marshaler.Marshal(res.OriginPool)
	if err != nil {
		return errs.Wrap(errs.ClassProtocol, err, "render origin pool")
	}
	code := http.StatusOK
	if ct == statemachine.CommandCreateOriginPool {
		code = http.StatusCreated
	}
	w.Header().Set("X-EdgeMesh-Config-Version", formatUint(res.ConfigVersion))
	return a.writeRaw(w, code, b)
}

func (a *API) handleDeletePool(w http.ResponseWriter, r *http.Request) error {
	if err := a.requireLeader(); err != nil {
		return err
	}
	res, err := a.propose(r.Context(), &statemachine.Command{
		Type:            statemachine.CommandDeleteOriginPool,
		TimestampUnixMs: time.Now().UnixMilli(),
		ID:              r.PathValue("id"),
		ExpectedVersion: parseVersion(r),
	})
	if err != nil {
		return err
	}
	return a.writeJSON(w, map[string]any{
		"deleted":        r.PathValue("id"),
		"config_version": res.ConfigVersion,
	})
}

// ---------------------------------------------------------------------------
// Settings and purge
// ---------------------------------------------------------------------------

func (a *API) handleGetSettings(w http.ResponseWriter, _ *http.Request) error {
	b, err := a.marshaler.Marshal(a.opts.StateMachine.Settings())
	if err != nil {
		return errs.Wrap(errs.ClassProtocol, err, "render settings")
	}
	return a.writeRaw(w, http.StatusOK, b)
}

func (a *API) handleSetSettings(w http.ResponseWriter, r *http.Request) error {
	if err := a.requireLeader(); err != nil {
		return err
	}
	var s edgemeshv1.GlobalSettings
	if err := a.decode(r, &s); err != nil {
		return err
	}
	res, err := a.propose(r.Context(), &statemachine.Command{
		Type:            statemachine.CommandSetGlobalSettings,
		TimestampUnixMs: time.Now().UnixMilli(),
		Settings:        &s,
	})
	if err != nil {
		return err
	}
	b, err := a.marshaler.Marshal(res.Settings)
	if err != nil {
		return errs.Wrap(errs.ClassProtocol, err, "render settings")
	}
	w.Header().Set("X-EdgeMesh-Config-Version", formatUint(res.ConfigVersion))
	return a.writeRaw(w, http.StatusOK, b)
}

// PurgeRequest is the body of POST /v1/cache/purge.
type PurgeRequest struct {
	// Scope is one of key, route, all.
	Scope    string `json:"scope"`
	RouteID  string `json:"route_id,omitempty"`
	CacheKey string `json:"cache_key,omitempty"`
}

func (a *API) handlePurge(w http.ResponseWriter, r *http.Request) error {
	if err := a.requireLeader(); err != nil {
		return err
	}
	var req PurgeRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}

	var scope edgemeshv1.PurgeScope
	switch req.Scope {
	case "key":
		scope = edgemeshv1.PurgeScope_PURGE_SCOPE_KEY
	case "route":
		scope = edgemeshv1.PurgeScope_PURGE_SCOPE_ROUTE
	case "all":
		scope = edgemeshv1.PurgeScope_PURGE_SCOPE_ALL
	default:
		return errs.New(errs.ClassValidation,
			"purge scope must be one of key, route, all; got %q", req.Scope)
	}

	res, err := a.propose(r.Context(), &statemachine.Command{
		Type:            statemachine.CommandPurgeCache,
		TimestampUnixMs: time.Now().UnixMilli(),
		Purge: &edgemeshv1.PurgeDirective{
			Scope: scope, RouteId: req.RouteID, CacheKey: req.CacheKey,
		},
	})
	if err != nil {
		return err
	}
	w.Header().Set("X-EdgeMesh-Config-Version", formatUint(res.ConfigVersion))
	return a.writeJSON(w, map[string]any{
		"scope":          req.Scope,
		"route_id":       req.RouteID,
		"cache_key":      req.CacheKey,
		"purge_version":  res.Purge.GetVersion(),
		"config_version": res.ConfigVersion,
	})
}

// propose submits a command and notifies the broadcaster on success.
func (a *API) propose(ctx context.Context, cmd *statemachine.Command) (statemachine.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, a.opts.ProposeTimeout)
	defer cancel()

	res, err := a.opts.Node.Propose(ctx, cmd)
	if err != nil {
		return res, err
	}
	if a.opts.OnWrite != nil {
		a.opts.OnWrite(res, cmd)
	}
	return res, nil
}

// requireLeader rejects a write on a follower with a usable leader hint.
func (a *API) requireLeader() error {
	if a.opts.Node.IsLeader() {
		return nil
	}
	leader := a.opts.Node.LeaderID()
	addr := ""
	if a.opts.LeaderAddress != nil {
		addr = a.opts.LeaderAddress(leader)
	}
	if leader == "" {
		return errs.New(errs.ClassUnavailable,
			"no raft leader is currently elected; retry shortly")
	}
	return &notLeaderError{leaderID: leader, address: addr}
}
