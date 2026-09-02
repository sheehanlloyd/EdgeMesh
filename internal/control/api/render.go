package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// notLeaderError carries the leader hint a client needs to redirect.
type notLeaderError struct {
	leaderID string
	address  string
}

func (e *notLeaderError) Error() string {
	if e.address != "" {
		return "not the raft leader; leader is " + e.leaderID + " at " + e.address
	}
	return "not the raft leader; leader is " + e.leaderID
}

// Class makes this error classify as not-leader, so it maps to the same HTTP
// status and metric label as any other not-leader error while still carrying
// the leader hint a client needs to redirect.
func (e *notLeaderError) Class() errs.Class { return errs.ClassNotLeader }

// ErrorResponse is the API's error body. It is deliberately small and carries
// no internal detail beyond a classified code and a human message.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	// LeaderID and LeaderAddress are set on a not-leader response so the CLI
	// can retry against the right node without a discovery round trip.
	LeaderID      string `json:"leader_id,omitempty"`
	LeaderAddress string `json:"leader_address,omitempty"`
}

// statusFor maps an error class to an HTTP status.
func statusFor(class errs.Class) int {
	switch class {
	case errs.ClassValidation:
		return http.StatusBadRequest
	case errs.ClassNotFound:
		return http.StatusNotFound
	case errs.ClassConflict:
		return http.StatusConflict
	case errs.ClassUnauthorized:
		return http.StatusUnauthorized
	case errs.ClassNotLeader, errs.ClassUnavailable:
		// 503 rather than a redirect: the CLI reads the leader hint from the
		// body, and a 307 would make a curl user silently re-POST their body to
		// a different host.
		return http.StatusServiceUnavailable
	case errs.ClassTimeout:
		return http.StatusGatewayTimeout
	case errs.ClassTooLarge:
		return http.StatusRequestEntityTooLarge
	case errs.ClassRateLimited:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

func (a *API) writeError(w http.ResponseWriter, r *http.Request, err error) {
	class := errs.ClassOf(err)
	code := statusFor(class)

	body := ErrorResponse{Error: string(class), Message: err.Error()}
	var nl *notLeaderError
	if errors.As(err, &nl) {
		body.LeaderID = nl.leaderID
		body.LeaderAddress = nl.address
		if nl.address != "" {
			// Advisory only; the CLI uses the body. A browser will not follow it
			// because the status is 503.
			w.Header().Set("X-EdgeMesh-Leader", nl.address)
		}
	}

	// A 5xx is a real problem worth logging; a 4xx is the client's mistake and
	// would otherwise let anyone fill the log by sending bad requests.
	if code >= 500 {
		a.log.Error("admin request failed",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", code),
			slog.String("error_class", string(class)),
			slog.String("error", err.Error()))
	} else {
		a.log.Debug("admin request rejected",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", code),
			slog.String("error_class", string(class)))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// writeJSON writes a 200 response. Every other status goes through writeRaw or
// the error renderer, so this helper does not take a code.
func (a *API) writeJSON(w http.ResponseWriter, body any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The response is already committed, so this can only be logged.
		a.log.Debug("failed to write admin response", slog.String("error", err.Error()))
	}
	return nil
}

func (a *API) writeRaw(w http.ResponseWriter, code int, body []byte) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(body); err != nil {
		a.log.Debug("failed to write admin response", slog.String("error", err.Error()))
	}
	return nil
}

// decode parses a protobuf-backed request body.
//
// Unknown fields are rejected rather than ignored: silently dropping a
// misspelled policy field would leave an operator believing they had configured
// something they had not.
func (a *API) decode(r *http.Request, m proto.Message) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return errs.Wrap(errs.ClassValidation, err, "read request body")
	}
	if len(body) == 0 {
		return errs.New(errs.ClassValidation, "a request body is required")
	}
	if err := a.unmarshaler.Unmarshal(body, m); err != nil {
		return errs.Wrap(errs.ClassValidation, err, "parse request body")
	}
	return nil
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errs.Wrap(errs.ClassValidation, err, "parse request body")
	}
	return nil
}

// parseVersion reads the optimistic-concurrency version from the query string.
// An unparseable value is treated as absent rather than as an error, because
// the check is opt-in.
func parseVersion(r *http.Request) uint64 {
	v := r.URL.Query().Get("version")
	if v == "" {
		return 0
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func formatUint(v uint64) string { return strconv.FormatUint(v, 10) }
