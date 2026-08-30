// Package health implements the standard liveness/readiness contract shared by
// every EdgeMesh process.
//
// The distinction between the two endpoints is the whole point:
//
//   - /healthz answers "is this process alive?" and must never depend on an
//     external system. A liveness probe that fails because a dependency is down
//     causes the orchestrator to restart a healthy process, turning a partial
//     outage into a total one.
//   - /readyz answers "can this process serve its intended traffic?" and may
//     consult internal subsystem state.
//
// An edge that has lost its control-plane connection but still holds a valid
// routing snapshot is therefore *ready*: it can serve every request it could
// serve a moment ago. This is a core demonstration point of the project.
package health

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
)

// Check reports whether one subsystem is ready.
type Check func() error

// Registry holds the process's readiness checks.
type Registry struct {
	mu     sync.RWMutex
	checks map[string]Check
	// live is flipped false only during shutdown, so an orchestrator stops
	// routing new traffic before the drain begins.
	live bool
}

// NewRegistry returns a registry that reports live immediately and ready once
// its checks pass.
func NewRegistry() *Registry {
	return &Registry{checks: make(map[string]Check), live: true}
}

// Register adds or replaces a readiness check.
func (r *Registry) Register(name string, c Check) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks[name] = c
}

// Deregister removes a check.
func (r *Registry) Deregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.checks, name)
}

// SetLive controls the liveness answer. It is set false at the start of a
// graceful shutdown.
func (r *Registry) SetLive(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.live = v
}

// Live reports process liveness.
func (r *Registry) Live() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.live
}

// Result is one check's outcome.
type Result struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// Report is the readiness response body.
type Report struct {
	Status string   `json:"status"`
	Checks []Result `json:"checks"`
}

// Ready evaluates every check and reports the aggregate outcome.
func (r *Registry) Ready() Report {
	r.mu.RLock()
	names := make([]string, 0, len(r.checks))
	for n := range r.checks {
		names = append(names, n)
	}
	checks := make(map[string]Check, len(r.checks))
	for n, c := range r.checks {
		checks[n] = c
	}
	r.mu.RUnlock()

	// Sorting makes the response stable, which matters for both humans reading
	// it and tests asserting on it.
	sort.Strings(names)

	rep := Report{Status: "ready", Checks: make([]Result, 0, len(names))}
	for _, n := range names {
		res := Result{Name: n, Status: "ready"}
		if err := checks[n](); err != nil {
			res.Status = "not_ready"
			res.Error = err.Error()
			rep.Status = "not_ready"
		}
		rep.Checks = append(rep.Checks, res)
	}
	return rep
}

// LivenessHandler serves /healthz.
func (r *Registry) LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if !r.Live() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "shutting_down"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// ReadinessHandler serves /readyz.
func (r *Registry) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		rep := r.Ready()
		code := http.StatusOK
		if rep.Status != "ready" {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, rep)
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	// A failed write to a probe client is not actionable and must not panic or
	// log-spam; the probe will simply retry.
	_ = json.NewEncoder(w).Encode(body)
}
