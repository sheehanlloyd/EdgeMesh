package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/control/membership"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	raftnode "github.com/sheehanlloyd/edgemesh/internal/raft/node"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
	"github.com/sheehanlloyd/edgemesh/internal/security"
)

// fakeNode applies commands directly, so the API's own behaviour is tested
// without standing up a Raft cluster. The consensus path has its own tests.
type fakeNode struct {
	mu       sync.Mutex
	sm       *statemachine.StateMachine
	index    uint64
	isLeader bool
	leaderID string
	// proposeErr, when set, is returned instead of applying.
	proposeErr error
	writes     []*statemachine.Command
}

func (f *fakeNode) Propose(_ context.Context, cmd *statemachine.Command) (statemachine.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.proposeErr != nil {
		return statemachine.Result{}, f.proposeErr
	}
	f.index++
	f.writes = append(f.writes, cmd)
	return f.sm.Apply(f.index, cmd)
}

func (f *fakeNode) Status() raftnode.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	role := raftnode.RoleFollower
	if f.isLeader {
		role = raftnode.RoleLeader
	}
	return raftnode.Status{
		NodeID: "cp-1", Role: role, Term: 3, LeaderID: f.leaderID,
		CommitIndex: f.index, LastApplied: f.index, LastLogIndex: f.index,
	}
}

func (f *fakeNode) IsLeader() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.isLeader
}

func (f *fakeNode) LeaderID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leaderID
}

func newAPI(t *testing.T, leader bool, auth *security.TokenAuthenticator) (*API, *fakeNode, *statemachine.StateMachine, http.Handler) {
	t.Helper()
	sm := statemachine.New()
	node := &fakeNode{sm: sm, isLeader: leader, leaderID: "cp-2"}
	if leader {
		node.leaderID = "cp-1"
	}
	members := membership.New(membership.Options{})
	members.SetLeader(leader)

	a, err := New(Options{
		Node: node, StateMachine: sm, Membership: members, Auth: auth,
		NodeID: "cp-1", Version: "test",
		LeaderAddress:  func(id string) string { return id + ":7100" },
		ProposeTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return a, node, sm, a.Handler()
}

func do(t *testing.T, h http.Handler, method, path, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var parsed map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &parsed)
	return w, parsed
}

const validPool = `{"id":"pool-1","origins":[{"id":"o1","scheme":"http","host":"origin-1","port":8080}]}`
const validRoute = `{"id":"r1","hostname":"demo.local","path_prefix":"/","origin_pool_id":"pool-1","enabled":true}`

func TestStatusAndRaft(t *testing.T) {
	_, _, _, h := newAPI(t, true, nil)

	w, body := do(t, h, http.MethodGet, "/v1/status", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if body["node_id"] != "cp-1" || body["is_leader"] != true {
		t.Fatalf("status body = %v", body)
	}

	w, body = do(t, h, http.MethodGet, "/v1/raft", "")
	if w.Code != http.StatusOK || body["role"] != "leader" {
		t.Fatalf("raft body = %v", body)
	}
}

func TestRouteLifecycle(t *testing.T) {
	_, _, sm, h := newAPI(t, true, nil)

	if w, _ := do(t, h, http.MethodPost, "/v1/origin-pools", validPool); w.Code != http.StatusCreated {
		t.Fatalf("pool create = %d", w.Code)
	}
	w, body := do(t, h, http.MethodPost, "/v1/routes", validRoute)
	if w.Code != http.StatusCreated {
		t.Fatalf("route create = %d: %v", w.Code, body)
	}
	if body["id"] != "r1" {
		t.Fatalf("created route = %v", body)
	}
	// A successful write reports the resulting config version, which is how a
	// client knows what it can expect edges to converge on.
	if w.Header().Get("X-EdgeMesh-Config-Version") == "" {
		t.Error("a write must report the resulting config version")
	}

	w, _ = do(t, h, http.MethodGet, "/v1/routes/r1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("route get = %d", w.Code)
	}

	w, body = do(t, h, http.MethodGet, "/v1/routes", "")
	if w.Code != http.StatusOK {
		t.Fatalf("route list = %d", w.Code)
	}
	if routes, ok := body["routes"].([]any); !ok || len(routes) != 1 {
		t.Fatalf("route list = %v", body)
	}

	updated := `{"id":"r1","hostname":"demo.local","path_prefix":"/api","origin_pool_id":"pool-1","enabled":true}`
	if w, _ := do(t, h, http.MethodPut, "/v1/routes/r1", updated); w.Code != http.StatusOK {
		t.Fatalf("route update = %d", w.Code)
	}
	if r, _ := sm.Route("r1"); r.GetPathPrefix() != "/api" {
		t.Fatal("update did not take effect")
	}

	if w, _ := do(t, h, http.MethodDelete, "/v1/routes/r1", ""); w.Code != http.StatusOK {
		t.Fatalf("route delete = %d", w.Code)
	}
	if _, ok := sm.Route("r1"); ok {
		t.Fatal("route survived deletion")
	}
}

// Validation must happen before the proposal so a malformed route costs a 400
// rather than a consensus round and a permanent log entry.
func TestInvalidRouteIsRejectedBeforeProposal(t *testing.T) {
	_, node, _, h := newAPI(t, true, nil)

	w, body := do(t, h, http.MethodPost, "/v1/routes", `{"id":"","hostname":"demo.local","path_prefix":"/"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %v", w.Code, body)
	}
	if body["error"] != string(errs.ClassValidation) {
		t.Fatalf("error class = %v", body["error"])
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	if len(node.writes) != 0 {
		t.Fatal("an invalid route reached Raft; validation must run first")
	}
}

func TestUnknownFieldInBodyIsRejected(t *testing.T) {
	_, _, _, h := newAPI(t, true, nil)
	// Silently ignoring a misspelled field would leave an operator believing
	// they configured something they did not.
	w, _ := do(t, h, http.MethodPost, "/v1/routes",
		`{"id":"r1","hostname":"demo.local","path_prefix":"/","origin_pool_id":"p","enabled":true,"typoed_field":1}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an unknown field", w.Code)
	}
}

func TestPathIDIsAuthoritativeOnUpdate(t *testing.T) {
	_, _, _, h := newAPI(t, true, nil)
	do(t, h, http.MethodPost, "/v1/origin-pools", validPool)
	do(t, h, http.MethodPost, "/v1/routes", validRoute)

	// A body naming a different route than the URL must be refused rather than
	// silently modifying the wrong object.
	w, _ := do(t, h, http.MethodPut, "/v1/routes/r1",
		`{"id":"other","hostname":"demo.local","path_prefix":"/","origin_pool_id":"pool-1","enabled":true}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a mismatched id", w.Code)
	}
}

func TestNotFoundAndConflict(t *testing.T) {
	_, _, _, h := newAPI(t, true, nil)
	if w, _ := do(t, h, http.MethodGet, "/v1/routes/missing", ""); w.Code != http.StatusNotFound {
		t.Fatalf("missing route = %d, want 404", w.Code)
	}
	do(t, h, http.MethodPost, "/v1/origin-pools", validPool)
	do(t, h, http.MethodPost, "/v1/routes", validRoute)
	if w, _ := do(t, h, http.MethodPost, "/v1/routes", validRoute); w.Code != http.StatusConflict {
		t.Fatalf("duplicate create = %d, want 409", w.Code)
	}
	// Optimistic concurrency: a stale version is a conflict.
	if w, _ := do(t, h, http.MethodPut, "/v1/routes/r1?version=999", validRoute); w.Code != http.StatusConflict {
		t.Fatalf("stale version = %d, want 409", w.Code)
	}
}

// A follower must refuse writes and hand back a usable leader hint.
func TestFollowerRefusesWritesWithALeaderHint(t *testing.T) {
	_, _, _, h := newAPI(t, false, nil)

	for _, c := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/routes", validRoute},
		{http.MethodPut, "/v1/routes/r1", validRoute},
		{http.MethodDelete, "/v1/routes/r1", ""},
		{http.MethodPost, "/v1/origin-pools", validPool},
		{http.MethodPost, "/v1/cache/purge", `{"scope":"all"}`},
	} {
		w, body := do(t, h, c.method, c.path, c.body)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s %s = %d, want 503", c.method, c.path, w.Code)
		}
		if body["leader_id"] != "cp-2" || body["leader_address"] != "cp-2:7100" {
			t.Fatalf("%s %s: missing leader hint: %v", c.method, c.path, body)
		}
		if body["error"] != string(errs.ClassNotLeader) {
			t.Fatalf("error class = %v, want not_leader", body["error"])
		}
	}

	// Reads that do not require leadership still work on a follower.
	if w, _ := do(t, h, http.MethodGet, "/v1/routes", ""); w.Code != http.StatusOK {
		t.Fatalf("follower route list = %d, want 200", w.Code)
	}
}

func TestNoLeaderReportsUnavailable(t *testing.T) {
	sm := statemachine.New()
	node := &fakeNode{sm: sm, isLeader: false, leaderID: ""}
	a, err := New(Options{Node: node, StateMachine: sm, NodeID: "cp-1"})
	if err != nil {
		t.Fatal(err)
	}
	w, body := do(t, a.Handler(), http.MethodPost, "/v1/routes", validRoute)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", w.Code)
	}
	if !strings.Contains(body["message"].(string), "no raft leader") {
		t.Fatalf("message = %v", body["message"])
	}
}

func TestPurgeScopes(t *testing.T) {
	_, _, _, h := newAPI(t, true, nil)

	for _, body := range []string{`{"scope":"all"}`, `{"scope":"route","route_id":"r1"}`, `{"scope":"key","cache_key":"k"}`} {
		w, parsed := do(t, h, http.MethodPost, "/v1/cache/purge", body)
		if w.Code != http.StatusOK {
			t.Fatalf("purge %s = %d: %v", body, w.Code, parsed)
		}
		if parsed["purge_version"] == nil {
			t.Fatalf("purge %s returned no version", body)
		}
	}
	for _, body := range []string{`{"scope":"bogus"}`, `{"scope":"route"}`, `{"scope":"key"}`, `{}`} {
		if w, _ := do(t, h, http.MethodPost, "/v1/cache/purge", body); w.Code != http.StatusBadRequest {
			t.Errorf("invalid purge %s = %d, want 400", body, w.Code)
		}
	}
}

func TestSettings(t *testing.T) {
	_, _, _, h := newAPI(t, true, nil)
	w, body := do(t, h, http.MethodGet, "/v1/settings", "")
	if w.Code != http.StatusOK || body["replication_factor"] == nil {
		t.Fatalf("settings get = %d %v", w.Code, body)
	}
	if w, _ := do(t, h, http.MethodPut, "/v1/settings",
		`{"replication_factor":3,"ring_virtual_nodes":256,"max_object_bytes":"1048576"}`); w.Code != http.StatusOK {
		t.Fatalf("settings update = %d", w.Code)
	}
	if w, _ := do(t, h, http.MethodPut, "/v1/settings", `{"replication_factor":0}`); w.Code != http.StatusBadRequest {
		t.Fatalf("invalid settings = %d, want 400", w.Code)
	}
}

// Authentication must guard the whole surface, not only writes: an unauthorized
// reader can still enumerate the routing topology.
func TestAuthGuardsEveryEndpoint(t *testing.T) {
	t.Setenv("EDGEMESH_TEST_API_TOKEN", "correct-horse-battery-staple")
	auth, err := security.NewTokenAuthenticator(config.AdminAuth{
		Enabled: true, TokenEnv: "EDGEMESH_TEST_API_TOKEN",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, h := newAPI(t, true, auth)

	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/v1/status"},
		{http.MethodGet, "/v1/raft"},
		{http.MethodGet, "/v1/routes"},
		{http.MethodPost, "/v1/routes"},
		{http.MethodGet, "/v1/origin-pools"},
		{http.MethodPost, "/v1/cache/purge"},
	} {
		w, _ := do(t, h, c.method, c.path, "")
		if w.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without credentials = %d, want 401", c.method, c.path, w.Code)
		}
	}

	// The same request with the correct credential is served.
	req := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	req.Header.Set("Authorization", "Bearer correct-horse-battery-staple")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("authenticated status = %d, want 200", w.Code)
	}
}

func TestOnWriteIsCalledForEveryMutation(t *testing.T) {
	sm := statemachine.New()
	node := &fakeNode{sm: sm, isLeader: true, leaderID: "cp-1"}
	var mu sync.Mutex
	var seen []statemachine.CommandType

	a, err := New(Options{
		Node: node, StateMachine: sm, NodeID: "cp-1",
		OnWrite: func(_ statemachine.Result, cmd *statemachine.Command) {
			mu.Lock()
			seen = append(seen, cmd.Type)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h := a.Handler()
	do(t, h, http.MethodPost, "/v1/origin-pools", validPool)
	do(t, h, http.MethodPost, "/v1/routes", validRoute)
	do(t, h, http.MethodPost, "/v1/cache/purge", `{"scope":"all"}`)

	mu.Lock()
	defer mu.Unlock()
	// Every mutation must notify the broadcaster, or edges never learn about it.
	want := []statemachine.CommandType{
		statemachine.CommandCreateOriginPool,
		statemachine.CommandCreateRoute,
		statemachine.CommandPurgeCache,
	}
	if len(seen) != len(want) {
		t.Fatalf("broadcast notifications = %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("notification %d = %v, want %v", i, seen[i], want[i])
		}
	}
}

func TestRequestBodyIsBounded(t *testing.T) {
	_, _, _, h := newAPI(t, true, nil)
	// An unbounded body would let a caller make the process buffer arbitrary
	// memory before anything validates it.
	huge := `{"id":"r1","hostname":"` + strings.Repeat("a", maxBodyBytes+1024) + `"}`
	w, _ := do(t, h, http.MethodPost, "/v1/routes", huge)
	if w.Code == http.StatusCreated {
		t.Fatal("an oversized body was accepted")
	}
}

func TestEmptyBodyIsRejected(t *testing.T) {
	_, _, _, h := newAPI(t, true, nil)
	if w, _ := do(t, h, http.MethodPost, "/v1/routes", ""); w.Code != http.StatusBadRequest {
		t.Fatalf("empty body = %d, want 400", w.Code)
	}
}

func TestRecordRequestReceivesOutcomes(t *testing.T) {
	sm := statemachine.New()
	node := &fakeNode{sm: sm, isLeader: true, leaderID: "cp-1"}
	var mu sync.Mutex
	results := map[string]string{}

	a, err := New(Options{
		Node: node, StateMachine: sm, NodeID: "cp-1",
		RecordRequest: func(endpoint, result string) {
			mu.Lock()
			results[endpoint] = result
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h := a.Handler()
	do(t, h, http.MethodGet, "/v1/status", "")
	do(t, h, http.MethodGet, "/v1/routes/missing", "")

	mu.Lock()
	defer mu.Unlock()
	if results["status"] != "ok" {
		t.Fatalf("status result = %q", results["status"])
	}
	if results["routes.get"] != string(errs.ClassNotFound) {
		t.Fatalf("routes.get result = %q, want not_found", results["routes.get"])
	}
}
