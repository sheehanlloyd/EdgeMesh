package node

import (
	"context"
	"sync"
	"testing"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
	"github.com/sheehanlloyd/edgemesh/internal/raft/storage"
)

// memNetwork is an in-process Raft transport with controllable partitions.
//
// Testing consensus against containers is slow and flaky; testing it against a
// network whose partitions are explicit makes every failure scenario in the
// PRD's matrix reproducible and fast. Delivery is synchronous, so a test that
// has observed a message knows it was delivered.
type memNetwork struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	// partitioned[a][b] blocks traffic from a to b. Partitions are directional
	// so asymmetric splits can be modelled.
	partitioned map[string]map[string]bool
	// down marks a node as crashed: it neither sends nor receives.
	down map[string]bool
	// delay is applied before delivering, letting a test model a slow link.
	delay map[string]time.Duration
	// dropVotes optionally suppresses vote responses to force a split.
	rpcCount map[string]int
}

func newMemNetwork() *memNetwork {
	return &memNetwork{
		nodes:       make(map[string]*Node),
		partitioned: make(map[string]map[string]bool),
		down:        make(map[string]bool),
		delay:       make(map[string]time.Duration),
		rpcCount:    make(map[string]int),
	}
}

func (m *memNetwork) register(id string, n *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[id] = n
}

// blocked reports whether traffic from -> to is suppressed.
func (m *memNetwork) blocked(from, to string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.down[from] || m.down[to] {
		return true
	}
	if p, ok := m.partitioned[from]; ok && p[to] {
		return true
	}
	return false
}

func (m *memNetwork) target(id string) *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodes[id]
}

// isolate cuts a node off from every other node in both directions.
func (m *memNetwork) isolate(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.partitioned[id] == nil {
		m.partitioned[id] = make(map[string]bool)
	}
	for other := range m.nodes {
		if other == id {
			continue
		}
		m.partitioned[id][other] = true
		if m.partitioned[other] == nil {
			m.partitioned[other] = make(map[string]bool)
		}
		m.partitioned[other][id] = true
	}
}

// heal restores a node's connectivity.
func (m *memNetwork) heal(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.partitioned, id)
	for _, p := range m.partitioned {
		delete(p, id)
	}
}

func (m *memNetwork) healAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partitioned = make(map[string]map[string]bool)
}

// crash stops delivering to or from a node, modelling a process that died.
func (m *memNetwork) crash(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.down[id] = true
}

func (m *memNetwork) revive(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.down, id)
}

var errUnreachable = errs.New(errs.ClassUnavailable, "peer is unreachable")

// transportFor returns the Transport view of the network for one node.
func (m *memNetwork) transportFor(from string) Transport {
	return &memTransport{net: m, from: from}
}

type memTransport struct {
	net  *memNetwork
	from string
}

func (t *memTransport) RequestVote(ctx context.Context, peer string, req *edgemeshv1.RequestVoteRequest) (*edgemeshv1.RequestVoteResponse, error) {
	if t.net.blocked(t.from, peer) {
		return nil, errUnreachable
	}
	n := t.net.target(peer)
	if n == nil {
		return nil, errUnreachable
	}
	t.net.count("RequestVote")
	resp, err := n.HandleRequestVote(ctx, req)
	if err != nil {
		return nil, err
	}
	// The response travels back over the same link.
	if t.net.blocked(peer, t.from) {
		return nil, errUnreachable
	}
	return resp, nil
}

func (t *memTransport) AppendEntries(ctx context.Context, peer string, req *edgemeshv1.AppendEntriesRequest) (*edgemeshv1.AppendEntriesResponse, error) {
	if t.net.blocked(t.from, peer) {
		return nil, errUnreachable
	}
	n := t.net.target(peer)
	if n == nil {
		return nil, errUnreachable
	}
	t.net.count("AppendEntries")
	resp, err := n.HandleAppendEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	if t.net.blocked(peer, t.from) {
		return nil, errUnreachable
	}
	return resp, nil
}

func (t *memTransport) InstallSnapshot(ctx context.Context, peer string, req *edgemeshv1.InstallSnapshotRequest) (*edgemeshv1.InstallSnapshotResponse, error) {
	if t.net.blocked(t.from, peer) {
		return nil, errUnreachable
	}
	n := t.net.target(peer)
	if n == nil {
		return nil, errUnreachable
	}
	t.net.count("InstallSnapshot")
	resp, err := n.HandleInstallSnapshot(ctx, req)
	if err != nil {
		return nil, err
	}
	if t.net.blocked(peer, t.from) {
		return nil, errUnreachable
	}
	return resp, nil
}

func (m *memNetwork) count(op string) {
	m.mu.Lock()
	m.rpcCount[op]++
	m.mu.Unlock()
}

// counted reports how many times op has been sent. Reading the map directly is
// a race: the counter is incremented from every node's replication goroutine.
func (m *memNetwork) counted(op string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rpcCount[op]
}

// ---------------------------------------------------------------------------
// Cluster harness
// ---------------------------------------------------------------------------

type cluster struct {
	t       *testing.T
	net     *memNetwork
	ids     []string
	nodes   map[string]*Node
	stores  map[string]*storage.Store
	dirs    map[string]string
	sms     map[string]*statemachine.StateMachine
	cancels map[string]context.CancelFunc
	// history records every (term, leader) observed, which the safety
	// invariants are checked against.
	histMu  sync.Mutex
	leaders map[uint64]map[string]bool
}

// clusterOptions tunes the harness. Real (not mock) clocks are used because
// Raft's correctness is about *relative* timing across independent goroutines,
// which a single mock clock cannot faithfully model. Timeouts are set small so
// tests stay fast.
type clusterOptions struct {
	ids             []string
	electionMin     time.Duration
	electionMax     time.Duration
	heartbeat       time.Duration
	snapshotEntries uint64
	preVote         bool
	maxEntries      int
}

func defaultClusterOptions() clusterOptions {
	return clusterOptions{
		ids:         []string{"cp-1", "cp-2", "cp-3"},
		electionMin: 150 * time.Millisecond,
		electionMax: 300 * time.Millisecond,
		heartbeat:   40 * time.Millisecond,
		maxEntries:  64,
	}
}

func newCluster(t *testing.T, o clusterOptions) *cluster {
	t.Helper()
	if len(o.ids) == 0 {
		o = defaultClusterOptions()
	}
	c := &cluster{
		t:       t,
		net:     newMemNetwork(),
		ids:     o.ids,
		nodes:   make(map[string]*Node),
		stores:  make(map[string]*storage.Store),
		dirs:    make(map[string]string),
		sms:     make(map[string]*statemachine.StateMachine),
		cancels: make(map[string]context.CancelFunc),
		leaders: make(map[uint64]map[string]bool),
	}

	for _, id := range o.ids {
		dir := t.TempDir()
		c.dirs[id] = dir
		c.startNode(id, o)
	}
	t.Cleanup(c.stop)
	return c
}

// startNode builds and starts one node, reusing its data directory so a restart
// exercises real recovery from disk.
func (c *cluster) startNode(id string, o clusterOptions) {
	c.t.Helper()

	store, err := storage.Open(storage.Options{Dir: c.dirs[id], NoSync: true})
	if err != nil {
		c.t.Fatalf("open store for %s: %v", id, err)
	}
	sm := statemachine.New()

	n, err := New(Config{
		ID:                  id,
		Peers:               o.ids,
		ElectionTimeoutMin:  o.electionMin,
		ElectionTimeoutMax:  o.electionMax,
		HeartbeatInterval:   o.heartbeat,
		SnapshotEntries:     o.snapshotEntries,
		MaxEntriesPerAppend: o.maxEntries,
		RPCTimeout:          200 * time.Millisecond,
		PreVote:             o.preVote,
		Store:               store,
		Transport:           c.net.transportFor(id),
		StateMachine:        sm,
		Logger:              testLogger(c.t),
		Observer:            &recordingObserver{c: c},
	})
	if err != nil {
		c.t.Fatalf("new node %s: %v", id, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.nodes[id] = n
	c.stores[id] = store
	c.sms[id] = sm
	c.cancels[id] = cancel
	c.net.register(id, n)

	if err := n.Start(ctx); err != nil {
		c.t.Fatalf("start node %s: %v", id, err)
	}
}

// restart stops a node and brings it back with the same data directory.
func (c *cluster) restart(id string, o clusterOptions) {
	c.t.Helper()
	c.stopNode(id)
	c.startNode(id, o)
}

func (c *cluster) stopNode(id string) {
	if cancel, ok := c.cancels[id]; ok {
		cancel()
	}
	if n, ok := c.nodes[id]; ok {
		n.Stop()
	}
	if s, ok := c.stores[id]; ok {
		_ = s.Close()
	}
	delete(c.nodes, id)
	delete(c.stores, id)
	delete(c.cancels, id)
}

func (c *cluster) stop() {
	for _, id := range c.ids {
		c.stopNode(id)
	}
}

// recordingObserver captures leadership history so the "at most one leader per
// term" invariant can be asserted over the whole run rather than at one instant.
type recordingObserver struct{ c *cluster }

func (o *recordingObserver) OnRoleChange(_, to Role, term uint64) {}
func (o *recordingObserver) OnCommit(uint64)                      {}
func (o *recordingObserver) OnApply(uint64, *statemachine.Command, statemachine.Result, time.Duration, error) {
}
func (o *recordingObserver) OnLeaderChange(leaderID string, term uint64) {
	o.c.histMu.Lock()
	defer o.c.histMu.Unlock()
	if o.c.leaders[term] == nil {
		o.c.leaders[term] = make(map[string]bool)
	}
	o.c.leaders[term][leaderID] = true
}

// assertAtMostOneLeaderPerTerm checks Raft's central safety property across the
// entire observed history.
func (c *cluster) assertAtMostOneLeaderPerTerm() {
	c.t.Helper()
	c.histMu.Lock()
	defer c.histMu.Unlock()
	for term, ls := range c.leaders {
		if len(ls) > 1 {
			names := make([]string, 0, len(ls))
			for id := range ls {
				names = append(names, id)
			}
			c.t.Fatalf("term %d had multiple leaders: %v", term, names)
		}
	}
}

// waitForLeader blocks until exactly one node reports itself leader.
func (c *cluster) waitForLeader(timeout time.Duration) (string, uint64) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []string
		var term uint64
		for id, n := range c.nodes {
			if n.IsLeader() {
				leaders = append(leaders, id)
				term = n.Term()
			}
		}
		if len(leaders) == 1 {
			return leaders[0], term
		}
		if len(leaders) > 1 {
			// Two nodes may briefly both believe they lead if one has not yet
			// processed a higher term. Verify they are in different terms,
			// which is legal, and keep waiting for convergence.
			terms := map[uint64]string{}
			for _, id := range leaders {
				t := c.nodes[id].Term()
				if other, dup := terms[t]; dup {
					c.t.Fatalf("two leaders in term %d: %s and %s", t, other, id)
				}
				terms[t] = id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected within %s", timeout)
	return "", 0
}

// waitFor polls cond until it holds or the timeout expires.
func (c *cluster) waitFor(timeout time.Duration, desc string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("condition never held within %s: %s", timeout, desc)
}

// live returns the nodes that are neither crashed nor stopped.
func (c *cluster) live() map[string]*Node {
	out := make(map[string]*Node, len(c.nodes))
	c.net.mu.RLock()
	defer c.net.mu.RUnlock()
	for id, n := range c.nodes {
		if !c.net.down[id] {
			out[id] = n
		}
	}
	return out
}

// proposeRoute submits a route creation through the current leader.
func (c *cluster) proposeRoute(ctx context.Context, leaderID, routeID string) (statemachine.Result, error) {
	cmd := &statemachine.Command{
		Type:            statemachine.CommandCreateRoute,
		TimestampUnixMs: time.Now().UnixMilli(),
		Route: &edgemeshv1.Route{
			Id: routeID, Hostname: routeID + ".local", PathPrefix: "/",
			OriginPoolId: "pool-1", Enabled: true,
		},
	}
	return c.nodes[leaderID].Propose(ctx, cmd)
}

// assertLogsConsistent checks that no two healthy nodes' committed prefixes
// disagree, which is the log-matching property.
func (c *cluster) assertLogsConsistent() {
	c.t.Helper()
	type snapshot struct {
		id      string
		entries map[uint64]uint64 // index -> term
		commit  uint64
	}
	var snaps []snapshot
	for id, n := range c.live() {
		n.mu.Lock()
		s := snapshot{id: id, entries: map[uint64]uint64{}, commit: n.commitIndex}
		for i := n.rlog.FirstIndex(); i <= n.rlog.LastIndex(); i++ {
			if t, err := n.rlog.Term(i); err == nil {
				s.entries[i] = t
			}
		}
		n.mu.Unlock()
		snaps = append(snaps, s)
	}
	for i := 0; i < len(snaps); i++ {
		for j := i + 1; j < len(snaps); j++ {
			a, b := snaps[i], snaps[j]
			limit := a.commit
			if b.commit < limit {
				limit = b.commit
			}
			for idx := uint64(1); idx <= limit; idx++ {
				ta, oka := a.entries[idx]
				tb, okb := b.entries[idx]
				if oka && okb && ta != tb {
					c.t.Fatalf("committed log diverged at index %d: %s has term %d, %s has term %d",
						idx, a.id, ta, b.id, tb)
				}
			}
		}
	}
}
