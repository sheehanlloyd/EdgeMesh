// Package membership tracks edge-node liveness on the control-plane leader.
//
// # Why heartbeats are not replicated
//
// Edge liveness changes constantly and is cheap to rediscover: a node that is
// wrongly believed dead simply rejoins on its next heartbeat. Committing every
// heartbeat through Raft would put a consensus round on a two-second timer per
// edge, dominate the log, and force snapshots purely to discard liveness churn.
//
// So liveness is *leader-local ephemeral state*. What the edges consume is the
// derived membership snapshot, which the leader versions and broadcasts. When
// leadership moves, the new leader rebuilds membership from the heartbeats it
// then receives, so within one heartbeat interval the view is correct again.
//
// The trade-off is explicit: membership is eventually consistent, cache
// ownership may briefly disagree across edges during a leadership change, and
// that disagreement costs at worst a duplicate origin fetch. Configuration,
// which cannot tolerate that, goes through Raft instead.
package membership

import (
	"log/slog"
	"sort"
	"sync"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/pb"
)

// Options configures a Tracker.
type Options struct {
	// HeartbeatInterval is the cadence edges are told to use.
	HeartbeatInterval time.Duration
	// SuspectAfterMissed heartbeats marks a node suspect. A suspect node stays
	// on the ring: removing it on the first missed beat would reshuffle cache
	// ownership for a transient blip.
	SuspectAfterMissed int
	// DeadAfterMissed heartbeats removes a node from the ring.
	DeadAfterMissed int
	Clock           clock.Clock
	Logger          *slog.Logger
	// OnChange fires when the healthy membership set changes. It runs outside
	// the tracker's lock and must not block.
	OnChange func(*edgemeshv1.Membership)
}

func (o *Options) applyDefaults() {
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = 2 * time.Second
	}
	if o.SuspectAfterMissed <= 0 {
		o.SuspectAfterMissed = 2
	}
	if o.DeadAfterMissed <= o.SuspectAfterMissed {
		o.DeadAfterMissed = o.SuspectAfterMissed + 1
	}
	if o.Clock == nil {
		o.Clock = clock.New()
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Tracker holds ephemeral edge liveness.
type Tracker struct {
	opts Options

	mu    sync.RWMutex
	nodes map[string]*edgemeshv1.EdgeNode
	// version increments whenever the *ring-relevant* membership changes, which
	// is the set of nodes in a non-dead state. A heartbeat that changes only a
	// node's applied config version does not bump it: doing so would rebroadcast
	// the ring to every edge several times a second for no reason.
	version uint64
	// leader reports whether this node currently owns liveness tracking.
	leader bool
}

// New builds a tracker.
func New(o Options) *Tracker {
	o.applyDefaults()
	return &Tracker{opts: o, nodes: make(map[string]*edgemeshv1.EdgeNode)}
}

// SetLeader tells the tracker whether this node leads.
//
// On losing leadership the roster is cleared: stale liveness from a term this
// node no longer owns would otherwise be broadcast if it regained leadership.
func (t *Tracker) SetLeader(isLeader bool) {
	t.mu.Lock()
	was := t.leader
	t.leader = isLeader
	if was && !isLeader {
		t.nodes = make(map[string]*edgemeshv1.EdgeNode)
		t.opts.Logger.Info("cleared edge membership after losing leadership")
	}
	t.mu.Unlock()
}

// IsLeader reports whether this tracker is authoritative.
func (t *Tracker) IsLeader() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.leader
}

// Register records or refreshes an edge node from its registration.
func (t *Tracker) Register(node *edgemeshv1.EdgeNode) error {
	if node.GetId() == "" {
		return errs.New(errs.ClassValidation, "edge registration requires a node id")
	}
	if node.GetPeerAddress() == "" {
		return errs.New(errs.ClassValidation,
			"edge %q must advertise a peer address; peers cannot dial a node without one", node.GetId())
	}

	now := t.opts.Clock.Now()
	t.mu.Lock()
	prev, existed := t.nodes[node.GetId()]
	n := pb.Clone(node)
	n.LastSeenUnixMs = now.UnixMilli()
	n.State = edgemeshv1.NodeState_NODE_STATE_HEALTHY
	if n.GetWeight() == 0 {
		n.Weight = 1
	}
	// A re-registration (after a leadership change, say) carries no stats, so
	// the last reading is preserved rather than cleared.
	if n.GetStats() == nil && prev.GetStats() != nil {
		n.Stats = pb.Clone(prev.GetStats())
	}
	t.nodes[n.GetId()] = n

	// A new node, a returning node, or one whose peer address changed all alter
	// ring ownership and therefore bump the version.
	changed := !existed ||
		prev.GetState() != edgemeshv1.NodeState_NODE_STATE_HEALTHY ||
		prev.GetPeerAddress() != n.GetPeerAddress() ||
		prev.GetWeight() != n.GetWeight()
	if changed {
		t.version++
	}
	snap := t.snapshotLocked()
	t.mu.Unlock()

	if changed {
		t.opts.Logger.Info("edge node registered",
			slog.String("edge_node_id", n.GetId()),
			slog.String("peer_address", n.GetPeerAddress()),
			slog.String("region", n.GetRegion()),
			slog.Uint64("membership_version", snap.GetVersion()))
		t.notify(snap)
	}
	return nil
}

// Heartbeat refreshes a node's liveness.
//
// It reports reregister=true when the leader has no record of the node, which
// happens after a leadership change: the edge then re-sends its full
// registration rather than the leader guessing at its peer address.
func (t *Tracker) Heartbeat(nodeID string, configVersion uint64, stats *edgemeshv1.EdgeStats) (reregister bool, err error) {
	if nodeID == "" {
		return false, errs.New(errs.ClassValidation, "heartbeat requires a node id")
	}
	now := t.opts.Clock.Now()

	t.mu.Lock()
	n, ok := t.nodes[nodeID]
	if !ok {
		t.mu.Unlock()
		return true, nil
	}
	wasUnhealthy := n.GetState() != edgemeshv1.NodeState_NODE_STATE_HEALTHY
	n.LastSeenUnixMs = now.UnixMilli()
	n.State = edgemeshv1.NodeState_NODE_STATE_HEALTHY
	n.ConfigVersionApplied = configVersion
	// Reported occupancy is retained for the admin API. A heartbeat carrying no
	// stats leaves the previous reading in place rather than blanking it: an
	// older edge build that does not report them should show as "unknown once",
	// not flip the column empty on every other beat.
	if stats != nil {
		n.Stats = pb.Clone(stats)
	}

	if wasUnhealthy {
		t.version++
	}
	var snap *edgemeshv1.Membership
	if wasUnhealthy {
		snap = t.snapshotLocked()
	}
	t.mu.Unlock()

	if wasUnhealthy {
		t.opts.Logger.Info("edge node recovered",
			slog.String("edge_node_id", nodeID),
			slog.Uint64("membership_version", snap.GetVersion()))
		t.notify(snap)
	}
	return false, nil
}

// Sweep re-evaluates liveness and reports whether membership changed.
//
// It is called on a timer by the control process. Doing the evaluation on a
// sweep rather than with a timer per node keeps the goroutine count independent
// of the fleet size.
func (t *Tracker) Sweep() bool {
	now := t.opts.Clock.Now()
	suspectAfter := t.opts.HeartbeatInterval * time.Duration(t.opts.SuspectAfterMissed)
	deadAfter := t.opts.HeartbeatInterval * time.Duration(t.opts.DeadAfterMissed)

	type transition struct {
		id       string
		from, to edgemeshv1.NodeState
	}
	var transitions []transition

	t.mu.Lock()
	if !t.leader {
		t.mu.Unlock()
		return false
	}
	ringChanged := false
	for id, n := range t.nodes {
		since := now.Sub(time.UnixMilli(n.GetLastSeenUnixMs()))
		var want edgemeshv1.NodeState
		switch {
		case since >= deadAfter:
			want = edgemeshv1.NodeState_NODE_STATE_DEAD
		case since >= suspectAfter:
			want = edgemeshv1.NodeState_NODE_STATE_SUSPECT
		default:
			want = edgemeshv1.NodeState_NODE_STATE_HEALTHY
		}
		if want == n.GetState() {
			continue
		}
		transitions = append(transitions, transition{id: id, from: n.GetState(), to: want})
		// Only entering or leaving the dead state changes ring ownership; a
		// suspect node keeps serving and keeps its keys.
		if want == edgemeshv1.NodeState_NODE_STATE_DEAD ||
			n.GetState() == edgemeshv1.NodeState_NODE_STATE_DEAD {
			ringChanged = true
		}
		n.State = want
	}
	if ringChanged {
		t.version++
	}
	snap := t.snapshotLocked()
	t.mu.Unlock()

	for _, tr := range transitions {
		attrs := []any{
			slog.String("edge_node_id", tr.id),
			slog.String("from", tr.from.String()),
			slog.String("to", tr.to.String()),
			slog.Uint64("membership_version", snap.GetVersion()),
		}
		// A node going dead removes it from the ring and remaps its keys, which
		// is worth a warning; the other transitions are routine.
		if tr.to == edgemeshv1.NodeState_NODE_STATE_DEAD {
			t.opts.Logger.Warn("edge node state changed", attrs...)
		} else {
			t.opts.Logger.Info("edge node state changed", attrs...)
		}
	}
	if ringChanged {
		t.notify(snap)
	}
	return ringChanged
}

// Remove drops a node entirely, used when an edge shuts down cleanly.
func (t *Tracker) Remove(nodeID string) bool {
	t.mu.Lock()
	_, ok := t.nodes[nodeID]
	if ok {
		delete(t.nodes, nodeID)
		t.version++
	}
	snap := t.snapshotLocked()
	t.mu.Unlock()

	if ok {
		t.opts.Logger.Info("edge node deregistered",
			slog.String("edge_node_id", nodeID),
			slog.Uint64("membership_version", snap.GetVersion()))
		t.notify(snap)
	}
	return ok
}

func (t *Tracker) notify(snap *edgemeshv1.Membership) {
	if t.opts.OnChange != nil {
		t.opts.OnChange(snap)
	}
}

// Snapshot returns the current membership.
//
// Only healthy and suspect nodes appear: a dead node must not receive cache
// ownership, and including it would send every request for its keys into a
// timeout.
func (t *Tracker) Snapshot() *edgemeshv1.Membership {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.snapshotLocked()
}

func (t *Tracker) snapshotLocked() *edgemeshv1.Membership {
	out := make([]*edgemeshv1.EdgeNode, 0, len(t.nodes))
	for _, n := range t.nodes {
		if n.GetState() == edgemeshv1.NodeState_NODE_STATE_DEAD {
			continue
		}
		c := pb.Clone(n)
		// Stats are stripped here, not carried. This snapshot defines ring
		// ownership and is broadcast to every edge on every version change; it
		// has to be byte-stable for a given version, and cache occupancy
		// changes on every heartbeat. Admin callers read the retained values
		// through All() instead.
		c.Stats = nil
		out = append(out, c)
	}
	// Sorting makes the snapshot byte-stable, so two edges building a ring from
	// the same version produce identical ownership.
	sort.Slice(out, func(i, j int) bool { return out[i].GetId() < out[j].GetId() })
	return &edgemeshv1.Membership{Nodes: out, Version: t.version}
}

// All returns every tracked node including dead ones, for the admin API.
func (t *Tracker) All() []*edgemeshv1.EdgeNode {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]*edgemeshv1.EdgeNode, 0, len(t.nodes))
	for _, n := range t.nodes {
		out = append(out, pb.Clone(n))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetId() < out[j].GetId() })
	return out
}

// Counts reports how many nodes are in each state, for metrics.
func (t *Tracker) Counts() map[edgemeshv1.NodeState]int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[edgemeshv1.NodeState]int, 3)
	for _, n := range t.nodes {
		out[n.GetState()]++
	}
	return out
}

// Version reports the current membership version.
func (t *Tracker) Version() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.version
}
