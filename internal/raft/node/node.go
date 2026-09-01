// Package node implements EdgeMesh's Raft consensus engine.
//
// This is an original implementation of the algorithm described in "In Search
// of an Understandable Consensus Algorithm" (Ongaro & Ousterhout). It covers
// leader election, log replication, persistence, snapshotting, and
// InstallSnapshot recovery. Cluster membership is static: joint consensus is
// explicitly out of scope for V1 and is documented as such.
//
// # Concurrency model
//
// All consensus state lives behind one mutex and is mutated only by methods on
// Node. Long-running work (replicating to a peer, sending a snapshot, applying
// committed entries to the state machine) happens on separate goroutines that
// communicate through channels and never hold the lock across a network call.
// Holding the consensus lock across an RPC is the single most common way a Raft
// implementation deadlocks under partition, so the rule here is absolute:
// no network I/O while holding mu.
//
// # What is *not* implemented, and why
//
//   - Dynamic membership / joint consensus: out of scope for V1. The cluster is
//     three statically configured members.
//   - Read-index / lease reads: admin reads are served by the leader after it
//     confirms leadership through its normal heartbeat cycle. Linearizable
//     follower reads are not claimed.
package node

import (
	"context"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	raftlog "github.com/sheehanlloyd/edgemesh/internal/raft/log"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
	"github.com/sheehanlloyd/edgemesh/internal/raft/storage"
)

// Role is a node's Raft role.
type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

// Transport sends Raft RPCs to peers. It is an interface so that deterministic
// tests can substitute an in-memory network with controllable partitions and
// latency, which is how the failure matrix is exercised without containers.
type Transport interface {
	RequestVote(ctx context.Context, peerID string, req *edgemeshv1.RequestVoteRequest) (*edgemeshv1.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peerID string, req *edgemeshv1.AppendEntriesRequest) (*edgemeshv1.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, peerID string, req *edgemeshv1.InstallSnapshotRequest) (*edgemeshv1.InstallSnapshotResponse, error)
}

// Store is the durable state Raft depends on.
type Store interface {
	Load() (storage.PersistentState, error)
	SaveTermVote(term uint64, votedFor string) error
	SaveCommitIndex(index uint64) error
	Append(entries []raftlog.Entry) error
	TruncateSuffix(from uint64) error
	TruncatePrefix(through uint64) error
	SaveSnapshot(storage.Snapshot) error
	LoadSnapshot() (storage.Snapshot, bool, error)
}

// Observer receives state transitions for metrics and logging. Callbacks run
// outside the consensus lock and must not block.
type Observer interface {
	OnRoleChange(from, to Role, term uint64)
	OnCommit(index uint64)
	OnApply(index uint64, cmd *statemachine.Command, res statemachine.Result, d time.Duration, err error)
	OnLeaderChange(leaderID string, term uint64)
}

// NoopObserver satisfies Observer without doing anything.
type NoopObserver struct{}

func (NoopObserver) OnRoleChange(Role, Role, uint64) {}
func (NoopObserver) OnCommit(uint64)                 {}
func (NoopObserver) OnApply(uint64, *statemachine.Command, statemachine.Result, time.Duration, error) {
}
func (NoopObserver) OnLeaderChange(string, uint64) {}

// Config configures a Raft node.
type Config struct {
	ID                 string
	Peers              []string // every member including this node
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	// SnapshotEntries triggers a snapshot after this many entries accumulate
	// beyond the last snapshot. Zero disables count-based snapshotting.
	SnapshotEntries uint64
	SnapshotBytes   uint64
	// MaxEntriesPerAppend bounds a single AppendEntries payload.
	MaxEntriesPerAppend int
	RPCTimeout          time.Duration
	// PreVote runs a probing round before incrementing the term, so a node
	// returning from a partition cannot disrupt a healthy leader.
	PreVote bool

	Store        Store
	Transport    Transport
	StateMachine *statemachine.StateMachine
	Clock        clock.Clock
	Logger       *slog.Logger
	Observer     Observer
	// Rand seeds election-timeout jitter. Nil uses a per-node seeded source.
	Rand *rand.Rand
}

func (c *Config) validate() error {
	if c.ID == "" {
		return errs.New(errs.ClassValidation, "raft: node id is required")
	}
	if len(c.Peers) < 3 {
		return errs.New(errs.ClassValidation, "raft: at least 3 peers are required, got %d", len(c.Peers))
	}
	found := false
	for _, p := range c.Peers {
		if p == c.ID {
			found = true
		}
	}
	if !found {
		return errs.New(errs.ClassValidation, "raft: peer list must include this node's id %q", c.ID)
	}
	if c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		return errs.New(errs.ClassValidation,
			"raft: election timeouts must satisfy 0 < min (%s) < max (%s)",
			c.ElectionTimeoutMin, c.ElectionTimeoutMax)
	}
	if c.HeartbeatInterval <= 0 || c.HeartbeatInterval*2 > c.ElectionTimeoutMin {
		return errs.New(errs.ClassValidation,
			"raft: heartbeat interval (%s) must be well below election_timeout_min (%s)",
			c.HeartbeatInterval, c.ElectionTimeoutMin)
	}
	if c.Store == nil {
		return errs.New(errs.ClassValidation, "raft: a store is required")
	}
	if c.Transport == nil {
		return errs.New(errs.ClassValidation, "raft: a transport is required")
	}
	if c.StateMachine == nil {
		return errs.New(errs.ClassValidation, "raft: a state machine is required")
	}
	return nil
}

// proposal is an in-flight client write awaiting commit.
type proposal struct {
	index  uint64
	term   uint64
	result chan proposalResult
}

type proposalResult struct {
	res statemachine.Result
	err error
}

// peerState is the leader's replication bookkeeping for one follower.
type peerState struct {
	nextIndex  uint64
	matchIndex uint64
	// snapshotting is set while an InstallSnapshot stream is in flight so the
	// replicator does not start a second one.
	snapshotting bool
	// inflight prevents overlapping AppendEntries to the same peer, which would
	// make nextIndex updates race.
	inflight bool
}

// Node is a Raft consensus participant.
type Node struct {
	cfg   Config
	log   *slog.Logger
	clk   clock.Clock
	obs   Observer
	rng   *rand.Rand
	peers []string

	// smMu guards the state machine and the durable snapshot as a single unit:
	// Apply, Serialize, and Restore, together with the SaveSnapshot and
	// TruncatePrefix calls that accompany them.
	//
	// It is separate from mu because applying an entry and writing a snapshot
	// are both slow, and holding the consensus lock across them would stall
	// elections and replication. But they cannot simply run unlocked either:
	// restoring a leader's snapshot while the apply loop is mid-Apply leaves the
	// state machine holding a stale entry layered on top of restored state, and
	// a local snapshot racing an install can persist snapshot metadata that
	// disagrees with the bytes it was written with.
	//
	// Lock ordering is smMu before mu. Never acquire smMu while holding mu.
	smMu sync.Mutex

	mu sync.Mutex
	// --- persistent state (durable before use) ---
	currentTerm uint64
	votedFor    string
	rlog        *raftlog.Log
	// --- volatile state ---
	role        Role
	leaderID    string
	commitIndex uint64
	lastApplied uint64
	// --- leader state ---
	peerState map[string]*peerState
	// pending maps a log index to the client waiting on it.
	pending map[uint64]*proposal
	// electionDeadline is when a follower/candidate starts a new election.
	electionDeadline time.Time
	// lastLeaderContact is when this node last received a valid AppendEntries
	// or InstallSnapshot from a leader.
	//
	// It is deliberately separate from electionDeadline. The deadline is reset
	// whenever *this* node campaigns, so it cannot answer "am I still hearing
	// from a healthy leader?". A node that keeps retrying a pre-vote round
	// would keep pushing its own deadline out and conclude a dead leader was
	// still alive. Leader contact is only ever refreshed by an actual leader.
	lastLeaderContact time.Time
	// votesReceived tracks the current election.
	votesReceived map[string]bool
	preVoteRound  bool

	// applyCh wakes the apply loop when commitIndex advances.
	applyCh chan struct{}
	// replicateCh wakes the replication loop when there is new work.
	replicateCh chan struct{}

	started  bool
	stopOnce sync.Once
	stopped  chan struct{}
	wg       sync.WaitGroup
}

// New builds a Raft node and restores durable state.
func New(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.New()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Observer == nil {
		cfg.Observer = NoopObserver{}
	}
	if cfg.MaxEntriesPerAppend <= 0 {
		cfg.MaxEntriesPerAppend = 64
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = 2 * time.Second
	}

	rng := cfg.Rand
	if rng == nil {
		// Seeding from the node ID gives each node a distinct but reproducible
		// jitter sequence, which is what keeps three nodes from timing out in
		// lockstep and split-voting forever.
		var seed int64
		for _, c := range cfg.ID {
			seed = seed*31 + int64(c)
		}
		seed += cfg.Clock.Now().UnixNano()
		rng = rand.New(rand.NewSource(seed))
	}

	peers := make([]string, 0, len(cfg.Peers)-1)
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			peers = append(peers, p)
		}
	}
	sort.Strings(peers)

	n := &Node{
		cfg:         cfg,
		log:         cfg.Logger,
		clk:         cfg.Clock,
		obs:         cfg.Observer,
		rng:         rng,
		peers:       peers,
		role:        RoleFollower,
		peerState:   make(map[string]*peerState, len(peers)),
		pending:     make(map[uint64]*proposal),
		applyCh:     make(chan struct{}, 1),
		replicateCh: make(chan struct{}, 1),
		stopped:     make(chan struct{}),
	}
	if err := n.restore(); err != nil {
		return nil, err
	}
	return n, nil
}

// restore loads durable state and rebuilds the in-memory log.
func (n *Node) restore() error {
	st, err := n.cfg.Store.Load()
	if err != nil {
		return err
	}
	n.currentTerm = st.CurrentTerm
	n.votedFor = st.VotedFor

	if st.LastIncludedIndex > 0 {
		n.rlog = raftlog.NewWithSnapshot(st.LastIncludedIndex, st.LastIncludedTerm)
		if len(st.SnapshotData) > 0 {
			if err := n.cfg.StateMachine.Restore(st.SnapshotData); err != nil {
				return errs.Wrap(errs.ClassStorage, err, "restore state machine from snapshot")
			}
		}
		n.lastApplied = st.LastIncludedIndex
		n.commitIndex = st.LastIncludedIndex
	} else {
		n.rlog = raftlog.New()
	}

	// Entries beyond the snapshot are replayed into the log. They are applied
	// by the apply loop once commitIndex is re-established, which restores the
	// state machine to exactly the durable committed point.
	for _, e := range st.Entries {
		if e.Index <= n.rlog.SnapshotIndex() {
			continue
		}
		if err := n.rlog.Append(e); err != nil {
			return errs.Wrap(errs.ClassStorage, err, "replay persisted log entry %d", e.Index)
		}
	}
	if st.CommitIndex > n.commitIndex {
		// The commit index is only a hint, and it must never exceed what the
		// log actually holds after a partial write.
		n.commitIndex = min64(st.CommitIndex, n.rlog.LastIndex())
	}

	n.log.Info("raft state restored",
		slog.String("node_id", n.cfg.ID),
		slog.Uint64("term", n.currentTerm),
		slog.String("voted_for", n.votedFor),
		slog.Uint64("snapshot_index", n.rlog.SnapshotIndex()),
		slog.Uint64("last_index", n.rlog.LastIndex()),
		slog.Uint64("commit_index", n.commitIndex))
	return nil
}

// Start launches the node's goroutines.
func (n *Node) Start(ctx context.Context) error {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return errs.New(errs.ClassValidation, "raft: node already started")
	}
	n.started = true
	n.resetElectionTimerLocked()
	n.mu.Unlock()

	n.wg.Add(3)
	go n.runElectionLoop(ctx)
	go n.runReplicationLoop(ctx)
	go n.runApplyLoop(ctx)
	return nil
}

// Stop halts the node and waits for its goroutines. It is idempotent.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopped) })
	n.wg.Wait()

	// Fail any proposal still waiting so a caller is never left blocked on a
	// node that has shut down.
	n.mu.Lock()
	for idx, p := range n.pending {
		p.result <- proposalResult{err: errs.New(errs.ClassUnavailable, "raft node stopped before index %d committed", idx)}
		close(p.result)
		delete(n.pending, idx)
	}
	n.mu.Unlock()
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
