package node

import (
	"context"
	"log/slog"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	raftlog "github.com/sheehanlloyd/edgemesh/internal/raft/log"
)

// runElectionLoop drives election timeouts and leader heartbeats.
//
// It ticks well below the election timeout rather than sleeping until the exact
// deadline, so that a deadline reset from an incoming AppendEntries takes effect
// without needing to interrupt a sleeping timer.
func (n *Node) runElectionLoop(ctx context.Context) {
	defer n.wg.Done()

	tick := n.cfg.HeartbeatInterval / 2
	if tick <= 0 {
		tick = 10 * time.Millisecond
	}
	t := n.clk.NewTicker(tick)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopped:
			return
		case <-t.C():
			n.tick(ctx)
		}
	}
}

func (n *Node) tick(ctx context.Context) {
	n.mu.Lock()
	switch n.role {
	case RoleLeader:
		n.mu.Unlock()
		// A leader's heartbeat is an empty AppendEntries; the replication loop
		// handles both heartbeats and real entries through the same path.
		n.signalReplicate()
		return
	case RoleFollower, RoleCandidate:
		if n.clk.Now().Before(n.electionDeadline) {
			n.mu.Unlock()
			return
		}
		// The election timeout elapsed without leader contact, so the leader is
		// presumed gone. Clearing the hint stops this node from reporting a
		// dead leader to clients and from treating itself as "still served".
		if n.role == RoleFollower && n.leaderID != "" &&
			n.clk.Now().Sub(n.lastLeaderContact) >= n.cfg.ElectionTimeoutMin {
			n.leaderID = ""
		}
	}
	// The deadline has passed: start an election.
	n.startElectionLocked(ctx)
	n.mu.Unlock()
}

// resetElectionTimerLocked picks a fresh randomized deadline.
//
// Randomization is what breaks split votes: with identical timeouts every node
// would time out together, all become candidates, split the vote, and repeat
// indefinitely.
func (n *Node) resetElectionTimerLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	jitter := time.Duration(n.rng.Int63n(int64(span) + 1))
	n.electionDeadline = n.clk.Now().Add(n.cfg.ElectionTimeoutMin + jitter)
}

// startElectionLocked begins a new election. The caller must hold n.mu.
func (n *Node) startElectionLocked(ctx context.Context) {
	if n.cfg.PreVote {
		// A pre-vote round asks "would you vote for me?" without incrementing
		// the term. A node returning from a partition therefore cannot force a
		// healthy leader to step down merely by having advanced its own term
		// while isolated.
		n.startPreVoteLocked(ctx)
		return
	}
	n.becomeCandidateLocked()
	n.broadcastVoteRequestLocked(ctx, false)
}

func (n *Node) startPreVoteLocked(ctx context.Context) {
	n.preVoteRound = true
	n.votesReceived = map[string]bool{n.cfg.ID: true}
	n.resetElectionTimerLocked()
	n.log.Debug("starting pre-vote round",
		slog.String("node_id", n.cfg.ID), slog.Uint64("term", n.currentTerm))
	n.broadcastVoteRequestLocked(ctx, true)
}

// becomeCandidateLocked advances the term and votes for itself.
func (n *Node) becomeCandidateLocked() {
	n.setRoleLocked(RoleCandidate)
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.leaderID = ""
	n.preVoteRound = false
	n.votesReceived = map[string]bool{n.cfg.ID: true}
	n.resetElectionTimerLocked()

	// The term and vote must be durable before any vote request goes out. A
	// crash after sending but before persisting could let this node vote again
	// in the same term after restarting.
	if err := n.cfg.Store.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
		n.log.Error("failed to persist candidacy",
			slog.String("node_id", n.cfg.ID),
			slog.Uint64("term", n.currentTerm),
			slog.String("error", err.Error()))
	}
	n.log.Info("starting election",
		slog.String("node_id", n.cfg.ID), slog.Uint64("term", n.currentTerm))
}

// broadcastVoteRequestLocked sends RequestVote to every peer. The caller must
// hold n.mu; the RPCs themselves run on separate goroutines without the lock.
func (n *Node) broadcastVoteRequestLocked(ctx context.Context, preVote bool) {
	req := &edgemeshv1.RequestVoteRequest{
		Term:         n.currentTerm,
		CandidateId:  n.cfg.ID,
		LastLogIndex: n.rlog.LastIndex(),
		LastLogTerm:  n.rlog.LastTerm(),
		PreVote:      preVote,
	}
	if preVote {
		// A pre-vote asks about the term this node *would* campaign in.
		req.Term = n.currentTerm + 1
	}
	electionTerm := n.currentTerm

	for _, peer := range n.peers {
		go n.requestVoteFrom(ctx, peer, req, preVote, electionTerm)
	}
}

func (n *Node) requestVoteFrom(ctx context.Context, peer string, req *edgemeshv1.RequestVoteRequest, preVote bool, electionTerm uint64) {
	rpcCtx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	defer cancel()

	resp, err := n.cfg.Transport.RequestVote(rpcCtx, peer, req)
	if err != nil {
		n.log.Debug("request vote failed",
			slog.String("node_id", n.cfg.ID),
			slog.String("peer_id", peer),
			slog.String("error", err.Error()))
		return
	}
	n.handleVoteResponse(ctx, peer, resp, preVote, electionTerm)
}

func (n *Node) handleVoteResponse(ctx context.Context, peer string, resp *edgemeshv1.RequestVoteResponse, preVote bool, electionTerm uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// A real (non-pre-vote) response carrying a higher term means this node is
	// stale and must step down immediately.
	if !preVote && resp.GetTerm() > n.currentTerm {
		n.stepDownLocked(resp.GetTerm(), "")
		return
	}
	// Discard a response from an election this node has already left.
	if n.currentTerm != electionTerm {
		return
	}
	if preVote != n.preVoteRound {
		return
	}
	if preVote && n.role == RoleLeader {
		return
	}
	if !preVote && n.role != RoleCandidate {
		return
	}
	if !resp.GetVoteGranted() {
		return
	}

	if n.votesReceived == nil {
		n.votesReceived = map[string]bool{}
	}
	n.votesReceived[peer] = true
	if len(n.votesReceived) < n.majority() {
		return
	}

	if preVote {
		// The pre-vote round succeeded: this node can win, so campaign for
		// real.
		n.becomeCandidateLocked()
		n.broadcastVoteRequestLocked(ctx, false)
		return
	}
	n.becomeLeaderLocked()
}

// majority is the smallest number of votes that constitutes a quorum.
func (n *Node) majority() int { return (len(n.peers)+1)/2 + 1 }

// becomeLeaderLocked assumes leadership and appends a no-op entry.
func (n *Node) becomeLeaderLocked() {
	if n.role == RoleLeader {
		return
	}
	n.setRoleLocked(RoleLeader)
	n.leaderID = n.cfg.ID
	n.votesReceived = nil
	n.preVoteRound = false

	next := n.rlog.LastIndex() + 1
	n.peerState = make(map[string]*peerState, len(n.peers))
	for _, p := range n.peers {
		n.peerState[p] = &peerState{nextIndex: next}
	}

	n.log.Info("became leader",
		slog.String("node_id", n.cfg.ID),
		slog.Uint64("term", n.currentTerm),
		slog.Uint64("last_index", n.rlog.LastIndex()))

	// A new leader appends a no-op entry in its own term.
	//
	// Raft forbids a leader from committing an entry from a *previous* term by
	// counting replicas (paper figure 8): doing so can commit an entry that a
	// later leader overwrites. Committing a fresh no-op in the current term
	// indirectly commits everything before it, which is both safe and what
	// makes a newly elected leader's state immediately readable.
	entry := raftlog.Entry{
		Index: n.rlog.LastIndex() + 1,
		Term:  n.currentTerm,
		Type:  edgemeshv1.EntryType_ENTRY_TYPE_NOOP,
	}
	if err := n.appendLocalLocked(entry); err != nil {
		n.log.Error("failed to append leader no-op",
			slog.String("node_id", n.cfg.ID), slog.String("error", err.Error()))
	}
	go n.obs.OnLeaderChange(n.cfg.ID, n.currentTerm)
	n.signalReplicate()
}

// stepDownLocked reverts to follower at a higher term.
func (n *Node) stepDownLocked(term uint64, leaderID string) {
	changed := term > n.currentTerm
	if changed {
		n.currentTerm = term
		n.votedFor = ""
		// A term change must be durable before this node acts on it.
		if err := n.cfg.Store.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
			n.log.Error("failed to persist term change",
				slog.String("node_id", n.cfg.ID),
				slog.Uint64("term", term),
				slog.String("error", err.Error()))
		}
	}
	wasLeader := n.role == RoleLeader
	n.setRoleLocked(RoleFollower)
	n.preVoteRound = false
	n.votesReceived = nil
	if leaderID != "" && n.leaderID != leaderID {
		n.leaderID = leaderID
		go n.obs.OnLeaderChange(leaderID, term)
	} else if changed {
		n.leaderID = leaderID
	}
	n.resetElectionTimerLocked()

	if wasLeader {
		n.log.Info("stepped down from leadership",
			slog.String("node_id", n.cfg.ID),
			slog.Uint64("new_term", term),
			slog.String("new_leader", leaderID))
		// Every proposal this node was holding is now unresolvable: it may or
		// may not commit under the new leader, and the client must retry rather
		// than be told it succeeded.
		n.failPendingLocked(errs.New(errs.ClassNotLeader,
			"leadership lost at term %d before the write committed", term))
	}
}

func (n *Node) setRoleLocked(r Role) {
	if n.role == r {
		return
	}
	from := n.role
	n.role = r
	term := n.currentTerm
	go n.obs.OnRoleChange(from, r, term)
}

// failPendingLocked resolves every waiting proposal with err.
func (n *Node) failPendingLocked(err error) {
	for idx, p := range n.pending {
		p.result <- proposalResult{err: err}
		close(p.result)
		delete(n.pending, idx)
	}
}

// HandleRequestVote processes an inbound RequestVote RPC.
func (n *Node) HandleRequestVote(_ context.Context, req *edgemeshv1.RequestVoteRequest) (*edgemeshv1.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &edgemeshv1.RequestVoteResponse{Term: n.currentTerm, VoterId: n.cfg.ID}

	// A pre-vote is answered without any persistent effect: it neither advances
	// this node's term nor records a vote. That is exactly what makes it safe
	// for a partitioned node to ask.
	if req.GetPreVote() {
		resp.VoteGranted = n.wouldGrantPreVoteLocked(req)
		return resp, nil
	}

	if req.GetTerm() < n.currentTerm {
		// A stale candidate is rejected and told the real term so it steps down.
		return resp, nil
	}
	if req.GetTerm() > n.currentTerm {
		n.stepDownLocked(req.GetTerm(), "")
		resp.Term = n.currentTerm
	}

	// One vote per term.
	if n.votedFor != "" && n.votedFor != req.GetCandidateId() {
		return resp, nil
	}
	// The election restriction: only a candidate whose log is at least as up to
	// date may win, which is what guarantees a new leader holds every committed
	// entry.
	if !n.rlog.IsUpToDate(req.GetLastLogIndex(), req.GetLastLogTerm()) {
		n.log.Debug("rejecting vote for a candidate with a stale log",
			slog.String("node_id", n.cfg.ID),
			slog.String("candidate_id", req.GetCandidateId()),
			slog.Uint64("candidate_last_index", req.GetLastLogIndex()),
			slog.Uint64("candidate_last_term", req.GetLastLogTerm()),
			slog.Uint64("local_last_index", n.rlog.LastIndex()),
			slog.Uint64("local_last_term", n.rlog.LastTerm()))
		return resp, nil
	}

	n.votedFor = req.GetCandidateId()
	// The vote must be durable *before* the response is sent. Responding first
	// and persisting after would let a crash in that window produce a second
	// vote in the same term, which can elect two leaders.
	if err := n.cfg.Store.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
		n.votedFor = ""
		return nil, errs.Wrap(errs.ClassStorage, err, "persist vote for %q", req.GetCandidateId())
	}
	n.resetElectionTimerLocked()
	resp.VoteGranted = true

	n.log.Debug("granted vote",
		slog.String("node_id", n.cfg.ID),
		slog.String("candidate_id", req.GetCandidateId()),
		slog.Uint64("term", n.currentTerm))
	return resp, nil
}

// wouldGrantPreVoteLocked answers a pre-vote probe.
//
// A pre-vote is granted only when the asking node could actually win: its term
// must be plausible, its log must be up to date, and this node must not still
// be hearing from a healthy leader. That last condition is what stops a
// rejoining partitioned node from disrupting a working cluster.
//
// "Still hearing from a leader" is measured from lastLeaderContact, not from
// this node's own election deadline. Using the deadline would be a deadlock:
// each surviving node resets its deadline whenever it campaigns, so after a
// real leader crash every node would look "recently served" to every other one
// and no pre-vote would ever be granted, and the cluster would sit leaderless
// forever.
//
// A leader answers for itself. It is, by definition, still hearing from a
// leader, so it refuses. Leaving this out defeats the whole mechanism: in a
// three-node cluster a rejoining node needs exactly one vote besides its own,
// the healthy follower refuses because it is being heartbeated, and the leader
// would hand over the deciding vote to depose itself. The same holds at five
// nodes, where two partitioned nodes plus an obliging leader make a majority.
func (n *Node) wouldGrantPreVoteLocked(req *edgemeshv1.RequestVoteRequest) bool {
	if n.role == RoleLeader {
		return false
	}
	if n.role == RoleFollower && n.leaderID != "" &&
		n.clk.Now().Sub(n.lastLeaderContact) < n.cfg.ElectionTimeoutMin {
		return false
	}
	if req.GetTerm() < n.currentTerm {
		return false
	}
	return n.rlog.IsUpToDate(req.GetLastLogIndex(), req.GetLastLogTerm())
}

// Role reports the node's current role.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Term reports the current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// LeaderID reports the last known leader, empty when none is known.
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// IsLeader reports whether this node currently believes it leads.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == RoleLeader
}
