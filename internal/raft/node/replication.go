package node

import (
	"context"
	"log/slog"
	"sort"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	raftlog "github.com/sheehanlloyd/edgemesh/internal/raft/log"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
)

// Propose submits a command for replication.
//
// It returns only after the entry is committed by a majority and applied to the
// state machine, which is what makes an admin write's success meaningful: a
// caller that receives success knows the configuration survives any single node
// failure. An uncommitted entry is never acknowledged as successful.
func (n *Node) Propose(ctx context.Context, cmd *statemachine.Command) (statemachine.Result, error) {
	data, err := cmd.Encode()
	if err != nil {
		return statemachine.Result{}, err
	}

	n.mu.Lock()
	if n.role != RoleLeader {
		leader := n.leaderID
		n.mu.Unlock()
		return statemachine.Result{}, errs.New(errs.ClassNotLeader,
			"not the leader; current leader is %q", leader)
	}
	entry := raftlog.Entry{
		Index: n.rlog.LastIndex() + 1,
		Term:  n.currentTerm,
		Type:  edgemeshv1.EntryType_ENTRY_TYPE_COMMAND,
		Data:  data,
	}
	if err := n.appendLocalLocked(entry); err != nil {
		n.mu.Unlock()
		return statemachine.Result{}, err
	}
	p := &proposal{index: entry.Index, term: entry.Term, result: make(chan proposalResult, 1)}
	n.pending[entry.Index] = p
	n.mu.Unlock()

	n.signalReplicate()

	select {
	case r := <-p.result:
		return r.res, r.err
	case <-ctx.Done():
		// The caller gave up, but the entry may still commit. Removing the
		// waiter is all that is safe to do: the log entry itself cannot be
		// withdrawn, and pretending it failed would be a lie if it commits.
		n.mu.Lock()
		delete(n.pending, entry.Index)
		n.mu.Unlock()
		return statemachine.Result{}, errs.Wrap(errs.ClassTimeout, ctx.Err(),
			"proposal at index %d did not commit before the deadline", entry.Index)
	case <-n.stopped:
		return statemachine.Result{}, errs.New(errs.ClassUnavailable, "raft node is shutting down")
	}
}

// appendLocalLocked appends to the in-memory log after persisting it.
//
// Persistence comes first. An entry the leader has counted in its own log but
// not written to disk could vanish on restart while remote followers still hold
// it, breaking the leader-completeness invariant.
func (n *Node) appendLocalLocked(e raftlog.Entry) error {
	if err := n.cfg.Store.Append([]raftlog.Entry{e}); err != nil {
		return err
	}
	if err := n.rlog.Append(e); err != nil {
		return err
	}
	return nil
}

// signalReplicate nudges the replication loop without blocking.
func (n *Node) signalReplicate() {
	select {
	case n.replicateCh <- struct{}{}:
	default: // a wake-up is already queued
	}
}

func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

// runReplicationLoop drives AppendEntries to every follower.
func (n *Node) runReplicationLoop(ctx context.Context) {
	defer n.wg.Done()

	// The heartbeat ticker guarantees an idle leader still refreshes follower
	// election timers, which is what keeps a healthy cluster stable.
	t := n.clk.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopped:
			return
		case <-t.C():
			n.replicateToAll(ctx)
		case <-n.replicateCh:
			n.replicateToAll(ctx)
		}
	}
}

func (n *Node) replicateToAll(ctx context.Context) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	n.mu.Unlock()

	for _, p := range peers {
		go n.replicateTo(ctx, p)
	}
}

// replicateTo sends one AppendEntries (or InstallSnapshot) to a peer.
func (n *Node) replicateTo(ctx context.Context, peer string) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	ps, ok := n.peerState[peer]
	if !ok {
		n.mu.Unlock()
		return
	}
	// One outstanding RPC per peer. Overlapping calls would race on nextIndex
	// and could rewind a follower that has already advanced.
	if ps.inflight || ps.snapshotting {
		n.mu.Unlock()
		return
	}

	prevIndex := ps.nextIndex - 1
	prevTerm, err := n.rlog.Term(prevIndex)
	if err != nil {
		// The entries this follower needs have been compacted away, so the only
		// way to catch it up is to ship the snapshot.
		ps.snapshotting = true
		term := n.currentTerm
		n.mu.Unlock()
		n.sendSnapshot(ctx, peer, term)
		return
	}

	entries, err := n.rlog.Slice(ps.nextIndex, n.rlog.LastIndex()+1, n.cfg.MaxEntriesPerAppend)
	if err != nil {
		if errs.IsClass(err, errs.ClassNotFound) {
			ps.snapshotting = true
			term := n.currentTerm
			n.mu.Unlock()
			n.sendSnapshot(ctx, peer, term)
			return
		}
		n.mu.Unlock()
		n.log.Error("failed to read log for replication",
			slog.String("peer_id", peer), slog.String("error", err.Error()))
		return
	}

	ps.inflight = true
	req := &edgemeshv1.AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderId:     n.cfg.ID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: n.commitIndex,
		Entries:      make([]*edgemeshv1.LogEntry, 0, len(entries)),
	}
	for _, e := range entries {
		req.Entries = append(req.Entries, e.ToProto())
	}
	term := n.currentTerm
	lastSent := prevIndex + uint64(len(entries))
	n.mu.Unlock()

	rpcCtx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	resp, rpcErr := n.cfg.Transport.AppendEntries(rpcCtx, peer, req)
	cancel()

	n.mu.Lock()
	if ps2, ok := n.peerState[peer]; ok {
		ps2.inflight = false
	}
	if rpcErr != nil {
		n.mu.Unlock()
		n.log.Debug("append entries failed",
			slog.String("peer_id", peer), slog.String("error", rpcErr.Error()))
		return
	}
	n.handleAppendResponseLocked(peer, req, resp, term, lastSent)
	n.mu.Unlock()

	// If there is more to send, keep going rather than waiting for the next
	// heartbeat: a follower that is far behind should catch up promptly.
	n.mu.Lock()
	more := n.role == RoleLeader && n.peerState[peer] != nil &&
		n.peerState[peer].nextIndex <= n.rlog.LastIndex()
	n.mu.Unlock()
	if more {
		n.signalReplicate()
	}
}

// handleAppendResponseLocked updates replication state. The caller holds n.mu.
func (n *Node) handleAppendResponseLocked(peer string, req *edgemeshv1.AppendEntriesRequest, resp *edgemeshv1.AppendEntriesResponse, sentTerm, lastSent uint64) {
	if resp.GetTerm() > n.currentTerm {
		n.stepDownLocked(resp.GetTerm(), "")
		return
	}
	// A response from an earlier term is stale: acting on it could rewind a
	// follower that has since been advanced by the current term's replication.
	if n.role != RoleLeader || n.currentTerm != sentTerm {
		return
	}
	ps, ok := n.peerState[peer]
	if !ok {
		return
	}

	if resp.GetSuccess() {
		if lastSent > ps.matchIndex {
			ps.matchIndex = lastSent
		}
		ps.nextIndex = ps.matchIndex + 1
		n.advanceCommitLocked()
		return
	}

	// The consistency check failed. The follower's hint tells the leader where
	// to resume, skipping an entire conflicting term in one round trip instead
	// of decrementing nextIndex one entry at a time.
	next := ps.nextIndex
	if conflictTerm := resp.GetConflictTerm(); conflictTerm != 0 {
		// Find the leader's last entry in the conflicting term; resume just
		// after it if present, otherwise at the follower's first index for it.
		if idx, found := n.lastIndexOfTermLocked(conflictTerm); found {
			next = idx + 1
		} else {
			next = resp.GetConflictIndex()
		}
	} else if resp.GetConflictIndex() != 0 {
		next = resp.GetConflictIndex()
	} else if next > 1 {
		// No hint: fall back to probing one entry at a time.
		next--
	}
	if next < 1 {
		next = 1
	}
	if next >= ps.nextIndex {
		// The hint did not move us backwards, which would loop forever. Fall
		// back to a single decrement.
		if ps.nextIndex > 1 {
			next = ps.nextIndex - 1
		} else {
			next = 1
		}
	}
	ps.nextIndex = next
	n.log.Debug("append entries rejected, backing up",
		slog.String("peer_id", peer),
		slog.Uint64("prev_log_index", req.GetPrevLogIndex()),
		slog.Uint64("conflict_index", resp.GetConflictIndex()),
		slog.Uint64("conflict_term", resp.GetConflictTerm()),
		slog.Uint64("next_index", next))
	n.signalReplicate()
}

// lastIndexOfTermLocked finds the highest local index with the given term.
func (n *Node) lastIndexOfTermLocked(term uint64) (uint64, bool) {
	for i := n.rlog.LastIndex(); i > n.rlog.SnapshotIndex(); i-- {
		t, err := n.rlog.Term(i)
		if err != nil {
			return 0, false
		}
		if t == term {
			return i, true
		}
		if t < term {
			return 0, false
		}
	}
	return 0, false
}

// advanceCommitLocked recomputes the commit index from follower match indices.
//
// The critical rule (paper section 5.4.2): a leader may only mark an entry
// committed by counting replicas when that entry is from the leader's *current*
// term. Counting replicas of an older entry can commit something a future
// leader legally overwrites. Older entries commit indirectly, once an entry
// from the current term commits above them, which is what the leader's no-op
// entry guarantees will happen promptly.
func (n *Node) advanceCommitLocked() {
	matches := make([]uint64, 0, len(n.peers)+1)
	matches = append(matches, n.rlog.LastIndex()) // the leader itself
	for _, p := range n.peers {
		if ps, ok := n.peerState[p]; ok {
			matches = append(matches, ps.matchIndex)
		} else {
			matches = append(matches, 0)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })

	// The index replicated on a majority is the (majority-1)th largest.
	candidate := matches[n.majority()-1]
	if candidate <= n.commitIndex {
		return
	}
	term, err := n.rlog.Term(candidate)
	if err != nil || term != n.currentTerm {
		return
	}

	n.commitIndex = candidate
	if err := n.cfg.Store.SaveCommitIndex(n.commitIndex); err != nil {
		n.log.Warn("failed to persist commit index",
			slog.Uint64("commit_index", n.commitIndex), slog.String("error", err.Error()))
	}
	go n.obs.OnCommit(candidate)
	n.signalApply()
}

// HandleAppendEntries processes an inbound AppendEntries RPC.
func (n *Node) HandleAppendEntries(_ context.Context, req *edgemeshv1.AppendEntriesRequest) (*edgemeshv1.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &edgemeshv1.AppendEntriesResponse{
		Term:         n.currentTerm,
		FollowerId:   n.cfg.ID,
		LastLogIndex: n.rlog.LastIndex(),
	}

	// A leader from a stale term is rejected outright.
	if req.GetTerm() < n.currentTerm {
		return resp, nil
	}

	// Any valid AppendEntries proves a current leader exists, so a candidate
	// steps down and a follower refreshes its election timer.
	if req.GetTerm() > n.currentTerm || n.role != RoleFollower {
		n.stepDownLocked(req.GetTerm(), req.GetLeaderId())
	} else if n.leaderID != req.GetLeaderId() {
		n.leaderID = req.GetLeaderId()
		go n.obs.OnLeaderChange(req.GetLeaderId(), req.GetTerm())
	}
	n.resetElectionTimerLocked()
	// Only an actual leader refreshes this, which is what makes it a reliable
	// answer to "is a leader still serving this cluster?".
	n.lastLeaderContact = n.clk.Now()
	resp.Term = n.currentTerm

	// The log-consistency check: this follower must hold prev_log_index with
	// prev_log_term, otherwise the logs have diverged and the leader must back
	// up.
	localPrevTerm, err := n.rlog.Term(req.GetPrevLogIndex())
	if err != nil || localPrevTerm != req.GetPrevLogTerm() {
		ci, ct := n.rlog.ConflictHint(req.GetPrevLogIndex())
		resp.ConflictIndex, resp.ConflictTerm = ci, ct
		resp.LastLogIndex = n.rlog.LastIndex()
		return resp, nil
	}

	entries := make([]raftlog.Entry, 0, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		entries = append(entries, raftlog.FromProto(e))
	}

	if len(entries) > 0 {
		// Delete any conflicting suffix, then append what is new. Entries that
		// already match are skipped: re-appending them would be wasted disk I/O
		// and, worse, truncating at a matching index could remove committed
		// entries that a delayed duplicate RPC re-sends. A zero conflict index
		// means everything is already present and consistent.
		if conflict := n.rlog.FindConflict(entries); conflict != 0 {
			if conflict <= n.commitIndex {
				// A conflict at or below the commit index would mean deleting
				// committed data, which Raft's invariants make impossible.
				// Refusing loudly is better than silently corrupting the log.
				return nil, errs.New(errs.ClassProtocol,
					"refusing to truncate committed log at index %d (commit index %d)",
					conflict, n.commitIndex)
			}
			if err := n.rlog.TruncateSuffix(conflict); err != nil {
				return nil, err
			}
			if err := n.cfg.Store.TruncateSuffix(conflict); err != nil {
				return nil, err
			}
			// Append only the entries at or after the conflict point.
			var toAppend []raftlog.Entry
			for _, e := range entries {
				if e.Index >= conflict {
					toAppend = append(toAppend, e)
				}
			}
			// Durability first, exactly as on the leader.
			if err := n.cfg.Store.Append(toAppend); err != nil {
				return nil, err
			}
			for _, e := range toAppend {
				if err := n.rlog.Append(e); err != nil {
					return nil, err
				}
			}
		}
	}

	// Adopt the leader's commit index, bounded by what this follower actually
	// holds: committing past the end of the local log would apply entries that
	// are not there.
	if lc := req.GetLeaderCommit(); lc > n.commitIndex {
		newCommit := min64(lc, n.rlog.LastIndex())
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			if err := n.cfg.Store.SaveCommitIndex(n.commitIndex); err != nil {
				n.log.Warn("failed to persist commit index",
					slog.Uint64("commit_index", n.commitIndex), slog.String("error", err.Error()))
			}
			go n.obs.OnCommit(newCommit)
			n.signalApply()
		}
	}

	resp.Success = true
	resp.LastLogIndex = n.rlog.LastIndex()
	return resp, nil
}

// runApplyLoop applies committed entries to the state machine in index order.
//
// Application is single-goroutine and strictly ordered, which is what makes the
// state machine deterministic across replicas: the same entries applied in the
// same order must produce the same state.
func (n *Node) runApplyLoop(ctx context.Context) {
	defer n.wg.Done()

	// A slow ticker is a safety net for a missed wake-up signal; the channel is
	// the normal path.
	t := n.clk.NewTicker(50 * time.Millisecond)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopped:
			return
		case <-n.applyCh:
			n.applyCommitted()
			n.maybeSnapshot()
		case <-t.C():
			n.applyCommitted()
			n.maybeSnapshot()
		}
	}
}

func (n *Node) applyCommitted() {
	// Held for the whole batch so an InstallSnapshot cannot restore the state
	// machine underneath an entry that is part-way through being applied.
	n.smMu.Lock()
	defer n.smMu.Unlock()

	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		next := n.lastApplied + 1
		entry, err := n.rlog.At(next)
		if err != nil {
			// The entry was compacted, meaning a snapshot already covers it.
			if errs.IsClass(err, errs.ClassNotFound) && next <= n.rlog.SnapshotIndex() {
				n.lastApplied = n.rlog.SnapshotIndex()
				n.mu.Unlock()
				continue
			}
			n.mu.Unlock()
			n.log.Error("failed to read a committed entry for apply",
				slog.Uint64("index", next), slog.String("error", err.Error()))
			return
		}
		n.lastApplied = next
		waiter := n.pending[next]
		delete(n.pending, next)
		entryTerm := entry.Term
		n.mu.Unlock()

		// The state machine is applied outside the consensus lock so a slow
		// apply cannot stall elections or replication.
		if entry.Type == edgemeshv1.EntryType_ENTRY_TYPE_NOOP {
			if waiter != nil {
				waiter.result <- proposalResult{}
				close(waiter.result)
			}
			continue
		}

		cmd, decErr := statemachine.Decode(entry.Data)
		if decErr != nil {
			n.log.Error("failed to decode a committed command",
				slog.Uint64("index", next), slog.String("error", decErr.Error()))
			if waiter != nil {
				waiter.result <- proposalResult{err: decErr}
				close(waiter.result)
			}
			continue
		}

		start := n.clk.Now()
		res, applyErr := n.cfg.StateMachine.Apply(next, cmd)
		// Called synchronously, and with the duration it already measures.
		//
		// The observer sets gauges and logs; it does not block, so a goroutine
		// bought nothing and cost ordering: concurrent OnApply calls could set
		// raft_last_applied out of order and make the gauge move backwards.
		n.obs.OnApply(next, cmd, res, n.clk.Now().Sub(start), applyErr)

		if waiter != nil {
			// A proposal only succeeds if the entry that committed is the one
			// this caller proposed. A different term at the same index means a
			// new leader overwrote it.
			if waiter.term != entryTerm {
				waiter.result <- proposalResult{err: errs.New(errs.ClassNotLeader,
					"log index %d was overwritten by a new leader", next)}
			} else {
				waiter.result <- proposalResult{res: res, err: applyErr}
			}
			close(waiter.result)
		}
	}
}

// CommitIndex reports the highest committed index.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// LastApplied reports the highest applied index.
func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// LastLogIndex reports the highest index in the log.
func (n *Node) LastLogIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rlog.LastIndex()
}

// Status is a snapshot of the node's consensus state for the admin API.
type Status struct {
	NodeID        string            `json:"node_id"`
	Role          Role              `json:"role"`
	Term          uint64            `json:"term"`
	LeaderID      string            `json:"leader_id"`
	CommitIndex   uint64            `json:"commit_index"`
	LastApplied   uint64            `json:"last_applied"`
	LastLogIndex  uint64            `json:"last_log_index"`
	SnapshotIndex uint64            `json:"snapshot_index"`
	LogEntries    int               `json:"log_entries"`
	Peers         map[string]uint64 `json:"peer_match_index"`
}

// Status returns the node's consensus state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()

	peers := make(map[string]uint64, len(n.peerState))
	for id, ps := range n.peerState {
		peers[id] = ps.matchIndex
	}
	return Status{
		NodeID:        n.cfg.ID,
		Role:          n.role,
		Term:          n.currentTerm,
		LeaderID:      n.leaderID,
		CommitIndex:   n.commitIndex,
		LastApplied:   n.lastApplied,
		LastLogIndex:  n.rlog.LastIndex(),
		SnapshotIndex: n.rlog.SnapshotIndex(),
		LogEntries:    n.rlog.Len(),
		Peers:         peers,
	}
}

// ReplicationLag reports how far each follower trails the leader.
func (n *Node) ReplicationLag() map[string]uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	last := n.rlog.LastIndex()
	out := make(map[string]uint64, len(n.peerState))
	for id, ps := range n.peerState {
		if last > ps.matchIndex {
			out[id] = last - ps.matchIndex
		} else {
			out[id] = 0
		}
	}
	return out
}
