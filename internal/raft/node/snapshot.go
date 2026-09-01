package node

import (
	"context"
	"log/slog"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/raft/storage"
)

// maybeSnapshot takes a snapshot when the log has grown past its thresholds.
//
// Snapshotting bounds two costs that otherwise grow without limit: the disk the
// log occupies, and the time a restarting node spends replaying it. Both are
// unbounded in a long-running cluster without compaction.
func (n *Node) maybeSnapshot() {
	// Taken before the thresholds are read, not after, so the decision cannot be
	// made against an applied index that an InstallSnapshot then moves.
	n.smMu.Lock()
	defer n.smMu.Unlock()

	n.mu.Lock()
	if n.cfg.SnapshotEntries == 0 && n.cfg.SnapshotBytes == 0 {
		n.mu.Unlock()
		return
	}
	applied := n.lastApplied
	snapIndex := n.rlog.SnapshotIndex()
	entriesSince := applied - snapIndex
	bytesHeld := n.rlog.Bytes()

	byCount := n.cfg.SnapshotEntries > 0 && entriesSince >= n.cfg.SnapshotEntries
	bySize := n.cfg.SnapshotBytes > 0 && bytesHeld >= n.cfg.SnapshotBytes
	if !byCount && !bySize {
		n.mu.Unlock()
		return
	}
	// The snapshot must describe a point that is both applied and present in
	// the log, so the term can be recorded alongside it.
	appliedTerm, err := n.rlog.Term(applied)
	if err != nil {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	if err := n.snapshotLocked(applied, appliedTerm); err != nil {
		n.log.Error("snapshot failed",
			slog.String("node_id", n.cfg.ID),
			slog.Uint64("index", applied),
			slog.String("error", err.Error()))
	}
}

// Snapshot captures the state machine at index and compacts the log prefix.
//
// The ordering here is the whole correctness argument: serialize, write the
// snapshot durably, and only then discard the log prefix it replaces. Compacting
// first would open a window where a crash loses both the entries and the
// snapshot meant to stand in for them.
func (n *Node) Snapshot(index, term uint64) error {
	n.smMu.Lock()
	defer n.smMu.Unlock()
	return n.snapshotLocked(index, term)
}

// snapshotLocked is Snapshot's body and requires smMu to be held.
func (n *Node) snapshotLocked(index, term uint64) error {
	data, err := n.cfg.StateMachine.Serialize()
	if err != nil {
		return err
	}

	if err := n.cfg.Store.SaveSnapshot(storage.Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Data:              data,
	}); err != nil {
		return err
	}
	// The snapshot is durable; the prefix it covers is now redundant.
	if err := n.cfg.Store.TruncatePrefix(index); err != nil {
		return err
	}

	n.mu.Lock()
	err = n.rlog.Compact(index, term)
	entriesLeft := n.rlog.Len()
	n.mu.Unlock()
	if err != nil {
		return err
	}

	n.log.Info("snapshot taken",
		slog.String("node_id", n.cfg.ID),
		slog.Uint64("last_included_index", index),
		slog.Uint64("last_included_term", term),
		slog.Int("snapshot_bytes", len(data)),
		slog.Int("log_entries_retained", entriesLeft))
	return nil
}

// sendSnapshot streams the newest snapshot to a lagging follower.
//
// This path exists precisely because compaction removed the entries the
// follower needs. Without InstallSnapshot, a follower that falls behind by more
// than one snapshot interval could never rejoin.
func (n *Node) sendSnapshot(ctx context.Context, peer string, term uint64) {
	defer func() {
		n.mu.Lock()
		if ps, ok := n.peerState[peer]; ok {
			ps.snapshotting = false
		}
		n.mu.Unlock()
	}()

	snap, ok, err := n.cfg.Store.LoadSnapshot()
	if err != nil || !ok {
		if err != nil {
			n.log.Error("failed to load snapshot for replication",
				slog.String("peer_id", peer), slog.String("error", err.Error()))
		}
		return
	}

	n.log.Info("installing snapshot on a lagging follower",
		slog.String("node_id", n.cfg.ID),
		slog.String("peer_id", peer),
		slog.Uint64("last_included_index", snap.LastIncludedIndex),
		slog.Int("bytes", len(snap.Data)))

	rpcCtx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout*10)
	defer cancel()

	req := &edgemeshv1.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          n.cfg.ID,
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Offset:            0,
		Data:              snap.Data,
		Done:              true,
	}
	resp, err := n.cfg.Transport.InstallSnapshot(rpcCtx, peer, req)
	if err != nil {
		n.log.Warn("install snapshot failed",
			slog.String("peer_id", peer), slog.String("error", err.Error()))
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.GetTerm() > n.currentTerm {
		n.stepDownLocked(resp.GetTerm(), "")
		return
	}
	if n.role != RoleLeader || n.currentTerm != term {
		return
	}
	if ps, ok := n.peerState[peer]; ok {
		// The follower now holds everything through the snapshot point.
		if snap.LastIncludedIndex > ps.matchIndex {
			ps.matchIndex = snap.LastIncludedIndex
		}
		ps.nextIndex = ps.matchIndex + 1
		n.advanceCommitLocked()
	}
	n.signalReplicate()
}

// HandleInstallSnapshot applies a leader's snapshot to this follower.
func (n *Node) HandleInstallSnapshot(_ context.Context, req *edgemeshv1.InstallSnapshotRequest) (*edgemeshv1.InstallSnapshotResponse, error) {
	n.mu.Lock()

	resp := &edgemeshv1.InstallSnapshotResponse{Term: n.currentTerm, FollowerId: n.cfg.ID}

	if req.GetTerm() < n.currentTerm {
		n.mu.Unlock()
		return resp, nil
	}
	if req.GetTerm() > n.currentTerm || n.role != RoleFollower {
		n.stepDownLocked(req.GetTerm(), req.GetLeaderId())
	} else if n.leaderID != req.GetLeaderId() {
		n.leaderID = req.GetLeaderId()
	}
	n.resetElectionTimerLocked()
	n.lastLeaderContact = n.clk.Now()
	resp.Term = n.currentTerm
	n.mu.Unlock()

	// Quiesce the state machine before touching it. The leader is answered on
	// the terms recorded above regardless of how long this waits, so a slow
	// apply delays the install but never the election timer.
	n.smMu.Lock()
	defer n.smMu.Unlock()

	// Re-read the position now that the apply loop is held off. Checking before
	// waiting would be checking a value that the apply loop was still free to
	// move. A snapshot that is older than what this node already has is
	// discarded: installing it would move the state machine backwards.
	n.mu.Lock()
	stale := req.GetLastIncludedIndex() <= n.rlog.SnapshotIndex() ||
		req.GetLastIncludedIndex() <= n.lastApplied
	n.mu.Unlock()
	if stale {
		resp.BytesStored = uint64(len(req.GetData()))
		return resp, nil
	}

	// Persist before adopting, so a crash mid-install leaves the node with
	// either its old consistent state or the new one, never a mixture.
	if err := n.cfg.Store.SaveSnapshot(storage.Snapshot{
		LastIncludedIndex: req.GetLastIncludedIndex(),
		LastIncludedTerm:  req.GetLastIncludedTerm(),
		Data:              req.GetData(),
	}); err != nil {
		return nil, err
	}
	if err := n.cfg.StateMachine.Restore(req.GetData()); err != nil {
		return nil, errs.Wrap(errs.ClassStorage, err, "restore state machine from installed snapshot")
	}
	// Reclaiming the log prefix is housekeeping, not correctness: the snapshot
	// is durable and recovery reads it before any surviving entry. Returning
	// here would be the worst outcome available, because the state machine has
	// already moved to the snapshot point while the in-memory log still claims
	// the old position, so the node would re-apply entries it has just applied
	// and report a position to the leader that its state machine has passed.
	// Log it and finish adopting instead.
	if err := n.cfg.Store.TruncatePrefix(req.GetLastIncludedIndex()); err != nil {
		n.log.Warn("failed to reclaim the log prefix after installing a snapshot",
			slog.String("node_id", n.cfg.ID),
			slog.Uint64("last_included_index", req.GetLastIncludedIndex()),
			slog.String("error", err.Error()))
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Discard the entire local log. Retaining a suffix would require proving it
	// is consistent with the leader's history past the snapshot point, which
	// the InstallSnapshot RPC gives no way to check.
	n.rlog.Restore(req.GetLastIncludedIndex(), req.GetLastIncludedTerm())
	n.lastApplied = req.GetLastIncludedIndex()
	if n.commitIndex < req.GetLastIncludedIndex() {
		n.commitIndex = req.GetLastIncludedIndex()
		if err := n.cfg.Store.SaveCommitIndex(n.commitIndex); err != nil {
			n.log.Warn("failed to persist commit index after snapshot install",
				slog.String("error", err.Error()))
		}
	}
	resp.BytesStored = uint64(len(req.GetData()))

	n.log.Info("installed leader snapshot",
		slog.String("node_id", n.cfg.ID),
		slog.String("leader_id", req.GetLeaderId()),
		slog.Uint64("last_included_index", req.GetLastIncludedIndex()),
		slog.Int("bytes", len(req.GetData())))
	return resp, nil
}

// SnapshotIndex reports the last index covered by the newest snapshot.
func (n *Node) SnapshotIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rlog.SnapshotIndex()
}

// LogLen reports the entries retained in memory.
func (n *Node) LogLen() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rlog.Len()
}
