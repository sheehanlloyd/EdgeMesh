// Package log implements the in-memory Raft log with snapshot-aware indexing.
//
// # Indexing
//
// Raft log indices are 1-based and never restart. After a snapshot compacts the
// prefix, index 1 no longer exists in memory, so this package maintains an
// explicit offset: entries[i] holds log index lastIncludedIndex+1+i. Every
// public method speaks absolute log indices; the offset arithmetic is contained
// here so no caller can get it wrong.
//
// The log is not internally synchronized. It is owned by exactly one goroutine
// (the Raft node's main loop), which is what makes the consensus state machine
// tractable to reason about.
package log

import (
	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// Entry is one replicated log record.
type Entry struct {
	Index uint64
	Term  uint64
	Type  edgemeshv1.EntryType
	Data  []byte
}

// ToProto converts an entry for transport.
func (e Entry) ToProto() *edgemeshv1.LogEntry {
	return &edgemeshv1.LogEntry{Index: e.Index, Term: e.Term, Type: e.Type, Data: e.Data}
}

// FromProto converts a transported entry.
func FromProto(p *edgemeshv1.LogEntry) Entry {
	return Entry{Index: p.GetIndex(), Term: p.GetTerm(), Type: p.GetType(), Data: p.GetData()}
}

// Log is a Raft log with a compacted prefix.
type Log struct {
	// entries holds indices (lastIncludedIndex, lastIncludedIndex+len].
	entries []Entry
	// lastIncludedIndex and lastIncludedTerm describe the newest snapshot. They
	// are zero when nothing has been compacted.
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	// bytes tracks the retained entries' payload size, which drives
	// size-based snapshot triggering.
	bytes uint64
}

// New returns an empty log.
func New() *Log { return &Log{} }

// NewWithSnapshot returns a log whose prefix is already compacted at a snapshot
// point. Used when restoring from disk.
func NewWithSnapshot(lastIncludedIndex, lastIncludedTerm uint64) *Log {
	return &Log{lastIncludedIndex: lastIncludedIndex, lastIncludedTerm: lastIncludedTerm}
}

// FirstIndex is the lowest index still retained in memory. On an empty,
// never-compacted log it is 1, meaning "the next entry would be index 1".
func (l *Log) FirstIndex() uint64 { return l.lastIncludedIndex + 1 }

// LastIndex is the highest index in the log, including the compacted prefix.
func (l *Log) LastIndex() uint64 { return l.lastIncludedIndex + uint64(len(l.entries)) }

// LastTerm is the term of the last entry, or the snapshot's term when the log
// holds no entries.
func (l *Log) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.lastIncludedTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// SnapshotIndex reports the last index covered by the newest snapshot.
func (l *Log) SnapshotIndex() uint64 { return l.lastIncludedIndex }

// SnapshotTerm reports the term of the last index covered by the snapshot.
func (l *Log) SnapshotTerm() uint64 { return l.lastIncludedTerm }

// Len reports the number of entries retained in memory.
func (l *Log) Len() int { return len(l.entries) }

// Bytes reports the retained payload size.
func (l *Log) Bytes() uint64 { return l.bytes }

// ErrCompacted means the requested index has been snapshotted away. The caller
// must fall back to InstallSnapshot rather than treating this as corruption.
var ErrCompacted = errs.New(errs.ClassNotFound, "raft log: index has been compacted")

// ErrUnavailable means the requested index is beyond the end of the log.
var ErrUnavailable = errs.New(errs.ClassNotFound, "raft log: index is not yet present")

// Term returns the term of the entry at index.
//
// Index 0 is the well-defined "before the beginning" position with term 0,
// which is what makes the AppendEntries consistency check work for the very
// first entry without a special case at the call site.
func (l *Log) Term(index uint64) (uint64, error) {
	switch {
	case index == 0:
		return 0, nil
	case index == l.lastIncludedIndex:
		return l.lastIncludedTerm, nil
	case index < l.lastIncludedIndex:
		return 0, ErrCompacted
	case index > l.LastIndex():
		return 0, ErrUnavailable
	}
	return l.entries[index-l.lastIncludedIndex-1].Term, nil
}

// At returns the entry at index.
func (l *Log) At(index uint64) (Entry, error) {
	switch {
	case index <= l.lastIncludedIndex:
		return Entry{}, ErrCompacted
	case index > l.LastIndex():
		return Entry{}, ErrUnavailable
	}
	return l.entries[index-l.lastIncludedIndex-1], nil
}

// Slice returns entries in [lo, hi), capped at max entries.
//
// max bounds a single AppendEntries payload so a follower that is thousands of
// entries behind is caught up over several RPCs instead of one enormous one.
func (l *Log) Slice(lo, hi uint64, max int) ([]Entry, error) {
	if lo > hi {
		return nil, errs.New(errs.ClassProtocol, "raft log: invalid slice [%d,%d)", lo, hi)
	}
	if lo <= l.lastIncludedIndex {
		return nil, ErrCompacted
	}
	if hi > l.LastIndex()+1 {
		return nil, ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	start := lo - l.lastIncludedIndex - 1
	end := hi - l.lastIncludedIndex - 1
	if max > 0 && int(end-start) > max {
		end = start + uint64(max)
	}
	// The result is copied: callers hand it to a transport that may retain it,
	// and a later truncation must not mutate an in-flight RPC's payload.
	out := make([]Entry, end-start)
	copy(out, l.entries[start:end])
	return out, nil
}

// Append adds entries to the end of the log.
//
// Indices are assigned by the caller (the leader) and validated here: an entry
// whose index is not exactly LastIndex()+1 indicates a bug in the caller, and
// silently accepting it would corrupt the log's invariants.
func (l *Log) Append(entries ...Entry) error {
	for _, e := range entries {
		if e.Index != l.LastIndex()+1 {
			return errs.New(errs.ClassProtocol,
				"raft log: entry index %d is not contiguous after last index %d", e.Index, l.LastIndex())
		}
		l.entries = append(l.entries, e)
		l.bytes += uint64(len(e.Data))
	}
	return nil
}

// TruncateSuffix removes every entry from index onwards.
//
// This is the repair step of AppendEntries: a follower whose log conflicts with
// the leader deletes the conflicting suffix before appending the leader's
// entries. It is the only operation that ever removes uncommitted entries, and
// Raft's safety argument depends on it never removing a committed one, which
// the caller guarantees by only truncating at or after the conflict point.
func (l *Log) TruncateSuffix(index uint64) error {
	if index <= l.lastIncludedIndex {
		return errs.New(errs.ClassProtocol,
			"raft log: cannot truncate at %d, already compacted through %d", index, l.lastIncludedIndex)
	}
	if index > l.LastIndex() {
		return nil // nothing to remove
	}
	cut := index - l.lastIncludedIndex - 1
	for _, e := range l.entries[cut:] {
		l.bytes -= uint64(len(e.Data))
	}
	l.entries = l.entries[:cut]
	return nil
}

// Compact discards the prefix through lastIncludedIndex after a snapshot has
// been durably written.
//
// Compaction happens only after the snapshot is on disk. Compacting first would
// create a window where a crash loses both the entries and the snapshot that
// was supposed to replace them.
func (l *Log) Compact(lastIncludedIndex, lastIncludedTerm uint64) error {
	if lastIncludedIndex < l.lastIncludedIndex {
		return errs.New(errs.ClassProtocol,
			"raft log: cannot compact backwards from %d to %d", l.lastIncludedIndex, lastIncludedIndex)
	}
	if lastIncludedIndex == l.lastIncludedIndex {
		return nil
	}
	if lastIncludedIndex > l.LastIndex() {
		// The snapshot covers more than this log holds, which happens when a
		// follower installs a leader's snapshot. Discard everything.
		l.entries = nil
		l.bytes = 0
		l.lastIncludedIndex = lastIncludedIndex
		l.lastIncludedTerm = lastIncludedTerm
		return nil
	}
	// Verify the compaction point agrees with what the log holds, so a snapshot
	// from a divergent history cannot be silently spliced in.
	if term, err := l.Term(lastIncludedIndex); err == nil && term != lastIncludedTerm {
		return errs.New(errs.ClassProtocol,
			"raft log: compaction point %d has term %d locally but %d in the snapshot",
			lastIncludedIndex, term, lastIncludedTerm)
	}
	cut := lastIncludedIndex - l.lastIncludedIndex
	for _, e := range l.entries[:cut] {
		l.bytes -= uint64(len(e.Data))
	}
	// Copy into a fresh slice so the discarded prefix's memory is released
	// rather than being pinned by a slice header pointing into the old array.
	remaining := make([]Entry, len(l.entries)-int(cut))
	copy(remaining, l.entries[cut:])
	l.entries = remaining
	l.lastIncludedIndex = lastIncludedIndex
	l.lastIncludedTerm = lastIncludedTerm
	return nil
}

// Restore replaces the entire log with a snapshot point, discarding all
// entries. Used when a follower installs a leader's snapshot.
func (l *Log) Restore(lastIncludedIndex, lastIncludedTerm uint64) {
	l.entries = nil
	l.bytes = 0
	l.lastIncludedIndex = lastIncludedIndex
	l.lastIncludedTerm = lastIncludedTerm
}

// IsUpToDate implements Raft's election restriction (paper section 5.4.1).
//
// A candidate's log is at least as up to date as this one when its last term is
// higher, or the terms match and its log is at least as long. This is precisely
// what prevents a node missing committed entries from winning an election and
// then overwriting them.
func (l *Log) IsUpToDate(candidateLastIndex, candidateLastTerm uint64) bool {
	lastTerm := l.LastTerm()
	if candidateLastTerm != lastTerm {
		return candidateLastTerm > lastTerm
	}
	return candidateLastIndex >= l.LastIndex()
}

// FindConflict locates where this log diverges from a leader's entries.
//
// It returns the index of the first entry whose term disagrees, or 0 when the
// entries are all already present and consistent. The caller truncates from the
// returned index and appends the remainder.
func (l *Log) FindConflict(entries []Entry) uint64 {
	for _, e := range entries {
		term, err := l.Term(e.Index)
		if err != nil {
			// Beyond the end of the log: everything from here is new.
			return e.Index
		}
		if term != e.Term {
			return e.Index
		}
	}
	return 0
}

// ConflictHint computes the fast-backup response for a failed consistency
// check (paper section 5.3's optimization).
//
// Without it a leader probing a badly diverged follower decrements nextIndex by
// one per round trip, which is O(log length) RPCs. The hint lets the leader skip
// an entire conflicting term in one step.
func (l *Log) ConflictHint(prevLogIndex uint64) (conflictIndex, conflictTerm uint64) {
	if prevLogIndex > l.LastIndex() {
		// The follower's log is simply too short. Tell the leader where it ends
		// so it resumes from there rather than probing downwards.
		return l.LastIndex() + 1, 0
	}
	term, err := l.Term(prevLogIndex)
	if err != nil {
		return l.FirstIndex(), 0
	}
	// Walk back to the first index of this conflicting term.
	first := prevLogIndex
	for first > l.FirstIndex() {
		t, err := l.Term(first - 1)
		if err != nil || t != term {
			break
		}
		first--
	}
	return first, term
}

// Entries returns every retained entry. Used for persistence and tests; the
// result shares backing memory and must not be mutated.
func (l *Log) Entries() []Entry { return l.entries }
