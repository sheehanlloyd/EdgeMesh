package storage

import (
	"path/filepath"
	"testing"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	raftlog "github.com/sheehanlloyd/edgemesh/internal/raft/log"
)

func open(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(Options{Dir: dir, NoSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func entry(index, term uint64, data string) raftlog.Entry {
	return raftlog.Entry{
		Index: index, Term: term,
		Type: edgemeshv1.EntryType_ENTRY_TYPE_COMMAND, Data: []byte(data),
	}
}

func TestOpenValidatesDir(t *testing.T) {
	if _, err := Open(Options{}); err == nil {
		t.Fatal("an empty data directory must be rejected")
	}
}

func TestFreshStoreLoadsZeroState(t *testing.T) {
	s := open(t, t.TempDir())
	st, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	// A node starting for the first time is the normal case, not an error.
	if st.CurrentTerm != 0 || st.VotedFor != "" || len(st.Entries) != 0 || st.LastIncludedIndex != 0 {
		t.Fatalf("fresh store returned non-zero state: %+v", st)
	}
}

func TestTermVoteSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	if err := s.SaveTermVote(7, "cp-2"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := open(t, dir)
	st, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.CurrentTerm != 7 || st.VotedFor != "cp-2" {
		t.Fatalf("term/vote lost across reopen: term=%d voted_for=%q", st.CurrentTerm, st.VotedFor)
	}
}

func TestAppendAndLoadPreservesOrder(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)

	var entries []raftlog.Entry
	// More than 256 entries so a byte-ordered key scan would visibly misorder
	// them if the encoding were little-endian.
	for i := uint64(1); i <= 300; i++ {
		entries = append(entries, entry(i, 1, "payload"))
	}
	if err := s.Append(entries); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := open(t, dir).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Entries) != 300 {
		t.Fatalf("loaded %d entries, want 300", len(st.Entries))
	}
	for i, e := range st.Entries {
		if e.Index != uint64(i+1) {
			t.Fatalf("entries came back out of order at position %d: index %d", i, e.Index)
		}
	}
}

func TestAppendEmptyIsNoop(t *testing.T) {
	s := open(t, t.TempDir())
	if err := s.Append(nil); err != nil {
		t.Fatal(err)
	}
	st, _ := s.Load()
	if len(st.Entries) != 0 {
		t.Fatal("an empty append wrote something")
	}
}

func TestTruncateSuffix(t *testing.T) {
	s := open(t, t.TempDir())
	var entries []raftlog.Entry
	for i := uint64(1); i <= 10; i++ {
		entries = append(entries, entry(i, 1, "d"))
	}
	if err := s.Append(entries); err != nil {
		t.Fatal(err)
	}
	if err := s.TruncateSuffix(6); err != nil {
		t.Fatal(err)
	}
	st, _ := s.Load()
	if len(st.Entries) != 5 {
		t.Fatalf("after truncation %d entries remain, want 5", len(st.Entries))
	}
	if st.Entries[len(st.Entries)-1].Index != 5 {
		t.Fatalf("last surviving index = %d, want 5", st.Entries[len(st.Entries)-1].Index)
	}
	// Re-appending over the truncated range must work.
	if err := s.Append([]raftlog.Entry{entry(6, 9, "new")}); err != nil {
		t.Fatal(err)
	}
	st, _ = s.Load()
	if st.Entries[5].Term != 9 {
		t.Fatal("re-append after truncation did not take effect")
	}
}

func TestTruncatePrefix(t *testing.T) {
	s := open(t, t.TempDir())
	var entries []raftlog.Entry
	for i := uint64(1); i <= 300; i++ {
		entries = append(entries, entry(i, 1, "d"))
	}
	if err := s.Append(entries); err != nil {
		t.Fatal(err)
	}
	if err := s.TruncatePrefix(280); err != nil {
		t.Fatal(err)
	}
	st, _ := s.Load()
	if len(st.Entries) != 20 {
		t.Fatalf("after compaction %d entries remain, want 20", len(st.Entries))
	}
	if st.Entries[0].Index != 281 {
		t.Fatalf("first surviving index = %d, want 281", st.Entries[0].Index)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)

	if _, ok, err := s.LoadSnapshot(); err != nil || ok {
		t.Fatalf("a fresh store must report no snapshot: ok=%v err=%v", ok, err)
	}

	snap := Snapshot{LastIncludedIndex: 42, LastIncludedTerm: 3, Data: []byte("state")}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.LoadSnapshot()
	if err != nil || !ok {
		t.Fatalf("LoadSnapshot: ok=%v err=%v", ok, err)
	}
	if got.LastIncludedIndex != 42 || got.LastIncludedTerm != 3 || string(got.Data) != "state" {
		t.Fatalf("snapshot round trip lost data: %+v", got)
	}

	// The snapshot must also come back through Load, which is the restart path.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := open(t, dir).Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.LastIncludedIndex != 42 || string(st.SnapshotData) != "state" {
		t.Fatalf("snapshot not restored by Load: %+v", st)
	}
}

func TestSnapshotReplacement(t *testing.T) {
	s := open(t, t.TempDir())
	if err := s.SaveSnapshot(Snapshot{LastIncludedIndex: 10, LastIncludedTerm: 1, Data: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSnapshot(Snapshot{LastIncludedIndex: 20, LastIncludedTerm: 2, Data: []byte("new")}); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got.LastIncludedIndex != 20 || string(got.Data) != "new" {
		t.Fatalf("snapshot not replaced: %+v", got)
	}
}

func TestCommitIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	if err := s.SaveCommitIndex(99); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := open(t, dir).Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.CommitIndex != 99 {
		t.Fatalf("commit index = %d, want 99", st.CommitIndex)
	}
}

// Load must copy values out of the transaction: bbolt values are only valid
// inside it, so a retained slice would alias freed memory.
func TestLoadedSnapshotDataIsIndependent(t *testing.T) {
	s := open(t, t.TempDir())
	if err := s.SaveSnapshot(Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("abc")}); err != nil {
		t.Fatal(err)
	}
	a, _, _ := s.LoadSnapshot()
	a.Data[0] = 'z'
	b, _, _ := s.LoadSnapshot()
	if string(b.Data) != "abc" {
		t.Fatalf("loaded snapshot data aliases store memory: %q", b.Data)
	}
}

func TestSecondOpenOnSameDirTimesOut(t *testing.T) {
	dir := t.TempDir()
	first := open(t, dir)
	_ = first

	// A second process opening the same data directory must fail clearly rather
	// than hang or silently corrupt the file.
	start := time.Now()
	_, err := Open(Options{Dir: dir, NoSync: true, Timeout: 200 * time.Millisecond})
	if err == nil {
		t.Fatal("a second open of the same data directory must fail")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("the lock timeout was not honoured: waited %s", elapsed)
	}
}

func TestPathReportsDatabaseFile(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	if s.Path() != filepath.Join(dir, "raft.db") {
		t.Fatalf("Path = %q", s.Path())
	}
}

func BenchmarkAppendBatch(b *testing.B) {
	s, err := Open(Options{Dir: b.TempDir(), NoSync: true})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close() //nolint:errcheck

	batch := make([]raftlog.Entry, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range batch {
			batch[j] = entry(uint64(i*64+j+1), 1, "payload-payload-payload")
		}
		if err := s.Append(batch); err != nil {
			b.Fatal(err)
		}
	}
}
