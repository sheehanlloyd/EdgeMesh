package log

import (
	"errors"
	"testing"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
)

func entry(index, term uint64, data string) Entry {
	return Entry{Index: index, Term: term, Type: edgemeshv1.EntryType_ENTRY_TYPE_COMMAND, Data: []byte(data)}
}

func appendN(t *testing.T, l *Log, term uint64, from, to uint64) {
	t.Helper()
	for i := from; i <= to; i++ {
		if err := l.Append(entry(i, term, "d")); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
}

func TestEmptyLog(t *testing.T) {
	l := New()
	if l.FirstIndex() != 1 || l.LastIndex() != 0 || l.LastTerm() != 0 || l.Len() != 0 {
		t.Fatalf("empty log invariants broken: first=%d last=%d term=%d len=%d",
			l.FirstIndex(), l.LastIndex(), l.LastTerm(), l.Len())
	}
	// Index 0 is the defined "before the beginning" position.
	if term, err := l.Term(0); err != nil || term != 0 {
		t.Fatalf("Term(0) = %d, %v; want 0, nil", term, err)
	}
	if _, err := l.Term(1); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Term(1) on an empty log = %v, want ErrUnavailable", err)
	}
}

func TestAppendRequiresContiguousIndices(t *testing.T) {
	l := New()
	if err := l.Append(entry(1, 1, "a")); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(entry(3, 1, "c")); err == nil {
		t.Fatal("a gap in indices must be rejected")
	}
	if err := l.Append(entry(1, 1, "dup")); err == nil {
		t.Fatal("a repeated index must be rejected")
	}
	if l.LastIndex() != 1 {
		t.Fatalf("a rejected append mutated the log: last=%d", l.LastIndex())
	}
}

func TestTermAndAt(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 3)
	appendN(t, l, 2, 4, 6)

	for _, c := range []struct{ index, term uint64 }{{1, 1}, {3, 1}, {4, 2}, {6, 2}} {
		got, err := l.Term(c.index)
		if err != nil || got != c.term {
			t.Errorf("Term(%d) = %d, %v; want %d", c.index, got, err, c.term)
		}
		e, err := l.At(c.index)
		if err != nil || e.Index != c.index || e.Term != c.term {
			t.Errorf("At(%d) = %+v, %v", c.index, e, err)
		}
	}
	if _, err := l.At(7); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("At past the end = %v, want ErrUnavailable", err)
	}
	if l.LastTerm() != 2 {
		t.Fatalf("LastTerm = %d, want 2", l.LastTerm())
	}
}

func TestSlice(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 10)

	got, err := l.Slice(3, 6, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Index != 3 || got[2].Index != 5 {
		t.Fatalf("Slice(3,6) = %v", got)
	}
	// max bounds a single AppendEntries payload.
	got, err = l.Slice(1, 11, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("max not honoured: got %d entries", len(got))
	}
	if got, err := l.Slice(5, 5, 0); err != nil || len(got) != 0 {
		t.Fatalf("empty slice = %v, %v", got, err)
	}
	if _, err := l.Slice(6, 3, 0); err == nil {
		t.Fatal("an inverted range must be rejected")
	}
	if _, err := l.Slice(1, 99, 0); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("slice past the end = %v, want ErrUnavailable", err)
	}
}

// Slice must copy: the transport may retain the result while the log is
// truncated underneath it.
func TestSliceCopiesEntries(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 5)
	got, err := l.Slice(1, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}
	appendN(t, l, 9, 2, 4)
	if got[1].Term != 1 {
		t.Fatal("truncation mutated a previously returned slice")
	}
}

func TestTruncateSuffix(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 5)
	before := l.Bytes()

	if err := l.TruncateSuffix(3); err != nil {
		t.Fatal(err)
	}
	if l.LastIndex() != 2 {
		t.Fatalf("LastIndex after truncation = %d, want 2", l.LastIndex())
	}
	if l.Bytes() >= before {
		t.Fatalf("byte accounting not reduced: %d -> %d", before, l.Bytes())
	}
	// Truncating past the end is a no-op, not an error: it happens naturally
	// when a follower's log is already shorter than the conflict point.
	if err := l.TruncateSuffix(99); err != nil {
		t.Fatalf("truncating past the end: %v", err)
	}
	// The log must be appendable again from the truncation point.
	if err := l.Append(entry(3, 7, "new")); err != nil {
		t.Fatal(err)
	}
	if term, _ := l.Term(3); term != 7 {
		t.Fatal("re-append after truncation did not take effect")
	}
}

func TestCompactAndSnapshotIndexing(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 10)

	if err := l.Compact(4, 1); err != nil {
		t.Fatal(err)
	}
	if l.SnapshotIndex() != 4 || l.SnapshotTerm() != 1 {
		t.Fatalf("snapshot point = (%d,%d)", l.SnapshotIndex(), l.SnapshotTerm())
	}
	if l.FirstIndex() != 5 || l.LastIndex() != 10 || l.Len() != 6 {
		t.Fatalf("post-compaction indexing broken: first=%d last=%d len=%d",
			l.FirstIndex(), l.LastIndex(), l.Len())
	}
	// The compaction point itself still answers Term, which the consistency
	// check needs.
	if term, err := l.Term(4); err != nil || term != 1 {
		t.Fatalf("Term at the compaction point = %d, %v", term, err)
	}
	if _, err := l.Term(3); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Term below the compaction point = %v, want ErrCompacted", err)
	}
	if _, err := l.At(4); !errors.Is(err, ErrCompacted) {
		t.Fatalf("At the compaction point = %v, want ErrCompacted", err)
	}
	// Absolute indexing must still be correct after compaction.
	e, err := l.At(7)
	if err != nil || e.Index != 7 {
		t.Fatalf("At(7) after compaction = %+v, %v", e, err)
	}
	got, err := l.Slice(5, 8, 0)
	if err != nil || len(got) != 3 || got[0].Index != 5 {
		t.Fatalf("Slice after compaction = %v, %v", got, err)
	}
	// Appending continues from the correct absolute index.
	if err := l.Append(entry(11, 2, "next")); err != nil {
		t.Fatal(err)
	}
}

func TestCompactRejectsDivergentSnapshot(t *testing.T) {
	l := New()
	appendN(t, l, 5, 1, 10)
	// The log says index 4 has term 5; a snapshot claiming term 9 comes from a
	// different history and must not be spliced in.
	if err := l.Compact(4, 9); err == nil {
		t.Fatal("a compaction point with a mismatched term must be rejected")
	}
}

func TestCompactIsMonotonic(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 10)
	if err := l.Compact(5, 1); err != nil {
		t.Fatal(err)
	}
	if err := l.Compact(3, 1); err == nil {
		t.Fatal("compacting backwards must be rejected")
	}
	// Recompacting at the same point is a no-op.
	if err := l.Compact(5, 1); err != nil {
		t.Fatal(err)
	}
}

func TestCompactBeyondLogDiscardsEverything(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 5)
	// A leader's snapshot can cover more than this follower holds.
	if err := l.Compact(20, 4); err != nil {
		t.Fatal(err)
	}
	if l.Len() != 0 || l.LastIndex() != 20 || l.LastTerm() != 4 {
		t.Fatalf("post-install state: len=%d last=%d term=%d", l.Len(), l.LastIndex(), l.LastTerm())
	}
	if err := l.Append(entry(21, 4, "x")); err != nil {
		t.Fatalf("append after installing a snapshot: %v", err)
	}
}

func TestTruncateBelowCompactionPointIsRejected(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 10)
	if err := l.Compact(5, 1); err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateSuffix(3); err == nil {
		t.Fatal("truncating into the compacted prefix must be rejected")
	}
}

func TestRestore(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 10)
	l.Restore(50, 7)
	if l.Len() != 0 || l.LastIndex() != 50 || l.LastTerm() != 7 || l.FirstIndex() != 51 {
		t.Fatalf("Restore state: len=%d last=%d term=%d first=%d",
			l.Len(), l.LastIndex(), l.LastTerm(), l.FirstIndex())
	}
}

// The election restriction is the safety property that prevents a node missing
// committed entries from becoming leader and overwriting them.
func TestIsUpToDate(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 3)
	appendN(t, l, 2, 4, 5) // last = (index 5, term 2)

	cases := []struct {
		name                string
		candIndex, candTerm uint64
		want                bool
	}{
		{"higher term with shorter log wins", 1, 3, true},
		{"lower term with longer log loses", 100, 1, false},
		{"same term longer log wins", 6, 2, true},
		{"same term same length ties in favour", 5, 2, true},
		{"same term shorter log loses", 4, 2, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := l.IsUpToDate(c.candIndex, c.candTerm); got != c.want {
				t.Fatalf("IsUpToDate(%d,%d) = %v, want %v", c.candIndex, c.candTerm, got, c.want)
			}
		})
	}
}

func TestFindConflict(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 3)
	appendN(t, l, 2, 4, 5)

	// Entries that already match report no conflict.
	if got := l.FindConflict([]Entry{entry(1, 1, ""), entry(2, 1, "")}); got != 0 {
		t.Fatalf("matching entries reported a conflict at %d", got)
	}
	// A term disagreement is reported at the first differing index.
	if got := l.FindConflict([]Entry{entry(4, 2, ""), entry(5, 3, "")}); got != 5 {
		t.Fatalf("conflict = %d, want 5", got)
	}
	// Entries beyond the log's end are all new.
	if got := l.FindConflict([]Entry{entry(6, 2, "")}); got != 6 {
		t.Fatalf("conflict = %d, want 6", got)
	}
}

func TestConflictHintSkipsWholeTerm(t *testing.T) {
	l := New()
	appendN(t, l, 1, 1, 2) // indices 1-2, term 1
	appendN(t, l, 5, 3, 8) // indices 3-8, term 5

	// A failure inside the term-5 run must point the leader at the start of
	// that run, not one index back.
	idx, term := l.ConflictHint(8)
	if idx != 3 || term != 5 {
		t.Fatalf("ConflictHint(8) = (%d,%d), want (3,5)", idx, term)
	}
	// A follower whose log is too short reports where its log ends.
	idx, term = l.ConflictHint(100)
	if idx != 9 || term != 0 {
		t.Fatalf("ConflictHint past the end = (%d,%d), want (9,0)", idx, term)
	}
}

func TestProtoRoundTrip(t *testing.T) {
	e := entry(7, 3, "payload")
	got := FromProto(e.ToProto())
	if got.Index != e.Index || got.Term != e.Term || got.Type != e.Type || string(got.Data) != "payload" {
		t.Fatalf("round trip lost data: %+v", got)
	}
}

func TestByteAccounting(t *testing.T) {
	l := New()
	if err := l.Append(Entry{Index: 1, Term: 1, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(Entry{Index: 2, Term: 1, Data: make([]byte, 50)}); err != nil {
		t.Fatal(err)
	}
	if l.Bytes() != 150 {
		t.Fatalf("Bytes = %d, want 150", l.Bytes())
	}
	if err := l.Compact(1, 1); err != nil {
		t.Fatal(err)
	}
	if l.Bytes() != 50 {
		t.Fatalf("Bytes after compaction = %d, want 50", l.Bytes())
	}
}

func BenchmarkLogAppend(b *testing.B) {
	l := New()
	data := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := l.Append(Entry{Index: uint64(i + 1), Term: 1, Data: data}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogSlice(b *testing.B) {
	l := New()
	for i := 1; i <= 100000; i++ {
		_ = l.Append(Entry{Index: uint64(i), Term: 1, Data: make([]byte, 64)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.Slice(1000, 1064, 64); err != nil {
			b.Fatal(err)
		}
	}
}
