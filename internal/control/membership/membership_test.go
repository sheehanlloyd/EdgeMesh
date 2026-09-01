package membership

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/clock"
)

func newTracker(t *testing.T, clk clock.Clock, onChange func(*edgemeshv1.Membership)) *Tracker {
	t.Helper()
	tr := New(Options{
		HeartbeatInterval:  time.Second,
		SuspectAfterMissed: 2,
		DeadAfterMissed:    3,
		Clock:              clk,
		OnChange:           onChange,
	})
	tr.SetLeader(true)
	return tr
}

func node(id string) *edgemeshv1.EdgeNode {
	return &edgemeshv1.EdgeNode{
		Id: id, Region: "local-a", Zone: "local-a1",
		PeerAddress: id + ":7200", Weight: 1,
	}
}

func TestRegisterAndSnapshot(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	tr := newTracker(t, clk, nil)

	for _, id := range []string{"edge-3", "edge-1", "edge-2"} {
		if err := tr.Register(node(id)); err != nil {
			t.Fatal(err)
		}
	}
	snap := tr.Snapshot()
	if len(snap.GetNodes()) != 3 {
		t.Fatalf("snapshot holds %d nodes, want 3", len(snap.GetNodes()))
	}
	// Sorting makes the snapshot byte-stable, so two edges building a ring from
	// the same version agree on ownership.
	for i, want := range []string{"edge-1", "edge-2", "edge-3"} {
		if snap.GetNodes()[i].GetId() != want {
			t.Fatalf("node %d = %q, want %q (snapshot must be sorted)", i, snap.GetNodes()[i].GetId(), want)
		}
	}
	if snap.GetVersion() != 3 {
		t.Fatalf("membership version = %d, want 3", snap.GetVersion())
	}
}

func TestRegisterValidation(t *testing.T) {
	tr := newTracker(t, clock.NewMock(time.Time{}), nil)
	if err := tr.Register(&edgemeshv1.EdgeNode{PeerAddress: "x:1"}); err == nil {
		t.Fatal("a registration without a node id must be rejected")
	}
	// A node peers cannot dial is useless on the ring.
	if err := tr.Register(&edgemeshv1.EdgeNode{Id: "edge-1"}); err == nil {
		t.Fatal("a registration without a peer address must be rejected")
	}
}

// Re-registering an unchanged node must not bump the version: every bump
// rebroadcasts the ring to every edge.
func TestIdempotentRegisterDoesNotChurnTheRing(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	tr := newTracker(t, clk, nil)

	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	v := tr.Version()
	for i := 0; i < 10; i++ {
		if err := tr.Register(node("edge-1")); err != nil {
			t.Fatal(err)
		}
	}
	if tr.Version() != v {
		t.Fatalf("version churned from %d to %d on unchanged re-registration", v, tr.Version())
	}
	// A changed peer address does alter ownership and must bump.
	changed := node("edge-1")
	changed.PeerAddress = "edge-1-new:7200"
	if err := tr.Register(changed); err != nil {
		t.Fatal(err)
	}
	if tr.Version() == v {
		t.Fatal("a changed peer address must bump the membership version")
	}
}

// A heartbeat is a liveness refresh, not a ring change: bumping the version on
// every heartbeat would rebroadcast the ring several times a second.
func TestHeartbeatDoesNotBumpVersion(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	tr := newTracker(t, clk, nil)
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	v := tr.Version()

	for i := 0; i < 20; i++ {
		clk.Advance(500 * time.Millisecond)
		reregister, err := tr.Heartbeat("edge-1", uint64(i), nil)
		if err != nil {
			t.Fatal(err)
		}
		if reregister {
			t.Fatal("a known node must not be asked to re-register")
		}
	}
	if tr.Version() != v {
		t.Fatalf("heartbeats churned the membership version from %d to %d", v, tr.Version())
	}
}

// After a leadership change the new leader has no roster, so it must ask edges
// to re-register rather than guessing their peer addresses.
func TestHeartbeatFromUnknownNodeRequestsReregistration(t *testing.T) {
	tr := newTracker(t, clock.NewMock(time.Time{}), nil)
	reregister, err := tr.Heartbeat("edge-unknown", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reregister {
		t.Fatal("an unknown node must be asked to re-register")
	}
}

func TestSweepTransitionsThroughSuspectToDead(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	var changes []uint64
	var mu sync.Mutex
	tr := newTracker(t, clk, func(m *edgemeshv1.Membership) {
		mu.Lock()
		changes = append(changes, m.GetVersion())
		mu.Unlock()
	})
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	if err := tr.Register(node("edge-2")); err != nil {
		t.Fatal(err)
	}

	// edge-2 keeps heartbeating; edge-1 goes silent.
	clk.Advance(2100 * time.Millisecond) // past suspect (2 intervals)
	if _, err := tr.Heartbeat("edge-2", 1, nil); err != nil {
		t.Fatal(err)
	}
	ringChanged := tr.Sweep()
	if ringChanged {
		t.Fatal("a node going suspect must not change the ring; it keeps serving its keys")
	}
	if len(tr.Snapshot().GetNodes()) != 2 {
		t.Fatal("a suspect node must remain on the ring")
	}

	clk.Advance(1100 * time.Millisecond) // past dead (3 intervals)
	if _, err := tr.Heartbeat("edge-2", 2, nil); err != nil {
		t.Fatal(err)
	}
	if !tr.Sweep() {
		t.Fatal("a node going dead must change the ring")
	}
	snap := tr.Snapshot()
	if len(snap.GetNodes()) != 1 || snap.GetNodes()[0].GetId() != "edge-2" {
		t.Fatalf("dead node still on the ring: %+v", snap.GetNodes())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(changes) == 0 {
		t.Fatal("no membership change was published")
	}
}

func TestRecoveryReturnsToTheRing(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	tr := newTracker(t, clk, nil)
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Second)
	tr.Sweep()
	if len(tr.Snapshot().GetNodes()) != 0 {
		t.Fatal("expected the node to be removed")
	}
	// A dead node's heartbeat is not enough; it must re-register because the
	// tracker discarded its address.
	if _, err := tr.Heartbeat("edge-1", 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	if len(tr.Snapshot().GetNodes()) != 1 {
		t.Fatal("a re-registered node must return to the ring")
	}
}

// Liveness is leader-local ephemeral state, so losing leadership must discard
// it rather than letting a stale roster be rebroadcast later.
func TestLosingLeadershipClearsTheRoster(t *testing.T) {
	tr := newTracker(t, clock.NewMock(time.Time{}), nil)
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	if len(tr.All()) != 1 {
		t.Fatal("expected one registered node")
	}
	tr.SetLeader(false)
	if len(tr.All()) != 0 {
		t.Fatal("losing leadership must clear the roster")
	}
	if tr.IsLeader() {
		t.Fatal("IsLeader must report false")
	}
}

// A follower must not evaluate liveness: it has no authoritative view.
func TestSweepIsANoopOnAFollower(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	tr := newTracker(t, clk, nil)
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	tr.SetLeader(false)
	clk.Advance(time.Hour)
	if tr.Sweep() {
		t.Fatal("a follower must not publish membership changes")
	}
}

func TestRemove(t *testing.T) {
	tr := newTracker(t, clock.NewMock(time.Time{}), nil)
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	if !tr.Remove("edge-1") {
		t.Fatal("Remove reported nothing removed")
	}
	if tr.Remove("edge-1") {
		t.Fatal("removing a missing node must report false")
	}
	if len(tr.Snapshot().GetNodes()) != 0 {
		t.Fatal("node survived removal")
	}
}

func TestCounts(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	tr := newTracker(t, clk, nil)
	for _, id := range []string{"edge-1", "edge-2"} {
		if err := tr.Register(node(id)); err != nil {
			t.Fatal(err)
		}
	}
	counts := tr.Counts()
	if counts[edgemeshv1.NodeState_NODE_STATE_HEALTHY] != 2 {
		t.Fatalf("counts = %v", counts)
	}
}

// Snapshots must be copies: a caller mutating one would corrupt the roster.
func TestSnapshotReturnsCopies(t *testing.T) {
	tr := newTracker(t, clock.NewMock(time.Time{}), nil)
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	snap := tr.Snapshot()
	snap.GetNodes()[0].PeerAddress = "hijacked:1"
	if tr.Snapshot().GetNodes()[0].GetPeerAddress() != "edge-1:7200" {
		t.Fatal("mutating a snapshot corrupted the roster")
	}
}

// Run with -race.
func TestTrackerConcurrency(t *testing.T) {
	tr := New(Options{HeartbeatInterval: 10 * time.Millisecond, SuspectAfterMissed: 2, DeadAfterMissed: 3})
	tr.SetLeader(true)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				id := "edge-" + string(rune('a'+i%5))
				switch i % 4 {
				case 0:
					_ = tr.Register(node(id))
				case 1:
					_, _ = tr.Heartbeat(id, uint64(i), nil)
				case 2:
					tr.Sweep()
				case 3:
					tr.Snapshot()
					tr.Counts()
					tr.All()
				}
			}
		}(w)
	}
	wg.Wait()
}

// Edge cache stats reported on a heartbeat must be retained for the admin API.
//
// They were previously accepted and discarded, so `edgemeshctl nodes` could not
// show occupancy that the edges were already sending on every beat.
func TestHeartbeatRetainsReportedStats(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	tr := newTracker(t, clk, nil)
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}

	stats := &edgemeshv1.EdgeStats{
		InflightRequests: 4, CacheObjects: 120, CacheBytes: 8192, RequestsPerSecond: 12.5,
	}
	if reregister, err := tr.Heartbeat("edge-1", 7, stats); err != nil || reregister {
		t.Fatalf("Heartbeat() = %v, %v; want false, nil", reregister, err)
	}

	all := tr.All()
	if len(all) != 1 {
		t.Fatalf("All() returned %d nodes, want 1", len(all))
	}
	got := all[0].GetStats()
	if got.GetCacheObjects() != 120 || got.GetCacheBytes() != 8192 {
		t.Errorf("cache stats = %d objects / %d bytes, want 120 / 8192",
			got.GetCacheObjects(), got.GetCacheBytes())
	}
	if got.GetInflightRequests() != 4 {
		t.Errorf("inflight = %d, want 4", got.GetInflightRequests())
	}
	if got.GetRequestsPerSecond() != 12.5 {
		t.Errorf("rps = %v, want 12.5", got.GetRequestsPerSecond())
	}

	// The retained value must be a copy. Mutating what the caller passed in
	// must not reach through into the tracker's roster.
	stats.CacheObjects = 999
	if tr.All()[0].GetStats().GetCacheObjects() != 120 {
		t.Error("the tracker retained the caller's EdgeStats by reference")
	}
}

// The membership snapshot defines ring ownership and must stay byte-stable for
// a given version. Stats change on every heartbeat, so they must not ride in it.
func TestSnapshotOmitsStats(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	tr := newTracker(t, clk, nil)
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Heartbeat("edge-1", 1, &edgemeshv1.EdgeStats{CacheObjects: 5}); err != nil {
		t.Fatal(err)
	}

	before, err := proto.Marshal(tr.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range tr.Snapshot().GetNodes() {
		if n.GetStats() != nil {
			t.Fatalf("node %q carried stats into the ring snapshot", n.GetId())
		}
	}

	// A heartbeat that changes only the stats must leave the snapshot bytes and
	// the membership version untouched.
	if _, err := tr.Heartbeat("edge-1", 1, &edgemeshv1.EdgeStats{CacheObjects: 5000}); err != nil {
		t.Fatal(err)
	}
	after, err := proto.Marshal(tr.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Error("changing reported stats altered the ring membership snapshot")
	}
	if tr.All()[0].GetStats().GetCacheObjects() != 5000 {
		t.Error("the admin view did not observe the newer stats")
	}
}

// A re-registration carries no stats, and must not blank the last reading.
func TestReRegisterPreservesStats(t *testing.T) {
	tr := newTracker(t, clock.NewMock(time.Time{}), nil)
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Heartbeat("edge-1", 1, &edgemeshv1.EdgeStats{CacheObjects: 42}); err != nil {
		t.Fatal(err)
	}
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	if got := tr.All()[0].GetStats().GetCacheObjects(); got != 42 {
		t.Errorf("cache objects after re-registration = %d, want 42", got)
	}
}

// A heartbeat with no stats leaves the previous reading in place rather than
// clearing it, so an edge build that does not report them does not make the
// column flicker.
func TestHeartbeatWithoutStatsKeepsThePreviousReading(t *testing.T) {
	tr := newTracker(t, clock.NewMock(time.Time{}), nil)
	if err := tr.Register(node("edge-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Heartbeat("edge-1", 1, &edgemeshv1.EdgeStats{CacheObjects: 9}); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Heartbeat("edge-1", 2, nil); err != nil {
		t.Fatal(err)
	}
	if got := tr.All()[0].GetStats().GetCacheObjects(); got != 9 {
		t.Errorf("cache objects = %d, want the retained 9", got)
	}
}
