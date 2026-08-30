package ring

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func nodes(ids ...string) []Node {
	out := make([]Node, 0, len(ids))
	for _, id := range ids {
		out = append(out, Node{ID: id, PeerAddress: id + ":7200", Weight: 1})
	}
	return out
}

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("route-a|GET|/asset/%d.bin", i)
	}
	return out
}

// Determinism is the property the whole L2 design rests on: two edges holding
// the same membership must independently agree on every key's owner.
func TestRingDeterministicAcrossInputOrder(t *testing.T) {
	a := New(nodes("edge-1", "edge-2", "edge-3"), 128)
	b := New(nodes("edge-3", "edge-1", "edge-2"), 128)

	if a.Len() != 3 || b.Len() != 3 {
		t.Fatalf("expected 3 nodes, got %d and %d", a.Len(), b.Len())
	}
	for _, k := range keys(5000) {
		oa, ok := a.Owner(k)
		if !ok {
			t.Fatalf("key %q had no owner", k)
		}
		ob, _ := b.Owner(k)
		if oa.ID != ob.ID {
			t.Fatalf("ownership differs for %q: %s vs %s", k, oa.ID, ob.ID)
		}
	}
}

func TestRingEveryKeyMapsToLiveNode(t *testing.T) {
	members := nodes("edge-1", "edge-2", "edge-3")
	r := New(members, 128)
	live := map[string]bool{}
	for _, n := range members {
		live[n.ID] = true
	}
	for _, k := range keys(20000) {
		o, ok := r.Owner(k)
		if !ok {
			t.Fatalf("key %q had no owner", k)
		}
		if !live[o.ID] {
			t.Fatalf("key %q owned by unknown node %q", k, o.ID)
		}
	}
}

func TestRingEmptyOwnsNothing(t *testing.T) {
	r := New(nil, 128)
	if !r.Empty() {
		t.Fatal("expected empty ring")
	}
	if _, ok := r.Owner("anything"); ok {
		t.Fatal("empty ring must not report an owner")
	}
	if got := r.Owners("anything", 3); got != nil {
		t.Fatalf("empty ring must return no owners, got %v", got)
	}
}

func TestRingOwnersAreDistinctPhysicalNodes(t *testing.T) {
	r := New(nodes("edge-1", "edge-2", "edge-3", "edge-4", "edge-5"), 128)
	for _, k := range keys(5000) {
		for n := 1; n <= 5; n++ {
			owners := r.Owners(k, n)
			if len(owners) != n {
				t.Fatalf("key %q asked for %d owners, got %d", k, n, len(owners))
			}
			seen := map[string]bool{}
			for _, o := range owners {
				if seen[o.ID] {
					t.Fatalf("key %q got duplicate physical node %q in %v", k, o.ID, owners)
				}
				seen[o.ID] = true
			}
			// The first owner must always equal Owner().
			primary, _ := r.Owner(k)
			if owners[0].ID != primary.ID {
				t.Fatalf("Owners()[0]=%s disagrees with Owner()=%s", owners[0].ID, primary.ID)
			}
		}
	}
}

func TestRingOwnersClampsToMembership(t *testing.T) {
	r := New(nodes("edge-1", "edge-2"), 64)
	got := r.Owners("k", 5)
	if len(got) != 2 {
		t.Fatalf("expected owners clamped to 2 members, got %d", len(got))
	}
}

// The headline consistent-hashing property: growing 3 -> 4 must move roughly
// 1/4 of keys, where modulo hashing moves the large majority.
func TestRingKeyMovementBeatsModulo(t *testing.T) {
	ks := keys(100000)

	before := New(nodes("edge-1", "edge-2", "edge-3"), 128)
	after := New(nodes("edge-1", "edge-2", "edge-3", "edge-4"), 128)

	moved := 0
	for _, k := range ks {
		a, _ := before.Owner(k)
		b, _ := after.Owner(k)
		if a.ID != b.ID {
			moved++
		}
	}
	ringPct := float64(moved) / float64(len(ks)) * 100

	// Modulo baseline over the same hash and key set.
	modBefore := func(k string) uint64 { return HashKey(k) % 3 }
	modAfter := func(k string) uint64 { return HashKey(k) % 4 }
	movedMod := 0
	for _, k := range ks {
		if modBefore(k) != modAfter(k) {
			movedMod++
		}
	}
	modPct := float64(movedMod) / float64(len(ks)) * 100

	t.Logf("3->4 key movement: ring=%.2f%% modulo=%.2f%%", ringPct, modPct)

	// Theoretical ideal is 25%. Allow slack for virtual-node imbalance.
	if ringPct > 32 {
		t.Fatalf("ring moved %.2f%% of keys on 3->4; expected close to 25%%", ringPct)
	}
	if ringPct >= modPct {
		t.Fatalf("ring movement %.2f%% did not beat modulo %.2f%%", ringPct, modPct)
	}
}

// Removing a node must only remap the keys that node owned. Every other key
// keeps its owner: that is what makes membership churn cheap.
func TestRingRemovalOnlyRemapsOrphanedKeys(t *testing.T) {
	before := New(nodes("edge-1", "edge-2", "edge-3", "edge-4"), 128)
	after := New(nodes("edge-1", "edge-2", "edge-3"), 128)

	for _, k := range keys(50000) {
		a, _ := before.Owner(k)
		b, _ := after.Owner(k)
		if a.ID == "edge-4" {
			if b.ID == "edge-4" {
				t.Fatalf("key %q still owned by removed node", k)
			}
			continue
		}
		if a.ID != b.ID {
			t.Fatalf("key %q moved from %s to %s despite its owner surviving", k, a.ID, b.ID)
		}
	}
}

// Balance is statistical, not exact. With V virtual nodes per physical node the
// standard deviation of a node's share is roughly 1/sqrt(V), so 128 vnodes
// leaves a worst-case deviation in the mid-teens of percent on a 3-node ring.
// The test asserts the bound that actually holds at the configured default and
// then asserts the property that matters: more virtual nodes tighten the
// distribution monotonically.
func TestRingDistributionBalance(t *testing.T) {
	ks := keys(100000)

	deviation := func(vnodes int) float64 {
		r := New(nodes("edge-1", "edge-2", "edge-3"), vnodes)
		counts := map[string]int{}
		for _, k := range ks {
			o, _ := r.Owner(k)
			counts[o.ID]++
		}
		mean := float64(len(ks)) / 3
		worst := 0.0
		for id, c := range counts {
			dev := math.Abs(float64(c)-mean) / mean * 100
			t.Logf("vnodes=%d %s: %d keys (%.2f%% from mean)", vnodes, id, c, dev)
			if dev > worst {
				worst = dev
			}
		}
		return worst
	}

	atDefault := deviation(DefaultVirtualNodes)
	if atDefault > 20 {
		t.Fatalf("worst-case imbalance at %d vnodes was %.2f%%, above the 20%% bound",
			DefaultVirtualNodes, atDefault)
	}

	atHigh := deviation(2048)
	if atHigh >= atDefault {
		t.Fatalf("raising virtual nodes did not improve balance: %d vnodes=%.2f%%, 2048 vnodes=%.2f%%",
			DefaultVirtualNodes, atDefault, atHigh)
	}
	if atHigh > 5 {
		t.Fatalf("worst-case imbalance at 2048 vnodes was %.2f%%, above the 5%% bound", atHigh)
	}
}

func TestRingIsOwner(t *testing.T) {
	r := New(nodes("edge-1", "edge-2", "edge-3"), 128)
	k := "route-a|GET|/x"
	owners := r.Owners(k, 2)
	if !r.IsOwner(k, owners[0].ID, 2) || !r.IsOwner(k, owners[1].ID, 2) {
		t.Fatal("both selected owners must report as owners")
	}
	// The node that is neither of the two owners must not claim ownership.
	for _, n := range r.Nodes() {
		if n.ID != owners[0].ID && n.ID != owners[1].ID {
			if r.IsOwner(k, n.ID, 2) {
				t.Fatalf("%s wrongly claims ownership of %q at RF=2", n.ID, k)
			}
		}
	}
}

func TestRingNodeLookup(t *testing.T) {
	r := New(nodes("edge-1", "edge-2"), 32)
	n, ok := r.Node("edge-2")
	if !ok || n.PeerAddress != "edge-2:7200" {
		t.Fatalf("lookup failed: %+v ok=%v", n, ok)
	}
	if _, ok := r.Node("edge-9"); ok {
		t.Fatal("unknown node must not resolve")
	}
}

func TestRingVersionIsCarriedNotHashed(t *testing.T) {
	base := New(nodes("edge-1", "edge-2", "edge-3"), 128)
	tagged := base.WithVersion(42)
	if tagged.Version() != 42 {
		t.Fatalf("version not carried: %d", tagged.Version())
	}
	if base.Version() != 0 {
		t.Fatal("WithVersion must not mutate the source ring")
	}
	for _, k := range keys(1000) {
		a, _ := base.Owner(k)
		b, _ := tagged.Owner(k)
		if a.ID != b.ID {
			t.Fatalf("version tag changed ownership of %q", k)
		}
	}
}

// Holder swaps must be safe against concurrent hot-path readers. Run with -race.
func TestHolderConcurrentReadDuringReplacement(t *testing.T) {
	h := NewHolder()
	h.Store(New(nodes("edge-1", "edge-2", "edge-3"), 128))

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				r := h.Load()
				k := fmt.Sprintf("key-%d-%d", seed, n)
				n++
				if r.Empty() {
					t.Errorf("holder produced an empty ring")
					return
				}
				owners := r.Owners(k, 2)
				if len(owners) == 0 {
					t.Errorf("no owners for %q", k)
					return
				}
			}
		}(i)
	}

	writer := []([]Node){
		nodes("edge-1", "edge-2", "edge-3"),
		nodes("edge-1", "edge-2", "edge-3", "edge-4"),
		nodes("edge-2", "edge-3"),
		nodes("edge-1", "edge-2", "edge-3", "edge-4", "edge-5"),
	}
	for i := 0; i < 400; i++ {
		h.Store(New(writer[i%len(writer)], 128).WithVersion(uint64(i)))
	}
	close(stop)
	wg.Wait()
}

func TestHolderIgnoresNilStore(t *testing.T) {
	h := NewHolder()
	r := New(nodes("edge-1"), 8)
	h.Store(r)
	h.Store(nil)
	if h.Load().Len() != 1 {
		t.Fatal("nil Store must not clear the published ring")
	}
}

func BenchmarkRingOwner(b *testing.B) {
	r := New(nodes("edge-1", "edge-2", "edge-3"), 128)
	ks := keys(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := r.Owner(ks[i&1023]); !ok {
			b.Fatal("no owner")
		}
	}
}

func BenchmarkRingOwners2(b *testing.B) {
	r := New(nodes("edge-1", "edge-2", "edge-3"), 128)
	ks := keys(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(r.Owners(ks[i&1023], 2)) != 2 {
			b.Fatal("bad owners")
		}
	}
}

func BenchmarkRingBuild(b *testing.B) {
	ns := nodes("edge-1", "edge-2", "edge-3", "edge-4", "edge-5")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = New(ns, 128)
	}
}
