package l2

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/cache/l1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/peer"
	"github.com/sheehanlloyd/edgemesh/internal/ring"
)

// fakeOrigin counts fills and can be made to fail or block.
type fakeOrigin struct {
	fills atomic.Int64
	err   error
	gate  chan struct{}
	ttl   time.Duration
	// uncacheable produces an object with no expiry, which means "serve but do
	// not store".
	uncacheable bool
}

func (f *fakeOrigin) Fetch(ctx context.Context, req peer.FetchRequest) (*cache.Object, error) {
	f.fills.Add(1)
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	now := time.Now()
	obj := &cache.Object{
		Key: req.CacheKey, RouteID: req.RouteID, Status: 200,
		Header: http.Header{}, Body: []byte("origin-body"), StoredAt: now,
	}
	if !f.uncacheable {
		ttl := f.ttl
		if ttl == 0 {
			ttl = time.Hour
		}
		obj.ExpiresAt = now.Add(ttl)
		obj.StaleUntil = obj.ExpiresAt
	}
	return obj, nil
}

// fakePeers records peer traffic and can fail selected addresses.
type fakePeers struct {
	mu        sync.Mutex
	fetches   map[string]int
	replicas  map[string]int
	failFetch map[string]bool
	failRepl  map[string]bool
	// object is returned by a successful GetOrFetch.
	object *cache.Object
	// outcome is what the owner claims about how it answered. Empty means the
	// owner served it from its own tier.
	outcome cache.Outcome
}

func newFakePeers() *fakePeers {
	return &fakePeers{
		fetches: map[string]int{}, replicas: map[string]int{},
		failFetch: map[string]bool{}, failRepl: map[string]bool{},
	}
}

func (f *fakePeers) GetOrFetch(_ context.Context, addr string, req peer.FetchRequest) (*cache.Object, cache.Outcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetches[addr]++
	if f.failFetch[addr] {
		return nil, cache.OutcomeMiss, errs.New(errs.ClassPeerFailure, "peer %q is unreachable", addr)
	}
	outcome := f.outcome
	if outcome == "" {
		outcome = cache.OutcomeHit
	}
	if f.object != nil {
		return f.object, outcome, nil
	}
	now := time.Now()
	return &cache.Object{
		Key: req.CacheKey, RouteID: req.RouteID, Status: 200,
		Header: http.Header{}, Body: []byte("peer-body"),
		StoredAt: now, ExpiresAt: now.Add(time.Hour), StaleUntil: now.Add(time.Hour),
	}, outcome, nil
}

func (f *fakePeers) Replicate(_ context.Context, addr, _ string, _ *cache.Object) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replicas[addr]++
	if f.failRepl[addr] {
		return errs.New(errs.ClassPeerFailure, "replica refused by %q", addr)
	}
	return nil
}

func (f *fakePeers) counts() (fetches, replicas map[string]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fetches = map[string]int{}
	replicas = map[string]int{}
	for k, v := range f.fetches {
		fetches[k] = v
	}
	for k, v := range f.replicas {
		replicas[k] = v
	}
	return
}

func newTier(t *testing.T) *l1.Cache {
	t.Helper()
	c, err := l1.New(l1.Options{MaxBytes: 1 << 22, Shards: 8, MaxObjectBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func newCoordinator(t *testing.T, nodeID string, nodes []string, origin OriginFetcher, peers PeerClient) (*Coordinator, *l1.Cache, *l1.Cache) {
	t.Helper()
	l2Tier, front := newTier(t), newTier(t)
	holder := ring.NewHolder()

	if nodes != nil {
		rn := make([]ring.Node, 0, len(nodes))
		for _, id := range nodes {
			rn = append(rn, ring.Node{ID: id, PeerAddress: id + ":7200", Weight: 1})
		}
		holder.Store(ring.New(rn, 128).WithVersion(1))
	}

	c, err := New(context.Background(), Options{
		NodeID: nodeID, Local: l2Tier, FrontTier: front, Ring: holder,
		Peers: peers, Origin: origin, ReplicationFactor: 2,
		ReplicationWorkers: 2, ReplicationQueue: 64,
		ReplicationTimeout: time.Second, MaxObjectBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})
	return c, l2Tier, front
}

func req(key string) peer.FetchRequest {
	return peer.FetchRequest{RouteID: "route-a", CacheKey: key, Method: "GET", Path: "/x"}
}

// ownedKey finds a key whose primary owner is want, so ownership-dependent
// behaviour can be tested deterministically instead of hoping for a hash.
func ownedKey(t *testing.T, nodes []string, want string) string {
	t.Helper()
	rn := make([]ring.Node, 0, len(nodes))
	for _, id := range nodes {
		rn = append(rn, ring.Node{ID: id, PeerAddress: id + ":7200", Weight: 1})
	}
	r := ring.New(rn, 128)
	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("route-a\x1fGET\x1f/key-%d\x1f\x1f", i)
		if o, ok := r.Owner(k); ok && o.ID == want {
			return k
		}
	}
	t.Fatalf("no key found owned by %q", want)
	return ""
}

// With no ring membership there is no distributed tier, so the only correct
// behaviour is a local fill rather than a failure.
func TestEmptyRingFillsLocally(t *testing.T) {
	origin := &fakeOrigin{}
	c, _, _ := newCoordinator(t, "edge-1", nil, origin, newFakePeers())

	obj, outcome, err := c.Get(context.Background(), req("k"))
	if err != nil || obj == nil {
		t.Fatalf("get on an empty ring: %v", err)
	}
	if outcome != cache.OutcomeMiss {
		t.Fatalf("outcome = %q, want MISS", outcome)
	}
	if origin.fills.Load() != 1 {
		t.Fatalf("origin fills = %d, want 1", origin.fills.Load())
	}
}

func TestLocalOwnerFillsAndCaches(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3"}
	key := ownedKey(t, nodes, "edge-1")
	origin := &fakeOrigin{}
	peers := newFakePeers()
	c, l2Tier, _ := newCoordinator(t, "edge-1", nodes, origin, peers)

	obj, _, err := c.Get(context.Background(), req(key))
	if err != nil || obj == nil {
		t.Fatal(err)
	}
	if origin.fills.Load() != 1 {
		t.Fatalf("origin fills = %d, want 1", origin.fills.Load())
	}
	// A second request hits the local tier rather than the origin.
	if _, outcome, err := c.Get(context.Background(), req(key)); err != nil || outcome != cache.OutcomeHit {
		t.Fatalf("second get: outcome=%q err=%v", outcome, err)
	}
	if origin.fills.Load() != 1 {
		t.Fatal("a cached object was re-fetched from origin")
	}
	if _, ok := l2Tier.Peek(key); !ok {
		t.Fatal("the object was not stored in the distributed tier")
	}
	// No peer fetch should have happened: this node is the owner.
	fetches, _ := peers.counts()
	if len(fetches) != 0 {
		t.Fatalf("the owner made peer fetches: %v", fetches)
	}
}

// PEER_HIT must mean the object was already somewhere in the cluster.
//
// The owner answers a peer request either from its own tier or by fetching from
// the origin on the caller's behalf, and both come back over the same stream.
// Labelling the second case PEER_HIT credits the cache with an origin fetch it
// did not save, and makes the first request for a cold object report a hit. The
// owner reports which it was, and the ingress edge has to use that.
func TestOwnerFillingFromOriginIsNotReportedAsAPeerHit(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3"}
	key := ownedKey(t, nodes, "edge-2")
	origin := &fakeOrigin{}
	peers := newFakePeers()
	// The owner had to go to the origin for this one.
	peers.outcome = cache.OutcomeMiss
	c, _, _ := newCoordinator(t, "edge-1", nodes, origin, peers)

	obj, outcome, err := c.Get(context.Background(), req(key))
	if err != nil || obj == nil {
		t.Fatal(err)
	}
	if outcome != cache.OutcomeMiss {
		t.Fatalf("outcome = %q, want MISS: the owner fetched this from the origin", outcome)
	}
}

// The owner fills; other edges stream the result. This is what makes a cold
// object cost one origin fetch cluster-wide instead of one per edge.
func TestRemoteOwnerIsAsked(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3"}
	key := ownedKey(t, nodes, "edge-2")
	origin := &fakeOrigin{}
	peers := newFakePeers()
	c, _, _ := newCoordinator(t, "edge-1", nodes, origin, peers)

	obj, outcome, err := c.Get(context.Background(), req(key))
	if err != nil || obj == nil {
		t.Fatal(err)
	}
	if outcome != cache.OutcomePeerHit {
		t.Fatalf("outcome = %q, want PEER_HIT", outcome)
	}
	if string(obj.Body) != "peer-body" {
		t.Fatalf("body = %q, want the peer's object", obj.Body)
	}
	// The ingress node must not have gone to origin itself.
	if origin.fills.Load() != 0 {
		t.Fatalf("ingress fetched from origin despite a reachable owner: %d", origin.fills.Load())
	}
	fetches, _ := peers.counts()
	if fetches["edge-2:7200"] != 1 {
		t.Fatalf("peer fetches = %v, want one to the owner", fetches)
	}
}

// The defining availability property: a dead owner must not fail the request.
func TestUnreachableOwnerFallsBackToAReplica(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3"}
	key := ownedKey(t, nodes, "edge-2")
	origin := &fakeOrigin{}
	peers := newFakePeers()
	peers.failFetch["edge-2:7200"] = true
	c, _, _ := newCoordinator(t, "edge-1", nodes, origin, peers)

	obj, outcome, err := c.Get(context.Background(), req(key))
	if err != nil || obj == nil {
		t.Fatalf("a dead owner failed the request: %v", err)
	}
	fetches, _ := peers.counts()
	// The owner was tried, then a replica.
	if fetches["edge-2:7200"] != 1 {
		t.Fatalf("owner was not attempted: %v", fetches)
	}
	if outcome != cache.OutcomePeerHit && outcome != cache.OutcomeDegraded {
		t.Fatalf("outcome = %q", outcome)
	}
}

func TestAllPeersUnreachableFallsBackToOrigin(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3"}
	key := ownedKey(t, nodes, "edge-2")
	origin := &fakeOrigin{}
	peers := newFakePeers()
	for _, id := range nodes {
		peers.failFetch[id+":7200"] = true
	}
	c, _, _ := newCoordinator(t, "edge-1", nodes, origin, peers)

	obj, outcome, err := c.Get(context.Background(), req(key))
	if err != nil || obj == nil {
		t.Fatalf("the request failed despite a reachable origin: %v", err)
	}
	if outcome != cache.OutcomeDegraded {
		t.Fatalf("outcome = %q, want DEGRADED", outcome)
	}
	if origin.fills.Load() != 1 {
		t.Fatalf("origin fills = %d, want 1 degraded fill", origin.fills.Load())
	}
	if string(obj.Body) != "origin-body" {
		t.Fatalf("body = %q", obj.Body)
	}
}

// One origin fill per key, however many concurrent misses arrive.
func TestConcurrentMissesAreCoalesced(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3"}
	key := ownedKey(t, nodes, "edge-1")
	origin := &fakeOrigin{gate: make(chan struct{})}
	c, _, _ := newCoordinator(t, "edge-1", nodes, origin, newFakePeers())

	const callers = 40
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := c.Get(context.Background(), req(key)); err != nil {
				t.Errorf("get: %v", err)
			}
		}()
	}
	// Let every caller reach the fill before releasing it.
	deadline := time.Now().Add(3 * time.Second)
	for c.Coalescer().Waiters() < callers && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(origin.gate)
	wg.Wait()

	if got := origin.fills.Load(); got != 1 {
		t.Fatalf("%d origin fills for %d concurrent misses; coalescing failed", got, callers)
	}
}

func TestReplicationToSuccessors(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3"}
	key := ownedKey(t, nodes, "edge-1")
	peers := newFakePeers()
	c, _, _ := newCoordinator(t, "edge-1", nodes, &fakeOrigin{}, peers)

	if _, _, err := c.Get(context.Background(), req(key)); err != nil {
		t.Fatal(err)
	}
	// Replication is asynchronous, so poll rather than assuming it completed.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, replicas := peers.counts()
		if len(replicas) > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the owner never replicated the object to a successor")
}

// A replica write must never block a client response: cached data is derivative.
func TestReplicationFailureDoesNotFailTheRequest(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3"}
	key := ownedKey(t, nodes, "edge-1")
	peers := newFakePeers()
	for _, id := range nodes {
		peers.failRepl[id+":7200"] = true
	}
	c, _, _ := newCoordinator(t, "edge-1", nodes, &fakeOrigin{}, peers)

	obj, _, err := c.Get(context.Background(), req(key))
	if err != nil || obj == nil {
		t.Fatalf("a failed replica write failed the client request: %v", err)
	}
}

func TestStoreReplica(t *testing.T) {
	c, l2Tier, _ := newCoordinator(t, "edge-1", []string{"edge-1", "edge-2"}, &fakeOrigin{}, newFakePeers())
	now := time.Now()

	obj := &cache.Object{
		Key: "k", RouteID: "route-a", Status: 200, Header: http.Header{},
		Body: []byte("replica"), StoredAt: now,
		ExpiresAt: now.Add(time.Hour), StaleUntil: now.Add(time.Hour),
	}
	if err := c.StoreReplica(obj); err != nil {
		t.Fatal(err)
	}
	if _, ok := l2Tier.Peek("k"); !ok {
		t.Fatal("the replica was not stored")
	}

	if err := c.StoreReplica(nil); err == nil {
		t.Fatal("a nil replica must be rejected")
	}
	// An already-expired replica would occupy budget a live object could use.
	expired := obj.Clone()
	expired.Key = "expired"
	expired.ExpiresAt = now.Add(-time.Hour)
	expired.StaleUntil = expired.ExpiresAt
	if err := c.StoreReplica(expired); err == nil {
		t.Fatal("an expired replica must be rejected")
	}
	// An oversized replica must be refused rather than blowing the budget.
	big := obj.Clone()
	big.Key = "big"
	big.Body = make([]byte, (1<<20)+1)
	if err := c.StoreReplica(big); err == nil || !errs.IsClass(err, errs.ClassTooLarge) {
		t.Fatalf("oversized replica err = %v", err)
	}
}

// An uncacheable response is served but must not be stored.
func TestUncacheableResponseIsServedNotStored(t *testing.T) {
	nodes := []string{"edge-1"}
	key := ownedKey(t, nodes, "edge-1")
	c, l2Tier, _ := newCoordinator(t, "edge-1", nodes, &fakeOrigin{uncacheable: true}, newFakePeers())

	obj, _, err := c.Get(context.Background(), req(key))
	if err != nil || obj == nil {
		t.Fatal(err)
	}
	if _, ok := l2Tier.Peek(key); ok {
		t.Fatal("an object with no expiry was stored")
	}
}

func TestPurgeClearsBothTiers(t *testing.T) {
	c, l2Tier, front := newCoordinator(t, "edge-1", []string{"edge-1"}, &fakeOrigin{}, newFakePeers())
	now := time.Now()

	store := func(tier *l1.Cache, key string) {
		if err := tier.Put(key, &cache.Object{
			Key: key, RouteID: "route-a", Status: 200, Header: http.Header{},
			Body: []byte("x"), StoredAt: now,
			ExpiresAt: now.Add(time.Hour), StaleUntil: now.Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
	}
	key := "route-a\x1fGET\x1f/x\x1f\x1f"
	store(l2Tier, key)
	store(front, key)

	// A purge that cleared only L2 would leave the stale object serving from
	// L1, which is the difference between a purge that works and one that
	// silently does nothing.
	if n := c.Purge(edgemeshv1.PurgeScope_PURGE_SCOPE_KEY, "", key, 1); n != 2 {
		t.Fatalf("purged %d objects, want 2 (one per tier)", n)
	}
	if _, ok := front.Peek(key); ok {
		t.Fatal("the front tier still holds the purged object")
	}
	if _, ok := l2Tier.Peek(key); ok {
		t.Fatal("the distributed tier still holds the purged object")
	}
}

func TestPurgeScopes(t *testing.T) {
	c, l2Tier, front := newCoordinator(t, "edge-1", []string{"edge-1"}, &fakeOrigin{}, newFakePeers())
	now := time.Now()
	put := func(tier *l1.Cache, route string, i int) {
		k := fmt.Sprintf("%s\x1fGET\x1f/x/%d\x1f\x1f", route, i)
		_ = tier.Put(k, &cache.Object{
			Key: k, RouteID: route, Status: 200, Header: http.Header{}, Body: []byte("x"),
			StoredAt: now, ExpiresAt: now.Add(time.Hour), StaleUntil: now.Add(time.Hour),
		})
	}
	for i := 0; i < 5; i++ {
		put(l2Tier, "route-a", i)
		put(front, "route-a", i)
		put(l2Tier, "route-b", i)
	}
	if n := c.Purge(edgemeshv1.PurgeScope_PURGE_SCOPE_ROUTE, "route-a", "", 1); n != 10 {
		t.Fatalf("route purge removed %d, want 10", n)
	}
	if l2Tier.Len() != 5 {
		t.Fatalf("route purge removed the wrong route's objects: %d remain", l2Tier.Len())
	}
	if n := c.Purge(edgemeshv1.PurgeScope_PURGE_SCOPE_ALL, "", "", 2); n != 5 {
		t.Fatalf("purge-all removed %d, want 5", n)
	}
}

// Config events can arrive out of order after a reconnect; re-applying an older
// purge would needlessly evict objects fetched since.
func TestPurgeIgnoresStaleVersions(t *testing.T) {
	c, l2Tier, _ := newCoordinator(t, "edge-1", []string{"edge-1"}, &fakeOrigin{}, newFakePeers())
	now := time.Now()
	key := "route-a\x1fGET\x1f/x\x1f\x1f"

	c.Purge(edgemeshv1.PurgeScope_PURGE_SCOPE_ALL, "", "", 10)

	_ = l2Tier.Put(key, &cache.Object{
		Key: key, RouteID: "route-a", Status: 200, Header: http.Header{}, Body: []byte("x"),
		StoredAt: now, ExpiresAt: now.Add(time.Hour), StaleUntil: now.Add(time.Hour),
	})
	// A reordered older purge must be ignored.
	if n := c.Purge(edgemeshv1.PurgeScope_PURGE_SCOPE_ALL, "", "", 5); n != 0 {
		t.Fatalf("a stale purge removed %d objects", n)
	}
	if _, ok := l2Tier.Peek(key); !ok {
		t.Fatal("a stale purge evicted a newer object")
	}
	// A newer purge still applies.
	if n := c.Purge(edgemeshv1.PurgeScope_PURGE_SCOPE_ALL, "", "", 11); n != 1 {
		t.Fatalf("a newer purge removed %d objects, want 1", n)
	}
}

func TestNewValidatesOptions(t *testing.T) {
	ctx := context.Background()
	if _, err := New(ctx, Options{NodeID: "e1", Ring: ring.NewHolder()}); err == nil {
		t.Fatal("a coordinator without a local tier must be rejected")
	}
	if _, err := New(ctx, Options{NodeID: "e1", Local: newTier(t)}); err == nil {
		t.Fatal("a coordinator without a ring must be rejected")
	}
	if _, err := New(ctx, Options{Local: newTier(t), Ring: ring.NewHolder()}); err == nil {
		t.Fatal("a coordinator without a node id must be rejected")
	}
}

func TestReadinessAndStats(t *testing.T) {
	c, _, _ := newCoordinator(t, "edge-1", []string{"edge-1"}, &fakeOrigin{}, newFakePeers())
	if c.Ready() {
		t.Fatal("a coordinator must start not ready")
	}
	c.SetReady(true)
	c.SetConfigVersion(9)
	if !c.Ready() || c.ConfigVersion() != 9 || c.NodeID() != "edge-1" {
		t.Fatal("accessors disagree with what was set")
	}
	if _, _, err := c.Get(context.Background(), req("k")); err != nil {
		t.Fatal(err)
	}
	objects, bytes := c.Stats()
	if objects == 0 || bytes == 0 {
		t.Fatalf("stats = %d objects, %d bytes", objects, bytes)
	}
}

// Run with -race.
func TestCoordinatorConcurrency(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3"}
	c, _, _ := newCoordinator(t, "edge-1", nodes, &fakeOrigin{}, newFakePeers())

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				k := fmt.Sprintf("route-a\x1fGET\x1f/k%d\x1f\x1f", i%20)
				switch i % 5 {
				case 0, 1, 2:
					_, _, _ = c.Get(context.Background(), req(k))
				case 3:
					c.Purge(edgemeshv1.PurgeScope_PURGE_SCOPE_KEY, "", k, uint64(i))
				case 4:
					c.Stats()
					c.ReplicationQueueDepth()
				}
			}
		}(w)
	}
	wg.Wait()
}
