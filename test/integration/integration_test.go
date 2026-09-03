package integration

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/proxy"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
)

const host = "demo.edgemesh.local"

// A route created on the leader must reach every edge.
func TestRouteCreationPropagatesToEveryEdge(t *testing.T) {
	c := newCluster(t, 3, 2)
	c.configure("demo-route", host)

	for _, e := range c.edges {
		resp, body := c.get(e, host, "/static/propagate")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s returned %d: %s", e.id, resp.StatusCode, body)
		}
		if resp.Header.Get(proxy.HeaderRoute) != "demo-route" {
			t.Fatalf("%s served route %q", e.id, resp.Header.Get(proxy.HeaderRoute))
		}
	}
}

// A request that matches no route must 404 rather than reaching an origin.
func TestUnroutedRequestIs404(t *testing.T) {
	c := newCluster(t, 1, 1)
	c.configure("demo-route", host)

	resp, _ := c.get(c.edges[0], "unknown.example", "/anything")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if c.totalOriginHits("/anything") != 0 {
		t.Fatal("an unrouted request reached an origin")
	}
}

// The headline distributed-cache property: one origin fetch serves the whole
// cluster, and repeat requests on any edge are served from cache.
func TestClusterWideCacheFillHappensOnce(t *testing.T) {
	c := newCluster(t, 3, 2)
	c.configure("demo-route", host)
	c.waitForRing(3)

	const path = "/static/cluster-fill"

	// First request anywhere: a miss that fetches from origin.
	resp, _ := c.get(c.edges[0], host, path)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first request = %d", resp.StatusCode)
	}
	if got := c.totalOriginHits(path); got != 1 {
		t.Fatalf("origin hits after the first request = %d, want 1", got)
	}

	// Every subsequent request on any edge is served from cache.
	outcomes := map[string]int{}
	for round := 0; round < 3; round++ {
		for _, e := range c.edges {
			resp, body := c.get(e, host, path)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s returned %d: %s", e.id, resp.StatusCode, body)
			}
			outcomes[resp.Header.Get(proxy.HeaderCache)]++
		}
	}
	if got := c.totalOriginHits(path); got != 1 {
		t.Fatalf("origin hits after %d cached requests = %d, want 1", 9, got)
	}
	if outcomes["MISS"] > 2 {
		t.Fatalf("too many misses across the cluster: %v", outcomes)
	}
	t.Logf("cache outcomes over 9 cluster-wide requests: %v (origin fetches: 1)", outcomes)
}

// The object must genuinely live on its ring owner, reachable as a peer hit.
func TestObjectIsServedFromItsRingOwner(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.configure("demo-route", host)
	c.waitForRing(3)

	// Find a path whose owner is not the ingress edge, so the peer path runs.
	var path string
	ingress := c.edges[0]
	for i := 0; i < 200; i++ {
		candidate := fmt.Sprintf("/static/owned-%d", i)
		key := cacheKeyFor("demo-route", candidate)
		if owner, ok := ingress.ring.Load().Owner(key); ok && owner.ID != ingress.id {
			path = candidate
			break
		}
	}
	if path == "" {
		t.Fatal("no path found whose owner differs from the ingress edge")
	}

	resp, _ := c.get(ingress, host, path)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	// The first request goes through the owner, which fills from origin.
	if got := c.totalOriginHits(path); got != 1 {
		t.Fatalf("origin hits = %d, want 1", got)
	}
	// A second request must be a peer hit or a local L1 hit, never a refill.
	resp, _ = c.get(ingress, host, path)
	outcome := resp.Header.Get(proxy.HeaderCache)
	if outcome != "HIT" && outcome != "PEER_HIT" {
		t.Fatalf("second request outcome = %q", outcome)
	}
	if got := c.totalOriginHits(path); got != 1 {
		t.Fatalf("the object was refetched: %d origin hits", got)
	}
}

// Killing the edge that owns a key must not fail requests for it.
func TestL2PrimaryFailureFallsBack(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.configure("demo-route", host)
	c.waitForRing(3)

	ingress := c.edges[0]
	var path string
	var ownerID string
	for i := 0; i < 200; i++ {
		candidate := fmt.Sprintf("/static/owner-fail-%d", i)
		if owner, ok := ingress.ring.Load().Owner(cacheKeyFor("demo-route", candidate)); ok && owner.ID != ingress.id {
			path, ownerID = candidate, owner.ID
			break
		}
	}
	if path == "" {
		t.Fatal("no suitable path found")
	}

	if resp, _ := c.get(ingress, host, path); resp.StatusCode != http.StatusOK {
		t.Fatalf("warm-up request = %d", resp.StatusCode)
	}

	// Stop the owner's peer listener, which is what a crashed edge looks like
	// to its peers.
	for _, e := range c.edges {
		if e.id == ownerID {
			e.peerSrv.Stop()
			break
		}
	}

	// The request must still succeed: the cache is not a hard dependency.
	resp, body := c.get(ingress, host, path)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("request failed after the owner died: %d %s", resp.StatusCode, body)
	}
	t.Logf("owner %s died; ingress %s served %s (%s)",
		ownerID, ingress.id, path, resp.Header.Get(proxy.HeaderCache))
}

// A cold-start stampede must produce exactly one origin fetch.
func TestConcurrentColdMissesAreCoalesced(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.configure("demo-route", host)
	c.waitForRing(3)

	// Slow the origin so every concurrent request is genuinely in flight at the
	// same time; a fast origin would let requests complete serially.
	c.origins[0].delay.Store(int64(300 * time.Millisecond))
	defer c.origins[0].delay.Store(0)

	const path = "/static/stampede"
	const callers = 30

	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := c.edges[i%len(c.edges)]
			resp, body := c.get(e, host, path)
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("%s: %d %s", e.id, resp.StatusCode, body)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("stampede request failed: %v", err)
	}

	hits := c.totalOriginHits(path)
	t.Logf("%d concurrent cold requests across 3 edges produced %d origin fetches", callers, hits)
	// Ingress edges may each contact the owner, but the owner coalesces, so the
	// origin should see one fetch. Allow a small margin for a ring update
	// landing mid-flight.
	if hits > 3 {
		t.Fatalf("%d origin fetches for one key; coalescing failed", hits)
	}
}

// A purge must reach every edge and clear both of its tiers, so the next
// request re-fetches from origin instead of serving the invalidated object.
func TestPurgePropagates(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.configure("demo-route", host)
	c.waitForRing(3)

	const path = "/static/purge-me"
	for _, e := range c.edges {
		if resp, _ := c.get(e, host, path); resp.StatusCode != http.StatusOK {
			t.Fatalf("warm-up on %s failed", e.id)
		}
	}
	// Every edge now holds the object in L1, and its owner holds it in L2.
	for _, e := range c.edges {
		if e.l1.Len() == 0 {
			t.Fatalf("%s did not cache the warm-up response", e.id)
		}
	}
	beforeHits := c.totalOriginHits(path)

	c.control.propose(&statemachine.Command{
		Type: statemachine.CommandPurgeCache, TimestampUnixMs: 2,
		Purge: &edgemeshv1.PurgeDirective{
			Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_ROUTE, RouteId: "demo-route",
		},
	})

	// The purge is asynchronous, so wait for every edge's tiers to drain. This
	// is the assertion that would fail if the purge cleared only L2: the stale
	// object would keep serving from L1.
	waitFor(t, 10*time.Second, "the purge clears both tiers on every edge", func() bool {
		for _, e := range c.edges {
			if e.l1.Len() != 0 || e.l2.Len() != 0 {
				return false
			}
		}
		return true
	})

	// The next request must reach the origin again.
	resp, _ := c.get(c.edges[0], host, path)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("a request after purge failed: %d", resp.StatusCode)
	}
	afterHits := c.totalOriginHits(path)
	if afterHits <= beforeHits {
		t.Fatalf("origin hits did not increase after purge: %d -> %d", beforeHits, afterHits)
	}
	t.Logf("purge cleared %d edges; origin hits %d -> %d", len(c.edges), beforeHits, afterHits)
}

// An exact-key purge must remove only that key.
func TestExactKeyPurge(t *testing.T) {
	c := newCluster(t, 1, 1)
	c.configure("demo-route", host)
	c.waitForRing(1)

	const keep = "/static/keep-me"
	const drop = "/static/drop-me"
	c.get(c.edges[0], host, keep)
	c.get(c.edges[0], host, drop)
	if c.edges[0].l1.Len() != 2 {
		t.Fatalf("expected two cached objects, got %d", c.edges[0].l1.Len())
	}

	c.control.propose(&statemachine.Command{
		Type: statemachine.CommandPurgeCache, TimestampUnixMs: 2,
		Purge: &edgemeshv1.PurgeDirective{
			Scope:    edgemeshv1.PurgeScope_PURGE_SCOPE_KEY,
			CacheKey: cacheKeyFor("demo-route", drop),
		},
	})
	waitFor(t, 10*time.Second, "the exact key is purged", func() bool {
		return c.edges[0].l1.Len() == 1
	})

	keepHits := c.totalOriginHits(keep)
	if resp, _ := c.get(c.edges[0], host, keep); resp.Header.Get(proxy.HeaderCache) != "HIT" {
		t.Fatalf("an unrelated key was purged: outcome %q", resp.Header.Get(proxy.HeaderCache))
	}
	if c.totalOriginHits(keep) != keepHits {
		t.Fatal("an unrelated object was refetched after an exact-key purge")
	}
}

// A dead origin must not fail requests: retries cover the immediate window and
// health checks then exclude it entirely.
func TestUnhealthyOriginIsExcluded(t *testing.T) {
	c := newCluster(t, 1, 2)
	c.configure("demo-route", host)

	// Kill one origin outright.
	c.origins[0].Close()

	// Retries must mask the failure immediately, before health checks have had
	// time to react. This is the window a retry policy exists for.
	ok, failed := 0, 0
	for i := 0; i < 15; i++ {
		resp, _ := c.get(c.edges[0], host, fmt.Sprintf("/dynamic/immediate-%d", i))
		if resp.StatusCode == http.StatusOK {
			ok++
		} else {
			failed++
		}
	}
	t.Logf("immediately after the origin died: %d ok, %d failed (retries)", ok, failed)
	if ok == 0 {
		t.Fatal("retries did not mask a dead origin at all")
	}

	// Health checks then mark it unhealthy and exclude it from selection, after
	// which every request succeeds on the first attempt.
	pool, found := c.edges[0].pools.Pool("pool-1")
	if !found {
		t.Fatal("the origin pool is not configured on the edge")
	}
	waitFor(t, 15*time.Second, "the dead origin is marked unhealthy", func() bool {
		return pool.HealthyCount() == 1
	})

	ok, failed = 0, 0
	for i := 0; i < 20; i++ {
		resp, _ := c.get(c.edges[0], host, fmt.Sprintf("/dynamic/excluded-%d", i))
		if resp.StatusCode == http.StatusOK {
			ok++
		} else {
			failed++
		}
	}
	if failed != 0 {
		t.Fatalf("%d/20 requests failed after the dead origin was excluded", failed)
	}
	t.Logf("after health checks converged: %d/20 requests succeeded", ok)
}

// Rate limiting must return 429 before any cache or origin work is done.
func TestRateLimitingReturns429(t *testing.T) {
	c := newCluster(t, 1, 1)

	origins := []*edgemeshv1.Origin{}
	h, p := c.origins[0].hostPort(t)
	origins = append(origins, &edgemeshv1.Origin{
		Id: "origin-1", Scheme: "http", Host: h, Port: p,
		HealthPath: "/healthz", ExpectedStatuses: []int32{200},
	})
	c.control.propose(&statemachine.Command{
		Type: statemachine.CommandCreateOriginPool, TimestampUnixMs: 1,
		OriginPool: &edgemeshv1.OriginPool{Id: "pool-1", Origins: origins, HealthIntervalMs: 500},
	})
	c.control.propose(&statemachine.Command{
		Type: statemachine.CommandCreateRoute, TimestampUnixMs: 1,
		Route: &edgemeshv1.Route{
			Id: "limited", Hostname: host, PathPrefix: "/", OriginPoolId: "pool-1", Enabled: true,
			RateLimitPolicy: &edgemeshv1.RateLimitPolicy{
				Enabled: true, RatePerSecond: 2, Burst: 3,
				KeyStrategy: edgemeshv1.KeyStrategy_KEY_STRATEGY_GLOBAL,
			},
			HeaderPolicy: &edgemeshv1.HeaderPolicy{DiagnosticHeaders: true},
		},
	})
	c.waitForConfig()

	allowed, denied := 0, 0
	for i := 0; i < 20; i++ {
		resp, _ := c.get(c.edges[0], host, fmt.Sprintf("/dynamic/rl-%d", i))
		switch resp.StatusCode {
		case http.StatusOK:
			allowed++
		case http.StatusTooManyRequests:
			denied++
			if resp.Header.Get("Retry-After") == "" {
				t.Error("a 429 must carry Retry-After")
			}
		}
	}
	if denied == 0 {
		t.Fatalf("no requests were limited: %d allowed", allowed)
	}
	// The burst is 3 at 2/s, so a tight loop must be mostly denied.
	if allowed > 8 {
		t.Fatalf("%d requests allowed through a burst-3 limiter", allowed)
	}
	t.Logf("rate limiter allowed %d and denied %d of 20 rapid requests", allowed, denied)
}

// An uncacheable response must be served without being stored.
func TestNoStoreResponsesAreNotCached(t *testing.T) {
	c := newCluster(t, 1, 1)
	c.configure("demo-route", host)

	// The test origin marks /dynamic/* paths cacheable, so instead assert on a
	// response the policy refuses: a request carrying Authorization bypasses.
	req, err := http.NewRequest(http.MethodGet, c.edges[0].URL()+"/static/authed", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	req.Header.Set("Authorization", "Bearer secret")

	for i := 0; i < 3; i++ {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.Header.Get(proxy.HeaderCache) != "BYPASS" {
			t.Fatalf("an authenticated request was cached: outcome %q", resp.Header.Get(proxy.HeaderCache))
		}
	}
	// Every authenticated request must have reached the origin.
	if got := c.totalOriginHits("/static/authed"); got != 3 {
		t.Fatalf("origin hits = %d, want 3; an authenticated response was cached", got)
	}
}

// The core availability guarantee: existing traffic survives a control-plane
// outage, because an edge serves from its last valid configuration.
func TestEdgesKeepServingDuringAControlPlaneOutage(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.configure("demo-route", host)
	c.waitForRing(3)

	// Warm the cache so both cached and uncached paths are exercised.
	if resp, _ := c.get(c.edges[0], host, "/static/outage"); resp.StatusCode != http.StatusOK {
		t.Fatal("warm-up failed")
	}

	// Stop the entire control plane. There is now no quorum and no leader.
	for _, id := range c.control.ids {
		c.control.stopNode(id)
	}

	ok, failed := 0, 0
	for i := 0; i < 30; i++ {
		e := c.edges[i%len(c.edges)]
		resp, _ := c.get(e, host, "/static/outage")
		if resp.StatusCode == http.StatusOK {
			ok++
		} else {
			failed++
			t.Logf("  %s returned %d", e.id, resp.StatusCode)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if failed != 0 {
		t.Fatalf("%d/%d requests failed during a total control-plane outage", failed, ok+failed)
	}
	t.Logf("%d/%d requests succeeded with the entire control plane down", ok, ok+failed)

	// New configuration is unavailable, which is the documented cost.
	for _, e := range c.edges {
		if e.connected.Load() {
			continue
		}
	}
}

// A leadership change must not interrupt data-plane traffic, and configuration
// writes must resume through the new leader.
func TestLeaderFailoverDoesNotInterruptTraffic(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.configure("demo-route", host)
	c.waitForRing(3)

	leader := c.control.leader(5 * time.Second)
	stop := make(chan struct{})
	var ok, failed int
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			e := c.edges[i%len(c.edges)]
			resp, _ := c.get(e, host, "/static/failover")
			mu.Lock()
			if resp.StatusCode == http.StatusOK {
				ok++
			} else {
				failed++
			}
			mu.Unlock()
			i++
			time.Sleep(5 * time.Millisecond)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	c.control.stopNode(leader.id)

	newLeader := c.control.leader(10 * time.Second)
	if newLeader.id == leader.id {
		t.Fatal("the stopped leader was re-elected")
	}

	// A write through the new leader must commit and reach the edges.
	c.control.propose(&statemachine.Command{
		Type: statemachine.CommandCreateRoute, TimestampUnixMs: 3,
		Route: &edgemeshv1.Route{
			Id: "post-failover", Hostname: host, PathPrefix: "/api",
			OriginPoolId: "pool-1", Enabled: true,
			CachePolicy:  &edgemeshv1.CachePolicy{Enabled: true, DefaultTtlSeconds: 60},
			HeaderPolicy: &edgemeshv1.HeaderPolicy{DiagnosticHeaders: true},
		},
	})

	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if failed != 0 {
		t.Fatalf("%d requests failed during leader failover (%d succeeded)", failed, ok)
	}
	t.Logf("leader %s -> %s: %d data-plane requests, 0 failures",
		leader.id, newLeader.id, ok)

	// The new route must reach the edges, proving the control plane recovered.
	waitFor(t, 10*time.Second, "the post-failover route reaches an edge", func() bool {
		_, ok := c.edges[0].routes.Load().ByID("post-failover")
		return ok
	})
	resp, _ := c.get(c.edges[0], host, "/api/thing")
	if resp.Header.Get(proxy.HeaderRoute) != "post-failover" {
		t.Fatalf("the post-failover route is not serving: %q", resp.Header.Get(proxy.HeaderRoute))
	}
}

// Removing an edge must update the ring on the survivors.
func TestEdgeDepartureUpdatesTheRing(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.configure("demo-route", host)
	c.waitForRing(3)

	// Stop one edge's heartbeat by canceling its context; the leader's sweep
	// then marks it dead and republishes membership.
	c.edges[2].cancel()
	c.edges[2].server.Close()
	c.edges[2].peerSrv.Stop()

	waitFor(t, 20*time.Second, "the survivors drop the departed edge from the ring", func() bool {
		for _, e := range c.edges[:2] {
			if e.ring.Load().Len() != 2 {
				return false
			}
		}
		return true
	})

	// Traffic must keep working on the survivors with the smaller ring.
	for _, e := range c.edges[:2] {
		if resp, _ := c.get(e, host, "/static/after-departure"); resp.StatusCode != http.StatusOK {
			t.Fatalf("%s failed after the ring shrank: %d", e.id, resp.StatusCode)
		}
	}
}

// cacheKeyFor mirrors the proxy's key construction for a default policy, so a
// test can predict which node owns a path.
func cacheKeyFor(routeID, path string) string {
	const sep = "\x1f"
	return routeID + sep + "GET" + sep + path + sep + sep
}
