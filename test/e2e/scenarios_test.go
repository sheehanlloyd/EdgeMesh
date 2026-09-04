//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

// The end-to-end path: real processes, real sockets, cache working across the
// cluster.
func TestClusterServesTrafficAndCaches(t *testing.T) {
	c := newCluster(t)
	c.bootstrap(3, 3, 2)
	c.configure(2)

	const counter = "e2e-cache"
	path := "/counter/" + counter

	before := c.originHits(counter)
	resp, _, err := c.get(c.edgeURLs[0], path)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first request = %d", resp.StatusCode)
	}
	if outcome := resp.Header.Get("X-Edgemesh-Cache"); outcome != "MISS" {
		t.Fatalf("first request outcome = %q, want MISS", outcome)
	}
	afterFirst := c.originHits(counter)
	if afterFirst != before+1 {
		t.Fatalf("origin hits %d -> %d; a cold request must reach the origin exactly once",
			before, afterFirst)
	}

	// Every subsequent request across the whole cluster is served from cache.
	outcomes := map[string]int{}
	for round := 0; round < 3; round++ {
		for _, u := range c.edgeURLs {
			resp, _, err := c.get(u, path)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s returned %d", u, resp.StatusCode)
			}
			outcomes[resp.Header.Get("X-Edgemesh-Cache")]++
		}
	}
	if got := c.originHits(counter); got != afterFirst {
		t.Fatalf("origin hits rose to %d during 9 cached requests", got)
	}
	t.Logf("9 cluster-wide requests after the fill: %v, origin fetches: 1", outcomes)
}

// SIGTERM must drain in-flight requests rather than dropping them. This is the
// scenario the in-process harness structurally cannot test.
func TestGracefulShutdownDrainsInFlightRequests(t *testing.T) {
	c := newCluster(t)
	c.bootstrap(3, 3, 1)
	c.configure(1)

	// A fixed set of slow requests is launched, then SIGTERM is sent while they
	// are known to be in flight. Only these requests are measured: counting
	// requests started *after* the process is gone would measure the test's own
	// behaviour rather than the drain.
	const inFlight = 8
	results := make(chan error, inFlight)
	var wg sync.WaitGroup

	for i := 0; i < inFlight; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, _, err := c.get(c.edgeURLs[0], fmt.Sprintf("/delay/1500?n=%d", i))
			switch {
			case err != nil:
				results <- fmt.Errorf("request %d failed: %w", i, err)
			case resp.StatusCode != http.StatusOK:
				results <- fmt.Errorf("request %d got status %d", i, resp.StatusCode)
			default:
				results <- nil
			}
		}(i)
	}

	// Long enough for every request to have reached the origin and be waiting.
	time.Sleep(600 * time.Millisecond)

	start := time.Now()
	if err := c.terminate("edge-1", 15*time.Second); err != nil {
		t.Fatalf("edge-1 did not shut down gracefully: %v", err)
	}
	shutdownTime := time.Since(start)

	wg.Wait()
	close(results)

	completed, failed := 0, 0
	var firstErr error
	for err := range results {
		if err == nil {
			completed++
			continue
		}
		failed++
		if firstErr == nil {
			firstErr = err
		}
	}

	t.Logf("SIGTERM during %d in-flight requests: %d completed, %d failed; shutdown took %s",
		inFlight, completed, failed, shutdownTime.Round(time.Millisecond))

	// The whole point of the drain: work already accepted is finished, not
	// dropped.
	if failed != 0 {
		t.Fatalf("%d/%d in-flight requests were dropped by a graceful shutdown; first error: %v",
			failed, inFlight, firstErr)
	}
	// The drain must have actually waited for them. An instant shutdown would
	// mean the requests were dropped, not drained.
	if shutdownTime < 500*time.Millisecond {
		t.Fatalf("shutdown completed in %s, too fast to have drained 1.5s requests", shutdownTime)
	}
	// And it must not hang: the configured drain timeout is 5s.
	if shutdownTime > 10*time.Second {
		t.Fatalf("graceful shutdown took %s, which suggests it hung", shutdownTime)
	}
}

// An edge must recover its registration after a leadership change.
//
// This is a regression test for a self-perpetuating reconnect loop: a
// re-register signal raised while no config stream was running stayed buffered,
// so the next stream canceled itself before it could register, which made the
// new leader ask for re-registration again. The data plane kept working
// throughout, which is exactly what made the bug easy to miss: the edge was
// simply invisible to the control plane forever.
func TestEdgeReRegistersAfterLeadershipChange(t *testing.T) {
	c := newCluster(t)
	c.bootstrap(3, 3, 1)
	c.configure(1)

	// Edge membership is leader-only state, so the leader is the only node that
	// can answer this.
	c.waitFor(20*time.Second, "every edge registers with the original leader", func() bool {
		s, ok := c.leaderStatus()
		return ok && s.EdgeNodes == 3
	})

	leader := c.waitForLeader(20 * time.Second)
	c.kill(leader)
	c.waitFor(30*time.Second, "a new leader", func() bool {
		s, ok := c.anyStatus()
		return ok && s.LeaderID != "" && s.LeaderID != leader
	})

	// The new leader starts with an empty roster; every edge must reappear.
	c.waitFor(30*time.Second, "every edge re-registers with the new leader", func() bool {
		s, ok := c.leaderStatus()
		return ok && s.EdgeNodes == 3
	})

	after, _ := c.leaderStatus()
	t.Logf("after leadership moved to %s, %d edges re-registered", after.LeaderID, after.EdgeNodes)
}

// Committed configuration must survive killing a control node and restarting
// it, recovered from durable state on disk.
func TestCommittedConfigSurvivesRestart(t *testing.T) {
	c := newCluster(t)
	c.bootstrap(3, 1, 1)
	c.configure(1)

	// The leader has certainly applied the configuration that was just
	// committed; a follower may still be a moment behind.
	var before status
	c.waitFor(20*time.Second, "the leader reports the configured route", func() bool {
		s, ok := c.leaderStatus()
		if ok && s.Routes > 0 {
			before = s
			return true
		}
		return false
	})

	// Kill every control node abruptly, then bring them all back.
	for i := 1; i <= 3; i++ {
		c.kill(fmt.Sprintf("cp-%d", i))
	}
	for i := 1; i <= 3; i++ {
		c.start(fmt.Sprintf("cp-%d", i), "--config",
			fmt.Sprintf("%s/control-%d.yaml", c.dataDir, i))
	}
	c.waitForLeader(30 * time.Second)

	c.waitFor(20*time.Second, "configuration is restored from disk", func() bool {
		s, ok := c.leaderStatus()
		return ok && s.Routes == before.Routes && s.ConfigVersion >= before.ConfigVersion
	})

	after, _ := c.leaderStatus()
	t.Logf("across a full control-plane restart: routes %d -> %d, config version %d -> %d",
		before.Routes, after.Routes, before.ConfigVersion, after.ConfigVersion)

	// The data plane must work again once the control plane is back.
	resp, _, err := c.get(c.edgeURLs[0], "/static/after-restart")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("request after restart failed: %v status=%v", err, resp)
	}
}

// Killing the leader must not interrupt traffic, and the cluster must remain
// writable afterwards.
func TestLeaderCrashDoesNotInterruptTraffic(t *testing.T) {
	c := newCluster(t)
	c.bootstrap(3, 3, 2)
	c.configure(2)

	leader := c.waitForLeader(20 * time.Second)
	before, _ := c.anyStatus()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok, failed int
	var codes []int

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
			resp, _, err := c.get(c.edgeURLs[i%len(c.edgeURLs)], "/static/during-failover")
			mu.Lock()
			if err == nil && resp.StatusCode == http.StatusOK {
				ok++
			} else {
				failed++
				if resp != nil {
					codes = append(codes, resp.StatusCode)
				}
			}
			mu.Unlock()
			i++
			time.Sleep(10 * time.Millisecond)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	c.kill(leader)

	// Wait for a new leader while traffic continues.
	c.waitFor(30*time.Second, "a new leader", func() bool {
		s, ok := c.anyStatus()
		return ok && s.LeaderID != "" && s.LeaderID != leader
	})
	after, _ := c.anyStatus()

	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	t.Logf("leader %s (term %d) -> %s (term %d): %d requests, %d failures %v",
		leader, before.Term, after.LeaderID, after.Term, ok, failed, codes)

	if failed != 0 {
		t.Fatalf("%d data-plane requests failed during a control-plane election", failed)
	}
	if after.Term <= before.Term {
		t.Fatalf("term did not advance: %d -> %d", before.Term, after.Term)
	}

	// The cluster must accept configuration writes through the new leader.
	c.writeFile("route2.yaml", `id: post-crash-route
hostname: demo.edgemesh.local
path_prefix: /static
origin_pool_id: e2e-pool
enabled: true
cache_policy: {enabled: true, default_ttl_seconds: 60}
header_policy: {diagnostic_headers: true}
`)
	if out, err := c.ctl("routes", "apply", "-f", c.dataDir+"/route2.yaml"); err != nil {
		t.Fatalf("write through the new leader failed: %v\n%s", err, out)
	}
	c.waitFor(20*time.Second, "the new route reaches every edge", func() bool {
		for _, u := range c.edgeURLs {
			resp, _, err := c.get(u, "/static/post-crash")
			if err != nil || resp.Header.Get("X-Edgemesh-Route") != "post-crash-route" {
				return false
			}
		}
		return true
	})
}

// The whole control plane down must leave the data plane serving.
func TestDataPlaneSurvivesTotalControlPlaneOutage(t *testing.T) {
	c := newCluster(t)
	c.bootstrap(3, 3, 1)
	c.configure(1)

	// Warm the cache so both cached and uncached paths are exercised.
	if _, _, err := c.get(c.edgeURLs[0], "/static/outage"); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		c.kill(fmt.Sprintf("cp-%d", i))
	}

	ok, failed := 0, 0
	for i := 0; i < 40; i++ {
		resp, _, err := c.get(c.edgeURLs[i%len(c.edgeURLs)], "/static/outage")
		if err == nil && resp.StatusCode == http.StatusOK {
			ok++
		} else {
			failed++
			t.Logf("  request %d failed: %v", i, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("with the entire control plane down: %d/%d requests succeeded", ok, ok+failed)
	if failed != 0 {
		t.Fatalf("%d requests failed with the control plane down; the failure domains are not separate", failed)
	}

	// Edges must still report ready: they can serve everything they could
	// before, and marking them unready would turn a control-plane outage into a
	// data-plane one.
	for i := 1; i <= 3; i++ {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", basePortTelem+10+i))
		if err != nil {
			t.Fatalf("edge-%d readiness unreachable: %v", i, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("edge-%d reported not ready (%d) during a control-plane outage", i, resp.StatusCode)
		}
	}
}

// Killing the edge that owns a key must not fail requests for it.
func TestEdgeCrashFallsBackWithoutFailingRequests(t *testing.T) {
	c := newCluster(t)
	c.bootstrap(3, 3, 1)
	c.configure(1)

	// Warm several keys so at least some are owned by the node about to die.
	for i := 0; i < 20; i++ {
		if _, _, err := c.get(c.edgeURLs[0], fmt.Sprintf("/static/owned-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	c.kill("edge-3")

	ok, failed := 0, 0
	for i := 0; i < 40; i++ {
		resp, _, err := c.get(c.edgeURLs[i%2], fmt.Sprintf("/static/owned-%d", i%20))
		if err == nil && resp.StatusCode == http.StatusOK {
			ok++
		} else {
			failed++
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("after an edge crash: %d/%d requests succeeded on the survivors", ok, ok+failed)
	if failed != 0 {
		t.Fatalf("%d requests failed after one edge crashed; the cache must not be a hard dependency", failed)
	}
}

// The metrics endpoint must expose the series the dashboards query.
func TestMetricsAreExposed(t *testing.T) {
	c := newCluster(t)
	c.bootstrap(3, 1, 1)
	c.configure(1)

	for i := 0; i < 5; i++ {
		if _, _, err := c.get(c.edgeURLs[0], fmt.Sprintf("/static/metrics-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", basePortTelem+11))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck
	body := make([]byte, 512*1024)
	n, _ := resp.Body.Read(body)
	text := string(body[:n])

	for _, metric := range []string{
		"edgemesh_http_requests_total",
		"edgemesh_http_request_duration_seconds",
		"edgemesh_cache_requests_total",
		"edgemesh_origin_requests_total",
		"edgemesh_config_version_applied",
		"edgemesh_control_plane_connected",
		"edgemesh_ring_nodes",
		"go_goroutines",
	} {
		if !contains(text, metric) {
			t.Errorf("metric %q is not exposed", metric)
		}
	}

	// Control-plane metrics live on the control node's listener.
	cresp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", basePortTelem+1))
	if err != nil {
		t.Fatal(err)
	}
	defer cresp.Body.Close() //nolint:errcheck
	cn, _ := cresp.Body.Read(body)
	ctext := string(body[:cn])
	for _, metric := range []string{"edgemesh_raft_term", "edgemesh_raft_commit_index", "edgemesh_config_version"} {
		if !contains(ctext, metric) {
			t.Errorf("control metric %q is not exposed", metric)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
