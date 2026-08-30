package origin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/config"
)

func devGuard(t *testing.T) *Guard {
	t.Helper()
	g, err := NewGuard(config.OriginSecurity{AllowLoopback: true, AllowPrivateNetworks: true})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func poolMsg(origins ...*edgemeshv1.Origin) *edgemeshv1.OriginPool {
	return &edgemeshv1.OriginPool{
		Id:                 "pool-1",
		Origins:            origins,
		LoadBalancing:      edgemeshv1.LoadBalancing_LOAD_BALANCING_ROUND_ROBIN,
		HealthIntervalMs:   1000,
		HealthTimeoutMs:    500,
		UnhealthyThreshold: 2,
		HealthyThreshold:   1,
	}
}

func originMsg(id, host string, port uint32) *edgemeshv1.Origin {
	return &edgemeshv1.Origin{
		Id: id, Scheme: "http", Host: host, Port: port, Weight: 1,
		HealthPath: "/healthz", ExpectedStatuses: []int32{200},
	}
}

func TestCheckScheme(t *testing.T) {
	for _, s := range []string{"http", "https", "HTTP", "HTTPS"} {
		if err := CheckScheme(s); err != nil {
			t.Errorf("scheme %q rejected: %v", s, err)
		}
	}
	for _, s := range []string{"file", "unix", "ftp", "gopher", "", "javascript"} {
		if err := CheckScheme(s); err == nil {
			t.Errorf("scheme %q must be rejected", s)
		}
	}
}

func TestGuardDeniesInstanceMetadataEvenWhenPrivateIsAllowed(t *testing.T) {
	g, err := NewGuard(config.OriginSecurity{AllowLoopback: true, AllowPrivateNetworks: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"169.254.169.254", "169.254.170.2"} {
		if err := g.CheckAddr(netip.MustParseAddr(s)); err == nil {
			t.Errorf("instance metadata address %s must always be denied", s)
		}
	}
}

func TestGuardCategoryRules(t *testing.T) {
	strict, err := NewGuard(config.OriginSecurity{})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"127.0.0.1", "::1", "10.0.0.5", "192.168.1.1", "172.16.0.1", "fd00::1"} {
		if err := strict.CheckAddr(netip.MustParseAddr(s)); err == nil {
			t.Errorf("strict guard must deny %s", s)
		}
	}
	if err := strict.CheckAddr(netip.MustParseAddr("93.184.216.34")); err != nil {
		t.Errorf("public address rejected: %v", err)
	}
	// Link-local and multicast are denied regardless of configuration.
	permissive := devGuard(t)
	for _, s := range []string{"169.254.1.1", "fe80::1", "224.0.0.1", "0.0.0.0"} {
		if err := permissive.CheckAddr(netip.MustParseAddr(s)); err == nil {
			t.Errorf("permissive guard must still deny %s", s)
		}
	}
	// The dev guard permits what the demo needs.
	for _, s := range []string{"127.0.0.1", "::1", "172.20.0.3"} {
		if err := permissive.CheckAddr(netip.MustParseAddr(s)); err != nil {
			t.Errorf("dev guard rejected %s: %v", s, err)
		}
	}
}

func TestGuardExplicitAllowlistOverridesCategory(t *testing.T) {
	g, err := NewGuard(config.OriginSecurity{AllowedCIDRs: []string{"10.1.0.0/16"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CheckAddr(netip.MustParseAddr("10.1.2.3")); err != nil {
		t.Fatalf("allowlisted address rejected: %v", err)
	}
	// A private address outside the allowlist stays denied.
	if err := g.CheckAddr(netip.MustParseAddr("10.2.2.3")); err == nil {
		t.Fatal("address outside the allowlist must be denied")
	}
}

func TestGuardDenyWinsOverAllow(t *testing.T) {
	g, err := NewGuard(config.OriginSecurity{
		AllowedCIDRs: []string{"10.0.0.0/8"},
		DeniedCIDRs:  []string{"10.0.5.0/24"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CheckAddr(netip.MustParseAddr("10.0.5.1")); err == nil {
		t.Fatal("deny must take precedence over allow")
	}
	if err := g.CheckAddr(netip.MustParseAddr("10.0.6.1")); err != nil {
		t.Fatalf("allowed address rejected: %v", err)
	}
}

func TestGuardRejectsInvalidCIDR(t *testing.T) {
	if _, err := NewGuard(config.OriginSecurity{AllowedCIDRs: []string{"not-a-cidr"}}); err == nil {
		t.Fatal("invalid CIDR must be rejected at construction")
	}
}

func TestValidatePool(t *testing.T) {
	cases := []struct {
		name string
		p    *edgemeshv1.OriginPool
		ok   bool
	}{
		{"valid", poolMsg(originMsg("o1", "127.0.0.1", 8080)), true},
		{"nil", nil, false},
		{"no id", &edgemeshv1.OriginPool{Origins: []*edgemeshv1.Origin{originMsg("o1", "h", 80)}}, false},
		{"no origins", poolMsg(), false},
		{"duplicate origin id", poolMsg(originMsg("o1", "a", 80), originMsg("o1", "b", 80)), false},
		{"bad scheme", poolMsg(&edgemeshv1.Origin{Id: "o1", Scheme: "file", Host: "h", Port: 80}), false},
		{"no host", poolMsg(originMsg("o1", "", 80)), false},
		{"port zero", poolMsg(originMsg("o1", "h", 0)), false},
		{"port too high", poolMsg(originMsg("o1", "h", 70000)), false},
		{"relative health path", poolMsg(&edgemeshv1.Origin{
			Id: "o1", Scheme: "http", Host: "h", Port: 80, HealthPath: "healthz"}), false},
		{"bad expected status", poolMsg(&edgemeshv1.Origin{
			Id: "o1", Scheme: "http", Host: "h", Port: 80, ExpectedStatuses: []int32{7}}), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidatePool(c.p)
			if c.ok != (err == nil) {
				t.Fatalf("ValidatePool err = %v, want ok=%v", err, c.ok)
			}
		})
	}
}

func TestBuildPoolAppliesDefaults(t *testing.T) {
	p, err := BuildPool(context.Background(), &edgemeshv1.OriginPool{
		Id:      "pool-1",
		Origins: []*edgemeshv1.Origin{{Id: "o1", Scheme: "http", Host: "127.0.0.1", Port: 8080}},
	}, nil, devGuard(t))
	if err != nil {
		t.Fatal(err)
	}
	e := p.Endpoints[0]
	if e.Weight != 1 || e.HealthPath != "/healthz" || len(e.ExpectedStatuses) != 1 || e.ExpectedStatuses[0] != 200 {
		t.Fatalf("defaults not applied: %+v", e)
	}
	if e.BaseURL() != "http://127.0.0.1:8080" || e.Addr() != "127.0.0.1:8080" {
		t.Fatalf("addresses not precomputed: base=%q addr=%q", e.BaseURL(), e.Addr())
	}
	if p.HealthInterval != 5*time.Second || p.UnhealthyThreshold != 3 || p.HealthyThreshold != 2 {
		t.Fatalf("pool defaults not applied: %+v", p)
	}
	if e.Health() != HealthUnknown {
		t.Fatalf("initial health = %q, want unknown", e.Health())
	}
	if !e.Healthy() {
		t.Fatal("an unchecked endpoint must be eligible for traffic")
	}
}

func TestBuildPoolPreservesHealthAcrossConfigChange(t *testing.T) {
	msg := poolMsg(originMsg("o1", "127.0.0.1", 8080))
	first, err := BuildPool(context.Background(), msg, nil, devGuard(t))
	if err != nil {
		t.Fatal(err)
	}
	first.Endpoints[0].health.Store(HealthUnhealthy)

	// A change that does not alter the endpoint's identity keeps its health.
	msg.HealthIntervalMs = 2000
	second, err := BuildPool(context.Background(), msg, first, devGuard(t))
	if err != nil {
		t.Fatal(err)
	}
	if second.Endpoints[0].Health() != HealthUnhealthy {
		t.Fatal("health state was discarded by an unrelated config change")
	}

	// Changing the address makes it a different server; history must reset.
	msg.Origins[0].Port = 9090
	third, err := BuildPool(context.Background(), msg, second, devGuard(t))
	if err != nil {
		t.Fatal(err)
	}
	if third.Endpoints[0].Health() != HealthUnknown {
		t.Fatal("health state must reset when the endpoint address changes")
	}
}

func TestBuildPoolRejectsDeniedOrigin(t *testing.T) {
	strict, err := NewGuard(config.OriginSecurity{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildPool(context.Background(), poolMsg(originMsg("o1", "127.0.0.1", 8080)), nil, strict); err == nil {
		t.Fatal("a guarded loopback origin must be rejected at build time")
	}
}

func TestRoundRobinDistributesAndSkipsUnhealthy(t *testing.T) {
	p, err := BuildPool(context.Background(), poolMsg(
		originMsg("o1", "127.0.0.1", 8001),
		originMsg("o2", "127.0.0.1", 8002),
		originMsg("o3", "127.0.0.1", 8003),
	), nil, devGuard(t))
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for i := 0; i < 300; i++ {
		e, err := p.Select(nil)
		if err != nil {
			t.Fatal(err)
		}
		counts[e.ID]++
	}
	for _, id := range []string{"o1", "o2", "o3"} {
		if counts[id] != 100 {
			t.Fatalf("round robin uneven: %v", counts)
		}
	}

	p.Endpoints[1].health.Store(HealthUnhealthy)
	counts = map[string]int{}
	for i := 0; i < 100; i++ {
		e, err := p.Select(nil)
		if err != nil {
			t.Fatal(err)
		}
		counts[e.ID]++
	}
	if counts["o2"] != 0 {
		t.Fatalf("unhealthy origin received traffic: %v", counts)
	}
	if counts["o1"]+counts["o3"] != 100 {
		t.Fatalf("healthy origins did not absorb the traffic: %v", counts)
	}
}

func TestSelectHonoursExclusions(t *testing.T) {
	p, err := BuildPool(context.Background(), poolMsg(
		originMsg("o1", "127.0.0.1", 8001),
		originMsg("o2", "127.0.0.1", 8002),
	), nil, devGuard(t))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		e, err := p.Select(map[string]bool{"o1": true})
		if err != nil {
			t.Fatal(err)
		}
		if e.ID != "o2" {
			t.Fatalf("excluded origin was selected: %s", e.ID)
		}
	}
	// Excluding everything fails fast rather than returning a bad origin.
	if _, err := p.Select(map[string]bool{"o1": true, "o2": true}); err == nil {
		t.Fatal("expected ErrNoHealthyOrigin when every origin is excluded")
	}
}

// The documented V1 policy: fail fast with no healthy origin rather than
// sending traffic to a known-broken server.
func TestSelectFailsFastWhenAllUnhealthy(t *testing.T) {
	p, err := BuildPool(context.Background(), poolMsg(
		originMsg("o1", "127.0.0.1", 8001),
		originMsg("o2", "127.0.0.1", 8002),
	), nil, devGuard(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range p.Endpoints {
		e.health.Store(HealthUnhealthy)
	}
	if _, err := p.Select(nil); err == nil {
		t.Fatal("expected a fast failure when every origin is unhealthy")
	}
	if p.HealthyCount() != 0 {
		t.Fatal("HealthyCount disagrees with endpoint state")
	}
}

func TestWeightedRoundRobin(t *testing.T) {
	msg := poolMsg(
		&edgemeshv1.Origin{Id: "heavy", Scheme: "http", Host: "127.0.0.1", Port: 8001, Weight: 3},
		&edgemeshv1.Origin{Id: "light", Scheme: "http", Host: "127.0.0.1", Port: 8002, Weight: 1},
	)
	msg.LoadBalancing = edgemeshv1.LoadBalancing_LOAD_BALANCING_WEIGHTED_ROUND_ROBIN
	p, err := BuildPool(context.Background(), msg, nil, devGuard(t))
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for i := 0; i < 400; i++ {
		e, err := p.Select(nil)
		if err != nil {
			t.Fatal(err)
		}
		counts[e.ID]++
	}
	if counts["heavy"] != 300 || counts["light"] != 100 {
		t.Fatalf("weights not honoured: %v", counts)
	}
}

func TestSelectOnEmptyPool(t *testing.T) {
	var p *Pool
	if _, err := p.Select(nil); err == nil {
		t.Fatal("nil pool must fail")
	}
	if p.HealthyCount() != 0 {
		t.Fatal("nil pool must report zero healthy")
	}
	if _, ok := p.Endpoint("x"); ok {
		t.Fatal("nil pool must not resolve an endpoint")
	}
}

func TestHealthCheckerThresholds(t *testing.T) {
	var fail atomic.Bool
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	host, portStr, _ := splitHostPort(t, srv.URL)
	port, _ := strconv.Atoi(portStr)

	msg := poolMsg(originMsg("o1", host, uint32(port)))
	msg.UnhealthyThreshold = 3
	msg.HealthyThreshold = 2
	p, err := BuildPool(context.Background(), msg, nil, devGuard(t))
	if err != nil {
		t.Fatal(err)
	}

	clk := clock.NewMock(time.Time{})
	var transitions []Health
	c := NewChecker(CheckerOptions{Clock: clk, Client: srv.Client()})
	c.OnTransition = func(_ string, _ *Endpoint, _, to Health) { transitions = append(transitions, to) }
	c.SetPools(map[string]*Pool{p.ID: p})

	ctx := context.Background()
	check := func(n int) {
		for i := 0; i < n; i++ {
			c.CheckDue(ctx)
			clk.Advance(p.HealthInterval)
		}
	}

	check(2)
	if p.Endpoints[0].Health() != HealthHealthy {
		t.Fatalf("health = %q after 2 successes with threshold 2", p.Endpoints[0].Health())
	}

	fail.Store(true)
	check(2)
	if p.Endpoints[0].Health() != HealthHealthy {
		t.Fatal("endpoint went unhealthy before its threshold")
	}
	check(1)
	if p.Endpoints[0].Health() != HealthUnhealthy {
		t.Fatalf("health = %q after 3 failures with threshold 3", p.Endpoints[0].Health())
	}

	fail.Store(false)
	check(2)
	if p.Endpoints[0].Health() != HealthHealthy {
		t.Fatal("endpoint did not recover")
	}
	if len(transitions) < 3 {
		t.Fatalf("expected healthy->unhealthy->healthy transitions, got %v", transitions)
	}

	stats := c.Stats()
	if len(stats) != 1 || stats[0].Checks == 0 || stats[0].Failures == 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestHealthCheckerRespectsInterval(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	host, portStr, _ := splitHostPort(t, srv.URL)
	port, _ := strconv.Atoi(portStr)
	p, err := BuildPool(context.Background(), poolMsg(originMsg("o1", host, uint32(port))), nil, devGuard(t))
	if err != nil {
		t.Fatal(err)
	}

	clk := clock.NewMock(time.Time{})
	c := NewChecker(CheckerOptions{Clock: clk, Client: srv.Client()})
	c.SetPools(map[string]*Pool{p.ID: p})

	c.CheckDue(context.Background())
	first := hits.Load()
	// Calling again before the interval elapses must not re-probe.
	c.CheckDue(context.Background())
	if hits.Load() != first {
		t.Fatalf("checker probed twice inside one interval: %d -> %d", first, hits.Load())
	}
	clk.Advance(p.HealthInterval)
	c.CheckDue(context.Background())
	if hits.Load() != first+1 {
		t.Fatalf("checker did not probe after the interval elapsed: %d", hits.Load())
	}
}

func TestHealthCheckerUnexpectedStatusIsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	host, portStr, _ := splitHostPort(t, srv.URL)
	port, _ := strconv.Atoi(portStr)

	msg := poolMsg(originMsg("o1", host, uint32(port))) // expects 200
	msg.UnhealthyThreshold = 1
	p, err := BuildPool(context.Background(), msg, nil, devGuard(t))
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewMock(time.Time{})
	c := NewChecker(CheckerOptions{Clock: clk, Client: srv.Client()})
	c.SetPools(map[string]*Pool{p.ID: p})
	c.CheckDue(context.Background())

	if p.Endpoints[0].Health() != HealthUnhealthy {
		t.Fatalf("204 must fail a check expecting 200, health = %q", p.Endpoints[0].Health())
	}
}

func TestCheckerStartStopHasNoLeak(t *testing.T) {
	c := NewChecker(CheckerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx, 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	cancel()
	c.Stop() // must return; a hang here is the leak this guards
}

func splitHostPort(t *testing.T, rawURL string) (string, string, error) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return u.Hostname(), u.Port(), nil
}
