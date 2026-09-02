package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/breaker"
	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/origin"
	"github.com/sheehanlloyd/edgemesh/internal/peer"
	"github.com/sheehanlloyd/edgemesh/internal/ratelimit"
	"github.com/sheehanlloyd/edgemesh/internal/routing"
)

// stubCache returns a canned object, recording what it was asked for.
type stubCache struct {
	mu       sync.Mutex
	requests []peer.FetchRequest
	obj      *cache.Object
	outcome  cache.Outcome
	err      error
}

func (s *stubCache) Get(_ context.Context, req peer.FetchRequest) (*cache.Object, cache.Outcome, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req)
	s.mu.Unlock()
	return s.obj, s.outcome, s.err
}

func (s *stubCache) lastRequest() (peer.FetchRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) == 0 {
		return peer.FetchRequest{}, false
	}
	return s.requests[len(s.requests)-1], true
}

// stubPools resolves a single pool.
type stubPools struct{ pool *origin.Pool }

func (s *stubPools) Pool(id string) (*origin.Pool, bool) {
	if s.pool == nil {
		return nil, false
	}
	return s.pool, true
}

// stubLimiters hands out one limiter for every route.
type stubLimiters struct {
	mu       sync.Mutex
	limiters map[string]*ratelimit.Limiter
}

func (s *stubLimiters) Limiter(route *edgemeshv1.Route) (*ratelimit.Limiter, bool) {
	p := route.GetRateLimitPolicy()
	if !p.GetEnabled() {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.limiters == nil {
		s.limiters = map[string]*ratelimit.Limiter{}
	}
	if l, ok := s.limiters[route.GetId()]; ok {
		return l, true
	}
	l, err := ratelimit.New(ratelimit.Options{Rate: p.GetRatePerSecond(), Burst: int(p.GetBurst())})
	if err != nil {
		return nil, false
	}
	s.limiters[route.GetId()] = l
	return l, true
}

func testRoute(id string, mutate func(*edgemeshv1.Route)) *edgemeshv1.Route {
	r := &edgemeshv1.Route{
		Id: id, Hostname: "demo.local", PathPrefix: "/",
		OriginPoolId: "pool-1", Enabled: true,
		CachePolicy:  &edgemeshv1.CachePolicy{Enabled: true, DefaultTtlSeconds: 60},
		HeaderPolicy: &edgemeshv1.HeaderPolicy{DiagnosticHeaders: true},
	}
	if mutate != nil {
		mutate(r)
	}
	return r
}

func cachedObject() *cache.Object {
	now := time.Now()
	return &cache.Object{
		Key: "k", RouteID: "r1", Status: http.StatusOK,
		Header:    http.Header{"Content-Type": []string{"text/plain"}},
		Body:      []byte("hello from cache"),
		StoredAt:  now.Add(-10 * time.Second),
		ExpiresAt: now.Add(time.Minute), StaleUntil: now.Add(time.Minute),
		OriginNodeID: "edge-1",
	}
}

func newTestHandler(t *testing.T, routes []*edgemeshv1.Route, c CacheCoordinator, pools PoolResolver) *Handler {
	t.Helper()
	holder := routing.NewHolder()
	holder.Store(routing.NewTable(routes, 1))

	trusted, err := NewTrustedProxies(nil)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := origin.NewGuard(config.OriginSecurity{AllowLoopback: true})
	if err != nil {
		t.Fatal(err)
	}
	proxyCfg := config.ProxyConfig{}
	proxyCfg.RequestTimeout = 5 * time.Second

	h, err := NewHandler(Options{
		NodeID: "edge-1", Routes: holder, Pools: pools, Cache: c,
		Origin: NewOriginClient(NewOriginTransport(proxyCfg, guard),
			breaker.NewGroup(breaker.Options{FailureThreshold: 3, Cooldown: time.Second})),
		Limiters: &stubLimiters{}, Trusted: trusted,
		MaxObjectBytes: 1 << 20, RequestTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func request(method, host, target string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	r.Host = host
	return r
}

func TestServesFromCache(t *testing.T) {
	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "demo.local", "/asset"))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if w.Body.String() != "hello from cache" {
		t.Fatalf("body = %q", w.Body.String())
	}
	if got := w.Header().Get(HeaderCache); got != string(cache.OutcomeHit) {
		t.Fatalf("cache header = %q", got)
	}
	if w.Header().Get(HeaderNode) != "edge-1" || w.Header().Get(HeaderRoute) != "r1" {
		t.Fatalf("diagnostic headers missing: %v", w.Header())
	}
	// Age must be recomputed at serve time, not stored.
	if age := w.Header().Get("Age"); age == "" || age == "0" {
		t.Fatalf("Age = %q; a 10-second-old object must report a positive age", age)
	}
}

// Diagnostic headers disclose internal topology, so they are opt-in.
func TestDiagnosticHeadersAreOptIn(t *testing.T) {
	route := testRoute("r1", func(r *edgemeshv1.Route) {
		r.HeaderPolicy = &edgemeshv1.HeaderPolicy{DiagnosticHeaders: false}
	})
	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{route}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "demo.local", "/asset"))

	for _, header := range []string{HeaderCache, HeaderNode, HeaderRoute, HeaderOrigin} {
		if v := w.Header().Get(header); v != "" {
			t.Errorf("%s was emitted without the route opting in: %q", header, v)
		}
	}
	// The request ID is always present: it is the correlation handle a client
	// needs to report a problem, not an internal disclosure.
	if w.Header().Get(HeaderRequestID) == "" {
		t.Error("the request id must always be returned")
	}
}

func TestUnroutedRequestIs404(t *testing.T) {
	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "unknown.example", "/asset"))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "no_route") {
		t.Fatalf("body = %q", w.Body.String())
	}
	if _, called := sc.lastRequest(); called {
		t.Fatal("an unrouted request reached the cache")
	}
}

// A route pointing at a missing pool must fail clearly rather than panicking.
func TestMissingOriginPoolIs503(t *testing.T) {
	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)}, sc, &stubPools{pool: nil})

	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "demo.local", "/asset"))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if !strings.Contains(w.Body.String(), "origin_pool_missing") {
		t.Fatalf("body = %q", w.Body.String())
	}
}

// The cache key must be built from the resolved route, not from raw request
// bytes, and must reach the coordinator intact.
func TestCacheKeyIsDerivedFromTheResolvedRoute(t *testing.T) {
	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "DEMO.local:8080", "/asset?v=1"))
	_ = w

	req, ok := sc.lastRequest()
	if !ok {
		t.Fatal("the cache was never consulted")
	}
	if req.RouteID != "r1" {
		t.Fatalf("route id = %q", req.RouteID)
	}
	if !strings.HasPrefix(req.CacheKey, "r1\x1f") {
		t.Fatalf("cache key %q is not prefixed by the route id", req.CacheKey)
	}
	// The raw Host, including its case and port, must not appear in the key.
	if strings.Contains(req.CacheKey, "DEMO") || strings.Contains(req.CacheKey, "8080") {
		t.Fatalf("raw host bytes leaked into the cache key: %q", req.CacheKey)
	}
	if req.RawQuery != "v=1" {
		t.Fatalf("query = %q", req.RawQuery)
	}
}

// An authenticated request must bypass the cache entirely and go to origin.
//
// This is the security-critical one: caching an authenticated response in a
// shared cache is how one user's data reaches another.
func TestAuthenticatedRequestBypassesTheCache(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		_, _ = w.Write([]byte("from origin"))
	}))
	defer backend.Close()

	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)},
		sc, &stubPools{pool: livePool(t, backend.URL)})

	r := request(http.MethodGet, "demo.local", "/asset")
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	// The response came from the origin, not from the cache's canned object.
	if w.Body.String() != "from origin" {
		t.Fatalf("body = %q; an authenticated request was served from cache", w.Body.String())
	}
	if got := w.Header().Get(HeaderCache); got != string(cache.OutcomeBypass) {
		t.Fatalf("cache outcome = %q, want BYPASS", got)
	}
	if _, consulted := sc.lastRequest(); consulted {
		t.Fatal("an authenticated request consulted the cache")
	}
}

// livePool builds a single-origin pool pointing at a test server.
func livePool(t *testing.T, serverURL string) *origin.Pool {
	t.Helper()
	u, err := url.Parse(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	guard, err := origin.NewGuard(config.OriginSecurity{AllowLoopback: true})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := origin.BuildPool(context.Background(), &edgemeshv1.OriginPool{
		Id: "pool-1",
		Origins: []*edgemeshv1.Origin{{
			Id: "origin-1", Scheme: "http", Host: u.Hostname(), Port: uint32(port),
			HealthPath: "/healthz", ExpectedStatuses: []int32{200},
		}},
	}, nil, guard)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestRateLimitingReturns429WithRetryAfter(t *testing.T) {
	route := testRoute("r1", func(r *edgemeshv1.Route) {
		r.RateLimitPolicy = &edgemeshv1.RateLimitPolicy{
			Enabled: true, RatePerSecond: 1, Burst: 2,
			KeyStrategy: edgemeshv1.KeyStrategy_KEY_STRATEGY_GLOBAL,
		}
	})
	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{route}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	allowed, denied := 0, 0
	for i := 0; i < 10; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, request(http.MethodGet, "demo.local", "/asset"))
		switch w.Code {
		case http.StatusOK:
			allowed++
		case http.StatusTooManyRequests:
			denied++
			if w.Header().Get("Retry-After") == "" {
				t.Error("a 429 must carry Retry-After")
			}
			if !strings.Contains(w.Body.String(), "rate_limited") {
				t.Errorf("429 body = %q", w.Body.String())
			}
		}
	}
	if allowed != 2 {
		t.Fatalf("%d requests allowed through a burst-2 limiter", allowed)
	}
	if denied != 8 {
		t.Fatalf("%d requests denied, want 8", denied)
	}
}

// Rate limiting must run before the cache, so a limited request costs a token
// check rather than a lookup.
func TestRateLimitRunsBeforeTheCache(t *testing.T) {
	route := testRoute("r1", func(r *edgemeshv1.Route) {
		r.RateLimitPolicy = &edgemeshv1.RateLimitPolicy{
			Enabled: true, RatePerSecond: 1, Burst: 1,
			KeyStrategy: edgemeshv1.KeyStrategy_KEY_STRATEGY_GLOBAL,
		}
	})
	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{route}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	// Exhaust the burst.
	h.ServeHTTP(httptest.NewRecorder(), request(http.MethodGet, "demo.local", "/asset"))
	before := len(sc.requests)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "demo.local", "/asset"))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", w.Code)
	}
	if len(sc.requests) != before {
		t.Fatal("a rate-limited request still consulted the cache")
	}
}

// Overload must produce a fast 503 rather than unbounded goroutines.
func TestAdmissionControlRejectsWhenSaturated(t *testing.T) {
	release := make(chan struct{})
	blocking := &blockingCache{release: release}

	holder := routing.NewHolder()
	holder.Store(routing.NewTable([]*edgemeshv1.Route{testRoute("r1", nil)}, 1))
	trusted, _ := NewTrustedProxies(nil)

	h, err := NewHandler(Options{
		NodeID: "edge-1", Routes: holder, Pools: &stubPools{pool: &origin.Pool{ID: "pool-1"}},
		Cache: blocking, Trusted: trusted,
		MaxConcurrent: 2, RequestTimeout: 5 * time.Second, MaxObjectBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.ServeHTTP(httptest.NewRecorder(), request(http.MethodGet, "demo.local", "/asset"))
		}()
	}
	// Wait for both slots to be occupied.
	deadline := time.Now().Add(2 * time.Second)
	for blocking.inFlight.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "demo.local", "/asset"))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when saturated", w.Code)
	}
	if !strings.Contains(w.Body.String(), "overloaded") {
		t.Fatalf("body = %q", w.Body.String())
	}

	close(release)
	wg.Wait()
}

type blockingCache struct {
	release  chan struct{}
	inFlight atomic.Int64
}

func (b *blockingCache) Get(ctx context.Context, _ peer.FetchRequest) (*cache.Object, cache.Outcome, error) {
	b.inFlight.Add(1)
	defer b.inFlight.Add(-1)
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, cache.OutcomeMiss, ctx.Err()
	}
	return cachedObject(), cache.OutcomeHit, nil
}

// A draining handler must refuse new work so the load balancer stops sending.
func TestDrainRefusesNewRequests(t *testing.T) {
	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	h.Drain()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "demo.local", "/asset"))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 while draining", w.Code)
	}
	if w.Header().Get("Connection") != "close" {
		t.Error("a draining response should ask the client to close the connection")
	}
}

// A request timeout must surface as 504, not as a generic bad-gateway.
func TestTimeoutMapsToGatewayTimeout(t *testing.T) {
	sc := &stubCache{err: context.DeadlineExceeded}
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "demo.local", "/asset"))

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504 for a deadline", w.Code)
	}
	if !strings.Contains(w.Body.String(), "origin_timeout") {
		t.Fatalf("body = %q", w.Body.String())
	}
}

// Error bodies must carry a classified code and a request ID, never internal
// detail.
func TestErrorBodiesAreBoundedAndNonSensitive(t *testing.T) {
	sc := &stubCache{err: context.DeadlineExceeded}
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "demo.local", "/asset"))

	body := w.Body.String()
	if len(body) > 256 {
		t.Fatalf("error body is %d bytes; it must stay small", len(body))
	}
	for _, leak := range []string{"goroutine", ".go:", "internal/"} {
		if strings.Contains(body, leak) {
			t.Fatalf("error body leaked internal detail (%q): %s", leak, body)
		}
	}
	if !strings.Contains(body, `"request_id"`) {
		t.Fatalf("error body carries no request id: %s", body)
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Error("an error response must not be cacheable")
	}
}

func TestInflightAccounting(t *testing.T) {
	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	if h.Inflight() != 0 {
		t.Fatalf("initial in-flight = %d", h.Inflight())
	}
	for i := 0; i < 5; i++ {
		h.ServeHTTP(httptest.NewRecorder(), request(http.MethodGet, "demo.local", "/asset"))
	}
	if h.Inflight() != 0 {
		t.Fatalf("in-flight leaked: %d", h.Inflight())
	}
}

// HEAD shares GET's cached object and must not return a body.
func TestHeadRequestReturnsNoBody(t *testing.T) {
	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodHead, "demo.local", "/asset"))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	req, ok := sc.lastRequest()
	if !ok {
		t.Fatal("HEAD did not consult the cache")
	}
	// HEAD and GET share a key: a HEAD response is a GET response without a body.
	if !strings.Contains(req.CacheKey, "\x1fGET\x1f") {
		t.Fatalf("HEAD keyed separately from GET: %q", req.CacheKey)
	}
}

// Run with -race.
func TestHandlerConcurrency(t *testing.T) {
	sc := &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)}, sc, &stubPools{pool: &origin.Pool{ID: "pool-1"}})

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, request(http.MethodGet, "demo.local", "/asset"))
				h.Inflight()
			}
		}()
	}
	wg.Wait()
	if h.Inflight() != 0 {
		t.Fatalf("in-flight leaked under concurrency: %d", h.Inflight())
	}
}

// The monotonic request counter is what lets the edge report a request rate to
// the control plane from two samples, without keeping a windowed estimate on
// the hot path.
func TestRequestCounterIsMonotonic(t *testing.T) {
	h := newTestHandler(t, []*edgemeshv1.Route{testRoute("r1", nil)},
		&stubCache{obj: cachedObject(), outcome: cache.OutcomeHit},
		&stubPools{pool: &origin.Pool{ID: "pool-1"}})

	if got := h.Requests(); got != 0 {
		t.Fatalf("a fresh handler reports %d requests, want 0", got)
	}
	for i := 0; i < 5; i++ {
		h.ServeHTTP(httptest.NewRecorder(), request("GET", "demo.local", "/a"))
	}
	if got := h.Requests(); got != 5 {
		t.Fatalf("Requests() = %d after 5 requests, want 5", got)
	}

	// A request that never matches a route is still an admitted request and
	// must be counted: a rate that ignored 404s would under-report load exactly
	// when a misconfiguration is generating it.
	h.ServeHTTP(httptest.NewRecorder(), request("GET", "unknown.local", "/a"))
	if got := h.Requests(); got != 6 {
		t.Fatalf("Requests() = %d after an unrouted request, want 6", got)
	}

	// A rejected request is not admitted and must not be counted.
	h.Drain()
	h.ServeHTTP(httptest.NewRecorder(), request("GET", "demo.local", "/a"))
	if got := h.Requests(); got != 6 {
		t.Fatalf("Requests() = %d after a drained request, want it unchanged at 6", got)
	}
}

// A cacheable response reports the tier it actually came from. The metric said
// L1 while the span on the same request said L2, so the two disagreed for every
// cacheable request.
func TestCacheMetricReportsTheDistributedTier(t *testing.T) {
	var tiers []cache.Tier
	holder := routing.NewHolder()
	holder.Store(routing.NewTable([]*edgemeshv1.Route{testRoute("r1", nil)}, 1))
	trusted, err := NewTrustedProxies(nil)
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(Options{
		NodeID: "edge-1", Routes: holder, Pools: &stubPools{pool: &origin.Pool{ID: "pool-1"}},
		Cache: &stubCache{obj: cachedObject(), outcome: cache.OutcomeHit}, Trusted: trusted,
		RequestTimeout: 5 * time.Second,
		Metrics: Metrics{
			CacheRequest: func(tier cache.Tier, _ cache.Outcome) {
				tiers = append(tiers, tier)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.ServeHTTP(httptest.NewRecorder(), request("GET", "demo.local", "/a"))

	if len(tiers) != 1 {
		t.Fatalf("CacheRequest fired %d times, want 1", len(tiers))
	}
	if tiers[0] != cache.TierL2 {
		t.Errorf("cache metric tier = %q, want %q to match the span on the same request",
			tiers[0], cache.TierL2)
	}
}

// twoOriginPool points two distinct origin IDs at one server, so a retry has
// somewhere to go: the retry loop excludes an origin by ID after it fails.
func twoOriginPool(t *testing.T, serverURL string) *origin.Pool {
	t.Helper()
	u, err := url.Parse(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	guard, err := origin.NewGuard(config.OriginSecurity{AllowLoopback: true})
	if err != nil {
		t.Fatal(err)
	}
	mk := func(id string) *edgemeshv1.Origin {
		return &edgemeshv1.Origin{
			Id: id, Scheme: "http", Host: u.Hostname(), Port: uint32(port),
			HealthPath: "/healthz", ExpectedStatuses: []int32{200},
		}
	}
	pool, err := origin.BuildPool(context.Background(), &edgemeshv1.OriginPool{
		Id: "pool-1", Origins: []*edgemeshv1.Origin{mk("origin-1"), mk("origin-2")},
	}, nil, guard)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

// A request body is a one-shot stream off the client connection. Retrying it
// would replay an empty body and silently send a different request than the
// client made, so a request carrying one is never retried.
//
// OPTIONS is the method that reaches this: it is idempotent, so the retry
// policy admits it, and it is permitted a body, so the handler forwards one.
func TestRequestCarryingABodyIsNotRetried(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	guard, err := origin.NewGuard(config.OriginSecurity{AllowLoopback: true})
	if err != nil {
		t.Fatal(err)
	}
	client := NewOriginClient(NewOriginTransport(config.ProxyConfig{}, guard),
		breaker.NewGroup(breaker.Options{FailureThreshold: 100, Cooldown: time.Minute}))
	route := testRoute("r1", func(r *edgemeshv1.Route) {
		r.RetryPolicy = &edgemeshv1.RetryPolicy{
			Enabled: true, MaxRetries: 2, BackoffBaseMs: 1, BackoffMaxMs: 2,
		}
	})
	pool := twoOriginPool(t, srv.URL)

	fetch := func(body io.Reader) int64 {
		hits.Store(0)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		//nolint:bodyclose // Fetch buffers and closes the origin response itself.
		_, _ = client.Fetch(ctx, FetchOptions{
			Pool: pool, Route: route, Method: http.MethodOptions,
			Path: "/x", Header: http.Header{}, Body: body, MaxBodyBytes: 1 << 20,
		})
		return hits.Load()
	}

	// Control: with no body the same request is retried onto the second origin.
	if got := fetch(nil); got < 2 {
		t.Fatalf("a bodyless OPTIONS produced %d origin attempts, want at least 2", got)
	}
	// With a body it must be attempted exactly once.
	if got := fetch(strings.NewReader("payload")); got != 1 {
		t.Errorf("an OPTIONS carrying a body produced %d origin attempts, want exactly 1", got)
	}
}
