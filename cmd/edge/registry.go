package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/breaker"
	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/cache/l1"
	"github.com/sheehanlloyd/edgemesh/internal/cache/l2"
	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/observability"
	"github.com/sheehanlloyd/edgemesh/internal/origin"
	"github.com/sheehanlloyd/edgemesh/internal/peer"
	"github.com/sheehanlloyd/edgemesh/internal/ratelimit"
	"github.com/sheehanlloyd/edgemesh/internal/ring"
	"github.com/sheehanlloyd/edgemesh/internal/routing"
)

// configRegistry holds the edge's applied configuration.
//
// Routes are published through an atomic snapshot so the request path reads
// them without locking. Origin pools and rate limiters are keyed maps guarded
// by a mutex: they change only on a configuration event, and a read-mostly
// RWMutex is cheaper than rebuilding a whole snapshot for a single pool update.
type configRegistry struct {
	cfg     *config.EdgeConfig
	log     *slog.Logger
	metrics *observability.Metrics

	routes *routing.Holder

	mu       sync.RWMutex
	pools    map[string]*origin.Pool
	limiters map[string]*ratelimit.Limiter
	// limiterPolicy remembers the policy each limiter was built from, so a
	// limiter is only rebuilt when its policy actually changed. Rebuilding on
	// every config event would reset every client's bucket.
	limiterPolicy map[string]limiterKey

	guard         *origin.Guard
	coordinator   *l2.Coordinator
	healthChecker *origin.Checker
	breakers      *breaker.Group

	configVersion uint64
}

type limiterKey struct {
	rate     float64
	burst    uint32
	strategy edgemeshv1.KeyStrategy
	header   string
}

func newConfigRegistry(cfg *config.EdgeConfig, log *slog.Logger, m *observability.Metrics) *configRegistry {
	return &configRegistry{
		cfg: cfg, log: log, metrics: m,
		routes:        routing.NewHolder(),
		pools:         make(map[string]*origin.Pool),
		limiters:      make(map[string]*ratelimit.Limiter),
		limiterPolicy: make(map[string]limiterKey),
	}
}

// Routes exposes the route holder for the proxy's hot path.
func (r *configRegistry) Routes() *routing.Holder { return r.routes }

// Pool resolves an origin pool by ID.
func (r *configRegistry) Pool(id string) (*origin.Pool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.pools[id]
	return p, ok
}

// Limiter returns the rate limiter for a route, creating it on first use.
func (r *configRegistry) Limiter(route *edgemeshv1.Route) (*ratelimit.Limiter, bool) {
	p := route.GetRateLimitPolicy()
	if !p.GetEnabled() {
		return nil, false
	}
	want := limiterKey{
		rate: p.GetRatePerSecond(), burst: p.GetBurst(),
		strategy: p.GetKeyStrategy(), header: p.GetHeaderName(),
	}

	r.mu.RLock()
	l, ok := r.limiters[route.GetId()]
	cur, hadPolicy := r.limiterPolicy[route.GetId()]
	r.mu.RUnlock()
	if ok && hadPolicy && cur == want {
		return l, true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Re-check under the write lock: another request may have built it.
	if l, ok := r.limiters[route.GetId()]; ok && r.limiterPolicy[route.GetId()] == want {
		return l, true
	}
	built, err := ratelimit.New(ratelimit.Options{
		Rate: p.GetRatePerSecond(), Burst: int(p.GetBurst()),
	})
	if err != nil {
		r.log.Error("failed to build a rate limiter",
			slog.String(observability.FieldRouteID, route.GetId()),
			slog.String("error", err.Error()))
		return nil, false
	}
	r.limiters[route.GetId()] = built
	r.limiterPolicy[route.GetId()] = want
	return built, true
}

// ApplySnapshot replaces the whole configuration.
//
// A snapshot that fails validation is rejected wholesale and the previous
// configuration is kept. Applying a partially valid snapshot would leave the
// edge in a state no control-plane version describes.
func (r *configRegistry) ApplySnapshot(ctx context.Context, snap *edgemeshv1.ConfigSnapshot) error {
	if snap == nil {
		return nil
	}
	if err := routing.ValidateSet(snap.GetRoutes()); err != nil {
		r.log.Error("rejecting an invalid configuration snapshot; keeping the previous one",
			slog.Uint64(observability.FieldConfigVersion, snap.GetConfigVersion()),
			slog.String("error", err.Error()))
		return err
	}

	newPools := make(map[string]*origin.Pool, len(snap.GetOriginPools()))
	r.mu.RLock()
	prev := r.pools
	r.mu.RUnlock()

	for _, p := range snap.GetOriginPools() {
		// The previous pool is passed so health state survives a config change
		// that did not alter an endpoint's identity.
		built, err := origin.BuildPool(ctx, p, prev[p.GetId()], r.guard)
		if err != nil {
			r.log.Error("rejecting an origin pool from the snapshot",
				slog.String("origin_pool_id", p.GetId()),
				slog.String("error", err.Error()))
			continue
		}
		newPools[p.GetId()] = built
	}

	r.mu.Lock()
	r.pools = newPools
	// Drop limiters for routes that no longer exist so their memory is
	// reclaimed rather than accumulating across config churn.
	live := make(map[string]struct{}, len(snap.GetRoutes()))
	for _, rt := range snap.GetRoutes() {
		live[rt.GetId()] = struct{}{}
	}
	for id := range r.limiters {
		if _, ok := live[id]; !ok {
			delete(r.limiters, id)
			delete(r.limiterPolicy, id)
		}
	}
	r.configVersion = snap.GetConfigVersion()
	r.mu.Unlock()

	r.routes.Store(routing.NewTable(snap.GetRoutes(), snap.GetConfigVersion()))
	if r.healthChecker != nil {
		r.healthChecker.SetPools(newPools)
	}
	if r.coordinator != nil {
		r.coordinator.SetConfigVersion(snap.GetConfigVersion())
	}
	r.metrics.ConfigVersionApplied.Set(float64(snap.GetConfigVersion()))

	r.log.Info("applied configuration snapshot",
		slog.Uint64(observability.FieldConfigVersion, snap.GetConfigVersion()),
		slog.Int("routes", len(snap.GetRoutes())),
		slog.Int("origin_pools", len(newPools)))
	return nil
}

// ApplyRoute applies a single route change on top of the current table.
func (r *configRegistry) ApplyRoute(route *edgemeshv1.Route, version uint64) error {
	current := r.routes.Load()
	set := make([]*edgemeshv1.Route, 0, current.Len()+1)
	for _, existing := range allRoutes(current) {
		if existing.GetId() != route.GetId() {
			set = append(set, existing)
		}
	}
	set = append(set, route)

	if err := routing.ValidateSet(set); err != nil {
		r.log.Error("rejecting an invalid route update; keeping the previous configuration",
			slog.String(observability.FieldRouteID, route.GetId()),
			slog.String("error", err.Error()))
		return err
	}
	r.routes.Store(routing.NewTable(set, version))
	r.setVersion(version)
	r.log.Info("applied route change",
		slog.String(observability.FieldRouteID, route.GetId()),
		slog.Uint64(observability.FieldConfigVersion, version))
	return nil
}

// DeleteRoute removes a route from the current table.
func (r *configRegistry) DeleteRoute(id string, version uint64) {
	current := r.routes.Load()
	set := make([]*edgemeshv1.Route, 0, current.Len())
	for _, existing := range allRoutes(current) {
		if existing.GetId() != id {
			set = append(set, existing)
		}
	}
	r.routes.Store(routing.NewTable(set, version))

	r.mu.Lock()
	delete(r.limiters, id)
	delete(r.limiterPolicy, id)
	r.mu.Unlock()

	r.setVersion(version)
	r.log.Info("removed route",
		slog.String(observability.FieldRouteID, id),
		slog.Uint64(observability.FieldConfigVersion, version))
}

// ApplyOriginPool applies a single origin-pool change.
func (r *configRegistry) ApplyOriginPool(ctx context.Context, p *edgemeshv1.OriginPool, version uint64) error {
	r.mu.RLock()
	prev := r.pools[p.GetId()]
	r.mu.RUnlock()

	built, err := origin.BuildPool(ctx, p, prev, r.guard)
	if err != nil {
		r.log.Error("rejecting an invalid origin pool update",
			slog.String("origin_pool_id", p.GetId()),
			slog.String("error", err.Error()))
		return err
	}

	r.mu.Lock()
	pools := make(map[string]*origin.Pool, len(r.pools)+1)
	for k, v := range r.pools {
		pools[k] = v
	}
	pools[p.GetId()] = built
	r.pools = pools
	r.mu.Unlock()

	if r.healthChecker != nil {
		r.healthChecker.SetPools(pools)
	}
	r.setVersion(version)
	r.log.Info("applied origin pool change",
		slog.String("origin_pool_id", p.GetId()),
		slog.Uint64(observability.FieldConfigVersion, version))
	return nil
}

// DeleteOriginPool removes an origin pool.
func (r *configRegistry) DeleteOriginPool(id string, version uint64) {
	r.mu.Lock()
	pools := make(map[string]*origin.Pool, len(r.pools))
	for k, v := range r.pools {
		if k != id {
			pools[k] = v
		}
	}
	r.pools = pools
	r.mu.Unlock()

	if r.healthChecker != nil {
		r.healthChecker.SetPools(pools)
	}
	r.setVersion(version)
}

// ConfigVersion reports the applied configuration version.
func (r *configRegistry) ConfigVersion() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.configVersion
}

func (r *configRegistry) setVersion(v uint64) {
	r.mu.Lock()
	if v > r.configVersion {
		r.configVersion = v
	}
	r.mu.Unlock()
	if r.coordinator != nil {
		r.coordinator.SetConfigVersion(v)
	}
	r.metrics.ConfigVersionApplied.Set(float64(v))
}

// allRoutes extracts every route from a table, enabled or not, so an update can
// be applied against the complete set rather than only the matchable subset.
func allRoutes(t *routing.Table) []*edgemeshv1.Route {
	ids := t.AllIDs()
	out := make([]*edgemeshv1.Route, 0, len(ids))
	for _, id := range ids {
		if r, ok := t.ByID(id); ok {
			out = append(out, r)
		}
	}
	return out
}

// tieredCache checks L1 before delegating to the distributed tier.
//
// L1 exists because a local map lookup is orders of magnitude cheaper than a
// gRPC round trip to a peer, even a fast one. The distributed tier is what makes
// the *cluster's* hit ratio high; L1 is what makes an individual hit fast.
type tieredCache struct {
	l1      *l1.Cache
	l2      *l2.Coordinator
	metrics *observability.Metrics
}

func (t *tieredCache) Get(ctx context.Context, req peer.FetchRequest) (*cache.Object, cache.Outcome, error) {
	if obj, ok := t.l1.Get(req.CacheKey); ok {
		t.metrics.CacheRequests.WithLabelValues(string(cache.TierL1), string(cache.OutcomeHit)).Inc()
		return obj, cache.OutcomeHit, nil
	}
	t.metrics.CacheRequests.WithLabelValues(string(cache.TierL1), string(cache.OutcomeMiss)).Inc()

	obj, outcome, err := t.l2.Get(ctx, req)
	if err != nil || obj == nil {
		return obj, outcome, err
	}
	// Populate L1 so a repeat request on this node skips the peer hop entirely.
	// A zero expiry means the object was not admissible, so it is served but
	// not remembered.
	if !obj.ExpiresAt.IsZero() {
		if err := t.l1.Put(req.CacheKey, obj); err != nil {
			t.metrics.CacheAdmissions.WithLabelValues(string(cache.TierL1), "rejected").Inc()
		}
	}
	return obj, outcome, nil
}

// runBreakerMetrics samples circuit-breaker state.
//
// State is sampled rather than pushed on every transition because the gauge
// must reflect the *current* state, including the implicit open -> half-open
// transition that happens when a cooldown elapses without any request arriving.
func runBreakerMetrics(ctx context.Context, breakers *breaker.Group, m *observability.Metrics) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()

	// A numeric encoding keeps the series graphable; the mapping is documented
	// on the metric's help text and in the dashboard panel title.
	value := map[breaker.State]float64{
		breaker.StateClosed:   0,
		breaker.StateHalfOpen: 1,
		breaker.StateOpen:     2,
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for originID, state := range breakers.States() {
				m.CircuitState.WithLabelValues(originID).Set(value[state])
			}
		}
	}
}

// runCacheMetrics samples cache and ring gauges on a timer. Sampling is cheaper
// than updating a gauge on every cache operation, which would put metric writes
// on the hot path.
func runCacheMetrics(ctx context.Context, l1Cache, l2Tier *l1.Cache,
	coordinator *l2.Coordinator, ringHolder *ring.Holder, m *observability.Metrics) {

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for tier, c := range map[cache.Tier]*l1.Cache{cache.TierL1: l1Cache, cache.TierL2: l2Tier} {
				s := c.Stats()
				m.CacheObjects.WithLabelValues(string(tier)).Set(float64(s.Objects))
				m.CacheBytes.WithLabelValues(string(tier)).Set(float64(s.Bytes))
			}
			r := ringHolder.Load()
			m.RingNodes.Set(float64(r.Len()))
			m.RingVersion.Set(float64(r.Version()))
			m.ReplicationQueued.Set(float64(coordinator.ReplicationQueueDepth()))
			m.CoalescedWaiters.Set(float64(coordinator.Coalescer().Waiters()))
		}
	}
}
