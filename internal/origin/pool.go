package origin

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// Health is an origin's observed health.
type Health string

const (
	HealthUnknown   Health = "unknown"
	HealthHealthy   Health = "healthy"
	HealthUnhealthy Health = "unhealthy"
)

// Endpoint is one origin server with its live health state.
//
// The struct is shared between the health checker and the request path, so
// every mutable field is atomic: taking a lock on origin selection would
// serialize the hot path behind the health checker.
type Endpoint struct {
	ID     string
	Scheme string
	Host   string
	Port   uint16
	Weight uint32

	HealthPath       string
	ExpectedStatuses []int

	// baseURL is precomputed so the request path never formats a URL.
	baseURL string
	// addr is the dial target, precomputed for the same reason.
	addr string

	health           atomic.Value // Health
	consecutiveOK    atomic.Int32
	consecutiveFail  atomic.Int32
	lastFailureUnixN atomic.Int64
	lastCheckUnixN   atomic.Int64
	// checksTotal and failuresTotal drive metrics.
	checksTotal   atomic.Uint64
	failuresTotal atomic.Uint64
}

// BaseURL returns the scheme://host:port prefix for this endpoint.
func (e *Endpoint) BaseURL() string { return e.baseURL }

// Addr returns the host:port dial target.
func (e *Endpoint) Addr() string { return e.addr }

// Health reports the endpoint's current health.
func (e *Endpoint) Health() Health {
	h, _ := e.health.Load().(Health)
	if h == "" {
		return HealthUnknown
	}
	return h
}

// Healthy reports whether the endpoint is eligible for traffic. An endpoint
// that has never been checked counts as eligible: refusing traffic until the
// first health check completes would make every cold start a brief outage.
func (e *Endpoint) Healthy() bool { return e.Health() != HealthUnhealthy }

// LastFailure reports when this endpoint last failed a check, zero if never.
func (e *Endpoint) LastFailure() time.Time {
	n := e.lastFailureUnixN.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// Pool is an immutable snapshot of an origin pool plus its live health state.
//
// Health state lives on the Endpoint structs, which are carried forward when a
// pool is rebuilt from a configuration change, so a config update does not
// discard everything the health checker has learned.
type Pool struct {
	ID            string
	LoadBalancing edgemeshv1.LoadBalancing
	Endpoints     []*Endpoint

	HealthInterval     time.Duration
	HealthTimeout      time.Duration
	UnhealthyThreshold int32
	HealthyThreshold   int32

	Version uint64

	// rr is the round-robin cursor. It is monotonic and wraps naturally.
	rr atomic.Uint64
	// weightedCursor drives weighted round robin.
	weightedCursor atomic.Uint64
}

// BuildPool converts a replicated OriginPool message into a live Pool.
//
// prev, when non-nil, supplies existing Endpoint structs so health state
// survives a configuration change that did not alter an endpoint's identity.
//
// ctx bounds the origin-address validation, which performs DNS resolution.
func BuildPool(ctx context.Context, p *edgemeshv1.OriginPool, prev *Pool, guard *Guard) (*Pool, error) {
	if p == nil {
		return nil, errs.New(errs.ClassValidation, "origin pool is nil")
	}
	if err := ValidatePool(p); err != nil {
		return nil, err
	}

	prevByID := map[string]*Endpoint{}
	if prev != nil {
		for _, e := range prev.Endpoints {
			prevByID[e.ID] = e
		}
	}

	out := &Pool{
		ID:                 p.GetId(),
		LoadBalancing:      p.GetLoadBalancing(),
		HealthInterval:     durationOrDefault(p.GetHealthIntervalMs(), 5*time.Second),
		HealthTimeout:      durationOrDefault(p.GetHealthTimeoutMs(), 2*time.Second),
		UnhealthyThreshold: int32OrDefault(p.GetUnhealthyThreshold(), 3),
		HealthyThreshold:   int32OrDefault(p.GetHealthyThreshold(), 2),
		Version:            p.GetVersion(),
	}

	for _, o := range p.GetOrigins() {
		if err := CheckScheme(o.GetScheme()); err != nil {
			return nil, err
		}
		if guard != nil {
			if err := guard.CheckHostPort(ctx, o.GetHost()); err != nil {
				return nil, errs.Wrap(errs.ClassValidation, err,
					"origin %q in pool %q is not permitted", o.GetId(), p.GetId())
			}
		}
		addr := net.JoinHostPort(o.GetHost(), strconv.Itoa(int(o.GetPort())))

		e, reused := prevByID[o.GetId()]
		if !reused || e.Scheme != o.GetScheme() || e.addr != addr {
			// A changed address is a different server; its health history no
			// longer applies.
			e = &Endpoint{}
			e.health.Store(HealthUnknown)
		}
		e.ID = o.GetId()
		e.Scheme = o.GetScheme()
		e.Host = o.GetHost()
		e.Port = uint16(o.GetPort())
		e.Weight = o.GetWeight()
		if e.Weight == 0 {
			e.Weight = 1
		}
		e.HealthPath = o.GetHealthPath()
		if e.HealthPath == "" {
			e.HealthPath = "/healthz"
		}
		e.ExpectedStatuses = make([]int, 0, len(o.GetExpectedStatuses()))
		for _, s := range o.GetExpectedStatuses() {
			e.ExpectedStatuses = append(e.ExpectedStatuses, int(s))
		}
		if len(e.ExpectedStatuses) == 0 {
			e.ExpectedStatuses = []int{http.StatusOK}
		}
		e.addr = addr
		e.baseURL = o.GetScheme() + "://" + addr

		out.Endpoints = append(out.Endpoints, e)
	}
	return out, nil
}

func durationOrDefault(ms uint32, d time.Duration) time.Duration {
	if ms == 0 {
		return d
	}
	return time.Duration(ms) * time.Millisecond
}

func int32OrDefault(v uint32, d int32) int32 {
	if v == 0 {
		return d
	}
	return int32(v)
}

// ValidatePool checks a pool message before it is proposed to Raft.
func ValidatePool(p *edgemeshv1.OriginPool) error {
	if p == nil {
		return errs.New(errs.ClassValidation, "origin pool is nil")
	}
	if p.GetId() == "" {
		return errs.New(errs.ClassValidation, "origin_pool.id is required")
	}
	if len(p.GetOrigins()) == 0 {
		return errs.New(errs.ClassValidation, "origin_pool %q must contain at least one origin", p.GetId())
	}
	seen := map[string]bool{}
	for i, o := range p.GetOrigins() {
		if o.GetId() == "" {
			return errs.New(errs.ClassValidation, "origin_pool %q origins[%d].id is required", p.GetId(), i)
		}
		if seen[o.GetId()] {
			return errs.New(errs.ClassValidation, "origin_pool %q has duplicate origin id %q", p.GetId(), o.GetId())
		}
		seen[o.GetId()] = true
		if err := CheckScheme(o.GetScheme()); err != nil {
			return err
		}
		if o.GetHost() == "" {
			return errs.New(errs.ClassValidation, "origin_pool %q origins[%d].host is required", p.GetId(), i)
		}
		if o.GetPort() == 0 || o.GetPort() > 65535 {
			return errs.New(errs.ClassValidation,
				"origin_pool %q origins[%d].port %d is out of range", p.GetId(), i, o.GetPort())
		}
		if hp := o.GetHealthPath(); hp != "" && hp[0] != '/' {
			return errs.New(errs.ClassValidation,
				"origin_pool %q origins[%d].health_path %q must start with '/'", p.GetId(), i, hp)
		}
		for _, s := range o.GetExpectedStatuses() {
			if s < 100 || s > 599 {
				return errs.New(errs.ClassValidation,
					"origin_pool %q origins[%d] has invalid expected status %d", p.GetId(), i, s)
			}
		}
	}
	return nil
}

// ErrNoHealthyOrigin is returned when the pool has no eligible endpoint.
var ErrNoHealthyOrigin = errs.New(errs.ClassUnavailable, "no healthy origin available")

// Select returns the next endpoint to try.
//
// exclude names endpoints already attempted for this request so a retry does
// not land on the same failing origin.
//
// When every endpoint is unhealthy the pool fails fast with
// ErrNoHealthyOrigin. This is the documented V1 policy: serving from a known
// unhealthy origin produces slow errors instead of fast ones, and a fast 503
// lets the client and the circuit breaker react correctly.
func (p *Pool) Select(exclude map[string]bool) (*Endpoint, error) {
	if p == nil || len(p.Endpoints) == 0 {
		return nil, ErrNoHealthyOrigin
	}
	if p.LoadBalancing == edgemeshv1.LoadBalancing_LOAD_BALANCING_WEIGHTED_ROUND_ROBIN {
		if e := p.selectWeighted(exclude); e != nil {
			return e, nil
		}
		return nil, ErrNoHealthyOrigin
	}

	n := len(p.Endpoints)
	start := p.rr.Add(1)
	for i := 0; i < n; i++ {
		e := p.Endpoints[int((start+uint64(i))%uint64(n))]
		if exclude[e.ID] || !e.Healthy() {
			continue
		}
		return e, nil
	}
	return nil, ErrNoHealthyOrigin
}

// selectWeighted picks proportionally to endpoint weight using a simple
// cursor over the expanded weight space. It is O(n) per call over the pool,
// which is fine for the handful of origins a pool holds.
func (p *Pool) selectWeighted(exclude map[string]bool) *Endpoint {
	var total uint64
	for _, e := range p.Endpoints {
		if exclude[e.ID] || !e.Healthy() {
			continue
		}
		total += uint64(e.Weight)
	}
	if total == 0 {
		return nil
	}
	pos := p.weightedCursor.Add(1) % total
	for _, e := range p.Endpoints {
		if exclude[e.ID] || !e.Healthy() {
			continue
		}
		w := uint64(e.Weight)
		if pos < w {
			return e
		}
		pos -= w
	}
	return nil
}

// HealthyCount reports how many endpoints are eligible for traffic.
func (p *Pool) HealthyCount() int {
	if p == nil {
		return 0
	}
	n := 0
	for _, e := range p.Endpoints {
		if e.Healthy() {
			n++
		}
	}
	return n
}

// Endpoint looks up an endpoint by ID.
func (p *Pool) Endpoint(id string) (*Endpoint, bool) {
	if p == nil {
		return nil, false
	}
	for _, e := range p.Endpoints {
		if e.ID == id {
			return e, true
		}
	}
	return nil, false
}

// Checker runs active health checks across every configured pool.
//
// Concurrency is bounded by a semaphore: a deployment with many origins must
// not spawn an unbounded number of simultaneous checks.
type Checker struct {
	client *http.Client
	clk    clock.Clock
	log    *slog.Logger
	sem    chan struct{}

	// OnTransition is called when an endpoint's health changes.
	OnTransition func(poolID string, e *Endpoint, from, to Health)

	mu    sync.RWMutex
	pools map[string]*Pool

	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
}

// CheckerOptions configures a Checker.
type CheckerOptions struct {
	Client *http.Client
	Clock  clock.Clock
	Logger *slog.Logger
	// MaxConcurrent bounds simultaneous in-flight health checks.
	MaxConcurrent int
}

// NewChecker builds a health checker.
func NewChecker(o CheckerOptions) *Checker {
	if o.Client == nil {
		o.Client = &http.Client{}
	}
	if o.Clock == nil {
		o.Clock = clock.New()
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.MaxConcurrent <= 0 {
		o.MaxConcurrent = 16
	}
	return &Checker{
		client: o.Client,
		clk:    o.Clock,
		log:    o.Logger,
		sem:    make(chan struct{}, o.MaxConcurrent),
		pools:  make(map[string]*Pool),
		stop:   make(chan struct{}),
	}
}

// SetPools replaces the checked pool set.
func (c *Checker) SetPools(pools map[string]*Pool) {
	c.mu.Lock()
	c.pools = pools
	c.mu.Unlock()
}

// Start runs the checker until ctx is canceled or Stop is called.
func (c *Checker) Start(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := c.clk.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stop:
				return
			case <-t.C():
				c.CheckDue(ctx)
			}
		}
	}()
}

// Stop halts the checker and waits for in-flight checks.
func (c *Checker) Stop() {
	c.stopOnce.Do(func() { close(c.stop) })
	c.wg.Wait()
}

// CheckDue checks every endpoint whose interval has elapsed. It is exported so
// tests drive checks deterministically instead of waiting on a ticker.
func (c *Checker) CheckDue(ctx context.Context) {
	now := c.clk.Now()

	c.mu.RLock()
	pools := make([]*Pool, 0, len(c.pools))
	for _, p := range c.pools {
		pools = append(pools, p)
	}
	c.mu.RUnlock()

	var wg sync.WaitGroup
	for _, p := range pools {
		for _, e := range p.Endpoints {
			last := e.lastCheckUnixN.Load()
			if last != 0 && now.Sub(time.Unix(0, last)) < p.HealthInterval {
				continue
			}
			// Claim the slot before spawning so two ticks cannot double-check
			// the same endpoint.
			if !e.lastCheckUnixN.CompareAndSwap(last, now.UnixNano()) {
				continue
			}
			select {
			case c.sem <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				return
			}
			wg.Add(1)
			go func(p *Pool, e *Endpoint) {
				defer wg.Done()
				defer func() { <-c.sem }()
				c.checkOne(ctx, p, e)
			}(p, e)
		}
	}
	wg.Wait()
}

// checkOne performs a single health probe and applies the threshold rules.
func (c *Checker) checkOne(ctx context.Context, p *Pool, e *Endpoint) {
	ctx, cancel := context.WithTimeout(ctx, p.HealthTimeout)
	defer cancel()

	e.checksTotal.Add(1)
	url := e.baseURL + e.HealthPath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		c.recordFailure(p, e, errs.Wrap(errs.ClassProtocol, err, "build health request"))
		return
	}
	req.Header.Set("User-Agent", "EdgeMesh-HealthCheck/1")

	resp, err := c.client.Do(req)
	if err != nil {
		c.recordFailure(p, e, errs.Wrap(errs.ClassOriginFailure, err, "health check %s", url))
		return
	}
	// The body is drained and discarded so the connection returns to the pool;
	// leaking it would defeat keep-alive for the health checker.
	_, _ = drainBody(resp)
	_ = resp.Body.Close()

	if !statusExpected(resp.StatusCode, e.ExpectedStatuses) {
		c.recordFailure(p, e, errs.New(errs.ClassOriginFailure,
			"health check %s returned %d, expected one of %v", url, resp.StatusCode, e.ExpectedStatuses))
		return
	}
	c.recordSuccess(p, e)
}

func statusExpected(status int, expected []int) bool {
	for _, s := range expected {
		if s == status {
			return true
		}
	}
	return false
}

func (c *Checker) recordSuccess(p *Pool, e *Endpoint) {
	e.consecutiveFail.Store(0)
	n := e.consecutiveOK.Add(1)
	if e.Health() != HealthHealthy && n >= p.HealthyThreshold {
		c.transition(p, e, HealthHealthy)
	}
}

func (c *Checker) recordFailure(p *Pool, e *Endpoint, cause error) {
	e.failuresTotal.Add(1)
	e.lastFailureUnixN.Store(c.clk.Now().UnixNano())
	e.consecutiveOK.Store(0)
	n := e.consecutiveFail.Add(1)
	if e.Health() != HealthUnhealthy && n >= p.UnhealthyThreshold {
		c.transition(p, e, HealthUnhealthy)
		c.log.Warn("origin marked unhealthy",
			slog.String("pool_id", p.ID),
			slog.String("origin_id", e.ID),
			slog.String("origin_addr", e.addr),
			slog.Int("consecutive_failures", int(n)),
			slog.String("error", cause.Error()),
			slog.String("error_class", string(errs.ClassOf(cause))))
	}
}

func (c *Checker) transition(p *Pool, e *Endpoint, to Health) {
	from := e.Health()
	if from == to {
		return
	}
	e.health.Store(to)
	if to == HealthHealthy && from == HealthUnhealthy {
		c.log.Info("origin recovered",
			slog.String("pool_id", p.ID),
			slog.String("origin_id", e.ID),
			slog.String("origin_addr", e.addr))
	}
	if cb := c.OnTransition; cb != nil {
		cb(p.ID, e, from, to)
	}
}

// Stats reports per-endpoint check counters for metrics.
type Stats struct {
	PoolID   string
	OriginID string
	Health   Health
	Checks   uint64
	Failures uint64
}

// Stats snapshots every endpoint's counters.
func (c *Checker) Stats() []Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []Stats
	for _, p := range c.pools {
		for _, e := range p.Endpoints {
			out = append(out, Stats{
				PoolID:   p.ID,
				OriginID: e.ID,
				Health:   e.Health(),
				Checks:   e.checksTotal.Load(),
				Failures: e.failuresTotal.Load(),
			})
		}
	}
	return out
}

// String renders an endpoint for logs.
func (e *Endpoint) String() string {
	return fmt.Sprintf("%s(%s)", e.ID, e.baseURL)
}
