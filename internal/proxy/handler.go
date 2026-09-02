package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/cache/key"
	"github.com/sheehanlloyd/edgemesh/internal/cache/policy"
	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/observability"
	"github.com/sheehanlloyd/edgemesh/internal/origin"
	"github.com/sheehanlloyd/edgemesh/internal/peer"
	"github.com/sheehanlloyd/edgemesh/internal/ratelimit"
	"github.com/sheehanlloyd/edgemesh/internal/routing"
)

// Diagnostic response headers. They are emitted only when a route enables them,
// because they disclose internal topology.
const (
	HeaderCache     = "X-EdgeMesh-Cache"
	HeaderNode      = "X-EdgeMesh-Node"
	HeaderOrigin    = "X-EdgeMesh-Origin"
	HeaderRequestID = "X-EdgeMesh-Request-Id"
	HeaderRoute     = "X-EdgeMesh-Route"
	HeaderAttempts  = "X-EdgeMesh-Origin-Attempts"
)

// CacheCoordinator is the L2 tier the handler consults.
type CacheCoordinator interface {
	Get(ctx context.Context, req peer.FetchRequest) (*cache.Object, cache.Outcome, error)
}

// PoolResolver resolves a route's origin pool.
type PoolResolver interface {
	Pool(id string) (*origin.Pool, bool)
}

// LimiterResolver returns the rate limiter for a route.
type LimiterResolver interface {
	Limiter(route *edgemeshv1.Route) (*ratelimit.Limiter, bool)
}

// Metrics receives request-path events. Every callback must be non-blocking.
type Metrics struct {
	Request        func(route, method, statusClass string, d time.Duration)
	InflightDelta  func(route string, delta float64)
	BodyBytes      func(route, direction string, n float64)
	CacheRequest   func(tier cache.Tier, outcome cache.Outcome)
	CacheAdmission func(tier cache.Tier, reason string)
	RateLimited    func(route string)
}

// Options configures the Handler.
type Options struct {
	NodeID   string
	Routes   *routing.Holder
	Pools    PoolResolver
	Cache    CacheCoordinator
	Origin   *OriginClient
	Limiters LimiterResolver
	Trusted  *TrustedProxies
	Clock    clock.Clock
	Logger   *slog.Logger
	Tracer   trace.Tracer
	Metrics  Metrics
	// MaxObjectBytes bounds a cacheable response body.
	MaxObjectBytes uint64
	// MaxConcurrent bounds in-flight public requests. Zero disables the bound.
	MaxConcurrent int
	// RequestTimeout bounds the whole request.
	RequestTimeout time.Duration
	// PropagateTrace injects W3C trace context into origin requests.
	PropagateTrace bool
}

// Handler is EdgeMesh's public HTTP handler.
type Handler struct {
	opts Options
	log  *slog.Logger
	clk  clock.Clock

	// sem bounds concurrent requests. Bounded concurrency is what converts an
	// overload into fast 503s rather than unbounded memory growth and a slow
	// death by goroutine.
	sem chan struct{}

	inflight atomic.Int64
	// requests counts every request admitted since start. It is monotonic so a
	// reader can derive a rate from two samples without the handler having to
	// keep a windowed estimate of its own.
	requests atomic.Uint64
	// draining stops accepting new requests during a graceful shutdown.
	draining atomic.Bool

	propagator propagation.TextMapPropagator
}

// NewHandler builds the proxy handler.
func NewHandler(o Options) (*Handler, error) {
	if o.Routes == nil {
		return nil, errs.New(errs.ClassValidation, "proxy: a route holder is required")
	}
	if o.Cache == nil {
		return nil, errs.New(errs.ClassValidation, "proxy: a cache coordinator is required")
	}
	if o.Clock == nil {
		o.Clock = clock.New()
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.MaxObjectBytes == 0 {
		o.MaxObjectBytes = 8 << 20
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 15 * time.Second
	}

	h := &Handler{
		opts:       o,
		log:        o.Logger,
		clk:        o.Clock,
		propagator: propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
	}
	if o.MaxConcurrent > 0 {
		h.sem = make(chan struct{}, o.MaxConcurrent)
	}
	return h, nil
}

// Drain stops admitting new requests so in-flight work can finish.
func (h *Handler) Drain() { h.draining.Store(true) }

// Inflight reports the current in-flight request count.
func (h *Handler) Inflight() int64 { return h.inflight.Load() }

// Requests reports the monotonic count of requests admitted since start.
//
// Two samples and the interval between them give a rate; keeping the counter
// monotonic here means callers that want different windows do not each need
// the handler to maintain one.
func (h *Handler) Requests() uint64 { return h.requests.Load() }

// ServeHTTP handles one public request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := h.clk.Now()
	requestID := requestIDFrom(r)

	if h.draining.Load() {
		// During a drain the load balancer should have stopped sending here
		// already; answering 503 with Connection: close speeds that along.
		w.Header().Set("Connection", "close")
		h.writeError(w, r, http.StatusServiceUnavailable, "draining", requestID, nil)
		return
	}

	// Admission control before any allocation-heavy work.
	if h.sem != nil {
		select {
		case h.sem <- struct{}{}:
			defer func() { <-h.sem }()
		default:
			h.writeError(w, r, http.StatusServiceUnavailable, "overloaded", requestID, nil)
			return
		}
	}

	h.inflight.Add(1)
	defer h.inflight.Add(-1)
	h.requests.Add(1)

	ctx, cancel := context.WithTimeout(r.Context(), h.opts.RequestTimeout)
	defer cancel()

	// Accept inbound trace context so a client-initiated trace continues here
	// rather than starting a disconnected one.
	ctx = h.propagator.Extract(ctx, propagation.HeaderCarrier(r.Header))

	var span trace.Span
	if h.opts.Tracer != nil {
		ctx, span = h.opts.Tracer.Start(ctx, "edge.request")
		defer span.End()
		span.SetAttributes(
			observability.AttrNodeID.String(h.opts.NodeID),
			attrString("http.request.method", r.Method),
		)
	}
	r = r.WithContext(ctx)

	route, ok := h.opts.Routes.Load().Match(r.Host, r.URL.Path)
	if !ok {
		h.record("", r.Method, http.StatusNotFound, start)
		h.writeError(w, r, http.StatusNotFound, "no_route", requestID, nil)
		return
	}
	routeID := route.GetId()
	if span != nil {
		span.SetAttributes(observability.AttrRouteID.String(routeID))
	}
	if h.opts.Metrics.InflightDelta != nil {
		h.opts.Metrics.InflightDelta(routeID, 1)
		defer h.opts.Metrics.InflightDelta(routeID, -1)
	}

	// Rate limiting runs before any cache or origin work, so a limited request
	// costs a token-bucket check rather than a cache lookup and a fetch.
	if !h.allowRate(route, r) {
		if h.opts.Metrics.RateLimited != nil {
			h.opts.Metrics.RateLimited(routeID)
		}
		h.writeRateLimited(w, r, route, requestID)
		h.record(routeID, r.Method, http.StatusTooManyRequests, start)
		return
	}

	pool, ok := h.opts.Pools.Pool(route.GetOriginPoolId())
	if !ok {
		h.log.Error("route references an unknown origin pool",
			slog.String(observability.FieldRouteID, routeID),
			slog.String("origin_pool_id", route.GetOriginPoolId()),
			slog.String(observability.FieldRequestID, requestID))
		h.record(routeID, r.Method, http.StatusServiceUnavailable, start)
		h.writeError(w, r, http.StatusServiceUnavailable, "origin_pool_missing", requestID, route)
		return
	}

	status, written, err := h.serve(ctx, w, r, route, pool, requestID, span)
	if err != nil && span != nil {
		observability.RecordError(span, err)
	}
	if h.opts.Metrics.BodyBytes != nil && written > 0 {
		h.opts.Metrics.BodyBytes(routeID, "response", float64(written))
	}
	h.record(routeID, r.Method, status, start)

	h.logRequest(ctx, r, route, status, written, requestID, h.clk.Now().Sub(start), err)
}

// serve resolves the request through cache and origin, writing the response.
func (h *Handler) serve(ctx context.Context, w http.ResponseWriter, r *http.Request,
	route *edgemeshv1.Route, pool *origin.Pool, requestID string, span trace.Span) (int, int64, error) {

	cachePolicy := route.GetCachePolicy()
	decision := policy.EvaluateRequest(r.Method, r.Header, cachePolicy)

	if !decision.Cacheable {
		// A non-cacheable request goes straight to origin and is streamed
		// through without ever being buffered for admission.
		if h.opts.Metrics.CacheRequest != nil {
			h.opts.Metrics.CacheRequest(cache.TierL1, cache.OutcomeBypass)
		}
		return h.proxyDirect(ctx, w, r, route, pool, requestID, cache.OutcomeBypass, span)
	}

	cacheKey := key.Build(key.Request{
		RouteID:  route.GetId(),
		Method:   r.Method,
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
		Header:   r.Header,
	}, cachePolicy, nil)

	fetchReq := peer.FetchRequest{
		RouteID:    route.GetId(),
		CacheKey:   cacheKey,
		Method:     r.Method,
		Path:       r.URL.Path,
		RawQuery:   r.URL.RawQuery,
		Header:     r.Header,
		ClientHost: r.Host,
	}

	// A client demanding revalidation bypasses the stored object but its fresh
	// response is still admitted for everyone else.
	if decision.NoCache {
		return h.proxyDirect(ctx, w, r, route, pool, requestID, cache.OutcomeBypass, span)
	}

	obj, outcome, err := h.opts.Cache.Get(ctx, fetchReq)
	if err != nil {
		return h.writeOriginError(w, r, route, requestID, err)
	}
	if obj == nil {
		return h.writeOriginError(w, r, route, requestID,
			errs.New(errs.ClassOriginFailure, "no response available"))
	}

	if span != nil {
		span.SetAttributes(
			observability.AttrCacheOutcome.String(string(outcome)),
			observability.AttrCacheTier.String(string(cache.TierL2)),
		)
	}
	if h.opts.Metrics.CacheRequest != nil {
		// TierL2: this outcome came from the distributed tier, which is what
		// the span above records too. Labelling the same event L1 in the metric
		// and L2 in the trace made the two disagree for every cacheable
		// request.
		h.opts.Metrics.CacheRequest(cache.TierL2, outcome)
	}

	h.setDiagnosticHeaders(w, route, requestID, outcome, obj.OriginNodeID, 0)
	n, err := peer.ObjectToHTTP(w, obj, h.clk.Now())
	return obj.Status, n, err
}

// proxyDirect forwards to origin without consulting or populating the cache.
func (h *Handler) proxyDirect(ctx context.Context, w http.ResponseWriter, r *http.Request,
	route *edgemeshv1.Route, pool *origin.Pool, requestID string, outcome cache.Outcome, span trace.Span) (int, int64, error) {

	header := r.Header.Clone()
	stripHopByHop(header)
	outReq := &http.Request{Header: header}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	h.opts.Trusted.setForwardingHeaders(outReq, r, scheme)
	header = outReq.Header
	header.Set(HeaderRequestID, requestID)
	if h.opts.PropagateTrace {
		h.propagator.Inject(ctx, propagation.HeaderCarrier(header))
	}

	var body io.Reader
	if r.Body != nil && r.Method != http.MethodGet && r.Method != http.MethodHead {
		body = r.Body
	}

	resp, err := h.opts.Origin.Fetch(ctx, FetchOptions{
		Pool: pool, Route: route,
		Method: r.Method, Path: r.URL.Path, RawQuery: r.URL.RawQuery,
		Header: header, Body: body, ClientHost: r.Host,
		MaxBodyBytes: int64(h.opts.MaxObjectBytes),
	})
	if err != nil {
		return h.writeOriginError(w, r, route, requestID, err)
	}

	// The cached path annotates its span with the cache outcome; without this
	// a bypassed request produced a span carrying no cache attributes at all,
	// so a trace could not distinguish "not cacheable" from "not instrumented".
	if span != nil {
		span.SetAttributes(
			observability.AttrCacheOutcome.String(string(outcome)),
			attrString("edgemesh.origin.id", resp.OriginID),
			attrInt("edgemesh.origin.attempts", resp.Attempts),
		)
	}

	h.setDiagnosticHeaders(w, route, requestID, outcome, resp.OriginID, resp.Attempts)
	for name, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(name, v)
		}
	}
	w.WriteHeader(resp.Status)
	if r.Method == http.MethodHead {
		return resp.Status, 0, nil
	}
	n, err := io.Copy(w, bytes.NewReader(resp.Body))
	return resp.Status, n, err
}

// allowRate applies the route's rate-limit policy.
func (h *Handler) allowRate(route *edgemeshv1.Route, r *http.Request) bool {
	p := route.GetRateLimitPolicy()
	if !p.GetEnabled() || h.opts.Limiters == nil {
		return true
	}
	limiter, ok := h.opts.Limiters.Limiter(route)
	if !ok {
		return true
	}
	allowed, _ := limiter.Allow(h.rateKey(p, r))
	return allowed
}

// rateKey derives the limiter key from the configured strategy.
func (h *Handler) rateKey(p *edgemeshv1.RateLimitPolicy, r *http.Request) string {
	switch p.GetKeyStrategy() {
	case edgemeshv1.KeyStrategy_KEY_STRATEGY_CLIENT_IP:
		return h.opts.Trusted.ClientIP(r)
	case edgemeshv1.KeyStrategy_KEY_STRATEGY_HEADER:
		// Only an administrator-configured header is read. Letting a client
		// choose the header would let it choose its own limiter bucket.
		if name := p.GetHeaderName(); name != "" {
			if v := r.Header.Get(name); v != "" {
				// The value is bounded so a hostile client cannot grow the
				// limiter's key space with megabyte header values.
				if len(v) > 128 {
					v = v[:128]
				}
				return v
			}
		}
		return "unkeyed"
	default:
		return "global"
	}
}

func (h *Handler) writeRateLimited(w http.ResponseWriter, r *http.Request, route *edgemeshv1.Route, requestID string) {
	p := route.GetRateLimitPolicy()
	if rate := p.GetRatePerSecond(); rate > 0 {
		// Retry-After is a whole number of seconds per RFC 9110, rounded up so
		// a client that obeys it actually succeeds.
		secs := int(1/rate) + 1
		w.Header().Set("Retry-After", strconv.Itoa(secs))
	}
	h.writeError(w, r, http.StatusTooManyRequests, "rate_limited", requestID, route)
}

// writeOriginError maps an origin failure onto a client-facing status.
func (h *Handler) writeOriginError(w http.ResponseWriter, r *http.Request,
	route *edgemeshv1.Route, requestID string, err error) (int, int64, error) {

	status := http.StatusBadGateway
	code := "origin_unavailable"
	switch errs.ClassOf(err) {
	case errs.ClassTimeout:
		status, code = http.StatusGatewayTimeout, "origin_timeout"
	case errs.ClassCircuitOpen:
		status, code = http.StatusServiceUnavailable, "circuit_open"
	case errs.ClassUnavailable:
		status, code = http.StatusServiceUnavailable, "no_healthy_origin"
	case errs.ClassCanceled:
		// The client hung up; there is nobody to write to and any status we
		// record would be fiction.
		status, code = 499, "client_disconnected"
		if errors.Is(err, context.Canceled) {
			return status, 0, err
		}
	case errs.ClassTooLarge:
		status, code = http.StatusRequestEntityTooLarge, "response_too_large"
	}
	h.writeError(w, r, status, code, requestID, route)
	return status, 0, err
}

// writeError emits a small, bounded error body carrying no internal detail.
func (h *Handler) writeError(w http.ResponseWriter, r *http.Request, status int, code, requestID string, route *edgemeshv1.Route) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set(HeaderRequestID, requestID)
	if route.GetHeaderPolicy().GetDiagnosticHeaders() && h.opts.NodeID != "" {
		w.Header().Set(HeaderNode, h.opts.NodeID)
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	// Hand-built rather than encoded: the fields are fixed and free of
	// characters needing escapes, and an error path should not be able to fail
	// on a marshaling error.
	_, _ = w.Write([]byte(`{"error":"` + code + `","request_id":"` + requestID + `"}`))
}

// setDiagnosticHeaders emits X-EdgeMesh-* headers when the route enables them.
func (h *Handler) setDiagnosticHeaders(w http.ResponseWriter, route *edgemeshv1.Route,
	requestID string, outcome cache.Outcome, originID string, attempts int) {

	w.Header().Set(HeaderRequestID, requestID)
	if !route.GetHeaderPolicy().GetDiagnosticHeaders() {
		return
	}
	w.Header().Set(HeaderCache, string(outcome))
	w.Header().Set(HeaderNode, h.opts.NodeID)
	w.Header().Set(HeaderRoute, route.GetId())
	if originID != "" {
		w.Header().Set(HeaderOrigin, originID)
	}
	if attempts > 1 {
		w.Header().Set(HeaderAttempts, strconv.Itoa(attempts))
	}
}

func (h *Handler) record(routeID, method string, status int, start time.Time) {
	if h.opts.Metrics.Request == nil {
		return
	}
	h.opts.Metrics.Request(routeID, method, observability.StatusClass(status), h.clk.Now().Sub(start))
}

// logRequest emits one structured access record.
func (h *Handler) logRequest(ctx context.Context, r *http.Request, route *edgemeshv1.Route,
	status int, written int64, requestID string, d time.Duration, err error) {

	log := observability.WithTrace(ctx, h.log).With(
		slog.String(observability.FieldRequestID, requestID),
		slog.String(observability.FieldRouteID, route.GetId()),
		slog.String("method", r.Method),
		slog.Int(observability.FieldStatus, status),
		slog.Int64("response_bytes", written),
		slog.Int64(observability.FieldDurationMS, d.Milliseconds()),
	)
	// The path is logged but never the query string, which routinely carries
	// tokens and personal data.
	log = log.With(slog.String("path", r.URL.Path))

	switch {
	case err != nil && errs.IsClass(err, errs.ClassCanceled):
		log.Debug("client disconnected")
	case err != nil:
		log.Warn("request failed",
			slog.String("error", err.Error()),
			slog.String(observability.FieldErrorClass, string(errs.ClassOf(err))))
	case status >= 500:
		log.Warn("request completed with a server error")
	default:
		log.Debug("request completed")
	}
}

// requestIDFrom reuses a supplied request ID or generates one.
//
// A client-supplied ID is accepted for trace correlation but bounded and
// sanitized: an unbounded value would end up in every log record for the
// request, and control characters would let a client forge log lines.
func requestIDFrom(r *http.Request) string {
	if v := r.Header.Get(HeaderRequestID); v != "" {
		if len(v) > 64 {
			v = v[:64]
		}
		clean := make([]byte, 0, len(v))
		for i := 0; i < len(v); i++ {
			c := v[i]
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
				(c >= '0' && c <= '9') || c == '-' || c == '_' {
				clean = append(clean, c)
			}
		}
		if len(clean) >= 8 {
			return string(clean)
		}
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A failed CSPRNG read must not fail the request; the ID is for
		// correlation, not security.
		return "req-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}
