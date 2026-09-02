package proxy

import (
	"context"
	"crypto/tls"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/breaker"
	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/origin"
)

// NewOriginTransport builds the shared HTTP transport for origin requests.
//
// Connection reuse is the single largest determinant of proxy overhead: without
// pooling, every request pays a TCP handshake (and a TLS handshake for HTTPS
// origins), which dominates the time spent on anything else the proxy does.
//
// The dial hook re-validates the resolved address against the SSRF guard. That
// second check is what closes the DNS-rebinding window: a hostname validated at
// configuration time may resolve elsewhere by the time a connection is made.
func NewOriginTransport(cfg config.ProxyConfig, guard *origin.Guard) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   cfg.OriginDialTimeout,
		KeepAlive: cfg.OriginKeepAlive,
	}
	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, errs.Wrap(errs.ClassValidation, err, "parse origin dial address %q", addr)
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, errs.Wrap(errs.ClassOriginFailure, err, "resolve origin host %q", host)
			}
			// Dial the first address that passes the guard; refusing outright
			// on the first denied address would break dual-stack hosts whose
			// v6 address happens to be filtered.
			var lastErr error
			for _, ip := range ips {
				a, ok := netip.AddrFromSlice(ip.IP)
				if !ok {
					continue
				}
				if guard != nil {
					if err := guard.CheckAddr(a); err != nil {
						lastErr = err
						continue
					}
				}
				conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(a.String(), port))
				if err != nil {
					lastErr = err
					continue
				}
				return conn, nil
			}
			if lastErr == nil {
				lastErr = errs.New(errs.ClassOriginFailure, "no usable address for origin host %q", host)
			}
			return nil, errs.Wrap(errs.ClassOriginFailure, lastErr, "dial origin %q", addr)
		},
		MaxIdleConns:          cfg.OriginMaxIdleConns,
		MaxIdleConnsPerHost:   cfg.OriginMaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.OriginIdleConnTimeout,
		ResponseHeaderTimeout: cfg.OriginResponseHeaderTimeout,
		ExpectContinueTimeout: time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ForceAttemptHTTP2:     true,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
}

// AttemptResult describes one origin attempt for metrics and retry decisions.
type AttemptResult struct {
	OriginID string
	Status   int
	Duration time.Duration
	Err      error
	// Retryable reports whether another attempt is permitted.
	Retryable bool
	// RetryReason is a bounded metric label.
	RetryReason string
}

// OriginClient performs origin requests with retries and circuit breaking.
type OriginClient struct {
	client   *http.Client
	breakers *breaker.Group
	rng      *rand.Rand
	rngMu    chan struct{}

	// OnAttempt records each attempt for metrics.
	OnAttempt func(AttemptResult)
}

// NewOriginClient builds the origin client.
//
// A nil breaker group is replaced with a default one rather than accepted: the
// group is consulted on every attempt, so a nil would turn the first request
// into a segfault on the hot path. Failing safe here removes that crash mode
// entirely.
func NewOriginClient(transport http.RoundTripper, breakers *breaker.Group) *OriginClient {
	if breakers == nil {
		breakers = breaker.NewGroup(breaker.Options{})
	}
	return &OriginClient{
		client: &http.Client{
			Transport: transport,
			// Redirects are not followed: a proxy must return the origin's 3xx
			// to the client, not silently resolve it, or the client's own
			// caching and origin attribution become wrong.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		breakers: breakers,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
		rngMu:    make(chan struct{}, 1),
	}
}

// FetchOptions describes one origin fetch.
type FetchOptions struct {
	Pool       *origin.Pool
	Route      *edgemeshv1.Route
	Method     string
	Path       string
	RawQuery   string
	Header     http.Header
	Body       io.Reader
	ClientHost string
	// MaxBodyBytes bounds a buffered response body.
	MaxBodyBytes int64
}

// Response is a buffered origin response.
type Response struct {
	Status   int
	Header   http.Header
	Body     []byte
	OriginID string
	Attempts int
	// Truncated is set when the body exceeded MaxBodyBytes, in which case the
	// response must be streamed rather than cached.
	Truncated bool
}

// Fetch performs an origin request with retries, honouring the circuit breaker.
//
// Retries are bounded, apply only to idempotent methods, and always stay inside
// the caller's deadline. An unbounded or unconditional retry policy converts a
// struggling origin into a retry storm that guarantees it stays down.
func (c *OriginClient) Fetch(ctx context.Context, o FetchOptions) (*Response, error) {
	policy := o.Route.GetRetryPolicy()
	maxAttempts := 1
	// A body is a one-shot stream from the client connection: the first attempt
	// consumes it, so a second would send an empty body and silently change the
	// request. OPTIONS is the case that reaches here, being both idempotent and
	// permitted a body. Buffering to make it replayable would let any client
	// pin proxy memory, so such a request is simply not retried.
	if policy.GetEnabled() && isIdempotent(o.Method) && o.Body == nil {
		maxAttempts = int(policy.GetMaxRetries()) + 1
		if maxAttempts > 3 {
			maxAttempts = 3
		}
	}

	excluded := make(map[string]bool, maxAttempts)
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, errs.Wrap(errs.ClassTimeout, err, "origin request abandoned")
		}
		if attempt > 0 {
			if err := c.backoff(ctx, policy, attempt); err != nil {
				return nil, err
			}
		}

		ep, err := o.Pool.Select(excluded)
		if err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}

		br := c.breakers.Get(ep.ID)
		if err := br.Allow(); err != nil {
			// A breaker rejection is a fast local failure. Excluding this
			// origin lets the next attempt try a healthy one immediately.
			excluded[ep.ID] = true
			lastErr = errs.Wrap(errs.ClassCircuitOpen, err, "origin %q", ep.ID)
			c.report(AttemptResult{OriginID: ep.ID, Err: lastErr, Retryable: true, RetryReason: "circuit_open"})
			continue
		}

		start := time.Now()
		resp, err := c.attempt(ctx, ep, o)
		d := time.Since(start)

		if err != nil {
			br.Failure()
			excluded[ep.ID] = true
			lastErr = err
			reason := retryReasonFor(err)
			c.report(AttemptResult{
				OriginID: ep.ID, Duration: d, Err: err,
				Retryable: attempt+1 < maxAttempts, RetryReason: reason,
			})
			continue
		}

		if shouldRetryStatus(resp.Status, policy) && attempt+1 < maxAttempts {
			// A retryable status still counts as a breaker failure: an origin
			// serving 503s is unhealthy even though it answered.
			br.Failure()
			excluded[ep.ID] = true
			lastErr = errs.New(errs.ClassOriginFailure,
				"origin %q returned a retryable status %d", ep.ID, resp.Status)
			c.report(AttemptResult{
				OriginID: ep.ID, Status: resp.Status, Duration: d,
				Retryable: true, RetryReason: "status_" + statusBucket(resp.Status),
			})
			continue
		}

		br.Success()
		resp.Attempts = attempt + 1
		c.report(AttemptResult{OriginID: ep.ID, Status: resp.Status, Duration: d})
		return resp, nil
	}

	if lastErr == nil {
		lastErr = errs.New(errs.ClassOriginFailure, "origin request failed with no attempts recorded")
	}
	return nil, lastErr
}

// attempt performs one origin request and buffers the response.
func (c *OriginClient) attempt(ctx context.Context, ep *origin.Endpoint, o FetchOptions) (*Response, error) {
	target := &url.URL{
		Scheme:   ep.Scheme,
		Host:     ep.Addr(),
		Path:     o.Path,
		RawQuery: o.RawQuery,
	}
	req, err := http.NewRequestWithContext(ctx, o.Method, target.String(), o.Body)
	if err != nil {
		return nil, errs.Wrap(errs.ClassValidation, err, "build origin request")
	}
	for name, values := range o.Header {
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}
	stripHopByHop(req.Header)

	// Host handling is per-route: some origins serve by virtual host and need
	// the client's Host, others expect their own.
	if o.Route.GetHeaderPolicy().GetPreserveHost() && o.ClientHost != "" {
		req.Host = o.ClientHost
	}
	applyRequestHeaderPolicy(req.Header, o.Route.GetHeaderPolicy())

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, classifyTransportError(err, ep.ID)
	}
	defer resp.Body.Close() //nolint:errcheck // body is fully drained below

	max := o.MaxBodyBytes
	if max <= 0 {
		max = 8 << 20
	}
	// Reading one byte past the limit is how truncation is detected without a
	// second read: if the limited reader yields max+1 bytes, the body is larger
	// than the cap and must not be cached.
	body, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return nil, errs.Wrap(errs.ClassOriginFailure, err, "read origin response from %q", ep.ID)
	}
	truncated := int64(len(body)) > max
	if truncated {
		body = body[:max]
	}

	header := resp.Header.Clone()
	sanitizeResponseHeaders(header)
	applyResponseHeaderPolicy(header, o.Route.GetHeaderPolicy())

	return &Response{
		Status:    resp.StatusCode,
		Header:    header,
		Body:      body,
		OriginID:  ep.ID,
		Truncated: truncated,
	}, nil
}

// backoff waits before a retry, staying inside the caller's deadline.
func (c *OriginClient) backoff(ctx context.Context, p *edgemeshv1.RetryPolicy, attempt int) error {
	base := time.Duration(p.GetBackoffBaseMs()) * time.Millisecond
	if base <= 0 {
		base = 20 * time.Millisecond
	}
	max := time.Duration(p.GetBackoffMaxMs()) * time.Millisecond
	if max <= 0 {
		max = 500 * time.Millisecond
	}
	// Exponential growth with full jitter. Jitter matters under load: without
	// it, every client that failed at the same moment retries at the same
	// moment, reproducing the burst that caused the failure.
	d := base << (attempt - 1)
	if d > max {
		d = max
	}
	c.rngMu <- struct{}{}
	jittered := time.Duration(c.rng.Int63n(int64(d) + 1))
	<-c.rngMu

	// Never sleep past the request deadline: waiting for a retry the caller
	// will not be around to receive is wasted origin capacity.
	if dl, ok := ctx.Deadline(); ok {
		if remaining := time.Until(dl); jittered > remaining {
			return errs.New(errs.ClassTimeout,
				"insufficient time remaining (%s) for a retry backoff of %s", remaining, jittered)
		}
	}
	t := time.NewTimer(jittered)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return errs.Wrap(errs.ClassTimeout, ctx.Err(), "retry backoff interrupted")
	}
}

func (c *OriginClient) report(r AttemptResult) {
	if c.OnAttempt != nil {
		c.OnAttempt(r)
	}
}

// isIdempotent reports whether a method may be retried.
//
// Retrying a POST could duplicate a side effect the origin already performed,
// which is why only safe methods are eligible.
func isIdempotent(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

// shouldRetryStatus reports whether a status warrants another attempt.
func shouldRetryStatus(status int, p *edgemeshv1.RetryPolicy) bool {
	if !p.GetEnabled() {
		return false
	}
	list := p.GetRetryOnStatuses()
	if len(list) == 0 {
		// The conservative default: only the statuses that mean "this origin
		// could not serve the request", never a 4xx the client caused.
		return status == http.StatusBadGateway ||
			status == http.StatusServiceUnavailable ||
			status == http.StatusGatewayTimeout
	}
	for _, s := range list {
		if int(s) == status {
			return true
		}
	}
	return false
}

// classifyTransportError maps a transport failure onto an error class.
func classifyTransportError(err error, originID string) error {
	if err == nil {
		return nil
	}
	var netErr net.Error
	if ok := asNetError(err, &netErr); ok && netErr.Timeout() {
		return errs.Wrap(errs.ClassTimeout, err, "origin %q timed out", originID)
	}
	if strings.Contains(err.Error(), "context canceled") {
		return errs.Wrap(errs.ClassCanceled, err, "origin %q request canceled", originID)
	}
	return errs.Wrap(errs.ClassOriginFailure, err, "origin %q request failed", originID)
}

func retryReasonFor(err error) string {
	switch errs.ClassOf(err) {
	case errs.ClassTimeout:
		return "timeout"
	case errs.ClassCircuitOpen:
		return "circuit_open"
	case errs.ClassCanceled:
		return "canceled"
	default:
		return "connection"
	}
}

func statusBucket(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	default:
		return "other"
	}
}

// applyRequestHeaderPolicy applies a route's request header rules.
func applyRequestHeaderPolicy(h http.Header, p *edgemeshv1.HeaderPolicy) {
	if p == nil {
		return
	}
	for _, name := range p.GetRequestRemove() {
		h.Del(name)
	}
	for name, value := range p.GetRequestSet() {
		h.Set(name, value)
	}
}

// applyResponseHeaderPolicy applies a route's response header rules.
func applyResponseHeaderPolicy(h http.Header, p *edgemeshv1.HeaderPolicy) {
	if p == nil {
		return
	}
	for _, name := range p.GetResponseRemove() {
		h.Del(name)
	}
	for name, value := range p.GetResponseSet() {
		h.Set(name, value)
	}
}

// ToCacheObject converts an origin response into a storable object.
func (r *Response) ToCacheObject(key, routeID string, storedAt, expiresAt, staleUntil time.Time, vary []string, negative bool) *cache.Object {
	return &cache.Object{
		Key:        key,
		RouteID:    routeID,
		Status:     r.Status,
		Header:     r.Header.Clone(),
		Body:       r.Body,
		StoredAt:   storedAt,
		ExpiresAt:  expiresAt,
		StaleUntil: staleUntil,
		Vary:       vary,
		Negative:   negative,
	}
}
