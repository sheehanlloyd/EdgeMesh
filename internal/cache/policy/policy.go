// Package policy decides what EdgeMesh may cache and for how long.
//
// The rules here are deliberately conservative. A shared cache that guesses
// wrong leaks one user's response to another, so every ambiguous case resolves
// to "do not cache". Loosening a rule requires an explicit route policy.
package policy

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/cache/key"
)

// DefaultMaxTTL caps any TTL a route or origin can ask for.
const DefaultMaxTTL = 24 * time.Hour

// DefaultNegativeTTL is the conservative lifetime for a cached error response.
const DefaultNegativeTTL = 10 * time.Second

// Reason explains a caching decision. It is a low-cardinality metric label and
// a diagnostic header value, never free text.
type Reason string

const (
	ReasonCacheable Reason = "cacheable"

	ReasonPolicyDisabled   Reason = "policy_disabled"
	ReasonMethodNotAllowed Reason = "method_not_allowed"
	ReasonAuthorization    Reason = "authorization_header"
	ReasonCookie           Reason = "request_cookie"
	ReasonRequestNoStore   Reason = "request_no_store"
	ReasonRangeRequest     Reason = "range_request"

	ReasonStatusNotAllowed Reason = "status_not_allowed"
	ReasonResponseNoStore  Reason = "response_no_store"
	ReasonResponsePrivate  Reason = "response_private"
	ReasonSetCookie        Reason = "set_cookie"
	ReasonVaryWildcard     Reason = "vary_wildcard"
	ReasonTooLarge         Reason = "too_large"
	ReasonNoTTL            Reason = "no_positive_ttl"
	ReasonUnknownLength    Reason = "unknown_length"
)

// RequestDecision is the result of evaluating an inbound request.
type RequestDecision struct {
	// Cacheable reports whether a stored object may be *served* for this
	// request and whether a fresh response may be admitted for it.
	Cacheable bool
	Reason    Reason
	// NoCache is set when the client demanded revalidation ("Cache-Control:
	// no-cache"). Such a request bypasses a stored object but its response may
	// still be admitted.
	NoCache bool
}

// EvaluateRequest applies request-side cacheability rules.
func EvaluateRequest(method string, h http.Header, p *edgemeshv1.CachePolicy) RequestDecision {
	if !p.GetEnabled() {
		return RequestDecision{Reason: ReasonPolicyDisabled}
	}
	// Only GET and HEAD are cacheable in V1. HEAD shares GET's key, so a HEAD
	// may be answered from a stored GET body by truncating it.
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead:
	default:
		return RequestDecision{Reason: ReasonMethodNotAllowed}
	}
	// A Range request would store a partial representation under the key of the
	// whole resource. V1 does not support partial objects, so it bypasses.
	if h.Get("Range") != "" {
		return RequestDecision{Reason: ReasonRangeRequest}
	}
	// Authenticated requests are per-user by default. Caching them in a shared
	// cache is the classic response-mixing vulnerability.
	if h.Get("Authorization") != "" {
		return RequestDecision{Reason: ReasonAuthorization}
	}
	if !cookiesAllowed(h, p.GetAllowedCookies()) {
		return RequestDecision{Reason: ReasonCookie}
	}

	cc := ParseCacheControl(h.Values("Cache-Control"))
	if cc.NoStore {
		return RequestDecision{Reason: ReasonRequestNoStore}
	}
	return RequestDecision{Cacheable: true, Reason: ReasonCacheable, NoCache: cc.NoCache}
}

// cookiesAllowed reports whether every cookie on the request is named in the
// route's allowlist. An empty allowlist means no cookies are tolerated, which
// is the safe default: a cookie usually implies a personalized response.
func cookiesAllowed(h http.Header, allowed []string) bool {
	raw := h.Values("Cookie")
	if len(raw) == 0 {
		return true
	}
	if len(allowed) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(allowed))
	for _, a := range allowed {
		set[strings.ToLower(strings.TrimSpace(a))] = struct{}{}
	}
	for _, line := range raw {
		for _, pair := range strings.Split(line, ";") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			name := pair
			if i := strings.IndexByte(pair, '='); i >= 0 {
				name = pair[:i]
			}
			if _, ok := set[strings.ToLower(strings.TrimSpace(name))]; !ok {
				return false
			}
		}
	}
	return true
}

// ResponseDecision is the result of evaluating an origin response for
// admission.
type ResponseDecision struct {
	Admit  bool
	Reason Reason
	// TTL is the freshness lifetime to store the object with. Only meaningful
	// when Admit is true.
	TTL time.Duration
	// StaleWhileRevalidate extends how long a stale object may be served while
	// an asynchronous refresh runs.
	StaleWhileRevalidate time.Duration
	// Vary lists the response's Vary header names, which the caller folds into
	// the stored object's key.
	Vary []string
	// Negative marks an admission made under the negative-caching rule so that
	// metrics and diagnostics can distinguish it from an ordinary 200.
	Negative bool
}

// EvaluateResponse applies response-side admission rules.
//
// size is the response body size in bytes, or -1 when the origin did not
// declare a length. maxObjectBytes bounds what may be stored. now comes from
// the caller's clock so that Expires resolution is deterministic under test.
func EvaluateResponse(status int, h http.Header, size int64, maxObjectBytes uint64, p *edgemeshv1.CachePolicy, now time.Time) ResponseDecision {
	if !p.GetEnabled() {
		return ResponseDecision{Reason: ReasonPolicyDisabled}
	}

	vary, wildcard := key.ParseVary(h.Values("Vary"))
	if wildcard {
		// "Vary: *" means the response depends on dimensions the cache cannot
		// observe. There is no safe key for it.
		return ResponseDecision{Reason: ReasonVaryWildcard}
	}

	cc := ParseCacheControl(h.Values("Cache-Control")).WithExpires(h, now)
	if cc.NoStore {
		return ResponseDecision{Reason: ReasonResponseNoStore}
	}
	if cc.Private {
		return ResponseDecision{Reason: ReasonResponsePrivate}
	}
	if len(h.Values("Set-Cookie")) > 0 && !p.GetAllowSetCookie() {
		// Storing a Set-Cookie response would hand one client's session cookie
		// to the next requester.
		return ResponseDecision{Reason: ReasonSetCookie}
	}
	if size >= 0 && uint64(size) > maxObjectBytes {
		return ResponseDecision{Reason: ReasonTooLarge}
	}

	negative := false
	if !statusAllowed(status, p.GetCacheableStatuses()) {
		if !p.GetNegativeCaching() || !negativeCacheable(status) {
			return ResponseDecision{Reason: ReasonStatusNotAllowed}
		}
		negative = true
	}

	ttl := resolveTTL(cc, p, negative)
	if ttl <= 0 {
		return ResponseDecision{Reason: ReasonNoTTL}
	}
	if max := maxTTL(p); ttl > max {
		ttl = max
	}

	swr := time.Duration(p.GetStaleWhileRevalidateSeconds()) * time.Second
	if cc.HasStaleWhileRevalidate {
		swr = cc.StaleWhileRevalidate
	}

	return ResponseDecision{
		Admit:                true,
		Reason:               ReasonCacheable,
		TTL:                  ttl,
		StaleWhileRevalidate: swr,
		Vary:                 vary,
		Negative:             negative,
	}
}

// resolveTTL applies the documented precedence:
//
//  1. s-maxage (shared-cache directive; wins because EdgeMesh is a shared cache)
//  2. max-age
//  3. Expires minus Date
//  4. the route's default TTL
//
// A negative admission ignores origin freshness entirely and uses the route's
// short negative TTL: an error response's own headers are not trustworthy
// guidance for how long to remember the error.
func resolveTTL(cc CacheControl, p *edgemeshv1.CachePolicy, negative bool) time.Duration {
	if negative {
		if s := p.GetNegativeTtlSeconds(); s > 0 {
			return time.Duration(s) * time.Second
		}
		return DefaultNegativeTTL
	}
	if cc.HasSMaxAge {
		return cc.SMaxAge
	}
	if cc.HasMaxAge {
		return cc.MaxAge
	}
	if cc.HasExpires {
		return cc.ExpiresIn
	}
	return time.Duration(p.GetDefaultTtlSeconds()) * time.Second
}

func maxTTL(p *edgemeshv1.CachePolicy) time.Duration {
	if s := p.GetMaxTtlSeconds(); s > 0 {
		return time.Duration(s) * time.Second
	}
	return DefaultMaxTTL
}

// statusAllowed reports whether status is admissible. An empty policy list
// means 200 only, which is the required V1 default.
func statusAllowed(status int, allowed []int32) bool {
	if len(allowed) == 0 {
		return status == http.StatusOK
	}
	for _, a := range allowed {
		if int(a) == status {
			return true
		}
	}
	return false
}

// negativeCacheable lists the error statuses that are safe to remember briefly.
// They are all origin-side conditions that a short cache smooths without
// hiding a recovery for long.
func negativeCacheable(status int) bool {
	switch status {
	case http.StatusNotFound,
		http.StatusGone,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	}
	return false
}

// CacheControl is the parsed subset of Cache-Control and Expires that EdgeMesh
// acts on.
type CacheControl struct {
	NoStore bool
	NoCache bool
	Private bool
	Public  bool

	MaxAge    time.Duration
	HasMaxAge bool

	SMaxAge    time.Duration
	HasSMaxAge bool

	StaleWhileRevalidate    time.Duration
	HasStaleWhileRevalidate bool

	MustRevalidate bool

	// ExpiresIn is derived by the caller through ParseExpires; it is carried
	// here so TTL resolution sees one struct.
	ExpiresIn  time.Duration
	HasExpires bool
}

// ParseCacheControl parses one or more Cache-Control header values.
//
// Unparseable or negative delta-seconds are treated as absent rather than as
// zero: RFC 9111 says a malformed directive should be ignored, and treating it
// as zero would silently disable caching.
func ParseCacheControl(values []string) CacheControl {
	var cc CacheControl
	for _, v := range values {
		for _, d := range strings.Split(v, ",") {
			d = strings.TrimSpace(d)
			if d == "" {
				continue
			}
			name, arg, hasArg := strings.Cut(d, "=")
			name = strings.ToLower(strings.TrimSpace(name))
			arg = strings.Trim(strings.TrimSpace(arg), `"`)

			switch name {
			case "no-store":
				cc.NoStore = true
			case "no-cache":
				cc.NoCache = true
			case "private":
				cc.Private = true
			case "public":
				cc.Public = true
			case "must-revalidate", "proxy-revalidate":
				cc.MustRevalidate = true
			case "max-age":
				if d, ok := parseDeltaSeconds(arg, hasArg); ok {
					cc.MaxAge, cc.HasMaxAge = d, true
				}
			case "s-maxage":
				if d, ok := parseDeltaSeconds(arg, hasArg); ok {
					cc.SMaxAge, cc.HasSMaxAge = d, true
				}
			case "stale-while-revalidate":
				if d, ok := parseDeltaSeconds(arg, hasArg); ok {
					cc.StaleWhileRevalidate, cc.HasStaleWhileRevalidate = d, true
				}
			}
		}
	}
	return cc
}

func parseDeltaSeconds(arg string, hasArg bool) (time.Duration, bool) {
	if !hasArg {
		return 0, false
	}
	n, err := strconv.ParseInt(arg, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	// Clamp absurd values so a hostile origin cannot request a century-long TTL
	// that overflows downstream arithmetic.
	if n > int64(DefaultMaxTTL/time.Second) {
		n = int64(DefaultMaxTTL / time.Second)
	}
	return time.Duration(n) * time.Second, true
}

// WithExpires folds an Expires/Date pair into cc when no explicit max-age or
// s-maxage directive is present. now is supplied by the caller's clock.
func (cc CacheControl) WithExpires(h http.Header, now time.Time) CacheControl {
	if cc.HasMaxAge || cc.HasSMaxAge {
		return cc
	}
	exp := h.Get("Expires")
	if exp == "" {
		return cc
	}
	t, err := http.ParseTime(exp)
	if err != nil {
		// An invalid Expires means "already expired" per RFC 9111.
		return cc
	}
	base := now
	if d := h.Get("Date"); d != "" {
		if dt, err := http.ParseTime(d); err == nil {
			base = dt
		}
	}
	if d := t.Sub(base); d > 0 {
		cc.ExpiresIn, cc.HasExpires = d, true
	}
	return cc
}
