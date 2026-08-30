// Package key constructs EdgeMesh cache keys.
//
// Cache-key construction is a security boundary. A key that is too coarse
// serves one client's response to another; a key that is too fine destroys the
// hit ratio. Everything in this package is therefore explicit and tested rather
// than derived from request bytes by convenience.
//
// # Key shape
//
//	route_id | method | path | canonical_query | vary_component
//
// The route ID is used rather than the raw Host header: the hostname has
// already been normalized through route resolution, so untrusted host bytes
// never reach the key. Components are joined with a separator that cannot occur
// in any component, which makes the encoding injective: no two distinct
// requests can produce the same key by shifting bytes across a boundary.
package key

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/url"
	"sort"
	"strings"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
)

// sep separates key components. 0x1f (unit separator) cannot appear in a
// normalized method, path, or query, so the joined form is unambiguous.
const sep = "\x1f"

// maxInlineKeyBytes bounds the literal key. Beyond it the tail is replaced by a
// hash so that a hostile long URL cannot inflate cache metadata memory.
const maxInlineKeyBytes = 512

// maxRouteIDBytes bounds how much of the route component is preserved verbatim
// when a key is hashed.
//
// The route ID must survive truncation intact, because route-wide purge matches
// on the "<route_id>\x1f" prefix. A truncated route ID would make purge silently
// miss the objects it is supposed to remove, the worst kind of failure, since
// the purge reports success. Route validation caps IDs well below this, so the
// bound is a structural guarantee rather than a limit anyone should reach.
const maxRouteIDBytes = 128

// Request is the minimal, already-normalized view of an HTTP request the key
// builder needs. It is constructed by the proxy after route resolution so that
// this package never touches a raw request.
type Request struct {
	RouteID string
	Method  string
	// Path is the URL path. It must already be normalized by the caller for
	// route matching; Build applies its own canonicalization for key purposes.
	Path string
	// RawQuery is the unparsed query string, without the leading '?'.
	RawQuery string
	// Header is the inbound request header set. Only headers named by the
	// policy or by an origin Vary are read.
	Header http.Header
}

// Build returns the cache key for req under policy.
//
// vary lists response Vary header names, if a prior response for this key told
// us what the origin varies on. On the request path it is empty; when storing a
// response the caller rebuilds the key with the observed Vary so subsequent
// lookups agree.
func Build(req Request, policy *edgemeshv1.CachePolicy, vary []string) string {
	var b strings.Builder
	b.Grow(len(req.RouteID) + len(req.Path) + len(req.RawQuery) + 32)

	b.WriteString(req.RouteID)
	b.WriteString(sep)
	b.WriteString(NormalizeMethod(req.Method))
	b.WriteString(sep)
	b.WriteString(normalizePath(req.Path))
	b.WriteString(sep)
	b.WriteString(CanonicalQuery(req.RawQuery, policy.GetCanonicalQuery()))
	b.WriteString(sep)
	b.WriteString(varyComponent(req.Header, policy.GetVaryHeaders(), vary))

	k := b.String()
	if len(k) <= maxInlineKeyBytes {
		return k
	}
	return truncate(req.RouteID, k)
}

// truncate replaces the tail of an oversized key with a hash of the whole key,
// preserving the route component verbatim.
//
// Hashing the *whole* key and keeping a raw prefix would be simpler, but the
// prefix would be a prefix of the concatenated string rather than of the route
// component, so a long enough route ID would be cut in half and route-wide
// purge would stop matching. Rebuilding the key as
// "<route_id>\x1f<hash>" keeps the purge prefix exact by construction, and the
// hash covers the entire original key so distinct requests still key distinctly.
func truncate(routeID, full string) string {
	sum := sha256.Sum256([]byte(full))
	digest := "h:" + base64.RawURLEncoding.EncodeToString(sum[:])

	id := routeID
	if len(id) > maxRouteIDBytes {
		// A route ID this long cannot occur: validation rejects it. Truncating
		// rather than panicking keeps a hostile or buggy caller from crashing
		// the request path, and the hash still disambiguates the result.
		id = id[:maxRouteIDBytes]
	}
	return id + sep + digest
}

// NormalizeMethod uppercases a method. HEAD shares GET's stored object: a HEAD
// response is a GET response without the body, so keying them apart would
// double-fill the cache for no benefit.
func NormalizeMethod(m string) string {
	m = strings.ToUpper(strings.TrimSpace(m))
	if m == http.MethodHead {
		return http.MethodGet
	}
	return m
}

// normalizePath keeps the path byte-exact apart from guaranteeing a leading
// slash. Collapsing "//" or resolving ".." here would let a client address one
// stored object by several paths, so it is deliberately not done: the origin,
// not the cache, decides what a path means.
func normalizePath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		return "/" + p
	}
	return p
}

// CanonicalQuery returns the key's query component.
//
// By default the raw query is preserved verbatim, because parameter order can
// be semantically meaningful to an origin and reordering would merge two
// distinct resources into one cache entry. A route may opt into sorting when
// its origin is known to be order-insensitive, which raises the hit ratio for
// clients that emit parameters in varying orders.
func CanonicalQuery(raw string, canonicalize bool) string {
	if raw == "" {
		return ""
	}
	if !canonicalize {
		return raw
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		// An unparseable query is preserved verbatim rather than dropped:
		// dropping it would key two different requests identically.
		return raw
	}
	names := make([]string, 0, len(values))
	for n := range values {
		names = append(names, n)
	}
	sort.Strings(names)

	var b strings.Builder
	for _, n := range names {
		vs := values[n]
		sort.Strings(vs)
		for _, v := range vs {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(url.QueryEscape(n))
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(v))
		}
	}
	return b.String()
}

// varyComponent renders the selected request headers into the key.
//
// The header set is the union of the route's configured vary_headers and any
// Vary names the origin declared. Names are lowercased and sorted so the
// component is stable, and values are included verbatim so two clients sending
// different values never share an entry.
func varyComponent(h http.Header, policyVary, responseVary []string) string {
	if len(policyVary) == 0 && len(responseVary) == 0 {
		return ""
	}
	set := make(map[string]struct{}, len(policyVary)+len(responseVary))
	for _, n := range policyVary {
		if n = canonicalHeaderName(n); n != "" {
			set[n] = struct{}{}
		}
	}
	for _, n := range responseVary {
		if n = canonicalHeaderName(n); n != "" {
			set[n] = struct{}{}
		}
	}
	if len(set) == 0 {
		return ""
	}
	names := make([]string, 0, len(set))
	for n := range set {
		names = append(names, n)
	}
	sort.Strings(names)

	var b strings.Builder
	for _, n := range names {
		if b.Len() > 0 {
			b.WriteByte('\x1e') // record separator between vary pairs
		}
		b.WriteString(n)
		b.WriteByte('=')
		// http.Header.Values canonicalizes the lookup name itself.
		b.WriteString(strings.Join(h.Values(n), ","))
	}
	return b.String()
}

// canonicalHeaderName lowercases and trims a header name, rejecting anything
// containing a separator byte so a configured name cannot forge key structure.
func canonicalHeaderName(n string) string {
	n = strings.ToLower(strings.TrimSpace(n))
	if n == "" || strings.ContainsAny(n, "\x1e\x1f:\r\n") {
		return ""
	}
	return n
}

// ParseVary splits a response Vary header into names. It reports wildcard=true
// for "Vary: *", which makes a response uncacheable in a shared cache because
// the set of varying dimensions is unknowable.
func ParseVary(values []string) (names []string, wildcard bool) {
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if part == "*" {
				return nil, true
			}
			if n := canonicalHeaderName(part); n != "" {
				names = append(names, n)
			}
		}
	}
	sort.Strings(names)
	return names, false
}

// RoutePrefix returns the key prefix owned by a route. Route-wide purge deletes
// every key with this prefix, which is exact because route_id is the first
// component and the separator cannot appear inside an ID.
func RoutePrefix(routeID string) string { return routeID + sep }

// RouteOf extracts the route ID from a key, reporting false for a malformed
// key. It exists so purge and metrics can attribute a key without a lookup.
func RouteOf(k string) (string, bool) {
	i := strings.Index(k, sep)
	if i < 0 {
		return "", false
	}
	return k[:i], true
}
