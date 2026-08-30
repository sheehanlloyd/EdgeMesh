// Package routing resolves an inbound request to a configured route.
//
// Routes are strongly consistent control-plane objects replicated through Raft.
// This package owns only the matching semantics and the immutable snapshot that
// request-serving goroutines read without locking.
//
// # Matching rules
//
//  1. The request hostname is normalized (lowercased, port and trailing dot
//     removed) and must match a route's hostname exactly.
//  2. Among the routes for that hostname, the longest matching path prefix wins.
//  3. Prefix matching respects slash boundaries: "/api" matches "/api" and
//     "/api/v1" but never "/apix".
//  4. Disabled routes never match.
//  5. A wildcard hostname "*" is accepted as an explicit catch-all and is only
//     consulted after exact-hostname matching fails. This keeps the demo usable
//     without weakening exact matching.
package routing

import (
	"net"
	"sort"
	"strings"
	"sync/atomic"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// Wildcard is the hostname that matches any host after exact matching fails.
const Wildcard = "*"

// Table is an immutable route-matching snapshot.
type Table struct {
	// byHost holds routes grouped by normalized hostname, each group sorted by
	// descending path-prefix length so the first match is the longest match.
	byHost map[string][]*edgemeshv1.Route
	byID   map[string]*edgemeshv1.Route
	// wildcard holds the "*" hostname group, consulted last.
	wildcard []*edgemeshv1.Route
	version  uint64
	count    int
}

// NewTable builds a matching snapshot. Disabled routes are dropped at build
// time so the hot path never evaluates them.
//
// NewTable does not validate: use Validate before accepting routes into the
// replicated state so an invalid set can never reach an edge.
func NewTable(routes []*edgemeshv1.Route, version uint64) *Table {
	t := &Table{
		byHost:  make(map[string][]*edgemeshv1.Route),
		byID:    make(map[string]*edgemeshv1.Route, len(routes)),
		version: version,
	}
	for _, r := range routes {
		if r == nil {
			continue
		}
		t.byID[r.GetId()] = r
		if !r.GetEnabled() {
			continue
		}
		host := NormalizeHostname(r.GetHostname())
		if host == Wildcard {
			t.wildcard = append(t.wildcard, r)
		} else {
			t.byHost[host] = append(t.byHost[host], r)
		}
		t.count++
	}
	for host := range t.byHost {
		sortByPrefixLength(t.byHost[host])
	}
	sortByPrefixLength(t.wildcard)
	return t
}

// sortByPrefixLength orders longest-prefix-first, breaking ties by route ID so
// the snapshot is deterministic for a given input set.
func sortByPrefixLength(rs []*edgemeshv1.Route) {
	sort.Slice(rs, func(i, j int) bool {
		li, lj := len(rs[i].GetPathPrefix()), len(rs[j].GetPathPrefix())
		if li != lj {
			return li > lj
		}
		return rs[i].GetId() < rs[j].GetId()
	})
}

// Version reports the config version this table was built from.
func (t *Table) Version() uint64 {
	if t == nil {
		return 0
	}
	return t.version
}

// Len reports the number of enabled routes.
func (t *Table) Len() int {
	if t == nil {
		return 0
	}
	return t.count
}

// ByID returns a route regardless of whether it is enabled.
func (t *Table) ByID(id string) (*edgemeshv1.Route, bool) {
	if t == nil {
		return nil, false
	}
	r, ok := t.byID[id]
	return r, ok
}

// AllIDs returns every route ID the table holds, including disabled routes,
// sorted for determinism. It exists so an incremental config update can be
// applied against the complete route set rather than only the matchable subset:
// a disabled route still occupies its (hostname, path_prefix) pair and must
// participate in conflict validation.
func (t *Table) AllIDs() []string {
	if t == nil {
		return nil
	}
	out := make([]string, 0, len(t.byID))
	for id := range t.byID {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Match resolves host and path to a route. host may include a port; path must
// already be the URL path (not the raw request target).
func (t *Table) Match(host, path string) (*edgemeshv1.Route, bool) {
	if t == nil {
		return nil, false
	}
	h := NormalizeHostname(host)
	p := NormalizePath(path)

	if r, ok := matchGroup(t.byHost[h], p); ok {
		return r, true
	}
	return matchGroup(t.wildcard, p)
}

func matchGroup(group []*edgemeshv1.Route, path string) (*edgemeshv1.Route, bool) {
	for _, r := range group {
		if PrefixMatches(r.GetPathPrefix(), path) {
			return r, true
		}
	}
	return nil, false
}

// PrefixMatches reports whether path lies under prefix, respecting slash
// boundaries.
//
// The boundary rule is what stops "/api" from capturing "/apix". A prefix that
// already ends in "/" needs no boundary check; otherwise the character after
// the prefix in path must be "/" (or the path must end exactly at the prefix).
func PrefixMatches(prefix, path string) bool {
	prefix = NormalizePath(prefix)
	if prefix == "/" {
		return true
	}
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	if len(path) == len(prefix) {
		return true
	}
	if strings.HasSuffix(prefix, "/") {
		return true
	}
	return path[len(prefix)] == '/'
}

// NormalizeHostname lowercases a host, strips any port and a single trailing
// dot, and removes IPv6 brackets. Matching operates on the result, never on raw
// request bytes.
func NormalizeHostname(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	host = strings.ToLower(host)
	// Strip the port when present. SplitHostPort fails on a bare host, which is
	// the common case, so its failure is not an error here.
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSuffix(host, ".")
	host = strings.TrimPrefix(host, "[")
	host = strings.TrimSuffix(host, "]")
	return host
}

// NormalizePath guarantees a leading slash and removes a trailing slash from
// anything longer than the root path, so "/api/" and "/api" are the same route.
func NormalizePath(path string) string {
	if path == "" {
		return "/"
	}
	if path[0] != '/' {
		path = "/" + path
	}
	for len(path) > 1 && strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}
	return path
}

// Validate checks a route for structural correctness. It is called by the admin
// API before a write is proposed to Raft so that an invalid route is rejected
// with a 400 and never consumes a consensus round.
func Validate(r *edgemeshv1.Route) error {
	if r == nil {
		return errs.New(errs.ClassValidation, "route is nil")
	}
	if strings.TrimSpace(r.GetId()) == "" {
		return errs.New(errs.ClassValidation, "route.id is required")
	}
	if len(r.GetId()) > 128 {
		return errs.New(errs.ClassValidation, "route.id must be at most 128 characters")
	}
	host := r.GetHostname()
	if strings.TrimSpace(host) == "" {
		return errs.New(errs.ClassValidation, "route.hostname is required")
	}
	if host != Wildcard {
		n := NormalizeHostname(host)
		if n == "" {
			return errs.New(errs.ClassValidation, "route.hostname %q normalizes to empty", host)
		}
		if strings.ContainsAny(n, " /\\?#") {
			return errs.New(errs.ClassValidation, "route.hostname %q contains invalid characters", host)
		}
	}
	p := r.GetPathPrefix()
	if p == "" {
		return errs.New(errs.ClassValidation, "route.path_prefix is required; use \"/\" to match every path")
	}
	if p[0] != '/' {
		return errs.New(errs.ClassValidation, "route.path_prefix %q must start with '/'", p)
	}
	if strings.ContainsAny(p, "?#") {
		return errs.New(errs.ClassValidation, "route.path_prefix %q must not contain a query or fragment", p)
	}
	if strings.TrimSpace(r.GetOriginPoolId()) == "" {
		return errs.New(errs.ClassValidation, "route.origin_pool_id is required")
	}
	if cp := r.GetCachePolicy(); cp != nil {
		if cp.GetMaxTtlSeconds() > 0 && cp.GetDefaultTtlSeconds() > cp.GetMaxTtlSeconds() {
			return errs.New(errs.ClassValidation,
				"route.cache_policy.default_ttl_seconds (%d) must not exceed max_ttl_seconds (%d)",
				cp.GetDefaultTtlSeconds(), cp.GetMaxTtlSeconds())
		}
		for _, s := range cp.GetCacheableStatuses() {
			if s < 100 || s > 599 {
				return errs.New(errs.ClassValidation, "route.cache_policy has invalid status %d", s)
			}
		}
	}
	if rl := r.GetRateLimitPolicy(); rl != nil && rl.GetEnabled() {
		if rl.GetRatePerSecond() <= 0 {
			return errs.New(errs.ClassValidation, "route.rate_limit_policy.rate_per_second must be positive when enabled")
		}
		if rl.GetBurst() == 0 {
			return errs.New(errs.ClassValidation, "route.rate_limit_policy.burst must be positive when enabled")
		}
		if rl.GetKeyStrategy() == edgemeshv1.KeyStrategy_KEY_STRATEGY_HEADER &&
			strings.TrimSpace(rl.GetHeaderName()) == "" {
			return errs.New(errs.ClassValidation,
				"route.rate_limit_policy.header_name is required for the header key strategy")
		}
	}
	if rp := r.GetRetryPolicy(); rp != nil && rp.GetEnabled() {
		// Unbounded retries turn one slow origin into a retry storm.
		if rp.GetMaxRetries() > 5 {
			return errs.New(errs.ClassValidation,
				"route.retry_policy.max_retries must be at most 5, got %d", rp.GetMaxRetries())
		}
		if rp.GetBackoffMaxMs() > 0 && rp.GetBackoffBaseMs() > rp.GetBackoffMaxMs() {
			return errs.New(errs.ClassValidation,
				"route.retry_policy.backoff_base_ms (%d) must not exceed backoff_max_ms (%d)",
				rp.GetBackoffBaseMs(), rp.GetBackoffMaxMs())
		}
	}
	return nil
}

// ValidateSet checks a complete route set for cross-route conflicts. Duplicate
// (hostname, path_prefix) pairs are rejected because they would make matching
// depend on insertion order.
func ValidateSet(routes []*edgemeshv1.Route) error {
	seenID := make(map[string]bool, len(routes))
	seenPair := make(map[string]string, len(routes))
	for _, r := range routes {
		if err := Validate(r); err != nil {
			return err
		}
		if seenID[r.GetId()] {
			return errs.New(errs.ClassValidation, "duplicate route id %q", r.GetId())
		}
		seenID[r.GetId()] = true

		pair := NormalizeHostname(r.GetHostname()) + "\x00" + NormalizePath(r.GetPathPrefix())
		if other, dup := seenPair[pair]; dup {
			return errs.New(errs.ClassValidation,
				"routes %q and %q both claim hostname %q with path prefix %q",
				other, r.GetId(), r.GetHostname(), r.GetPathPrefix())
		}
		seenPair[pair] = r.GetId()
	}
	return nil
}

// Holder publishes route tables to hot-path readers through an atomic swap, the
// same pattern as ring.Holder: readers never lock and never see a partial table.
type Holder struct {
	v atomic.Pointer[Table]
}

// NewHolder returns a Holder seeded with an empty table so Load never returns
// nil.
func NewHolder() *Holder {
	h := &Holder{}
	h.v.Store(NewTable(nil, 0))
	return h
}

// Store publishes a new table. Nil is ignored so a failed config apply cannot
// blank the routing hot path.
func (h *Holder) Store(t *Table) {
	if t == nil {
		return
	}
	h.v.Store(t)
}

// Load returns the current table. It never returns nil.
func (h *Holder) Load() *Table { return h.v.Load() }
