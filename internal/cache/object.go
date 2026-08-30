// Package cache defines the object model shared by both cache tiers.
package cache

import (
	"net/http"
	"time"
)

// Object is a cached HTTP response.
//
// Objects are immutable once stored. A stored object is shared by every reader
// without copying, so nothing may mutate Header or Body after Put; callers that
// need to modify a response must clone it first.
type Object struct {
	// Key is the cache key this object is stored under, carried so that a
	// replicated object can be re-keyed on the receiving node without a second
	// lookup.
	Key string
	// RouteID attributes the object for route-wide purge and metrics.
	RouteID string

	Status int
	Header http.Header
	Body   []byte

	// StoredAt is when the object entered the cache.
	StoredAt time.Time
	// ExpiresAt is when the object stops being fresh.
	ExpiresAt time.Time
	// StaleUntil is how long past ExpiresAt the object may still be served
	// while an asynchronous revalidation runs. Equal to ExpiresAt when
	// stale-while-revalidate is disabled.
	StaleUntil time.Time

	// Vary lists the request header names folded into this object's key.
	Vary []string
	// ChecksumSHA256 is a hex digest of Body, set on replicated objects so a
	// receiving node can detect corruption on the wire.
	ChecksumSHA256 string
	// OriginNodeID names the edge that filled this object, for diagnostics.
	OriginNodeID string
	// Negative marks an object admitted under the negative-caching rule.
	Negative bool
}

// Size reports the object's memory footprint for byte accounting.
//
// It counts the body, the key, and an estimate of header storage. Accounting
// does not need to be exact, but it must be monotonic in the real cost, or the
// byte budget stops bounding memory.
func (o *Object) Size() int64 {
	if o == nil {
		return 0
	}
	// Fixed overhead approximating the struct, map header, and slice headers.
	const structOverhead = 256
	n := int64(len(o.Body)) + int64(len(o.Key)) + int64(len(o.RouteID)) + structOverhead
	for name, values := range o.Header {
		n += int64(len(name)) + 48 // map entry + slice header
		for _, v := range values {
			n += int64(len(v)) + 16
		}
	}
	for _, v := range o.Vary {
		n += int64(len(v)) + 16
	}
	return n
}

// Fresh reports whether the object is within its freshness lifetime.
func (o *Object) Fresh(now time.Time) bool {
	return o != nil && now.Before(o.ExpiresAt)
}

// Stale reports whether the object is past freshness but still inside its
// stale-while-revalidate window.
func (o *Object) Stale(now time.Time) bool {
	return o != nil && !now.Before(o.ExpiresAt) && now.Before(o.StaleUntil)
}

// Expired reports whether the object may no longer be served at all.
func (o *Object) Expired(now time.Time) bool {
	if o == nil {
		return true
	}
	deadline := o.ExpiresAt
	if o.StaleUntil.After(deadline) {
		deadline = o.StaleUntil
	}
	return !now.Before(deadline)
}

// Age reports how long the object has been stored, which becomes the response's
// Age header.
func (o *Object) Age(now time.Time) time.Duration {
	if o == nil {
		return 0
	}
	d := now.Sub(o.StoredAt)
	if d < 0 {
		return 0
	}
	return d
}

// Clone returns a shallow copy with a copied header map. The body is shared:
// bodies are never mutated after Put.
func (o *Object) Clone() *Object {
	if o == nil {
		return nil
	}
	c := *o
	c.Header = o.Header.Clone()
	if o.Vary != nil {
		c.Vary = append([]string(nil), o.Vary...)
	}
	return &c
}

// Tier names a cache tier for metrics and diagnostics.
type Tier string

const (
	TierL1 Tier = "l1"
	TierL2 Tier = "l2"
)

// Outcome is the cache result for a request, used as a low-cardinality metric
// label and as the X-EdgeMesh-Cache diagnostic header value.
type Outcome string

const (
	OutcomeHit         Outcome = "HIT"
	OutcomeMiss        Outcome = "MISS"
	OutcomeBypass      Outcome = "BYPASS"
	OutcomeStale       Outcome = "STALE"
	OutcomeRevalidated Outcome = "REVALIDATED"
	// OutcomePeerHit marks a hit served from another edge's L2.
	OutcomePeerHit Outcome = "PEER_HIT"
	// OutcomeDegraded marks a fill that bypassed an unreachable peer owner.
	OutcomeDegraded Outcome = "DEGRADED"
)

// EvictionReason labels why an object left the cache.
type EvictionReason string

const (
	EvictionCapacity  EvictionReason = "capacity"
	EvictionExpired   EvictionReason = "expired"
	EvictionPurged    EvictionReason = "purged"
	EvictionReplaced  EvictionReason = "replaced"
	EvictionAdmission EvictionReason = "admission_rejected"
)
