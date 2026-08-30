// Package ratelimit implements in-process token-bucket rate limiting.
//
// Limits are node-local. EdgeMesh deliberately does not implement a globally
// exact distributed limit in V1: doing so would put a coordination round trip on
// the request hot path, which contradicts the project's separation of the
// control plane from the data plane. With N edges a configured rate of R is
// therefore an effective ceiling of up to N*R cluster-wide. This is documented
// rather than hidden, and is the correct trade-off for abuse damping.
//
// # Memory bounds
//
// A keyed limiter (per client IP, per header value) is an unbounded map unless
// something reclaims it. This implementation bounds memory two ways: entries
// idle longer than the eviction TTL are swept, and the map has a hard entry cap
// beyond which the least-recently-used entries are dropped. A dropped entry
// simply means the client starts with a full bucket again, which is the safe
// failure direction for availability.
package ratelimit

import (
	"container/list"
	"math"
	"sync"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// Bucket is a single token bucket.
//
// Tokens are refilled lazily from elapsed time rather than by a background
// goroutine: one goroutine per limiter key would be exactly the unbounded
// goroutine design this project forbids.
type Bucket struct {
	mu     sync.Mutex
	tokens float64
	rate   float64 // tokens per second
	burst  float64
	last   time.Time
	clk    clock.Clock
}

// NewBucket returns a bucket that starts full.
func NewBucket(rate float64, burst int, clk clock.Clock) (*Bucket, error) {
	if rate <= 0 {
		return nil, errs.New(errs.ClassValidation, "ratelimit: rate must be positive, got %v", rate)
	}
	if burst <= 0 {
		return nil, errs.New(errs.ClassValidation, "ratelimit: burst must be positive, got %d", burst)
	}
	if clk == nil {
		clk = clock.New()
	}
	return &Bucket{
		tokens: float64(burst),
		rate:   rate,
		burst:  float64(burst),
		last:   clk.Now(),
		clk:    clk,
	}, nil
}

// Allow consumes one token, reporting whether the request may proceed and, when
// denied, how long the caller should wait before retrying.
func (b *Bucket) Allow() (allowed bool, retryAfter time.Duration) {
	return b.AllowN(1)
}

// AllowN consumes n tokens atomically.
func (b *Bucket) AllowN(n float64) (bool, time.Duration) {
	if n <= 0 {
		return true, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refillLocked()
	if b.tokens >= n {
		b.tokens -= n
		return true, 0
	}
	// Retry-After is the time until the bucket accumulates the shortfall,
	// rounded up so a client that obeys it always succeeds.
	deficit := n - b.tokens
	wait := time.Duration(math.Ceil(deficit / b.rate * float64(time.Second)))
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	return false, wait
}

// Tokens reports the currently available tokens. Exported for tests and
// diagnostics; the request path uses Allow.
func (b *Bucket) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked()
	return b.tokens
}

func (b *Bucket) refillLocked() {
	now := b.clk.Now()
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		// A non-monotonic clock must not mint tokens.
		b.last = now
		return
	}
	b.last = now
	b.tokens += elapsed.Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}

// Options configures a keyed Limiter.
type Options struct {
	Rate  float64
	Burst int
	// MaxKeys caps resident buckets. Zero uses DefaultMaxKeys.
	MaxKeys int
	// IdleTTL reclaims a bucket untouched for this long. Zero uses
	// DefaultIdleTTL.
	IdleTTL time.Duration
	Clock   clock.Clock
}

// Defaults for keyed limiter memory bounds.
const (
	DefaultMaxKeys = 100000
	DefaultIdleTTL = 10 * time.Minute
)

// Limiter is a bounded map of token buckets keyed by client identity.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*list.Element
	lru     *list.List // front = most recently used
	rate    float64
	burst   int
	maxKeys int
	idleTTL time.Duration
	clk     clock.Clock

	evicted uint64
	denied  uint64
	allowed uint64
}

type keyedBucket struct {
	key      string
	bucket   *Bucket
	lastUsed time.Time
}

// New builds a keyed limiter.
func New(o Options) (*Limiter, error) {
	if o.Rate <= 0 {
		return nil, errs.New(errs.ClassValidation, "ratelimit: rate must be positive, got %v", o.Rate)
	}
	if o.Burst <= 0 {
		return nil, errs.New(errs.ClassValidation, "ratelimit: burst must be positive, got %d", o.Burst)
	}
	if o.MaxKeys <= 0 {
		o.MaxKeys = DefaultMaxKeys
	}
	if o.IdleTTL <= 0 {
		o.IdleTTL = DefaultIdleTTL
	}
	if o.Clock == nil {
		o.Clock = clock.New()
	}
	return &Limiter{
		buckets: make(map[string]*list.Element),
		lru:     list.New(),
		rate:    o.Rate,
		burst:   o.Burst,
		maxKeys: o.MaxKeys,
		idleTTL: o.IdleTTL,
		clk:     o.Clock,
	}, nil
}

// Allow consumes a token for key.
func (l *Limiter) Allow(key string) (bool, time.Duration) {
	now := l.clk.Now()

	l.mu.Lock()
	elem, ok := l.buckets[key]
	if ok {
		l.lru.MoveToFront(elem)
		kb := elem.Value.(*keyedBucket)
		kb.lastUsed = now
		b := kb.bucket
		l.mu.Unlock()
		return l.record(b.Allow())
	}
	// Create the bucket, evicting the least recently used entry when at the cap.
	b, err := NewBucket(l.rate, l.burst, l.clk)
	if err != nil {
		// Rate and burst were validated in New, so this is unreachable; fail
		// open rather than denying traffic on an impossible condition.
		l.mu.Unlock()
		return true, 0
	}
	if len(l.buckets) >= l.maxKeys {
		l.evictOldestLocked()
	}
	kb := &keyedBucket{key: key, bucket: b, lastUsed: now}
	l.buckets[key] = l.lru.PushFront(kb)
	l.mu.Unlock()

	return l.record(b.Allow())
}

func (l *Limiter) record(allowed bool, retryAfter time.Duration) (bool, time.Duration) {
	l.mu.Lock()
	if allowed {
		l.allowed++
	} else {
		l.denied++
	}
	l.mu.Unlock()
	return allowed, retryAfter
}

func (l *Limiter) evictOldestLocked() {
	back := l.lru.Back()
	if back == nil {
		return
	}
	kb := back.Value.(*keyedBucket)
	delete(l.buckets, kb.key)
	l.lru.Remove(back)
	l.evicted++
}

// Sweep reclaims buckets idle beyond the TTL and reports how many were removed.
// It walks from the LRU end and stops at the first live entry, so its cost is
// proportional to what it reclaims rather than to the map size.
func (l *Limiter) Sweep() int {
	cutoff := l.clk.Now().Add(-l.idleTTL)
	n := 0
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		back := l.lru.Back()
		if back == nil {
			return n
		}
		kb := back.Value.(*keyedBucket)
		if kb.lastUsed.After(cutoff) {
			return n
		}
		delete(l.buckets, kb.key)
		l.lru.Remove(back)
		n++
	}
}

// Stats reports limiter counters.
type Stats struct {
	Keys    int
	Allowed uint64
	Denied  uint64
	Evicted uint64
}

// Stats returns a counter snapshot.
func (l *Limiter) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Stats{Keys: len(l.buckets), Allowed: l.allowed, Denied: l.denied, Evicted: l.evicted}
}

// Len reports the resident bucket count.
func (l *Limiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}
