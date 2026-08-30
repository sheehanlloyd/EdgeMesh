// Package l1 implements EdgeMesh's bounded, sharded, in-memory cache.
//
// It backs both cache tiers: the node-local L1 and the node's share of the
// distributed L2. The two differ only in their byte budget and in who writes to
// them, so one implementation serves both.
//
// # Design
//
// The cache is split into a power-of-two number of shards, each an independent
// map plus intrusive LRU list under its own mutex. Sharding is what keeps a
// multi-core proxy from serializing every lookup behind one lock; the shard is
// selected by the high bits of the key hash so it is independent of the ring's
// use of the same hash.
//
// The byte budget is enforced per shard (total/shards). A per-shard budget
// means eviction never needs a global lock, at the cost of slightly uneven
// utilization, the same trade-off the shard count itself makes.
//
// Expiration is lazy on lookup plus a bounded periodic sweep. There is no
// goroutine per object and no unbounded background work.
package l1

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/cache/key"
	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// Admitter decides whether a candidate object earns a place in a full shard.
// It is the seam that lets the LRU baseline and the TinyLFU-inspired policy be
// compared on identical cache machinery.
type Admitter interface {
	// Touch records an access to key, whether or not it hit.
	Touch(key string)
	// Admit reports whether candidate should displace victim. victim is the
	// key the shard would evict to make room, or "" when the shard has room.
	Admit(candidate, victim string) bool
	// Name identifies the policy for metrics and documentation.
	Name() string
}

// alwaysAdmit is the LRU baseline: any candidate displaces the LRU victim.
type alwaysAdmit struct{}

func (alwaysAdmit) Touch(string)              {}
func (alwaysAdmit) Admit(string, string) bool { return true }
func (alwaysAdmit) Name() string              { return "lru" }

// Stats is a point-in-time snapshot of cache counters.
type Stats struct {
	Objects    int64
	Bytes      int64
	MaxBytes   int64
	Hits       int64
	Misses     int64
	Expired    int64
	Evictions  int64
	Rejections int64
	Puts       int64
	Deletes    int64
}

// Options configures a Cache.
type Options struct {
	// MaxBytes is the total byte budget across all shards.
	MaxBytes int64
	// Shards must be a positive power of two.
	Shards int
	// MaxObjectBytes rejects any single object larger than this.
	MaxObjectBytes int64
	// CleanupInterval drives the bounded expiry sweep. Zero disables it, which
	// leaves expiry entirely lazy, which is correct, but memory is only reclaimed on
	// access.
	CleanupInterval time.Duration
	// Clock is the time source. Nil uses the system clock.
	Clock clock.Clock
	// Admitter is the admission policy. Nil uses the LRU baseline.
	Admitter Admitter
	// OnEvict is called after an object leaves the cache. It must not block:
	// it runs while the shard lock is *not* held, but the caller is on the
	// request path.
	OnEvict func(obj *cache.Object, reason cache.EvictionReason)
}

// Cache is a bounded sharded LRU cache of HTTP objects.
type Cache struct {
	shards   []*shard
	mask     uint64
	maxObj   int64
	maxBytes int64
	clk      clock.Clock
	admitter Admitter
	onEvict  func(*cache.Object, cache.EvictionReason)

	// Counters are atomic so metric collection never contends with the
	// request path.
	hits       atomic.Int64
	misses     atomic.Int64
	expired    atomic.Int64
	evictions  atomic.Int64
	rejections atomic.Int64
	puts       atomic.Int64
	deletes    atomic.Int64

	stopOnce sync.Once
	stopped  chan struct{}
	wg       sync.WaitGroup
}

type entry struct {
	key  string
	obj  *cache.Object
	size int64
	// elem is this entry's position in the shard's LRU list.
	elem *list.Element
}

type shard struct {
	mu       sync.Mutex
	items    map[string]*entry
	lru      *list.List // front = most recently used
	bytes    int64
	maxBytes int64
}

// New builds a cache. It returns an error rather than panicking so that a bad
// configuration surfaces at startup with an actionable message.
func New(o Options) (*Cache, error) {
	if o.Shards <= 0 || o.Shards&(o.Shards-1) != 0 {
		return nil, errs.New(errs.ClassValidation, "l1: shards must be a positive power of two, got %d", o.Shards)
	}
	if o.MaxBytes <= 0 {
		return nil, errs.New(errs.ClassValidation, "l1: max_bytes must be positive, got %d", o.MaxBytes)
	}
	if o.MaxObjectBytes <= 0 {
		return nil, errs.New(errs.ClassValidation, "l1: max_object_bytes must be positive, got %d", o.MaxObjectBytes)
	}
	if int64(o.Shards) > o.MaxBytes {
		return nil, errs.New(errs.ClassValidation,
			"l1: %d shards cannot divide a %d byte budget", o.Shards, o.MaxBytes)
	}
	if o.Clock == nil {
		o.Clock = clock.New()
	}
	if o.Admitter == nil {
		o.Admitter = alwaysAdmit{}
	}

	c := &Cache{
		shards:   make([]*shard, o.Shards),
		mask:     uint64(o.Shards - 1),
		maxObj:   o.MaxObjectBytes,
		maxBytes: o.MaxBytes,
		clk:      o.Clock,
		admitter: o.Admitter,
		onEvict:  o.OnEvict,
		stopped:  make(chan struct{}),
	}
	per := o.MaxBytes / int64(o.Shards)
	for i := range c.shards {
		c.shards[i] = &shard{
			items:    make(map[string]*entry),
			lru:      list.New(),
			maxBytes: per,
		}
	}

	if o.CleanupInterval > 0 {
		c.wg.Add(1)
		go c.sweepLoop(o.CleanupInterval)
	}
	return c, nil
}

// PolicyName reports the admission policy in use.
func (c *Cache) PolicyName() string { return c.admitter.Name() }

// shardFor selects a shard from the high bits of the key hash. The ring uses
// the low-order behaviour of the same hash for placement, so taking the high
// bits here keeps shard selection and ring placement statistically independent.
func (c *Cache) shardFor(k string) *shard {
	return c.shards[(xxhash.Sum64String(k)>>32)&c.mask]
}

// Get returns a live object for k.
//
// An expired object is removed on the way past (lazy expiry) and reported as a
// miss. A stale-but-serveable object is returned: the caller decides whether to
// serve it while revalidating.
func (c *Cache) Get(k string) (*cache.Object, bool) {
	now := c.clk.Now()
	s := c.shardFor(k)

	s.mu.Lock()
	e, ok := s.items[k]
	if !ok {
		s.mu.Unlock()
		c.misses.Add(1)
		c.admitter.Touch(k)
		return nil, false
	}
	if e.obj.Expired(now) {
		c.removeLocked(s, e)
		s.mu.Unlock()
		c.expired.Add(1)
		c.misses.Add(1)
		c.admitter.Touch(k)
		c.notifyEvict(e.obj, cache.EvictionExpired)
		return nil, false
	}
	s.lru.MoveToFront(e.elem)
	obj := e.obj
	s.mu.Unlock()

	c.hits.Add(1)
	c.admitter.Touch(k)
	return obj, true
}

// Peek returns an object without updating recency or counters. It exists for
// diagnostics and tests; the request path must use Get.
func (c *Cache) Peek(k string) (*cache.Object, bool) {
	s := c.shardFor(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.items[k]
	if !ok || e.obj.Expired(c.clk.Now()) {
		return nil, false
	}
	return e.obj, true
}

// Put stores obj under k, replacing any existing entry.
//
// It reports an error only for an object that can never be stored (too large).
// A rejection by the admission policy is not an error: it is a normal outcome
// counted in Stats.Rejections.
func (c *Cache) Put(k string, obj *cache.Object) error {
	if obj == nil {
		return errs.New(errs.ClassValidation, "l1: cannot store a nil object")
	}
	size := obj.Size()
	if size > c.maxObj {
		c.rejections.Add(1)
		return errs.New(errs.ClassTooLarge,
			"l1: object %d bytes exceeds the %d byte per-object limit", size, c.maxObj)
	}
	s := c.shardFor(k)
	if size > s.maxBytes {
		// The object fits the per-object limit but not one shard's share of the
		// budget. Storing it would evict the entire shard for one object.
		c.rejections.Add(1)
		return errs.New(errs.ClassTooLarge,
			"l1: object %d bytes exceeds the %d byte per-shard budget", size, s.maxBytes)
	}

	var evicted []*cache.Object
	var replaced *cache.Object

	s.mu.Lock()
	if old, ok := s.items[k]; ok {
		replaced = old.obj
		c.removeLocked(s, old)
	}
	// Evict until the object fits, consulting the admission policy for the
	// first victim only: once the policy accepts the candidate, the remaining
	// evictions are simply the cost of making room for it.
	admitted := true
	for s.bytes+size > s.maxBytes {
		back := s.lru.Back()
		if back == nil {
			break
		}
		victim := back.Value.(*entry)
		if admitted && len(evicted) == 0 && !c.admitter.Admit(k, victim.key) {
			admitted = false
			break
		}
		c.removeLocked(s, victim)
		evicted = append(evicted, victim.obj)
	}
	if !admitted {
		s.mu.Unlock()
		c.rejections.Add(1)
		if replaced != nil {
			c.notifyEvict(replaced, cache.EvictionReplaced)
		}
		for _, o := range evicted {
			c.notifyEvict(o, cache.EvictionCapacity)
		}
		return nil
	}
	e := &entry{key: k, obj: obj, size: size}
	e.elem = s.lru.PushFront(e)
	s.items[k] = e
	s.bytes += size
	s.mu.Unlock()

	c.puts.Add(1)
	c.evictions.Add(int64(len(evicted)))
	if replaced != nil {
		c.notifyEvict(replaced, cache.EvictionReplaced)
	}
	for _, o := range evicted {
		c.notifyEvict(o, cache.EvictionCapacity)
	}
	return nil
}

// Delete removes k, reporting whether anything was removed.
func (c *Cache) Delete(k string) bool {
	s := c.shardFor(k)
	s.mu.Lock()
	e, ok := s.items[k]
	if ok {
		c.removeLocked(s, e)
	}
	s.mu.Unlock()
	if ok {
		c.deletes.Add(1)
		c.notifyEvict(e.obj, cache.EvictionPurged)
	}
	return ok
}

// DeleteRoute removes every object belonging to routeID and reports the count.
//
// It walks every shard, which is O(objects). Route purge is an administrative
// operation, not a hot path, and walking is far cheaper than maintaining a
// secondary index on every Put.
func (c *Cache) DeleteRoute(routeID string) int {
	prefix := key.RoutePrefix(routeID)
	var purged []*cache.Object

	for _, s := range c.shards {
		s.mu.Lock()
		for k, e := range s.items {
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				c.removeLocked(s, e)
				purged = append(purged, e.obj)
			}
		}
		s.mu.Unlock()
	}
	c.deletes.Add(int64(len(purged)))
	for _, o := range purged {
		c.notifyEvict(o, cache.EvictionPurged)
	}
	return len(purged)
}

// Clear removes everything and reports the count.
func (c *Cache) Clear() int {
	var purged []*cache.Object
	for _, s := range c.shards {
		s.mu.Lock()
		for _, e := range s.items {
			purged = append(purged, e.obj)
		}
		s.items = make(map[string]*entry)
		s.lru.Init()
		s.bytes = 0
		s.mu.Unlock()
	}
	c.deletes.Add(int64(len(purged)))
	for _, o := range purged {
		c.notifyEvict(o, cache.EvictionPurged)
	}
	return len(purged)
}

// removeLocked drops e from s. The caller must hold s.mu.
func (c *Cache) removeLocked(s *shard, e *entry) {
	delete(s.items, e.key)
	s.lru.Remove(e.elem)
	s.bytes -= e.size
	if s.bytes < 0 {
		// Byte accounting must never go negative; if it does the size function
		// disagreed with itself between Put and remove, which is a bug worth
		// containing rather than propagating.
		s.bytes = 0
	}
}

func (c *Cache) notifyEvict(obj *cache.Object, reason cache.EvictionReason) {
	if c.onEvict != nil && obj != nil {
		c.onEvict(obj, reason)
	}
}

// Stats returns a counter snapshot.
func (c *Cache) Stats() Stats {
	var objects, bytes int64
	for _, s := range c.shards {
		s.mu.Lock()
		objects += int64(len(s.items))
		bytes += s.bytes
		s.mu.Unlock()
	}
	return Stats{
		Objects:    objects,
		Bytes:      bytes,
		MaxBytes:   c.maxBytes,
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		Expired:    c.expired.Load(),
		Evictions:  c.evictions.Load(),
		Rejections: c.rejections.Load(),
		Puts:       c.puts.Load(),
		Deletes:    c.deletes.Load(),
	}
}

// Len reports the object count.
func (c *Cache) Len() int {
	n := 0
	for _, s := range c.shards {
		s.mu.Lock()
		n += len(s.items)
		s.mu.Unlock()
	}
	return n
}

// Bytes reports the accounted byte usage.
func (c *Cache) Bytes() int64 {
	var n int64
	for _, s := range c.shards {
		s.mu.Lock()
		n += s.bytes
		s.mu.Unlock()
	}
	return n
}

// sweepLoop reclaims expired objects on a bounded schedule. One shard's lock is
// held at a time and each pass is O(objects in that shard), so the sweep never
// blocks the whole cache.
func (c *Cache) sweepLoop(interval time.Duration) {
	defer c.wg.Done()
	t := c.clk.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.stopped:
			return
		case <-t.C():
			c.Sweep()
		}
	}
}

// Sweep removes expired objects and reports the count. It is exported so tests
// can drive expiry deterministically instead of waiting for the ticker.
func (c *Cache) Sweep() int {
	now := c.clk.Now()
	var expired []*cache.Object
	for _, s := range c.shards {
		s.mu.Lock()
		for _, e := range s.items {
			if e.obj.Expired(now) {
				c.removeLocked(s, e)
				expired = append(expired, e.obj)
			}
		}
		s.mu.Unlock()
	}
	c.expired.Add(int64(len(expired)))
	for _, o := range expired {
		c.notifyEvict(o, cache.EvictionExpired)
	}
	return len(expired)
}

// Close stops the sweep goroutine and waits for it. It is idempotent.
func (c *Cache) Close(ctx context.Context) error {
	c.stopOnce.Do(func() { close(c.stopped) })
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errs.Wrap(errs.ClassTimeout, ctx.Err(), "l1: cache sweep did not stop in time")
	}
}
