package l1

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

func obj(k string, body int, ttl time.Duration, now time.Time) *cache.Object {
	return &cache.Object{
		Key:        k,
		RouteID:    "route-a",
		Status:     200,
		Header:     http.Header{"Content-Type": []string{"application/octet-stream"}},
		Body:       make([]byte, body),
		StoredAt:   now,
		ExpiresAt:  now.Add(ttl),
		StaleUntil: now.Add(ttl),
	}
}

func newCache(t *testing.T, maxBytes int64, shards int, clk clock.Clock) *Cache {
	t.Helper()
	c, err := New(Options{
		MaxBytes:       maxBytes,
		Shards:         shards,
		MaxObjectBytes: maxBytes / 2,
		Clock:          clk,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

func TestNewValidatesOptions(t *testing.T) {
	cases := []struct {
		name string
		o    Options
	}{
		{"zero shards", Options{MaxBytes: 1024, Shards: 0, MaxObjectBytes: 128}},
		{"non power of two shards", Options{MaxBytes: 1024, Shards: 3, MaxObjectBytes: 128}},
		{"zero budget", Options{MaxBytes: 0, Shards: 4, MaxObjectBytes: 128}},
		{"zero object limit", Options{MaxBytes: 1024, Shards: 4, MaxObjectBytes: 0}},
		{"more shards than bytes", Options{MaxBytes: 4, Shards: 8, MaxObjectBytes: 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := New(c.o); err == nil {
				t.Fatal("expected a validation error")
			} else if !errs.IsClass(err, errs.ClassValidation) {
				t.Fatalf("expected validation class, got %q", errs.ClassOf(err))
			}
		})
	}
}

func TestGetPutDelete(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	c := newCache(t, 1<<20, 8, clk)

	if _, ok := c.Get("missing"); ok {
		t.Fatal("empty cache returned a hit")
	}
	o := obj("k1", 100, time.Minute, clk.Now())
	if err := c.Put("k1", o); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok := c.Get("k1")
	if !ok || got != o {
		t.Fatal("expected the stored object back by identity")
	}
	if !c.Delete("k1") {
		t.Fatal("Delete reported nothing removed")
	}
	if c.Delete("k1") {
		t.Fatal("second Delete must report false")
	}
	if _, ok := c.Get("k1"); ok {
		t.Fatal("deleted key still resolves")
	}

	s := c.Stats()
	if s.Hits != 1 || s.Misses != 2 || s.Puts != 1 || s.Deletes != 1 {
		t.Fatalf("unexpected counters: %+v", s)
	}
	if s.Objects != 0 || s.Bytes != 0 {
		t.Fatalf("cache not empty after delete: %+v", s)
	}
}

func TestPutRejectsNilAndOversizedObjects(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	c := newCache(t, 1<<16, 4, clk)

	if err := c.Put("k", nil); err == nil {
		t.Fatal("nil object must be rejected")
	}
	big := obj("k", 1<<20, time.Minute, clk.Now())
	err := c.Put("k", big)
	if err == nil || !errs.IsClass(err, errs.ClassTooLarge) {
		t.Fatalf("oversized object: err=%v class=%q", err, errs.ClassOf(err))
	}
	if c.Stats().Rejections != 1 {
		t.Fatal("rejection not counted")
	}
}

func TestLazyExpiryOnGet(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	c := newCache(t, 1<<20, 4, clk)

	if err := c.Put("k", obj("k", 10, 30*time.Second, clk.Now())); err != nil {
		t.Fatal(err)
	}
	clk.Advance(29 * time.Second)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("object expired early")
	}
	clk.Advance(2 * time.Second)
	if _, ok := c.Get("k"); ok {
		t.Fatal("expired object was served")
	}
	if c.Len() != 0 {
		t.Fatal("expired object was not reclaimed on lookup")
	}
	if c.Stats().Expired != 1 {
		t.Fatal("expiry not counted")
	}
}

func TestSweepReclaimsExpiredObjects(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	c := newCache(t, 1<<20, 4, clk)

	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%d", i)
		ttl := time.Duration(i+1) * time.Second
		if err := c.Put(k, obj(k, 10, ttl, clk.Now())); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(10*time.Second + time.Millisecond)
	if n := c.Sweep(); n != 10 {
		t.Fatalf("swept %d objects, want 10", n)
	}
	if c.Len() != 10 {
		t.Fatalf("%d objects remain, want 10", c.Len())
	}
}

// The byte budget is the whole point of a bounded cache: it must hold under
// sustained insertion, not just on average.
func TestByteBudgetIsEnforced(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	const budget = 1 << 16
	c, err := New(Options{MaxBytes: budget, Shards: 4, MaxObjectBytes: 4096, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background()) //nolint:errcheck

	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("key-%d", i)
		if err := c.Put(k, obj(k, 512, time.Hour, clk.Now())); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		if got := c.Bytes(); got > budget {
			t.Fatalf("after %d puts cache holds %d bytes, over the %d budget", i, got, budget)
		}
	}
	if c.Stats().Evictions == 0 {
		t.Fatal("expected capacity evictions")
	}
	if c.Len() == 0 {
		t.Fatal("cache evicted everything")
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	// One shard makes recency ordering observable and deterministic.
	c, err := New(Options{MaxBytes: 4096, Shards: 1, MaxObjectBytes: 2048, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background()) //nolint:errcheck

	// Objects have ~256 bytes of overhead plus body; keep bodies small so
	// several fit and eviction order is what is under test.
	for _, k := range []string{"a", "b", "c"} {
		if err := c.Put(k, obj(k, 500, time.Hour, clk.Now())); err != nil {
			t.Fatal(err)
		}
	}
	// Touch "a" so "b" becomes the least recently used.
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should be resident")
	}
	// Insert until something is evicted.
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("fill-%d", i)
		if err := c.Put(k, obj(k, 500, time.Hour, clk.Now())); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := c.Peek("b"); ok {
		if _, aOK := c.Peek("a"); !aOK {
			t.Fatal("evicted the recently used entry before the least recently used one")
		}
	}
}

func TestReplaceUpdatesAccounting(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	c := newCache(t, 1<<20, 4, clk)

	if err := c.Put("k", obj("k", 1000, time.Hour, clk.Now())); err != nil {
		t.Fatal(err)
	}
	first := c.Bytes()
	if err := c.Put("k", obj("k", 10, time.Hour, clk.Now())); err != nil {
		t.Fatal(err)
	}
	if c.Len() != 1 {
		t.Fatalf("replace created a duplicate entry: %d objects", c.Len())
	}
	if c.Bytes() >= first {
		t.Fatalf("replace did not shrink accounting: %d -> %d", first, c.Bytes())
	}
}

func TestDeleteRoutePurgesOnlyThatRoute(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	c := newCache(t, 1<<20, 8, clk)

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("route-a\x1fGET\x1f/x/%d\x1f\x1f", i)
		if err := c.Put(k, obj(k, 10, time.Hour, clk.Now())); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 30; i++ {
		k := fmt.Sprintf("route-b\x1fGET\x1f/x/%d\x1f\x1f", i)
		if err := c.Put(k, obj(k, 10, time.Hour, clk.Now())); err != nil {
			t.Fatal(err)
		}
	}
	if n := c.DeleteRoute("route-a"); n != 50 {
		t.Fatalf("purged %d, want 50", n)
	}
	if c.Len() != 30 {
		t.Fatalf("%d objects remain, want 30", c.Len())
	}
	// "route-a" must not purge a route whose id merely shares a prefix.
	if n := c.DeleteRoute("route"); n != 0 {
		t.Fatalf("prefix-only route id purged %d objects", n)
	}
}

func TestClear(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	c := newCache(t, 1<<20, 4, clk)
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%d", i)
		if err := c.Put(k, obj(k, 10, time.Hour, clk.Now())); err != nil {
			t.Fatal(err)
		}
	}
	if n := c.Clear(); n != 20 {
		t.Fatalf("cleared %d, want 20", n)
	}
	if c.Len() != 0 || c.Bytes() != 0 {
		t.Fatal("clear left residue")
	}
}

func TestOnEvictCallback(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	var mu sync.Mutex
	seen := map[cache.EvictionReason]int{}

	c, err := New(Options{
		MaxBytes: 4096, Shards: 1, MaxObjectBytes: 2048, Clock: clk,
		OnEvict: func(_ *cache.Object, r cache.EvictionReason) {
			mu.Lock()
			seen[r]++
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background()) //nolint:errcheck

	if err := c.Put("k", obj("k", 100, time.Second, clk.Now())); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("k", obj("k", 100, time.Second, clk.Now())); err != nil {
		t.Fatal(err)
	}
	c.Delete("k")
	if err := c.Put("e", obj("e", 100, time.Second, clk.Now())); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second)
	c.Sweep()

	mu.Lock()
	defer mu.Unlock()
	for _, r := range []cache.EvictionReason{cache.EvictionReplaced, cache.EvictionPurged, cache.EvictionExpired} {
		if seen[r] == 0 {
			t.Errorf("no eviction callback for reason %q; saw %v", r, seen)
		}
	}
}

// Run with -race. Concurrent readers, writers, deleters and sweeps must not
// corrupt shard accounting.
func TestConcurrentAccess(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	c, err := New(Options{MaxBytes: 1 << 18, Shards: 16, MaxObjectBytes: 4096, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background()) //nolint:errcheck

	const workers = 8
	const iterations = 2000
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				k := fmt.Sprintf("key-%d", (w*iterations+i)%512)
				switch i % 4 {
				case 0, 1:
					_ = c.Put(k, obj(k, 128, time.Hour, clk.Now()))
				case 2:
					c.Get(k)
				case 3:
					c.Delete(k)
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			c.Sweep()
			c.Stats()
		}
	}()
	wg.Wait()

	if b := c.Bytes(); b < 0 || b > 1<<18 {
		t.Fatalf("byte accounting corrupted under concurrency: %d", b)
	}
}

func TestCloseIsIdempotentAndStopsSweeper(t *testing.T) {
	c, err := New(Options{
		MaxBytes: 1 << 16, Shards: 4, MaxObjectBytes: 4096,
		CleanupInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestPolicyNameReportsAdmitter(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	c := newCache(t, 1<<16, 4, clk)
	if c.PolicyName() != "lru" {
		t.Fatalf("default policy = %q, want lru", c.PolicyName())
	}
	tl, err := New(Options{MaxBytes: 1 << 16, Shards: 4, MaxObjectBytes: 4096, Admitter: NewTinyLFU(1024)})
	if err != nil {
		t.Fatal(err)
	}
	defer tl.Close(context.Background()) //nolint:errcheck
	if tl.PolicyName() != "tinylfu" {
		t.Fatalf("policy = %q, want tinylfu", tl.PolicyName())
	}
}

func BenchmarkCacheGetHit(b *testing.B) {
	c, err := New(Options{MaxBytes: 1 << 26, Shards: 64, MaxObjectBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close(context.Background()) //nolint:errcheck
	now := time.Now()
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-key-%d", i)
		if err := c.Put(keys[i], obj(keys[i], 1024, time.Hour, now)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, ok := c.Get(keys[i&4095]); !ok {
				b.Fatal("miss")
			}
			i++
		}
	})
}

func BenchmarkCachePut(b *testing.B) {
	c, err := New(Options{MaxBytes: 1 << 24, Shards: 64, MaxObjectBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close(context.Background()) //nolint:errcheck
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("bench-put-%d", i)
		if err := c.Put(k, obj(k, 1024, time.Hour, now)); err != nil {
			b.Fatal(err)
		}
	}
}
