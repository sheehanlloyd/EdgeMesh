package ratelimit

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/clock"
)

func TestBucketValidation(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	if _, err := NewBucket(0, 1, clk); err == nil {
		t.Fatal("zero rate must be rejected")
	}
	if _, err := NewBucket(1, 0, clk); err == nil {
		t.Fatal("zero burst must be rejected")
	}
}

func TestBucketStartsFullAndDrains(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b, err := NewBucket(10, 5, clk)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if ok, _ := b.Allow(); !ok {
			t.Fatalf("burst token %d denied", i)
		}
	}
	ok, retry := b.Allow()
	if ok {
		t.Fatal("bucket must be empty after the burst")
	}
	if retry <= 0 {
		t.Fatal("a denial must report a positive Retry-After")
	}
}

func TestBucketRefillsAtConfiguredRate(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b, _ := NewBucket(10, 10, clk) // 10 tokens/sec
	for i := 0; i < 10; i++ {
		b.Allow()
	}
	if ok, _ := b.Allow(); ok {
		t.Fatal("expected an empty bucket")
	}
	// 500ms at 10/s yields 5 tokens.
	clk.Advance(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if ok, _ := b.Allow(); !ok {
			t.Fatalf("refilled token %d denied", i)
		}
	}
	if ok, _ := b.Allow(); ok {
		t.Fatal("refill exceeded the configured rate")
	}
}

func TestBucketDoesNotExceedBurst(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b, _ := NewBucket(100, 5, clk)
	clk.Advance(time.Hour)
	if got := b.Tokens(); got != 5 {
		t.Fatalf("tokens = %v, want the burst cap of 5", got)
	}
}

func TestRetryAfterIsSufficient(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b, _ := NewBucket(4, 1, clk) // one token per 250ms
	b.Allow()
	ok, retry := b.Allow()
	if ok {
		t.Fatal("expected denial")
	}
	// Obeying Retry-After must actually succeed, which is the contract the
	// header makes with the client.
	clk.Advance(retry)
	if ok, _ := b.Allow(); !ok {
		t.Fatalf("Retry-After of %s was insufficient", retry)
	}
}

func TestLimiterIsolatesKeys(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	l, err := New(Options{Rate: 1, Burst: 2, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow("client-a"); !ok {
			t.Fatal("client-a burst denied")
		}
	}
	if ok, _ := l.Allow("client-a"); ok {
		t.Fatal("client-a must be limited")
	}
	// A different key has its own bucket.
	if ok, _ := l.Allow("client-b"); !ok {
		t.Fatal("client-b must not be affected by client-a")
	}
}

func TestLimiterEnforcesKeyCap(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	l, _ := New(Options{Rate: 10, Burst: 10, MaxKeys: 16, Clock: clk})
	for i := 0; i < 1000; i++ {
		l.Allow(fmt.Sprintf("client-%d", i))
		if n := l.Len(); n > 16 {
			t.Fatalf("resident keys %d exceeded the cap of 16", n)
		}
	}
	if l.Stats().Evicted == 0 {
		t.Fatal("expected LRU evictions at the key cap")
	}
}

func TestLimiterSweepReclaimsIdleKeys(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	l, _ := New(Options{Rate: 10, Burst: 10, IdleTTL: time.Minute, Clock: clk})

	for i := 0; i < 50; i++ {
		l.Allow(fmt.Sprintf("old-%d", i))
	}
	clk.Advance(2 * time.Minute)
	for i := 0; i < 5; i++ {
		l.Allow(fmt.Sprintf("new-%d", i))
	}
	if n := l.Sweep(); n != 50 {
		t.Fatalf("swept %d keys, want 50", n)
	}
	if l.Len() != 5 {
		t.Fatalf("%d keys remain, want 5", l.Len())
	}
	// A second sweep must be a no-op.
	if n := l.Sweep(); n != 0 {
		t.Fatalf("second sweep removed %d keys", n)
	}
}

func TestLimiterStats(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	l, _ := New(Options{Rate: 1, Burst: 1, Clock: clk})
	l.Allow("k")
	l.Allow("k")
	s := l.Stats()
	if s.Allowed != 1 || s.Denied != 1 || s.Keys != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

// Run with -race.
func TestLimiterConcurrency(t *testing.T) {
	l, err := New(Options{Rate: 1000, Burst: 100, MaxKeys: 64})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				l.Allow(fmt.Sprintf("k-%d", i%128))
				if i%100 == 0 {
					l.Sweep()
					l.Stats()
				}
			}
		}(w)
	}
	wg.Wait()
	if n := l.Len(); n > 64 {
		t.Fatalf("key cap violated under concurrency: %d", n)
	}
}

func BenchmarkLimiterAllow(b *testing.B) {
	l, _ := New(Options{Rate: 1e9, Burst: 1 << 20, MaxKeys: 1024})
	keys := make([]string, 512)
	for i := range keys {
		keys[i] = fmt.Sprintf("client-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			l.Allow(keys[i&511])
			i++
		}
	})
}
