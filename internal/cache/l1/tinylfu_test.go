package l1

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/cache"
)

func TestTinyLFUDoorkeeperIgnoresFirstSighting(t *testing.T) {
	tl := NewTinyLFU(1024)
	tl.Touch("once")
	if got := tl.Estimate("once"); got != 0 {
		t.Fatalf("a single sighting must not enter the sketch, estimate=%d", got)
	}
	tl.Touch("once")
	if got := tl.Estimate("once"); got == 0 {
		t.Fatal("a second sighting must be recorded")
	}
}

func TestTinyLFUEstimateOrdersByFrequency(t *testing.T) {
	tl := NewTinyLFU(4096)
	for i := 0; i < 200; i++ {
		tl.Touch("hot")
	}
	for i := 0; i < 3; i++ {
		tl.Touch("warm")
	}
	tl.Touch("cold")

	hot, warm, cold := tl.Estimate("hot"), tl.Estimate("warm"), tl.Estimate("cold")
	if hot <= warm || warm <= cold {
		t.Fatalf("frequency order broken: hot=%d warm=%d cold=%d", hot, warm, cold)
	}
}

func TestTinyLFUAdmitsWhenShardHasRoom(t *testing.T) {
	tl := NewTinyLFU(1024)
	if !tl.Admit("anything", "") {
		t.Fatal("an empty victim means the shard has room; the candidate must be admitted")
	}
}

func TestTinyLFURejectsColdCandidateAgainstHotVictim(t *testing.T) {
	tl := NewTinyLFU(4096)
	for i := 0; i < 100; i++ {
		tl.Touch("hot")
	}
	tl.Touch("cold")
	tl.Touch("cold")

	if tl.Admit("cold", "hot") {
		t.Fatal("a cold candidate must not displace a hot victim")
	}
	if !tl.Admit("hot", "cold") {
		t.Fatal("a hot candidate must displace a cold victim")
	}
}

func TestTinyLFUAgingDecaysCounters(t *testing.T) {
	// A small sketch reaches its aging threshold quickly.
	tl := NewTinyLFU(128)
	for i := 0; i < 40; i++ {
		tl.Touch("hot")
	}
	before := tl.Estimate("hot")

	// Drive a large volume of unrelated traffic, which triggers aging passes.
	for i := 0; i < 20000; i++ {
		k := fmt.Sprintf("noise-%d", i)
		tl.Touch(k)
		tl.Touch(k)
	}
	after := tl.Estimate("hot")
	if after >= before && before == 15 {
		// Saturated counters may stay saturated if aging never ran; that is the
		// failure this test guards against.
		t.Fatalf("aging never decayed the counter: before=%d after=%d", before, after)
	}
}

func TestTinyLFUCountersSaturateWithoutOverflow(t *testing.T) {
	tl := NewTinyLFU(1 << 16)
	for i := 0; i < 100000; i++ {
		tl.Touch("k")
	}
	if got := tl.Estimate("k"); got < 0 || got > maxCount {
		t.Fatalf("counter escaped its 4-bit range: %d", got)
	}
}

// The reason TinyLFU exists: a scan of unique keys must not evict a working set
// that LRU would lose. This is the comparison the PRD asks the policy
// abstraction to make possible.
func TestTinyLFUResistsScanPollutionBetterThanLRU(t *testing.T) {
	const (
		workingSet = 200
		scanKeys   = 5000
		bodyBytes  = 64
	)

	build := func(a Admitter) *Cache {
		c, err := New(Options{
			MaxBytes:       220 * 512,
			Shards:         1,
			MaxObjectBytes: 4096,
			Admitter:       a,
		})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	put := func(c *Cache, k string) {
		now := time.Now()
		_ = c.Put(k, &cache.Object{
			Key: k, RouteID: "r", Status: 200,
			Header:     http.Header{},
			Body:       make([]byte, bodyBytes),
			StoredAt:   now,
			ExpiresAt:  now.Add(time.Hour),
			StaleUntil: now.Add(time.Hour),
		})
	}

	run := func(a Admitter) float64 {
		c := build(a)
		defer c.Close(context.Background()) //nolint:errcheck

		hotKey := func(i int) string { return fmt.Sprintf("hot-%d", i) }

		// Warm the working set hard so its frequency is unambiguous.
		for round := 0; round < 30; round++ {
			for i := 0; i < workingSet; i++ {
				k := hotKey(i)
				if _, ok := c.Get(k); !ok {
					put(c, k)
				}
			}
		}
		// Scan: every key is seen once and never again.
		for i := 0; i < scanKeys; i++ {
			k := fmt.Sprintf("scan-%d", i)
			if _, ok := c.Get(k); !ok {
				put(c, k)
			}
		}
		// Measure how much of the working set survived the scan.
		survived := 0
		for i := 0; i < workingSet; i++ {
			if _, ok := c.Peek(hotKey(i)); ok {
				survived++
			}
		}
		return float64(survived) / workingSet
	}

	lru := run(alwaysAdmit{})
	tlfu := run(NewTinyLFU(workingSet + scanKeys))
	t.Logf("working set retained after scan: lru=%.1f%% tinylfu=%.1f%%", lru*100, tlfu*100)

	if tlfu <= lru {
		t.Fatalf("tinylfu retained %.1f%% of the working set, no better than lru's %.1f%%", tlfu*100, lru*100)
	}
}

// Zipf is the realistic web-cache workload; the policy comparison the PRD asks
// for is reported here rather than asserted as a fixed number, because the
// winner is workload-dependent.
func TestPolicyHitRatioOnZipfWorkload(t *testing.T) {
	const (
		universe   = 20000
		requests   = 200000
		bodyBytes  = 64
		cacheBytes = 1500 * 512
	)

	run := func(a Admitter) float64 {
		c, err := New(Options{
			MaxBytes: cacheBytes, Shards: 8, MaxObjectBytes: 4096, Admitter: a,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(context.Background()) //nolint:errcheck

		// A fixed seed keeps the comparison reproducible run to run.
		z := rand.NewZipf(rand.New(rand.NewSource(42)), 1.07, 1, universe-1)
		hits := 0
		now := time.Now()
		for i := 0; i < requests; i++ {
			k := fmt.Sprintf("z-%d", z.Uint64())
			if _, ok := c.Get(k); ok {
				hits++
				continue
			}
			_ = c.Put(k, &cache.Object{
				Key: k, RouteID: "r", Status: 200,
				Header:     http.Header{},
				Body:       make([]byte, bodyBytes),
				StoredAt:   now,
				ExpiresAt:  now.Add(time.Hour),
				StaleUntil: now.Add(time.Hour),
			})
		}
		return float64(hits) / requests
	}

	lru := run(alwaysAdmit{})
	tlfu := run(NewTinyLFU(universe))
	t.Logf("zipf(s=1.07, n=%d) hit ratio over %d requests: lru=%.4f tinylfu=%.4f",
		universe, requests, lru, tlfu)

	// Both policies must actually cache something; the relative winner is
	// reported, not asserted, because it depends on the workload.
	if lru < 0.2 || tlfu < 0.2 {
		t.Fatalf("implausibly low hit ratios: lru=%.4f tinylfu=%.4f", lru, tlfu)
	}
	if math.IsNaN(lru) || math.IsNaN(tlfu) {
		t.Fatal("hit ratio is NaN")
	}
}

func BenchmarkTinyLFUTouch(b *testing.B) {
	tl := NewTinyLFU(1 << 16)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tl.Touch(keys[i&4095])
	}
}

func BenchmarkTinyLFUAdmit(b *testing.B) {
	tl := NewTinyLFU(1 << 16)
	for i := 0; i < 10000; i++ {
		tl.Touch(fmt.Sprintf("k-%d", i%512))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tl.Admit("k-1", "k-2")
	}
}
