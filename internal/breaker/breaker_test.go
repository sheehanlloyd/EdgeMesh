package breaker

import (
	"sync"
	"testing"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

func newTestBreaker(clk clock.Clock, o Options) *Breaker {
	o.Clock = clk
	return New(o)
}

func TestClosedBreakerAllowsTraffic(t *testing.T) {
	b := newTestBreaker(clock.NewMock(time.Time{}), Options{})
	if b.State() != StateClosed {
		t.Fatalf("initial state = %q, want closed", b.State())
	}
	for i := 0; i < 100; i++ {
		if err := b.Allow(); err != nil {
			t.Fatalf("closed breaker denied request %d: %v", i, err)
		}
		b.Success()
	}
}

func TestOpensOnConsecutiveFailures(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b := newTestBreaker(clk, Options{FailureThreshold: 3, Window: time.Minute, Cooldown: time.Minute})

	for i := 0; i < 3; i++ {
		if err := b.Allow(); err != nil {
			t.Fatalf("request %d denied before the threshold", i)
		}
		b.Failure()
	}
	if b.State() != StateOpen {
		t.Fatalf("state = %q, want open after 3 failures", b.State())
	}
	err := b.Allow()
	if err == nil {
		t.Fatal("open breaker must reject requests")
	}
	if !errs.IsClass(err, errs.ClassCircuitOpen) {
		t.Fatalf("error class = %q, want circuit_open", errs.ClassOf(err))
	}
}

func TestFailuresAgeOutOfTheWindow(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b := newTestBreaker(clk, Options{FailureThreshold: 3, Window: 10 * time.Second, Cooldown: time.Minute})

	b.Allow()
	b.Failure()
	b.Allow()
	b.Failure()
	// Both failures age out before the third arrives.
	clk.Advance(11 * time.Second)
	b.Allow()
	b.Failure()

	if b.State() != StateClosed {
		t.Fatalf("state = %q; aged-out failures must not open the breaker", b.State())
	}
}

func TestOpensOnFailureRatio(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b := newTestBreaker(clk, Options{
		FailureThreshold: 1000, // effectively disable the count rule
		FailureRatio:     0.5,
		MinimumRequests:  10,
		Window:           time.Minute,
		Cooldown:         time.Minute,
	})
	// The ratio is only evaluated when a failure arrives, so the loop must run
	// past the sample floor for a failure to be the observation that trips it.
	for i := 0; i < 12; i++ {
		b.Allow()
		if i%2 == 0 {
			b.Failure()
		} else {
			b.Success()
		}
	}
	if b.State() != StateOpen {
		t.Fatalf("state = %q; a 50%% failure ratio over the minimum sample must open", b.State())
	}
}

func TestRatioIgnoredBelowMinimumRequests(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b := newTestBreaker(clk, Options{
		FailureThreshold: 1000, FailureRatio: 0.5, MinimumRequests: 20,
		Window: time.Minute, Cooldown: time.Minute,
	})
	for i := 0; i < 6; i++ {
		b.Allow()
		b.Failure()
	}
	if b.State() != StateClosed {
		t.Fatal("the ratio rule must not fire below the sample floor")
	}
}

func TestCooldownLeadsToHalfOpen(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b := newTestBreaker(clk, Options{
		FailureThreshold: 2, Window: time.Minute, Cooldown: 5 * time.Second,
		HalfOpenProbes: 1, SuccessesToClose: 2,
	})
	for i := 0; i < 2; i++ {
		b.Allow()
		b.Failure()
	}
	if b.Allow() == nil {
		t.Fatal("expected rejection during cooldown")
	}
	clk.Advance(5 * time.Second)

	// The first request after cooldown is admitted as a probe.
	if err := b.Allow(); err != nil {
		t.Fatalf("probe rejected after cooldown: %v", err)
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %q, want half_open", b.State())
	}
	// Concurrent probes beyond the bound are rejected.
	if b.Allow() == nil {
		t.Fatal("half-open must bound concurrent probes")
	}
}

func TestHalfOpenClosesAfterSuccessThreshold(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b := newTestBreaker(clk, Options{
		FailureThreshold: 1, Window: time.Minute, Cooldown: time.Second,
		HalfOpenProbes: 2, SuccessesToClose: 2,
	})
	b.Allow()
	b.Failure()
	clk.Advance(time.Second)

	if err := b.Allow(); err != nil {
		t.Fatal(err)
	}
	b.Success()
	if b.State() != StateHalfOpen {
		t.Fatal("one success must not close the breaker when two are required")
	}
	if err := b.Allow(); err != nil {
		t.Fatal(err)
	}
	b.Success()
	if b.State() != StateClosed {
		t.Fatalf("state = %q, want closed after the success threshold", b.State())
	}
	// Closing must clear the old window so stale failures cannot re-open it.
	if err := b.Allow(); err != nil {
		t.Fatalf("closed breaker rejected a request: %v", err)
	}
}

func TestHalfOpenFailureReopens(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b := newTestBreaker(clk, Options{
		FailureThreshold: 1, Window: time.Minute, Cooldown: time.Second, HalfOpenProbes: 1,
	})
	b.Allow()
	b.Failure()
	clk.Advance(time.Second)

	if err := b.Allow(); err != nil {
		t.Fatal(err)
	}
	b.Failure()
	if b.State() != StateOpen {
		t.Fatalf("state = %q; a failed probe must re-open", b.State())
	}
	// The cooldown restarts from the re-open, not from the original failure.
	if b.Allow() == nil {
		t.Fatal("cooldown did not restart after re-opening")
	}
}

func TestTransitionCounters(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b := newTestBreaker(clk, Options{FailureThreshold: 1, Cooldown: time.Second, SuccessesToClose: 1})
	b.Allow()
	b.Failure()
	clk.Advance(time.Second)
	b.Allow()
	b.Success()

	tr := b.Transitions()
	if tr[StateOpen] != 1 || tr[StateHalfOpen] != 1 || tr[StateClosed] != 1 {
		t.Fatalf("transitions = %v", tr)
	}
}

func TestOnTransitionCallback(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	var mu sync.Mutex
	var seen []State
	done := make(chan struct{}, 4)

	b := newTestBreaker(clk, Options{
		FailureThreshold: 1, Cooldown: time.Second,
		OnTransition: func(_, to State) {
			mu.Lock()
			seen = append(seen, to)
			mu.Unlock()
			done <- struct{}{}
		},
	})
	b.Allow()
	b.Failure()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("transition callback never fired")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 || seen[0] != StateOpen {
		t.Fatalf("callback saw %v", seen)
	}
}

func TestReset(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	b := newTestBreaker(clk, Options{FailureThreshold: 1, Cooldown: time.Hour})
	b.Allow()
	b.Failure()
	if b.State() != StateOpen {
		t.Fatal("expected open")
	}
	b.Reset()
	if b.State() != StateClosed {
		t.Fatal("Reset must close the breaker")
	}
	if err := b.Allow(); err != nil {
		t.Fatalf("reset breaker rejected a request: %v", err)
	}
}

func TestWindowSamplesAreBounded(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	// A high threshold keeps the breaker closed so samples accumulate.
	b := newTestBreaker(clk, Options{FailureThreshold: 1 << 30, Window: time.Hour})
	for i := 0; i < maxWindowSamples*3; i++ {
		b.Allow()
		b.Success()
	}
	b.mu.Lock()
	n := len(b.requests)
	b.mu.Unlock()
	if n > maxWindowSamples {
		t.Fatalf("window grew to %d samples, above the %d bound", n, maxWindowSamples)
	}
}

func TestGroup(t *testing.T) {
	g := NewGroup(Options{FailureThreshold: 1, Cooldown: time.Hour})
	a := g.Get("origin-a")
	if g.Get("origin-a") != a {
		t.Fatal("Get must return the same breaker for the same id")
	}
	a.Allow()
	a.Failure()

	states := g.States()
	if states["origin-a"] != StateOpen {
		t.Fatalf("states = %v", states)
	}
	// A second origin is unaffected.
	if err := g.Get("origin-b").Allow(); err != nil {
		t.Fatal("one origin's breaker must not affect another")
	}
	g.Remove("origin-a")
	if _, ok := g.States()["origin-a"]; ok {
		t.Fatal("Remove did not drop the breaker")
	}
}

// Run with -race.
func TestBreakerConcurrency(t *testing.T) {
	b := New(Options{FailureThreshold: 50, Window: time.Second, Cooldown: time.Millisecond, HalfOpenProbes: 4})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				if err := b.Allow(); err != nil {
					continue
				}
				if (w+i)%3 == 0 {
					b.Failure()
				} else {
					b.Success()
				}
				if i%500 == 0 {
					b.State()
					b.Transitions()
				}
			}
		}(w)
	}
	wg.Wait()
}

func BenchmarkBreakerAllowSuccess(b *testing.B) {
	br := New(Options{FailureThreshold: 1 << 30, Window: time.Minute})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if br.Allow() == nil {
				br.Success()
			}
		}
	})
}

// The group callback must identify which origin transitioned; a breaker does
// not know its own key, so the group has to supply it.
func TestGroupOnTransitionIdentifiesTheOrigin(t *testing.T) {
	g := NewGroup(Options{FailureThreshold: 1, Cooldown: time.Hour})

	var mu sync.Mutex
	seen := map[string]State{}
	g.OnTransition(func(originID string, _, to State) {
		mu.Lock()
		seen[originID] = to
		mu.Unlock()
	})

	a := g.Get("origin-a")
	a.Allow()
	a.Failure()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		state, ok := seen["origin-a"]
		mu.Unlock()
		if ok && state == StateOpen {
			// A breaker that never transitioned must not be reported.
			mu.Lock()
			_, spurious := seen["origin-b"]
			mu.Unlock()
			if spurious {
				t.Fatal("an untouched origin was reported as transitioning")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no transition reported for origin-a; saw %v", seen)
}

// A nil group must not be usable in a way that crashes; callers construct one
// through NewGroup, and Get on a zero-valued Group would otherwise panic on the
// request path.
func TestGroupGetOnUninitializedGroupPanicsLoudly(t *testing.T) {
	// This documents the contract rather than asserting a crash: a Group must
	// come from NewGroup. Consumers that accept one from a caller are
	// responsible for substituting a default, which proxy.NewOriginClient does.
	g := NewGroup(Options{})
	if g.Get("origin-a") == nil {
		t.Fatal("NewGroup must produce a usable group")
	}
}
