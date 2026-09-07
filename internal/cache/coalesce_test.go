package cache

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

func testObject(key string) *Object {
	return &Object{Key: key, Status: 200, Header: http.Header{}, Body: []byte("body")}
}

func TestCoalescerCollapsesConcurrentMisses(t *testing.T) {
	c := NewCoalescer(context.Background())

	var fills atomic.Int64
	release := make(chan struct{})

	const callers = 50
	var wg sync.WaitGroup
	results := make([]*Object, callers)
	owners := make([]bool, callers)

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			obj, owner, err := c.Do(context.Background(), "k", func(ctx context.Context) (*Object, error) {
				fills.Add(1)
				<-release
				return testObject("k"), nil
			})
			if err != nil {
				t.Errorf("caller %d: %v", i, err)
				return
			}
			results[i], owners[i] = obj, owner
		}(i)
	}

	// Wait for every caller to be parked on the single fill.
	deadline := time.Now().Add(2 * time.Second)
	for c.Waiters() < callers && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()

	if got := fills.Load(); got != 1 {
		t.Fatalf("%d origin fills for one key; coalescing failed", got)
	}
	ownerCount := 0
	for i, obj := range results {
		if obj == nil {
			t.Fatalf("caller %d got no object", i)
		}
		if owners[i] {
			ownerCount++
		}
	}
	if ownerCount != 1 {
		t.Fatalf("%d callers claimed ownership of the fill, want 1", ownerCount)
	}
	if c.Shared() != callers-1 {
		t.Fatalf("Shared() = %d, want %d", c.Shared(), callers-1)
	}
	if c.Fills() != 1 {
		t.Fatalf("Fills() = %d, want 1", c.Fills())
	}
}

func TestCoalescerKeysAreIndependent(t *testing.T) {
	c := NewCoalescer(context.Background())
	var fills atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i))
			_, _, err := c.Do(context.Background(), key, func(ctx context.Context) (*Object, error) {
				fills.Add(1)
				return testObject(key), nil
			})
			if err != nil {
				t.Errorf("key %s: %v", key, err)
			}
		}(i)
	}
	wg.Wait()

	if got := fills.Load(); got != 10 {
		t.Fatalf("%d fills for 10 distinct keys, want 10", got)
	}
}

// The property that motivates owning this instead of using singleflight: one
// waiter giving up must not cancel a fill the others still need.
func TestAbandonedWaiterDoesNotCancelSharedFill(t *testing.T) {
	c := NewCoalescer(context.Background())

	fillStarted := make(chan struct{})
	release := make(chan struct{})
	var fillCanceled atomic.Bool

	// The impatient caller starts the fill and then leaves.
	shortCtx, shortCancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := c.Do(shortCtx, "k", func(ctx context.Context) (*Object, error) {
			close(fillStarted)
			select {
			case <-release:
				return testObject("k"), nil
			case <-ctx.Done():
				fillCanceled.Store(true)
				return nil, ctx.Err()
			}
		})
		firstDone <- err
	}()
	<-fillStarted

	// A patient caller joins the same fill.
	secondDone := make(chan *Object, 1)
	secondErr := make(chan error, 1)
	go func() {
		obj, owner, err := c.Do(context.Background(), "k", func(ctx context.Context) (*Object, error) {
			t.Error("the second caller must not start its own fill")
			return nil, nil
		})
		if owner {
			t.Error("the second caller must not own the fill")
		}
		secondErr <- err
		secondDone <- obj
	}()

	// Wait for the second caller to be parked, then abandon the first.
	deadline := time.Now().Add(2 * time.Second)
	for c.Waiters() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	shortCancel()

	if err := <-firstDone; err == nil {
		t.Fatal("the abandoning caller should have received its context error")
	} else if !errs.IsClass(err, errs.ClassTimeout) {
		t.Fatalf("abandoning caller error class = %q, want timeout", errs.ClassOf(err))
	}

	// The fill must still be running for the remaining waiter.
	time.Sleep(20 * time.Millisecond)
	if fillCanceled.Load() {
		t.Fatal("the shared fill was canceled when one waiter left")
	}

	close(release)
	if err := <-secondErr; err != nil {
		t.Fatalf("the patient caller failed: %v", err)
	}
	if obj := <-secondDone; obj == nil {
		t.Fatal("the patient caller got no object")
	}
}

// When every waiter leaves there is nobody to serve, so the fill is reclaimed.
func TestFillIsCanceledWhenAllWaitersLeave(t *testing.T) {
	c := NewCoalescer(context.Background())

	fillStarted := make(chan struct{})
	canceled := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = c.Do(ctx, "k", func(fillCtx context.Context) (*Object, error) {
			close(fillStarted)
			<-fillCtx.Done()
			close(canceled)
			return nil, fillCtx.Err()
		})
	}()

	<-fillStarted
	cancel()
	<-done

	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("the fill was not reclaimed after its last waiter left")
	}
}

// Each waiter's own deadline applies independently of the fill's.
func TestWaitersHaveIndependentDeadlines(t *testing.T) {
	c := NewCoalescer(context.Background())

	release := make(chan struct{})
	defer close(release)

	started := make(chan struct{})
	go func() {
		_, _, _ = c.Do(context.Background(), "k", func(ctx context.Context) (*Object, error) {
			close(started)
			<-release
			return testObject("k"), nil
		})
	}()
	<-started

	// A waiter with a short deadline gives up on its own schedule.
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, err := c.Do(short, "k", func(context.Context) (*Object, error) {
		t.Error("a joining waiter must not start a fill")
		return nil, nil
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("the short-deadline waiter should have timed out")
	}
	if elapsed > time.Second {
		t.Fatalf("the waiter's own deadline was not honoured: waited %s", elapsed)
	}
}

func TestFillErrorReachesEveryWaiter(t *testing.T) {
	c := NewCoalescer(context.Background())
	sentinel := errors.New("origin exploded")
	release := make(chan struct{})

	var wg sync.WaitGroup
	errsSeen := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := c.Do(context.Background(), "k", func(context.Context) (*Object, error) {
				<-release
				return nil, sentinel
			})
			errsSeen[i] = err
		}(i)
	}
	deadline := time.Now().Add(2 * time.Second)
	for c.Waiters() < 10 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()

	for i, err := range errsSeen {
		if !errors.Is(err, sentinel) {
			t.Fatalf("waiter %d got %v, want the fill's error", i, err)
		}
	}
}

// A panic inside a fill must not leave waiters blocked forever.
func TestPanicInFillIsContained(t *testing.T) {
	c := NewCoalescer(context.Background())

	done := make(chan error, 1)
	go func() {
		_, _, err := c.Do(context.Background(), "k", func(context.Context) (*Object, error) {
			panic("boom")
		})
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a panicking fill must produce an error")
		}
		if !errs.IsClass(err, errs.ClassOriginFailure) {
			t.Fatalf("error class = %q, want origin_failure", errs.ClassOf(err))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a panicking fill left its waiter blocked")
	}

	// The coalescer must remain usable afterwards.
	obj, _, err := c.Do(context.Background(), "k", func(context.Context) (*Object, error) {
		return testObject("k"), nil
	})
	if err != nil || obj == nil {
		t.Fatalf("coalescer unusable after a panic: obj=%v err=%v", obj, err)
	}
}

// A completed key must be removed so the next miss triggers a fresh fill
// rather than replaying a stale result forever.
func TestCompletedCallsAreReleased(t *testing.T) {
	c := NewCoalescer(context.Background())
	for i := 0; i < 5; i++ {
		if _, _, err := c.Do(context.Background(), "k", func(context.Context) (*Object, error) {
			return testObject("k"), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if c.InFlight() != 0 {
		t.Fatalf("%d calls still registered after completion", c.InFlight())
	}
	if c.Fills() != 5 {
		t.Fatalf("Fills() = %d; sequential calls must each fill", c.Fills())
	}
}

// Run with -race.
func TestCoalescerConcurrentMixedKeys(t *testing.T) {
	c := NewCoalescer(context.Background())
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := string(rune('a' + (i % 8)))
				ctx := context.Background()
				if i%7 == 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, time.Microsecond)
					defer cancel()
				}
				_, _, _ = c.Do(ctx, key, func(context.Context) (*Object, error) {
					return testObject(key), nil
				})
			}
		}(w)
	}
	wg.Wait()
	// Timed-out waiters return before the fill goroutine deletes the call.
	// Drain, don't assert instant emptiness — that's a race, not a leak.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (c.InFlight() != 0 || c.Waiters() != 0) {
		time.Sleep(time.Millisecond)
	}
	if c.InFlight() != 0 {
		t.Fatalf("%d calls leaked", c.InFlight())
	}
	if c.Waiters() != 0 {
		t.Fatalf("waiter count leaked: %d", c.Waiters())
	}
}

func BenchmarkCoalescerUncontended(b *testing.B) {
	c := NewCoalescer(context.Background())
	obj := testObject("k")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := c.Do(context.Background(), "k", func(context.Context) (*Object, error) {
			return obj, nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}
