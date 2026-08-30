// Package clock provides an injectable time source.
//
// Correctness-sensitive components (rate limiter, circuit breaker, cache TTLs,
// Raft timers) take a Clock so their tests advance time deterministically
// instead of sleeping.
package clock

import (
	"sync"
	"time"
)

// Clock is the subset of the time package that EdgeMesh depends on.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	NewTimer(d time.Duration) Timer
	NewTicker(d time.Duration) Ticker
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}

// Timer mirrors *time.Timer.
type Timer interface {
	C() <-chan time.Time
	Reset(d time.Duration) bool
	Stop() bool
}

// Ticker mirrors *time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// System is the production Clock backed by the time package.
type System struct{}

// New returns the system clock.
func New() Clock { return System{} }

func (System) Now() time.Time                  { return time.Now() }
func (System) Since(t time.Time) time.Duration { return time.Since(t) }
func (System) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}
func (System) Sleep(d time.Duration) { time.Sleep(d) }
func (System) NewTimer(d time.Duration) Timer {
	return &systemTimer{t: time.NewTimer(d)}
}
func (System) NewTicker(d time.Duration) Ticker {
	return &systemTicker{t: time.NewTicker(d)}
}

type systemTimer struct{ t *time.Timer }

func (s *systemTimer) C() <-chan time.Time        { return s.t.C }
func (s *systemTimer) Reset(d time.Duration) bool { return s.t.Reset(d) }
func (s *systemTimer) Stop() bool                 { return s.t.Stop() }

type systemTicker struct{ t *time.Ticker }

func (s *systemTicker) C() <-chan time.Time { return s.t.C }
func (s *systemTicker) Stop()               { s.t.Stop() }

// Mock is a manually advanced Clock for deterministic tests.
//
// Timers and tickers created from a Mock fire when Advance moves the clock past
// their deadline. Fires are delivered on buffered channels so a test that is not
// currently receiving never deadlocks Advance.
type Mock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*mockWaiter
}

type mockWaiter struct {
	deadline time.Time
	period   time.Duration // non-zero for tickers
	ch       chan time.Time
	stopped  bool
}

// NewMock returns a Mock clock started at start. A zero start uses a fixed,
// arbitrary but stable instant so test output is reproducible.
func NewMock(start time.Time) *Mock {
	if start.IsZero() {
		start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &Mock{now: start}
}

func (m *Mock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *Mock) Since(t time.Time) time.Duration { return m.Now().Sub(t) }

func (m *Mock) Sleep(d time.Duration) { <-m.After(d) }

func (m *Mock) After(d time.Duration) <-chan time.Time {
	return m.newWaiter(d, 0).ch
}

func (m *Mock) NewTimer(d time.Duration) Timer {
	return &mockTimer{m: m, w: m.newWaiter(d, 0)}
}

func (m *Mock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive interval for NewTicker")
	}
	return &mockTicker{m: m, w: m.newWaiter(d, d)}
}

func (m *Mock) newWaiter(d time.Duration, period time.Duration) *mockWaiter {
	m.mu.Lock()
	defer m.mu.Unlock()
	w := &mockWaiter{
		deadline: m.now.Add(d),
		period:   period,
		ch:       make(chan time.Time, 1),
	}
	m.waiters = append(m.waiters, w)
	return w
}

// Advance moves the clock forward and fires every waiter whose deadline has
// passed. Tickers reschedule; timers are removed.
func (m *Mock) Advance(d time.Duration) {
	m.mu.Lock()
	m.now = m.now.Add(d)
	now := m.now
	live := m.waiters[:0]
	type fire struct {
		ch chan time.Time
		at time.Time
	}
	var fires []fire
	for _, w := range m.waiters {
		if w.stopped {
			continue
		}
		fired := false
		for !w.deadline.After(now) {
			fires = append(fires, fire{ch: w.ch, at: w.deadline})
			fired = true
			if w.period == 0 {
				break
			}
			w.deadline = w.deadline.Add(w.period)
		}
		if w.period != 0 || !fired {
			live = append(live, w)
		}
	}
	m.waiters = live
	m.mu.Unlock()

	for _, f := range fires {
		select {
		case f.ch <- f.at:
		default: // a pending tick is already queued; drop like time.Ticker does
		}
	}
}

// Set moves the clock to an absolute instant. It panics on backwards movement,
// which would violate monotonicity assumptions in the components under test.
func (m *Mock) Set(t time.Time) {
	m.mu.Lock()
	d := t.Sub(m.now)
	m.mu.Unlock()
	if d < 0 {
		panic("clock: mock clock cannot move backwards")
	}
	m.Advance(d)
}

type mockTimer struct {
	m *Mock
	w *mockWaiter
}

func (t *mockTimer) C() <-chan time.Time { return t.w.ch }

func (t *mockTimer) Reset(d time.Duration) bool {
	t.m.mu.Lock()
	active := !t.w.stopped && t.w.deadline.After(t.m.now)
	t.w.deadline = t.m.now.Add(d)
	if t.w.stopped {
		t.w.stopped = false
		t.m.waiters = append(t.m.waiters, t.w)
	} else {
		found := false
		for _, w := range t.m.waiters {
			if w == t.w {
				found = true
				break
			}
		}
		if !found {
			t.m.waiters = append(t.m.waiters, t.w)
		}
	}
	t.m.mu.Unlock()
	return active
}

func (t *mockTimer) Stop() bool {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	active := !t.w.stopped && t.w.deadline.After(t.m.now)
	t.w.stopped = true
	return active
}

type mockTicker struct {
	m *Mock
	w *mockWaiter
}

func (t *mockTicker) C() <-chan time.Time { return t.w.ch }

func (t *mockTicker) Stop() {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	t.w.stopped = true
}
