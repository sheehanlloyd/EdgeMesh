// Package breaker implements a per-origin circuit breaker.
//
// The breaker converts a slow, failing origin into a fast local failure. Without
// it every request pays the full origin timeout before failing, which converts
// one unhealthy origin into exhausted proxy concurrency.
//
// # States
//
//	closed    -> requests flow; failures are counted in a sliding window
//	open      -> requests fail immediately; a cooldown timer runs
//	half_open -> a bounded number of probes are admitted to test recovery
//
// Breaker state is local to an edge node. It is derived from that node's own
// observations and is explicitly not consensus data: replicating it would put
// Raft on the request path for information that is cheap to rediscover.
package breaker

import (
	"sync"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// State is the breaker's current mode.
type State string

const (
	StateClosed   State = "closed"
	StateOpen     State = "open"
	StateHalfOpen State = "half_open"
)

// Options configures a Breaker.
type Options struct {
	// FailureThreshold is the number of failures within the window that opens
	// the breaker.
	FailureThreshold int
	// FailureRatio, when positive, additionally opens the breaker once the
	// failure ratio over at least MinimumRequests samples exceeds it. This
	// catches a high-volume origin failing 60% of the time, which a raw count
	// threshold would open on too slowly.
	FailureRatio float64
	// MinimumRequests is the sample floor for ratio evaluation.
	MinimumRequests int
	// Window is the sliding observation window.
	Window time.Duration
	// Cooldown is how long the breaker stays open before admitting probes.
	Cooldown time.Duration
	// HalfOpenProbes bounds concurrent probe requests in half-open state.
	HalfOpenProbes int
	// SuccessesToClose is how many consecutive probe successes close the
	// breaker.
	SuccessesToClose int
	Clock            clock.Clock
	// OnTransition is called after every state change, outside the lock. It
	// must not block.
	OnTransition func(from, to State)
}

func (o *Options) applyDefaults() {
	if o.FailureThreshold <= 0 {
		o.FailureThreshold = 5
	}
	if o.MinimumRequests <= 0 {
		o.MinimumRequests = 10
	}
	if o.Window <= 0 {
		o.Window = 10 * time.Second
	}
	if o.Cooldown <= 0 {
		o.Cooldown = 5 * time.Second
	}
	if o.HalfOpenProbes <= 0 {
		o.HalfOpenProbes = 1
	}
	if o.SuccessesToClose <= 0 {
		o.SuccessesToClose = 2
	}
	if o.Clock == nil {
		o.Clock = clock.New()
	}
}

// Breaker is a single circuit breaker.
type Breaker struct {
	opts Options

	mu    sync.Mutex
	state State
	// window holds outcome timestamps for the sliding window. It is bounded by
	// pruning on every observation, so a high-throughput origin does not grow
	// it without limit.
	failures []time.Time
	requests []time.Time
	// openedAt is when the breaker entered the open state.
	openedAt time.Time
	// probesInFlight bounds concurrent half-open probes.
	probesInFlight int
	// consecutiveProbeSuccesses counts successes since entering half-open.
	consecutiveProbeSuccesses int

	transitions map[State]uint64
}

// maxWindowSamples bounds the sliding-window slices regardless of throughput.
// Beyond this the oldest samples are dropped: the ratio over the most recent
// N observations is what the breaker actually needs.
const maxWindowSamples = 4096

// New builds a breaker.
func New(o Options) *Breaker {
	o.applyDefaults()
	return &Breaker{
		opts:        o,
		state:       StateClosed,
		transitions: make(map[State]uint64, 3),
	}
}

// ErrOpen is returned by Allow when the breaker is rejecting requests.
var ErrOpen = errs.New(errs.ClassCircuitOpen, "circuit breaker is open")

// Allow reports whether a request may proceed.
//
// When it returns nil the caller must call exactly one of Success or Failure
// for that request, otherwise a half-open probe slot leaks and the breaker
// never recovers.
func (b *Breaker) Allow() error {
	now := b.opts.Clock.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return nil

	case StateOpen:
		if now.Sub(b.openedAt) < b.opts.Cooldown {
			return ErrOpen
		}
		// The cooldown has elapsed: move to half-open and admit this request as
		// the first probe.
		b.transitionLocked(StateHalfOpen, now)
		b.probesInFlight = 1
		return nil

	case StateHalfOpen:
		if b.probesInFlight >= b.opts.HalfOpenProbes {
			return ErrOpen
		}
		b.probesInFlight++
		return nil
	}
	return nil
}

// Success records a successful request.
func (b *Breaker) Success() {
	now := b.opts.Clock.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateHalfOpen:
		if b.probesInFlight > 0 {
			b.probesInFlight--
		}
		b.consecutiveProbeSuccesses++
		if b.consecutiveProbeSuccesses >= b.opts.SuccessesToClose {
			b.transitionLocked(StateClosed, now)
		}
	case StateClosed:
		b.requests = appendSample(b.requests, now)
		b.pruneLocked(now)
	}
}

// Failure records a failed request and may open the breaker.
func (b *Breaker) Failure() {
	now := b.opts.Clock.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateHalfOpen:
		if b.probesInFlight > 0 {
			b.probesInFlight--
		}
		// A single probe failure re-opens: the origin has not recovered and
		// there is no value in spending more probes on it this cooldown.
		b.transitionLocked(StateOpen, now)

	case StateClosed:
		b.failures = appendSample(b.failures, now)
		b.requests = appendSample(b.requests, now)
		b.pruneLocked(now)
		if b.shouldOpenLocked() {
			b.transitionLocked(StateOpen, now)
		}
	}
}

func appendSample(s []time.Time, t time.Time) []time.Time {
	if len(s) >= maxWindowSamples {
		// Drop the oldest half in one copy rather than shifting on every append.
		s = append(s[:0], s[len(s)/2:]...)
	}
	return append(s, t)
}

func (b *Breaker) shouldOpenLocked() bool {
	if len(b.failures) >= b.opts.FailureThreshold {
		return true
	}
	if b.opts.FailureRatio > 0 && len(b.requests) >= b.opts.MinimumRequests {
		if float64(len(b.failures))/float64(len(b.requests)) >= b.opts.FailureRatio {
			return true
		}
	}
	return false
}

// pruneLocked drops samples that have aged out of the window.
func (b *Breaker) pruneLocked(now time.Time) {
	cutoff := now.Add(-b.opts.Window)
	b.failures = pruneBefore(b.failures, cutoff)
	b.requests = pruneBefore(b.requests, cutoff)
}

func pruneBefore(s []time.Time, cutoff time.Time) []time.Time {
	i := 0
	for i < len(s) && !s[i].After(cutoff) {
		i++
	}
	if i == 0 {
		return s
	}
	return append(s[:0], s[i:]...)
}

// transitionLocked changes state and resets the per-state bookkeeping. The
// caller must hold b.mu; the notification runs after the lock is released.
func (b *Breaker) transitionLocked(to State, now time.Time) {
	from := b.state
	if from == to {
		return
	}
	b.state = to
	b.transitions[to]++

	switch to {
	case StateOpen:
		b.openedAt = now
		b.probesInFlight = 0
		b.consecutiveProbeSuccesses = 0
	case StateHalfOpen:
		b.probesInFlight = 0
		b.consecutiveProbeSuccesses = 0
	case StateClosed:
		b.failures = b.failures[:0]
		b.requests = b.requests[:0]
		b.probesInFlight = 0
		b.consecutiveProbeSuccesses = 0
	}

	if cb := b.opts.OnTransition; cb != nil {
		// Run the callback without the lock so a slow observer cannot stall the
		// request path.
		go cb(from, to)
	}
}

// State reports the current state, applying the cooldown transition so that a
// caller observing state without calling Allow sees an accurate value.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateOpen && b.opts.Clock.Now().Sub(b.openedAt) >= b.opts.Cooldown {
		return StateHalfOpen
	}
	return b.state
}

// Transitions reports how many times the breaker entered each state.
func (b *Breaker) Transitions() map[State]uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[State]uint64, len(b.transitions))
	for k, v := range b.transitions {
		out[k] = v
	}
	return out
}

// Reset returns the breaker to closed and clears its window. It exists for
// administrative recovery and tests, not for the request path.
func (b *Breaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.transitionLocked(StateClosed, b.opts.Clock.Now())
}

// Group is a set of breakers keyed by origin ID.
//
// The key space is bounded by configuration, not by client input, so unlike the
// rate limiter this map needs no eviction: an operator cannot configure an
// unbounded number of origins.
type Group struct {
	mu       sync.RWMutex
	breakers map[string]*Breaker
	opts     Options
	// onTransition receives the origin id alongside the transition, which the
	// per-breaker callback cannot supply because a breaker does not know its
	// own key.
	onTransition func(originID string, from, to State)
}

// NewGroup builds a breaker group sharing one configuration.
func NewGroup(o Options) *Group {
	o.applyDefaults()
	return &Group{breakers: make(map[string]*Breaker), opts: o}
}

// OnTransition registers a callback invoked for every breaker's state change,
// identified by origin. It must be set before the group is used and must not
// block: it runs on the request path's goroutine.
func (g *Group) OnTransition(fn func(originID string, from, to State)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.onTransition = fn
}

// Get returns the breaker for id, creating it on first use.
func (g *Group) Get(id string) *Breaker {
	g.mu.RLock()
	b, ok := g.breakers[id]
	g.mu.RUnlock()
	if ok {
		return b
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	// Re-check under the write lock: another goroutine may have won the race.
	if existing, ok := g.breakers[id]; ok {
		return existing
	}
	opts := g.opts
	// The group's callback is composed with any per-breaker one so both fire.
	perBreaker := opts.OnTransition
	groupCallback := g.onTransition
	opts.OnTransition = func(from, to State) {
		if perBreaker != nil {
			perBreaker(from, to)
		}
		if groupCallback != nil {
			groupCallback(id, from, to)
		}
	}
	b = New(opts)
	g.breakers[id] = b
	return b
}

// States returns a snapshot of every breaker's state.
func (g *Group) States() map[string]State {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make(map[string]State, len(g.breakers))
	for id, b := range g.breakers {
		out[id] = b.State()
	}
	return out
}

// Remove drops a breaker, used when an origin leaves the configuration.
func (g *Group) Remove(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.breakers, id)
}
