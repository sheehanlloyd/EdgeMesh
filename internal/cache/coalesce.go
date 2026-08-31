package cache

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// Coalescer collapses concurrent misses for the same key into one origin fill.
//
// # Why this is not x/sync/singleflight
//
// The requirement that makes this worth owning is cancellation semantics. In
// singleflight the fill inherits the *first* caller's context, so when that
// caller disconnects the fill is canceled and every other waiter, who may have
// far longer deadlines, fails with it. Under a thundering herd that turns one
// impatient client into a failure for everyone waiting behind it.
//
// Here the fill runs under a context derived from the Coalescer's lifetime with
// its own upper bound, and each waiter selects on its *own* context. A waiter
// leaving takes nothing with it. The fill is only abandoned when every waiter
// has gone, which is tracked by a reference count.
type Coalescer struct {
	mu    sync.Mutex
	calls map[string]*call
	// base bounds the lifetime of every fill; it is the process context so
	// fills stop at shutdown rather than outliving it.
	base context.Context

	waiters atomic.Int64
	fills   atomic.Int64
	// shared counts requests that were served by someone else's fill, which is
	// exactly the thundering-herd suppression this exists to demonstrate.
	shared atomic.Int64
}

// call is one in-flight fill.
type call struct {
	done chan struct{}
	// refs counts live waiters. The fill is canceled when it reaches zero,
	// which reclaims work nobody is waiting for without punishing anyone who
	// still is.
	refs   atomic.Int64
	cancel context.CancelFunc

	obj *Object
	err error
}

// NewCoalescer builds a coalescer whose fills are bounded by ctx.
//
// That is the entire reason this exists rather than x/sync/singleflight. See
// the package comment and docs/adr/009.
//
//nolint:contextcheck // Deliberate: fills must NOT inherit a caller's context.
func NewCoalescer(ctx context.Context) *Coalescer {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Coalescer{calls: make(map[string]*call), base: ctx}
}

// FillFunc produces an object for a key. It receives a context that is
// independent of any single waiter.
type FillFunc func(ctx context.Context) (*Object, error)

// Do returns the object for key, performing at most one concurrent fill.
//
// The returned bool reports whether this caller performed the fill (true) or
// waited on another caller's (false), which the caller records as a metric and
// surfaces in traces.
//
// The fill descends from the coalescer's base context, never from this caller's,
// so one caller leaving cannot cancel work the others still need.
//
//nolint:contextcheck // Deliberate: the fill must not inherit a caller's context.
func (c *Coalescer) Do(ctx context.Context, key string, fill FillFunc) (*Object, bool, error) {
	c.mu.Lock()
	if existing, ok := c.calls[key]; ok {
		existing.refs.Add(1)
		c.mu.Unlock()
		c.shared.Add(1)
		obj, err := c.wait(ctx, existing)
		return obj, false, err
	}

	// This caller owns the fill. The fill context descends from the coalescer's
	// base context, never from this caller's request context.
	fillCtx, cancel := context.WithCancel(c.base)
	cl := &call{done: make(chan struct{}), cancel: cancel}
	cl.refs.Store(1)
	c.calls[key] = cl
	c.mu.Unlock()

	c.fills.Add(1)
	go func() {
		defer func() {
			// A panic inside a fill must not leave every waiter blocked
			// forever on a channel that never closes.
			if r := recover(); r != nil {
				cl.err = errs.New(errs.ClassOriginFailure, "cache fill panicked: %v", r)
			}
			c.mu.Lock()
			if cur, ok := c.calls[key]; ok && cur == cl {
				delete(c.calls, key)
			}
			c.mu.Unlock()
			cancel()
			close(cl.done)
		}()
		cl.obj, cl.err = fill(fillCtx)
	}()

	obj, err := c.wait(ctx, cl)
	return obj, true, err
}

// wait blocks until the fill completes or this caller's context ends.
func (c *Coalescer) wait(ctx context.Context, cl *call) (*Object, error) {
	c.waiters.Add(1)
	defer c.waiters.Add(-1)

	select {
	case <-cl.done:
		cl.refs.Add(-1)
		return cl.obj, cl.err

	case <-ctx.Done():
		// This waiter is leaving. The fill continues for everyone else and is
		// canceled only when the last waiter has gone.
		if cl.refs.Add(-1) <= 0 {
			cl.cancel()
		}
		return nil, errs.Wrap(errs.ClassTimeout, ctx.Err(), "waiting for a coalesced cache fill")
	}
}

// Waiters reports how many callers are currently blocked on a fill.
func (c *Coalescer) Waiters() int64 { return c.waiters.Load() }

// Fills reports how many origin fills have been started.
func (c *Coalescer) Fills() int64 { return c.fills.Load() }

// Shared reports how many requests were served by another caller's fill.
func (c *Coalescer) Shared() int64 { return c.shared.Load() }

// InFlight reports how many distinct keys are currently being filled.
func (c *Coalescer) InFlight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}
