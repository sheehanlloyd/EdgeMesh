# ADR-009: A custom coalescer instead of singleflight

**Status:** Accepted

## Context

When many requests miss on the same cold key simultaneously, only one origin
fetch should happen. `golang.org/x/sync/singleflight` is the standard answer and
is already an accepted dependency for this project.

## Decision

Implement coalescing in `internal/cache/coalesce.go` rather than using
singleflight.

## Rationale

Singleflight's fill inherits the **first caller's context**. When that caller
disconnects, the fill is canceled and every other waiter fails with it.

Under exactly the load coalescing exists to handle (a thundering herd on a cold
object, where the first arrival is often the one that gives up first) one
impatient client becomes a failure for everyone waiting behind it. That is the
opposite of the intended behaviour.

## Design

- The fill runs under a context derived from the **process**, with its own upper
  bound, never from any single caller.
- Each waiter selects on its own context, so waiters have independent deadlines.
- A reference count tracks live waiters; the fill is canceled only when the
  **last** one leaves, so work nobody wants is still reclaimed.
- A panic inside a fill is recovered and converted to an error, so it cannot
  leave every waiter blocked on a channel that never closes.

## Consequences

**Good.** One caller leaving cannot fail the others. Waiters keep independent
deadlines. Waiter and fill counts are exported as metrics, which makes the
thundering-herd suppression visible rather than assumed.

**Bad.** Roughly 130 lines of concurrency code that would otherwise be a
dependency, plus its own tests.

**Accepted.** The behaviour is directly asserted by
`TestAbandonedWaiterDoesNotCancelSharedFill`, and the integration suite measures
the end-to-end result: 30 concurrent cold requests across 3 edges produce 1
origin fetch.
