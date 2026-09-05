# ADR-005: Immutable snapshots on the hot path

**Status:** Accepted

## Context

Two structures are read on every request and updated on configuration change:
the route table and the consistent hash ring. Both are read far more often than
written: thousands of reads per second against a write every few minutes.

## Decision

Both are **immutable** once built. An update constructs an entirely new instance
and publishes it with a single atomic pointer store. Readers take one atomic
load.

## Alternatives considered

**`sync.RWMutex`.** Simple and adequate at low concurrency. Rejected for the
hottest structures because every request would take a read lock, and the
cache-line contention on the lock's internal state is itself a bottleneck at
high core counts.

**Copy-on-write with a mutex for writers only.** Effectively what this is; the
atomic pointer just removes the last lock from the read path.

**`sync.Map`.** Wrong shape: the ring needs sorted binary search, not key lookup.

## Consequences

**Good.** Readers never block, never wait on a writer, and never observe a
half-applied change. An in-flight request sees one coherent snapshot for its
whole lifetime. Reasoning about correctness is simple because the data cannot
change under the reader.

**Bad.** Every update allocates a fresh structure. For the ring that is
`nodes × 128` hashes plus a sort; for the route table it is a rebuild of the
grouping maps. Both happen on configuration change, not on the request path.

**Accepted.** Origin pools deliberately use an `RWMutex` instead, because they
carry mutable health state that the health checker updates continuously.
Rebuilding the whole pool set on every health transition would cost far more
than a read lock saves. Different access patterns, different answers.
