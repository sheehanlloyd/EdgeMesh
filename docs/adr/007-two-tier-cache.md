# ADR-007: Local L1 plus consistent-hash L2

**Status:** Accepted

## Context

With N edge nodes and a purely local cache, a cold object is fetched from origin
N times and the cluster's effective cache size is one node's worth. A purely
distributed cache means every hit pays a network round trip.

## Decision

Both. A node-local L1 in front of a distributed L2 whose ownership is determined
by a consistent hash ring.

## Alternatives considered

**Local cache only.** Simplest, no peer protocol. Rejected: N× origin load on
cold objects, and no aggregate capacity benefit from adding nodes.

**Distributed cache only.** Best hit ratio per byte. Rejected: every hit pays a
gRPC round trip even when the object is on the same machine.

**An external shared cache (Redis, memcached).** Would work and is what many
systems do. Rejected because it moves the interesting engineering (placement,
ownership, replication, failure handling) into someone else's system, and adds
an operational dependency on the request path.

## Consequences

**Good.** L1 makes an individual hit fast; L2 makes the cluster's hit ratio
high. A cold object costs one origin fetch cluster-wide rather than N. Aggregate
cache capacity grows with the fleet. Adding a node remaps ~1/N of the keyspace
instead of nearly all of it.

**Bad.** Two tiers means two places to invalidate, and an early version purged
only L2, leaving stale objects serving from L1 indefinitely. That was a real bug
with a real regression test. A peer hit costs a network round trip. The ring must
be consistent across edges or two nodes disagree about ownership.

**Accepted.** Ring disagreement during a membership change costs at worst a
duplicate origin fetch, which is why membership is allowed to be eventually
consistent. The peer path always falls back, owner then replicas then origin,
so the cache is never a hard dependency and a dead node cannot take out the
keys it owned.
