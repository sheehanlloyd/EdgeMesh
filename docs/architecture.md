# EdgeMesh Architecture

## The central idea

EdgeMesh separates two kinds of state that most systems conflate, and treats
them differently on purpose:

| | Configuration | Cached content |
|---|---|---|
| What it is | Routes, origin pools, policies | HTTP responses |
| Consistency | **Strong** (Raft) | **Eventual** |
| Cost of being wrong | Traffic goes to the wrong place, silently | One extra origin fetch |
| Cost of coordination | Paid once per admin write | Would be paid on every request |
| Where it lives | Control plane | Edge nodes |

Configuration is small, changes rarely, and must never diverge: two edges with
different route tables send identical requests to different origins, and
nothing in the system would report it. That justifies consensus.

Cached content is large, changes constantly, and is derivative: it can always be
recomputed from the origin. Replicating it through consensus would put a quorum
round trip on the request hot path to protect data that is, by definition,
disposable.

Everything else in the design follows from that split.

## Component layout

![EdgeMesh architecture: an admin API into a three-node Raft control plane, which streams configuration to three edge proxies that share a consistent-hash peer cache in front of an origin pool.](images/architecture.svg)

## Request path

![The request path: an L1 lookup, then the consistent-hash owner over the peer cache, then the origin. Four requests across three edges cost one origin fetch.](images/request-path.svg)

A request arriving at any edge takes this path:

1. **Admission.** A bounded semaphore caps in-flight requests. Over the bound,
   the edge returns `503` immediately rather than accumulating goroutines and
   memory until it dies.
2. **Route match.** The hostname is normalized and matched exactly; among that
   host's routes the longest path prefix wins, respecting slash boundaries so
   `/api` never captures `/apix`. The route table is an immutable snapshot read
   through one atomic pointer load, with no lock on the hot path.
3. **Rate limit.** A node-local token bucket. This runs *before* any cache or
   origin work, so a limited request costs a bucket check rather than a cache
   lookup and a fetch.
4. **Cacheability.** Conservative rules decide whether this request may be
   served from cache at all (see [cache.md](cache.md)). Anything ambiguous
   bypasses.
5. **L1 lookup.** A local sharded map. A hit ends the request here.
6. **L2 lookup.** The consistent hash ring names the key's owner. If this node
   is the owner it fills locally; otherwise it asks the owner over gRPC.
7. **Origin fetch.** The owner fetches once, coalescing concurrent misses for
   the same key, applies retry and circuit-breaker policy, and streams the
   result back.
8. **Response.** The object is stored in the ingress node's L1 and written to
   the client with a recomputed `Age`.

## Control write path

```text
edgemeshctl → admin API (any control node)
                │
                ├── not leader? 503 + leader hint, CLI retries there
                │
                ▼ (on the leader)
           validate  ─── invalid? 400, no consensus round spent
                │
                ▼
           Raft propose
                │
           replicate to a majority
                │
           commit
                │
           apply to the state machine (deterministic)
                │
                ├── return the resulting config version to the client
                └── broadcast the change to every connected edge
```

A write is acknowledged only after it is committed by a majority *and* applied.
A caller receiving success knows the change survives any single node failure.

## Why Raft is not on the request path

![Failure domains: with the whole control plane stopped the data plane served 30 of 30 requests, and it serves every request through a leader kill with no failures.](images/failure-domains.svg)

Raft appears exactly once in a request's life: never. The data plane reads a
locally cached configuration snapshot and consults the ring, both of which are
in-memory reads. The consequences are deliberate:

- **A control-plane outage does not stop traffic.** Edges keep serving from the
  last valid configuration. Only *new* configuration is unavailable.
- **Control-plane latency does not affect request latency.** A slow or
  leaderless control plane costs nothing on the hot path.
- **The control plane can be small.** Three nodes handle configuration for an
  arbitrary number of edges, because it is not in the request's way.

The cost is that configuration changes are eventually visible at the edges,
typically within milliseconds of a commit, and that an edge disconnected during
a change serves stale configuration until it reconnects. Both are acceptable
for routing configuration and would not be for, say, an authorization decision.

## Failure boundaries

| Failure | Data plane | Control plane |
|---|---|---|
| One control node down | unaffected | writable (majority survives) |
| Control leader down | unaffected | brief election, then writable |
| Two control nodes down | unaffected, serves cached config | **not writable** |
| All control nodes down | unaffected, serves cached config | unavailable |
| One edge down | its keys remap; requests continue | membership updates |
| L2 owner down | replica, then origin fallback | membership updates |
| One origin unhealthy | excluded after health checks | unaffected |
| All origins down | `503`, breaker opens | unaffected |

The row that matters most is the third: losing a control-plane majority makes
configuration read-only while leaving every request path working. That is the
architecture's central claim, and
[the integration suite asserts it](../test/integration/integration_test.go) by
stopping the entire control plane and requiring 100% data-plane success.

## Consistency model in detail

**Configuration is linearizable** through Raft. Admin reads are served by the
leader; followers redirect rather than answering from a possibly stale state
machine. EdgeMesh does not claim linearizable follower reads.

**Membership is eventually consistent.** Edge liveness is leader-local ephemeral
state, not replicated: committing every heartbeat would dominate the Raft log
and force snapshots purely to discard liveness churn. When leadership moves, the
new leader rebuilds its roster from the heartbeats it then receives. During that
window two edges may briefly disagree about ring ownership, which costs at worst
a duplicate origin fetch.

**Cached content is eventually consistent.** Replication to successor nodes is
asynchronous and droppable. A purge is versioned and propagates through the
config stream; edges apply it to both tiers and ignore stale versions so a
reordered event cannot resurrect purged data.

## Hot-path concurrency

Three structures are read on every request and updated on configuration change:

- the **route table**,
- the **consistent hash ring**,
- the **origin pool set**.

The first two use the same pattern: build a new immutable snapshot, publish it
with a single atomic pointer store. A reader takes one atomic load and then
works with a structure nobody will mutate. No reader ever blocks on an update,
and an in-flight request always sees one coherent snapshot rather than a
half-applied change.

Origin pools use an `RWMutex` instead, because they carry mutable health state
that the health checker updates continuously; rebuilding the whole set for each
health transition would be far more expensive than a read lock.

## Backpressure

Every path that could grow without bound has an explicit limit:

| Path | Bound | Behaviour at the limit |
|---|---|---|
| In-flight requests | semaphore | `503` immediately |
| Cache tiers | byte budget per shard | LRU eviction |
| Object size | `max_object_bytes` | not admitted; still served |
| Replication | bounded queue + worker pool | dropped with a metric |
| Rate limiter keys | LRU cap + idle sweep | oldest key evicted |
| Retries | max attempts, deadline-aware | fail fast |
| Raft AppendEntries | max entries per RPC | catch-up over several RPCs |
| Config stream per client | bounded queue | backlog replaced by a snapshot |
| Peer object transfer | chunked stream | bounded memory per transfer |

The replication queue is the clearest example of the design principle: cached
data is derivative, so when the queue is full a replica write is **dropped and
counted** rather than blocking a client response. Losing a replica costs one
extra origin fetch after a node failure; blocking a response costs the client
its latency budget for nothing.

## What EdgeMesh implements directly

Implemented in this repository:

- Raft: leader election, log replication, persistence, snapshots,
  InstallSnapshot, pre-vote
- The consistent hash ring, virtual nodes, and replica selection
- Both cache tiers: sharded storage, LRU accounting, TTL, admission policy, and
  a TinyLFU-inspired frequency filter
- HTTP cache-key construction and eligibility rules
- The route matcher
- Request coalescing with independent waiter cancellation
- The circuit breaker and token-bucket rate limiter
- Control/data-plane orchestration, config streaming, and membership tracking

Libraries used for commodity concerns:

- gRPC and Protocol Buffers for transport and serialization
- bbolt as the durable storage primitive under the Raft log
- xxHash as the hash primitive under the ring
- OpenTelemetry SDK, Prometheus client, `log/slog`
- `gopkg.in/yaml.v3` for configuration parsing

## Related documents

- [raft.md](raft.md): the consensus implementation and its failure matrix
- [cache.md](cache.md): cache tiers, keys, and invalidation
- [failure-model.md](failure-model.md): the full failure and chaos matrix
- [threat-model.md](threat-model.md): assets, boundaries, and mitigations
- [benchmarks.md](benchmarks.md): measurement methodology
- [adr/](adr/): the decisions behind these designs
