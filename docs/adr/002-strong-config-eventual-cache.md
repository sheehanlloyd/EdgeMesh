# ADR-002: Strong consistency for configuration, eventual for cache

**Status:** Accepted

## Context

EdgeMesh holds two kinds of state with very different properties:

- **Configuration**: routes, origin pools, policies. Small, changes rarely, and
  divergence is silent and harmful: two edges with different route tables send
  identical requests to different origins and nothing reports it.
- **Cached content**: HTTP responses. Large, changes constantly, and is
  derivative: it can always be recomputed from the origin.

Applying one consistency model to both would be wrong in one direction or the
other.

## Decision

Configuration is **strongly consistent** through Raft. Cached content is
**eventually consistent** and is never replicated through consensus.

## Consequences

**Good.** Configuration writes are linearizable and survive any single node
failure. Consensus cost is paid once per admin write, not once per request. The
data plane keeps serving during a control-plane outage, because it needs no
coordination to serve a request. The control plane stays small, three nodes
serve any number of edges, since it is not in the request's way.

**Bad.** Configuration changes are eventually visible at the edges, typically
within milliseconds. An edge disconnected during a change serves stale
configuration until it reconnects. Cached objects can differ across edges;
purges are versioned and propagate asynchronously rather than atomically.

**Accepted.** A stale cached object costs a client slightly outdated content or
one extra origin fetch. A stale *route* would send traffic to the wrong place,
which is why routes are the thing that gets consensus.

This split is the project's central design claim, and
`TestEdgesKeepServingDuringAControlPlaneOutage` asserts the availability half of
it by stopping the entire control plane and requiring 100% data-plane success.
