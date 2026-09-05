# ADR-006: Static Raft membership for V1

**Status:** Accepted

## Context

Raft supports membership changes through joint consensus, which lets a cluster
add and remove members without downtime. It is also the subtlest part of the
protocol and a well-known source of implementation bugs.

## Decision

V1 uses a **statically configured** three-member control plane. Membership is
fixed at bootstrap. Changing it requires a configuration change and a restart.

## Consequences

**Good.** The implementation is dramatically simpler: no joint consensus, no
configuration entries in the log, no transitional quorum rules. Every safety
property is easier to reason about and to test. The failure matrix covers what
is implemented rather than leaving the hardest part untested.

**Bad.** The control plane cannot be resized without downtime. Replacing a
failed node means reusing its identity and data directory. Rolling to a
five-node cluster is a maintenance operation, not an online one.

**Accepted.** For a three-node control plane holding configuration that changes
rarely, an occasional planned restart is a reasonable cost. The alternative,
shipping a partially correct joint-consensus implementation, would be worse
than not shipping it. This limitation is stated in `docs/raft.md` and in the
README rather than glossed over.

The **edge** plane, by contrast, has fully dynamic membership through heartbeats
and the consistent hash ring. Edges join and leave continuously with no
consensus involvement at all, which is exactly the point of ADR-002: the thing
that changes often does not pay for coordination.
