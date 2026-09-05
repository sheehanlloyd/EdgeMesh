# ADR-003: Implement Raft rather than embed a library

**Status:** Accepted

## Context

EdgeMesh needs a replicated, durable configuration store with leader election
and crash recovery. Mature options exist: `hashicorp/raft`, embedded etcd,
`etcd-io/raft`.

## Decision

Implement Raft in this repository, in `internal/raft`.

## Alternatives considered

**`hashicorp/raft`.** Battle-tested, would have been faster and safer. Rejected
because consensus is the subsystem this project exists to demonstrate; importing
it would leave the repository a wiring exercise.

**Embedded etcd.** Brings a full key-value store, watch semantics, and
operational maturity. Rejected as far more than EdgeMesh needs, and it would
obscure the very mechanism the project is about.

**A simpler protocol.** Single-leader with manual failover would have been much
less work and would not have provided the safety properties that make the
trade-off in ADR-002 defensible.

## Consequences

**Good.** The safety-critical parts (the election restriction, the current-term
commit rule, durable-vote-before-response, conflicting-suffix repair) are
written and tested here, and their reasoning is in the code where it can be
read. The failure matrix runs against an in-memory transport with controllable
partitions, so all ten required scenarios execute in about twelve seconds on
every commit.

**Bad.** This implementation has nowhere near the production hardening of
`hashicorp/raft`. Dynamic membership is not implemented (ADR-006). Bugs here are
this project's bugs.

**Accepted.** The risk is real and is stated in the README: EdgeMesh describes
itself as production-inspired, not production-ready. Two genuine bugs found
during development (a pre-vote deadlock and an event-reordering window) are
documented in `docs/raft.md` and `docs/engineering-notes.md` rather than quietly
fixed, because how they were found is part of what the project demonstrates.
