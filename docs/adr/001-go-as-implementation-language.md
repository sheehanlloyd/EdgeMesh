# ADR-001: Go as the implementation language

**Status:** Accepted

## Context

EdgeMesh is a network proxy with a consensus-based control plane. It needs
excellent HTTP and TLS support, cheap concurrency for per-connection work, a
mature gRPC implementation, small deployable artifacts, and tooling that makes
race conditions findable.

## Decision

Implement EdgeMesh in Go.

## Alternatives considered

**Rust.** Better performance ceiling and stronger compile-time guarantees about
aliasing. Rejected because the async ecosystem would add substantial incidental
complexity to what is fundamentally an exercise in distributed-systems design,
and because `go test -race` finding a data race at runtime is more valuable here
than the borrow checker preventing one at compile time. The interesting bugs in
this project are protocol bugs, not memory bugs.

**Java.** Excellent networking and consensus libraries. Rejected for GC pause
behaviour on a latency-sensitive path and for artifact size.

**C++.** Maximum control. Rejected because memory safety would dominate the
effort budget.

## Consequences

**Good.** `net/http` makes proxy behaviour visible rather than hidden behind a
framework. Goroutines make per-request concurrency natural. The race detector
catches concurrency bugs in CI. Static binaries produce scratch-based container
images with no shell for an attacker to pivot with. `log/slog`, `testing`,
`pprof`, and fuzzing are in the standard library.

**Bad.** GC pauses are a real latency factor under heavy allocation, which makes
allocation-aware coding on the hot path necessary, hence the immutable
snapshots in ADR-005 and the buffer reuse in the ring. Generics are recent
enough that some abstractions are more verbose than they would be elsewhere.

**Accepted.** Go's performance ceiling is lower than Rust's or C++'s. For this
project the design is the point, and Go's ceiling is far above what the design
needs to demonstrate.
