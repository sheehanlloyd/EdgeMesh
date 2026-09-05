# ADR-004: gRPC for internal protocols

**Status:** Accepted

## Context

Three internal protocols are needed: Raft consensus RPCs, an edge-to-control
configuration stream, and peer-to-peer cache transfer. They need typed
contracts, bidirectional streaming for config delivery, chunked transfer for
cache objects, and mutual authentication.

## Decision

Use gRPC with Protocol Buffers for all three, with mTLS.

## Alternatives considered

**HTTP/JSON.** Simplest and easiest to debug with curl. Rejected because the
config stream needs long-lived server streaming and cache transfer needs
efficient binary chunking; both are awkward over JSON.

**A custom binary protocol.** Maximum control and no dependency. Rejected as
effort spent on framing rather than on the algorithms that matter, and it would
need its own schema-evolution story.

## Consequences

**Good.** Protobuf gives explicit versioned contracts under `edgemesh.v1`.
Server streaming makes config delivery natural. Client streaming makes chunked
cache transfer natural. mTLS with `RequireAndVerifyClientCert` is one dial
option. Keepalives detect a silently dropped connection well before a write
would time out, which matters for Raft, where a peer that merely *looks*
reachable delays failure detection past the election timeout.

**Bad.** Debugging needs `grpcurl` rather than `curl`. Code generation is a
build step. The dependency is substantial.

**Accepted.** The public data plane stays plain HTTP over `net/http`, so the
proxy's behaviour remains visible and debuggable with ordinary tools. gRPC is
confined to internal traffic.

### A note on keepalives

The client keepalive interval must not be more frequent than the server's
enforcement minimum, or the server sends `GOAWAY too_many_pings` and tears the
connection down, which looks exactly like a network failure and causes
reconnect churn. `transport.DefaultDialOptions` and
`transport.DefaultServerOptions` are deliberately defined together so the two
cannot drift apart.
