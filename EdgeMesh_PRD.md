---
title: "EdgeMesh"
subtitle: "Product Requirements Document & Engineering Design Specification"
date: "Version 1.0, August 30, 2026"
---
<!-- STATIC_TOC_START -->
# Table of Contents

- [Implementation-Agent Directive](#implementation-agent-directive)
- [1. Executive Summary](#1-executive-summary)
- [2. Goals, Non-Goals, and Engineering Principles](#2-goals-non-goals-and-engineering-principles)
- [3. System Architecture](#3-system-architecture)
- [4. Recommended Technology Stack](#4-recommended-technology-stack)
- [5. Repository Structure](#5-repository-structure)
- [6. Functional Requirements: Edge Data Plane](#6-functional-requirements-edge-data-plane)
- [7. Caching Architecture](#7-caching-architecture)
- [8. Consistent Hash Ring](#8-consistent-hash-ring)
- [9. Control Plane and Raft](#9-control-plane-and-raft)
- [10. Edge Membership and Configuration Distribution](#10-edge-membership-and-configuration-distribution)
- [11. Admin API and CLI](#11-admin-api-and-cli)
- [12. Health, Readiness, and Graceful Lifecycle](#12-health-readiness-and-graceful-lifecycle)
- [13. Observability Requirements](#13-observability-requirements)
- [14. Security and Threat Model](#14-security-and-threat-model)
- [15. Configuration Model](#15-configuration-model)
- [16. Demo Origin Requirements](#16-demo-origin-requirements)
- [17. Local Developer Experience](#17-local-developer-experience)
- [18. Kubernetes and Helm](#18-kubernetes-and-helm)
- [19. Terraform / AWS EKS](#19-terraform-aws-eks)
- [20. Testing Strategy](#20-testing-strategy)
- [21. Failure and Chaos Test Matrix](#21-failure-and-chaos-test-matrix)
- [22. Performance and Benchmark Requirements](#22-performance-and-benchmark-requirements)
- [23. CI/CD Requirements](#23-cicd-requirements)
- [24. Error Model and Reliability Semantics](#24-error-model-and-reliability-semantics)
- [25. Concurrency and Backpressure Design](#25-concurrency-and-backpressure-design)
- [26. Data Structures and Internal Interfaces](#26-data-structures-and-internal-interfaces)
- [27. API/Protocol Compatibility](#27-apiprotocol-compatibility)
- [28. Documentation Deliverables](#28-documentation-deliverables)
- [29. Recruiter and Interview Presentation Requirements](#29-recruiter-and-interview-presentation-requirements)
- [30. Target Demo Story](#30-target-demo-story)
- [31. Acceptance Criteria / Definition of Done](#31-acceptance-criteria-definition-of-done)
- [32. Suggested Implementation Order](#32-suggested-implementation-order)
- [33. Quality Bar for Code](#33-quality-bar-for-code)
- [34. Performance-Engineering Checklist](#34-performance-engineering-checklist)
- [35. Resume Bullet Templates: DO NOT USE UNTIL VERIFIED](#35-resume-bullet-templates-do-not-use-until-verified)
- [36. Interview Story Bank](#36-interview-story-bank)
- [37. Optional P1: TinyLFU-Inspired Cache Admission](#37-optional-p1-tinylfu-inspired-cache-admission)
- [38. Optional STRETCH: HTTP/3 / QUIC Experiment](#38-optional-stretch-http3-quic-experiment)
- [39. Reference Documentation](#39-reference-documentation)
- [Appendix A: One-Shot Build Instruction for a Coding Agent](#appendix-a-one-shot-build-instruction-for-a-coding-agent)
- [Appendix B: Example Public README Architecture Block](#appendix-b-example-public-readme-architecture-block)
- [Appendix C: Final Pre-Publication Checklist](#appendix-c-final-pre-publication-checklist)

<!-- STATIC_TOC_END -->

# Implementation-Agent Directive

> **MANDATORY SOURCE-CONTROL RULE:** The implementation agent may create, edit, delete, format, build, test, benchmark, and lint project files, but it **MUST NOT run `git add`, `git commit`, `git push`, `git pull`, `git fetch`, `git merge`, `git rebase`, `git reset`, `git clean`, `git checkout`, `git switch`, create tags, modify branches, or otherwise mutate Git history or staging state.** Do not modify `.git/`. Leave all changes unstaged for the human owner to review. If Git commands are needed only to inspect state, use read-only commands such as `git status`, `git diff`, `git log`, or `git show` only.

This document is intended to be sufficiently complete that a capable coding agent or software engineer can implement EdgeMesh without inventing major product requirements. When a detail is ambiguous, prefer the simplest design consistent with the invariants, failure semantics, security model, and acceptance criteria in this PRD. Do not silently downgrade required behavior. If a stretch feature conflicts with a required V1 feature, ship the required V1 feature first.

The implementation agent must:

1. Read this entire PRD before making architectural changes.
2. Treat sections marked **REQUIRED V1** as release-blocking.
3. Treat sections marked **P1** as high-value follow-ons that should be implemented if the required V1 is complete and stable.
4. Treat sections marked **STRETCH** as optional portfolio enhancements.
5. Prefer correctness, observability, deterministic tests, and clear failure behavior over feature count.
6. Use production-quality error handling. Do not leave silent failures, swallowed errors, TODO-driven critical paths, or panic-based normal control flow.
7. Keep dependencies intentionally small. Use libraries for commodity concerns; implement the portfolio-defining algorithms in this document directly.
8. Never fabricate benchmark numbers, reliability claims, regions, traffic scale, user counts, or production usage. All public claims must be produced by reproducible tests on documented hardware.
9. Produce documentation as part of the implementation, not as an afterthought.
10. Never deploy paid cloud infrastructure automatically. Terraform code may be generated and validated, but `terraform apply` is human-invoked only.
11. Never run source-control mutation commands. In particular: **NO `git add`, NO `git commit`, NO `git push`.**

# 1. Executive Summary

## 1.1 Product statement

**EdgeMesh is a production-inspired distributed edge proxy, cache, and control-plane platform written primarily in Go.** It accepts HTTP traffic at multiple edge nodes, routes requests to configured origins, accelerates cacheable responses through a two-tier cache, distributes cache ownership using consistent hashing, and coordinates configuration through a three-node Raft control plane. The system is designed to remain observable and useful when nodes crash, origins become unhealthy, links become slow, or the control-plane leader changes.

The project is deliberately not a generic CRUD application and not an AI wrapper. Its value is demonstrating the engineering problems encountered in infrastructure companies: concurrency, network protocols, cache correctness, consensus, backpressure, health management, replication, failure detection, observability, security boundaries, deployment automation, and performance measurement.

## 1.2 Portfolio objective

A software engineer or recruiter should be able to scan the repository for 30-60 seconds and immediately find evidence of:

- Idiomatic modern Go and concurrency using goroutines, channels, contexts, synchronization, and bounded work.
- HTTP reverse-proxy behavior implemented above the standard library rather than delegated to NGINX.
- A consistent-hash ring implemented by the project with virtual nodes and deterministic membership updates.
- A custom Raft implementation for control-plane state, including leader election, replicated log, persistence, snapshots, and failure tests.
- An explicit consistency model: strong consistency for configuration; eventual consistency for derivative cached objects.
- Two-tier caching, cache admission/eviction, TTL handling, request coalescing, and safe cache-key construction.
- Health checking, timeouts, retries, circuit breaking, rate limiting, graceful degradation, and graceful shutdown.
- OpenTelemetry traces, Prometheus metrics, structured logs, Grafana dashboards, health endpoints, and readiness semantics.
- Chaos/failure testing and reproducible load benchmarks rather than unverified performance claims.
- Containers, Kubernetes, Helm, Terraform, and AWS EKS deployment artifacts without requiring cloud access for local development.
- Security considerations such as mTLS for internal RPCs, origin restrictions, hop-by-hop header stripping, admin authentication, input bounds, and secret hygiene.
- A repository that is explainable in an interview: design documents, ADRs, benchmark methodology, failure matrix, and concise trade-off discussions.

## 1.3 Project positioning

Use the following short description in GitHub metadata and the README hero section:

> **Distributed edge proxy and cache in Go with a custom Raft control plane, consistent hashing, peer replication, OpenTelemetry, Kubernetes, and reproducible chaos/load testing.**

The project must describe itself as **production-inspired**, **educational infrastructure**, or equivalent. Do not claim that it is production-ready unless that claim is later independently justified.

# 2. Goals, Non-Goals, and Engineering Principles

## 2.1 REQUIRED V1 goals

EdgeMesh V1 must:

1. Proxy HTTP requests from an edge node to one or more configured origin servers.
2. Support host + path-prefix routing with deterministic precedence.
3. Maintain a local in-memory L1 cache at each edge node.
4. Maintain a distributed L2 peer cache whose ownership is determined by a project-implemented consistent hash ring.
5. Support at least three edge nodes in the local demo.
6. Support at least three control-plane nodes in a static Raft cluster.
7. Implement Raft directly rather than embedding etcd, HashiCorp Raft, Consul, or another consensus library.
8. Replicate control-plane commands and survive leader loss without losing committed configuration.
9. Persist enough Raft state that a restarted control node can rejoin without resetting the cluster.
10. Stream configuration updates from the control plane to edge nodes.
11. Register edge nodes and track liveness through heartbeats without committing every heartbeat to Raft.
12. Keep serving data-plane traffic from the last valid configuration during temporary control-plane unavailability.
13. Implement request timeouts, bounded retries, circuit breaking, rate limiting, request coalescing, and graceful shutdown.
14. Expose Prometheus-compatible metrics and OpenTelemetry traces.
15. Ship a reproducible local multi-node environment using Docker Compose and/or kind.
16. Ship Kubernetes/Helm manifests and Terraform for AWS EKS, but never require AWS for tests.
17. Ship deterministic unit, integration, end-to-end, race, fuzz, failure, and benchmark coverage as specified later.
18. Include recruiter-facing documentation and an executable demo path.

## 2.2 P1 goals

After REQUIRED V1 is stable:

- Pluggable cache eviction policies with an LRU baseline and a TinyLFU-inspired admission policy.
- Stale-while-revalidate behavior.
- Negative caching for selected status codes with conservative TTLs.
- Snapshot compaction thresholds and install-snapshot RPC coverage under larger logs.
- Distributed cache invalidation by route/tag in addition to exact-key purge.
- Custom HPA metrics for edge autoscaling.
- Multi-AZ EKS example and topology-spread constraints.
- A richer CLI with watch/status tables.

## 2.3 STRETCH goals

Only pursue these after V1 and P1 are demonstrably correct:

- HTTP/3 over QUIC using `quic-go` and comparative packet-loss benchmarks.
- SWIM-style gossip membership for edges as an alternative to control-plane heartbeat discovery.
- Geo-aware placement and latency-weighted routing.
- DNS service or authoritative routing integration.
- Content prefetching based on access frequency.
- eBPF-based network observability.
- Multi-region AWS deployment.
- Persistent disk-backed cache blobs with checksum verification and restart recovery.

## 2.4 Explicit non-goals for V1

V1 does **not** need:

- A browser frontend or bespoke dashboard; Grafana is sufficient.
- Full RFC-complete CDN behavior.
- Arbitrary customer multi-tenancy or billing.
- Dynamic Raft membership / joint-consensus reconfiguration. The control-plane cluster is statically configured with three members.
- Full BGP, anycast, DNS, WAF, bot management, DDoS mitigation, or TLS certificate issuance.
- A globally distributed production environment.
- Strong consistency for cached objects.
- A custom TCP/IP stack.
- A database built from scratch.

## 2.5 Engineering principles

**Correctness before cleverness.** Failure behavior must be explicit and tested.

**Control plane and data plane are separate failure domains.** Existing proxy traffic should continue when the control plane is temporarily unavailable.

**Consensus only where necessary.** Route/origin/policy configuration is strongly consistent through Raft. Cache content is derivative and eventually consistent.

**Backpressure instead of unbounded concurrency.** Every queue, goroutine fan-out, retry path, and buffer must have a bound or cancellation path.

**Stream large bodies.** Avoid reading arbitrary HTTP bodies entirely into memory. Cached-object size is explicitly bounded.

**Observability is part of the design.** New subsystems require metrics/logs/traces alongside the code.

**Measured claims only.** Benchmarks must record hardware, command, concurrency, duration, dataset, and result.

# 3. System Architecture

## 3.1 Logical architecture

```text
                              ADMIN / CLI
                                  |
                           HTTPS / JSON API
                                  |
                    +-------------v-------------+
                    |   CONTROL PLANE CLUSTER   |
                    |  cp-1     cp-2     cp-3   |
                    |         Custom Raft       |
                    +------+----------+---------+
                           |          |
                  config stream     heartbeats
                       gRPC/mTLS      gRPC/mTLS
                           |          |
             +-------------+----------+-------------+
             |             |                        |
       +-----v-----+  +----v------+           +-----v-----+
       | edge-1    |  | edge-2    |           | edge-3    |
       | proxy     |  | proxy     |           | proxy     |
       | L1 cache  |  | L1 cache  |           | L1 cache  |
       | L2 peer   |  | L2 peer   |           | L2 peer   |
       +-----+-----+  +----+------+           +-----+-----+
             \             |                        /
              \------ consistent-hash peer -------/
                              |
                         cache miss
                              |
                      +-------v-------+
                      | Origin Pool   |
                      | app-1 / app-2 |
                      +---------------+
```

## 3.2 Components

### Edge node

Each edge node runs one Go process with:

- Public HTTP reverse-proxy listener.
- Route-matching engine.
- L1 in-memory cache.
- L2 peer-cache server/client.
- Consistent-hash ring snapshot.
- Origin health and circuit-breaker state.
- Rate limiter.
- Local request coalescing.
- Control-plane config watcher.
- Node heartbeat client.
- Metrics, trace, health, readiness, and debug endpoints.

### Control-plane node

Each control-plane node runs one Go process with:

- Raft consensus engine.
- Persistent Raft stable state and log.
- Replicated configuration state machine.
- Admin HTTP API.
- Internal Raft gRPC transport.
- Edge config-stream gRPC service.
- Edge heartbeat/liveness tracker on the current leader.
- Derived edge membership/ring configuration broadcaster.
- Metrics, traces, health, readiness, and debug status.

### CLI

`edgemeshctl` is a thin Go CLI that talks to the admin API. It is required because it creates an easy live-demo surface and makes control-plane operations visible without a custom UI.

### Demo origin

A small deterministic origin service is included for local demos and tests. It must expose cacheable, non-cacheable, delayed, error, and variable-size responses so reliability behavior can be demonstrated without external dependencies.

## 3.3 Control-plane / data-plane invariant

An edge node must cache the last accepted configuration snapshot locally in memory. When the control plane becomes unreachable:

- Existing routes continue to serve.
- Existing cached objects continue to serve subject to their normal TTL rules.
- New configuration is unavailable until reconnection.
- Edge readiness must remain **ready** if the node has a valid configuration snapshot and can serve traffic.
- Edge liveness remains independent of control-plane connectivity.
- A metric and warning log must indicate `control_plane_connected=0`.

This behavior is a core interview/demo point.

# 4. Recommended Technology Stack

Use current, supported releases and pin reproducible dependencies in `go.mod`/container images. As of this PRD, Go 1.27 is current and is the preferred toolchain baseline.

| Area | Choice | Rationale |
|---|---|---|
| Primary language | Go 1.27.x | Adds missing Go portfolio signal; strong standard networking/concurrency tooling |
| Public HTTP | `net/http` | Keeps proxy behavior visible and minimizes framework hiding |
| Internal RPC | gRPC + Protocol Buffers | Typed streaming/internal APIs and interview-relevant transport boundary |
| Raft transport | gRPC | Clear RPC contracts for RequestVote / AppendEntries / InstallSnapshot |
| Raft persistence | bbolt or a minimal dedicated stable-store layer | Consensus remains custom while persistence uses a reliable primitive |
| Hashing | project ring + `xxhash` or equivalent stable hash | Project owns ring algorithm; library only supplies hash primitive |
| Concurrency helpers | stdlib + `x/sync/singleflight` where appropriate | Avoids bespoke unsafe coalescing unless educationally valuable |
| Telemetry | OpenTelemetry Go | Traces and metrics context propagation |
| Metrics scrape | Prometheus client | Standard service metrics / Grafana data source |
| Logging | `log/slog` | Structured logging without another large dependency |
| Config files | YAML | Human-readable local/bootstrap configuration |
| Containers | Docker | Reproducible local services |
| Local cluster | Docker Compose and kind | Fast local demo plus Kubernetes parity |
| Kubernetes packaging | Helm | Parameterized deployable chart |
| Cloud IaC | Terraform + AWS EKS | Demonstrates cloud/IaC signal |
| Load test | k6 or Vegeta | Reproducible HTTP load generation |
| Fault injection | Toxiproxy and container kill/restart scripts | Portable latency/loss/failure scenarios |
| Security scan | `govulncheck`, CodeQL optional | Dependency/code vulnerability checks |
| Go lint | `golangci-lint` | Consolidated static checks |

Do not introduce Redis, PostgreSQL, Kafka, NGINX, Envoy, or etcd into the required path. Those systems would obscure the infrastructure EdgeMesh is intended to demonstrate.

# 5. Repository Structure

The implementation should converge on a structure close to:

```text
edgemesh/
├── cmd/
│   ├── edge/                 # Edge-node executable
│   ├── control/              # Control-plane executable
│   ├── edgemeshctl/          # Admin CLI
│   └── origin-demo/          # Deterministic demo/test origin
├── api/
│   └── proto/edgemesh/v1/
│       ├── raft.proto
│       ├── control.proto
│       └── peer_cache.proto
├── internal/
│   ├── cache/
│   │   ├── l1/
│   │   ├── l2/
│   │   ├── key/
│   │   └── policy/
│   ├── ring/
│   ├── proxy/
│   ├── routing/
│   ├── origin/
│   ├── peer/
│   ├── ratelimit/
│   ├── breaker/
│   ├── control/
│   │   ├── api/
│   │   ├── configstream/
│   │   └── membership/
│   ├── raft/
│   │   ├── node/
│   │   ├── transport/
│   │   ├── storage/
│   │   ├── log/
│   │   └── statemachine/
│   ├── observability/
│   ├── security/
│   ├── health/
│   └── config/
├── configs/
│   ├── local/
│   └── examples/
├── deploy/
│   ├── compose/
│   ├── kind/
│   ├── helm/edgemesh/
│   └── terraform/aws/
├── docs/
│   ├── architecture.md
│   ├── raft.md
│   ├── cache.md
│   ├── failure-model.md
│   ├── threat-model.md
│   ├── benchmarks.md
│   ├── demo.md
│   └── adr/
├── scripts/
│   ├── demo.sh
│   ├── generate-dev-certs.sh
│   ├── loadtest/
│   └── chaos/
├── test/
│   ├── integration/
│   ├── e2e/
│   └── fixtures/
├── .github/workflows/
├── Makefile
├── go.mod
├── go.sum
├── Dockerfile.edge
├── Dockerfile.control
├── Dockerfile.origin
├── README.md
├── SECURITY.md
├── CONTRIBUTING.md
└── LICENSE
```

Avoid a generic `utils` dumping ground. Package names should reflect domain ownership.

# 6. Functional Requirements: Edge Data Plane

## 6.1 Public request listener: REQUIRED V1

The edge process must expose a configurable public HTTP listener.

Required behavior:

- HTTP/1.1 supported.
- HTTP/2 supported when TLS is enabled through Go standard behavior.
- Configurable listen address.
- Per-request context with cancellation propagated to origin and peer calls.
- Configurable header read timeout, request timeout, idle timeout, and maximum header bytes.
- Streaming request/response bodies.
- Graceful shutdown on SIGTERM/SIGINT with a configurable drain timeout.
- Unique request ID generated when not supplied by a trusted upstream.
- Trace context accepted and propagated according to configured OpenTelemetry propagators.

The server must not blindly trust incoming forwarding headers. EdgeMesh owns the forwarding chain it emits.

## 6.2 Route matching: REQUIRED V1

Routes are strongly consistent control-plane objects.

A route contains at minimum:

```text
Route
- id: string
- hostname: string
- path_prefix: string
- origin_pool_id: string
- enabled: bool
- cache_policy: CachePolicy
- rate_limit_policy: RateLimitPolicy
- timeout_policy: TimeoutPolicy
- retry_policy: RetryPolicy
- header_policy: HeaderPolicy
- created_at: timestamp
- updated_at: timestamp
- version: uint64
```

Matching rules:

1. Exact normalized hostname match.
2. Among routes for the hostname, longest path prefix wins.
3. Disabled routes never match.
4. Configuration validation rejects duplicate `(hostname, path_prefix)` pairs.
5. Path-prefix matching must respect slash boundaries where applicable to prevent `/api` from unexpectedly matching `/apix`.

If no route matches, return `404` with a small EdgeMesh error body and request ID.

## 6.3 Origin pools: REQUIRED V1

An origin pool contains one or more origin endpoints.

```text
OriginPool
- id: string
- origins[]:
  - id: string
  - scheme: http | https
  - host: string
  - port: uint16
  - weight: uint32
  - health_path: string
  - expected_statuses: []int
- load_balancing: round_robin | weighted_round_robin
- health_interval
- unhealthy_threshold
- healthy_threshold
```

Required behavior:

- Round-robin is required; weighted round-robin is P1.
- Active health checks run with bounded concurrency.
- Unhealthy origins are excluded when another healthy origin exists.
- When all origins are unhealthy, traffic may use the least-recently-failed origin in a clearly logged degraded mode or fail fast with `503`; choose one policy and document it. Preferred default: fail fast with `503` after circuit-breaker evaluation.
- Origin HTTP transports reuse connections.
- Proxy must remove hop-by-hop headers as required by HTTP semantics.
- `Host` header behavior is configurable per route: preserve client host or rewrite to origin host.
- Add `Via` or an `X-EdgeMesh-*` diagnostic header only when diagnostic headers are enabled.

## 6.4 Retry policy: REQUIRED V1

Retries must be bounded and conservative.

Default:

- Retry only idempotent methods (`GET`, `HEAD`, optionally `OPTIONS`).
- Maximum 2 retries after the initial attempt.
- Retry on connection failure, timeout before headers, and selected `502/503/504` responses.
- Never retry after a response body has been partially sent to the client.
- Use exponential backoff with jitter within the remaining request deadline.
- Honor context cancellation immediately.
- Record retry reason metrics.

## 6.5 Circuit breaker: REQUIRED V1

Implement a per-origin circuit breaker with states:

- `closed`
- `open`
- `half_open`

Minimum algorithm:

- Sliding window or consecutive-failure threshold.
- Open after configured failure criteria.
- Remain open for cooldown.
- Allow a bounded number of half-open probes.
- Close on successful probe threshold.

Expose state and transition counters as metrics. State is local to each edge node; it is not consensus data.

## 6.6 Rate limiting: REQUIRED V1

Implement token-bucket rate limiting in-process.

Policy fields:

```text
RateLimitPolicy
- enabled: bool
- rate_per_second: float
- burst: int
- key_strategy: global | client_ip | header
- header_name: optional string
```

Required behavior:

- Bounded memory for keyed limiters; stale keys must expire.
- Header-key mode only permits headers explicitly configured by an administrator.
- On denial, return `429` and `Retry-After` when determinable.
- Rate-limit evaluation happens before expensive cache/origin work.
- Distributed/global exact rate limiting is a non-goal for V1; clearly document that limits are node-local.

# 7. Caching Architecture

## 7.1 Consistency model

Cached HTTP content is derivative data. EdgeMesh therefore uses **eventual consistency** for cache replication. A temporary peer failure must not block serving a valid local object or fetching from origin.

Configuration is not derivative and uses Raft.

This distinction must be highlighted in `docs/architecture.md` and the README.

## 7.2 L1 cache: REQUIRED V1

Each edge node maintains an in-memory L1 cache optimized for low-latency local hits.

Required implementation:

- Sharded map to reduce lock contention.
- LRU eviction within shards or another explicitly documented bounded policy.
- Configurable total byte budget.
- Per-object byte accounting.
- TTL expiration.
- Lazy expiration on lookup plus periodic bounded cleanup.
- No unbounded background goroutine creation.
- Cache hit, miss, eviction, bytes, object count, and expiration metrics.

The sharding count should be configurable or fixed to a reasoned power of two. Hash-to-shard selection must be deterministic.

## 7.3 Cache key: REQUIRED V1

Cache-key correctness is security- and correctness-sensitive.

Base key:

```text
route_id + normalized_method + normalized_path + canonical_query + selected_vary_headers
```

Rules:

- Only `GET` and `HEAD` are cacheable in V1.
- Normalize hostname through route resolution rather than raw untrusted bytes.
- Query parameter order must have a defined policy. Preferred: preserve original query by default; optional canonical sort can be configured per route only if semantics are understood.
- Respect `Vary` for a conservative supported set; default to rejecting cache admission when `Vary: *` is present.
- Requests with `Authorization` are not cached by default.
- Requests with cookies are not cached by default unless route policy explicitly allows selected cookie names.
- Responses with `Cache-Control: no-store` or `private` are not admitted.
- Never store `Set-Cookie` responses unless explicitly allowed by a route policy; default is no.

Add unit tests for cache-key collisions, query handling, headers, authorization, and cookie behavior.

## 7.4 Cache admission: REQUIRED V1

A response may be cached only when:

- Request method is cacheable.
- Response status is allowed by policy. Default required status: `200` only.
- Response does not prohibit shared caching.
- Object size is less than configured maximum, default 8 MiB.
- TTL is positive after policy resolution.
- No disallowed authentication/cookie semantics are present.

TTL resolution precedence should be documented. Recommended:

1. `s-maxage` if valid.
2. `max-age` if valid.
3. Route default TTL.
4. No admission if none exists and route default is disabled.

Cap TTL to a configured maximum.

## 7.5 Distributed L2 peer cache: REQUIRED V1

The L2 cache is a logical cache distributed across edge nodes.

For every cache key:

- Compute primary owner through the consistent hash ring.
- Select `R-1` clockwise successor nodes as replicas.
- Default replication factor `R=2` for a three-edge demo; configurable up to live node count.
- The primary owns `GetOrFetch` coordination for the key.

Request flow for a cacheable request:

```text
client
  -> ingress edge
      -> L1 lookup
          HIT -> return
          MISS
            -> calculate L2 primary
                -> if primary is local: GetOrFetch
                -> if primary is remote: gRPC GetOrFetch
                    -> primary L2 HIT -> stream object
                    -> primary L2 MISS -> primary fetches origin once
                         -> cache at primary
                         -> asynchronously replicate to successor(s)
                         -> stream result to ingress
            -> ingress stores successful cacheable result in L1
            -> response to client
```

This design intentionally centralizes same-key cache fill on the primary to reduce cross-node thundering herd.

If the primary is unavailable:

1. Try configured replica(s) in order.
2. If no peer is reachable, fetch directly from origin from the ingress edge.
3. Mark response/log/metrics as degraded peer-cache path.
4. Never fail a request solely because the peer cache is unavailable when the origin is reachable.

## 7.6 Peer cache RPC: REQUIRED V1

Define protobuf messages and a service with behavior equivalent to:

```proto
service PeerCache {
  rpc GetOrFetch(GetOrFetchRequest) returns (stream ObjectChunk);
  rpc PutReplica(stream ReplicaChunk) returns (PutReplicaResponse);
  rpc Purge(PurgeRequest) returns (PurgeResponse);
  rpc Health(PeerHealthRequest) returns (PeerHealthResponse);
}
```

`GetOrFetchRequest` should carry a normalized route ID, cache key, method, path/query, and a strictly allowlisted subset of headers needed for the origin request. Never forward arbitrary internal/admin headers.

Response stream metadata must include status, content headers, cache outcome, age, expiry, and checksum when applicable.

## 7.7 Request coalescing: REQUIRED V1

For a given cache key at the L2 primary, at most one origin fill should be active per process. Concurrent misses should wait on the same fill with context-aware cancellation.

Requirements:

- Do not let one canceled waiter cancel a fill still needed by other waiters.
- The origin request itself must still have an upper deadline.
- Waiters have independent deadlines.
- Expose coalesced waiter count and fill count metrics.

## 7.8 Cache invalidation: REQUIRED V1

The admin API must support exact-key purge and route-wide purge.

Purge semantics:

- Control-plane leader assigns a monotonically increasing purge/config version.
- Edges receive purge events through the config stream.
- Each edge purges L1 and local L2 entries matching the event.
- Peer nodes do not need a separate consensus round for cache deletion.
- Reordered events must be handled by version checks.

Tag/prefix purge is P1.

## 7.9 P1 cache-policy benchmark

Implement a policy abstraction that makes it possible to compare:

- LRU baseline.
- TinyLFU-inspired admission + segmented eviction or an equivalent frequency-aware policy.

Create a reproducible Zipf-like workload generator. Report hit ratio, throughput, and CPU/memory overhead. Do not claim one policy is superior without showing workload-specific data.

# 8. Consistent Hash Ring

## 8.1 REQUIRED V1 algorithm

Implement the ring in the project; do not use a ready-made consistent-hash package.

Required features:

- Virtual nodes, default 128 per physical edge node.
- Stable hash primitive such as xxHash.
- Sorted ring positions.
- Deterministic ownership given the same membership snapshot.
- `Owner(key)` operation.
- `Owners(key, replicationFactor)` operation returning unique physical nodes clockwise from primary.
- Add/remove node operations through immutable snapshot replacement or safe concurrent update.
- Concurrency-safe reads without holding a global write lock on the hot request path. Preferred design: build a new immutable ring snapshot and publish atomically.

## 8.2 Ring tests

Required tests:

- Same membership produces same ring ordering.
- Every key maps to a live node.
- Replicas contain no duplicate physical node.
- Adding one node to a 3-node ring moves materially fewer keys than modulo hashing.
- Distribution is reasonably balanced over at least 100k synthetic keys.
- Removing a node only remaps keys that need a new owner according to ring semantics.
- Race detector coverage during concurrent reads and ring replacement.

Benchmark and document key movement for `3 -> 4` and `4 -> 3` nodes.

# 9. Control Plane and Raft

## 9.1 Why Raft exists here

Raft protects **desired configuration state**, including routes, origin pools, policies, and purge/config versioning. It does not sit on the request hot path and does not replicate every cache object.

This separation is required because it yields an interview-quality trade-off: control correctness needs consensus; high-volume cached content does not.

## 9.2 Static control cluster: REQUIRED V1

Use exactly three statically configured control-plane members in the reference deployment:

```text
cp-1
cp-2
cp-3
```

Each node knows all peer IDs and Raft RPC addresses from bootstrap configuration.

Dynamic membership and joint consensus are out of scope for V1.

## 9.3 Raft state: REQUIRED V1

Persist at minimum:

```text
currentTerm
votedFor
log[]
lastIncludedIndex
lastIncludedTerm
commitIndex or recoverable equivalent
state-machine snapshot
```

Persist term/vote before responding where required by Raft safety.

## 9.4 Raft node states

Implement:

- follower
- candidate
- leader

Required timers:

- Randomized election timeout.
- Leader heartbeat interval comfortably below minimum election timeout.
- Timer resets only on valid events.

No fixed identical election timeout across nodes.

## 9.5 Required Raft RPCs

### RequestVote

Must implement:

- term comparison.
- candidate log up-to-date check.
- one vote per term semantics.
- persistent vote before successful response.

### AppendEntries

Must implement:

- heartbeat with zero entries.
- previous-log index/term consistency check.
- conflicting suffix deletion.
- append of new entries.
- leader commit propagation.
- follower application in order.

### InstallSnapshot

Required for completed V1 if log compaction is enabled; otherwise it becomes a release requirement before claiming restart resilience under long-running clusters. Preferred: implement it in V1.

## 9.6 Commit and apply semantics

A client write is successful only after:

1. Leader appends command locally.
2. Entry is replicated to a majority.
3. Entry is committed under Raft rules.
4. State machine applies the entry.
5. Leader returns success with the resulting configuration version.

Never acknowledge an uncommitted configuration write as successful.

## 9.7 State machine commands

Use explicit typed commands, for example:

```text
CreateRoute
UpdateRoute
DeleteRoute
CreateOriginPool
UpdateOriginPool
DeleteOriginPool
SetGlobalSettings
PurgeCache
```

Every applied command increments a monotonic `config_version` or produces a versioned result.

The state machine must be deterministic. It must not call the network, read wall-clock time for logic, or depend on random values during apply.

## 9.8 Read semantics

Required V1 behavior:

- Admin reads are served by the leader.
- A follower receiving an admin read or write returns a leader hint or proxies internally to the known leader.
- Do not claim linearizable follower reads.

The CLI should handle leader redirection transparently.

## 9.9 Snapshotting and compaction

Required design:

- Snapshot after configurable entry count and/or log byte threshold.
- Snapshot includes replicated application state plus last included index/term.
- Persist snapshot atomically.
- Compact log prefix only after snapshot durability.
- Restart restores snapshot and replays remaining log entries.
- InstallSnapshot allows a lagging follower to recover when required entries were compacted.

## 9.10 Raft failure tests: REQUIRED V1

Build deterministic tests using an in-memory/simulated transport where practical.

Minimum scenarios:

1. Three nodes elect exactly one leader.
2. Leader crash triggers new leader election.
3. Committed entry survives leader crash.
4. Isolated minority cannot commit writes.
5. Old leader returning with stale term steps down.
6. Conflicting uncommitted entries are repaired.
7. Follower restart catches up.
8. Snapshot + log replay restores state.
9. InstallSnapshot recovers a sufficiently stale follower.
10. Repeated election instability never produces two leaders in the same term.

Add race-detector coverage around election and replication state.

# 10. Edge Membership and Configuration Distribution

## 10.1 Edge registration: REQUIRED V1

An edge node starts with:

- stable process/node ID for the lifetime of the instance.
- advertised peer-cache RPC address.
- advertised public address if useful for demo metadata.
- region and zone labels.
- capacity weight (default 1; weighted ring is P1).

It registers with any configured control-plane endpoint and follows leader hints.

## 10.2 Heartbeats

Edge sends heartbeat to leader every 2 seconds by default.

Leader keeps ephemeral liveness state in memory:

- last seen.
- health metadata.
- active connections (optional).
- load estimate (optional).
- config version applied by edge.

Heartbeat events are **not** appended individually to Raft.

Node considered unhealthy after a configurable missed-heartbeat threshold, e.g. 3 intervals plus jitter tolerance.

When healthy edge membership changes, the leader emits a new versioned membership/ring snapshot to connected edges.

## 10.3 Configuration stream

Use a server-streaming gRPC API conceptually equivalent to:

```proto
service EdgeControl {
  rpc WatchConfig(WatchConfigRequest) returns (stream ConfigEvent);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}
```

`ConfigEvent` types:

- FullSnapshot
- RouteChanged
- OriginPoolChanged
- MembershipChanged
- PurgeEvent
- LeaderNotice / reconnect hint if needed

Edge behavior:

- Apply events in version order.
- Reject stale versions.
- If a version gap cannot be reconciled, request a full snapshot.
- Atomically publish new routing/ring snapshots to request-serving goroutines.
- Keep prior valid snapshot if the incoming snapshot fails validation.

# 11. Admin API and CLI

## 11.1 Admin API: REQUIRED V1

Expose leader-oriented JSON endpoints under `/v1`.

Minimum API:

```text
GET    /v1/status
GET    /v1/raft
GET    /v1/nodes
GET    /v1/routes
POST   /v1/routes
GET    /v1/routes/{id}
PUT    /v1/routes/{id}
DELETE /v1/routes/{id}
GET    /v1/origin-pools
POST   /v1/origin-pools
GET    /v1/origin-pools/{id}
PUT    /v1/origin-pools/{id}
DELETE /v1/origin-pools/{id}
POST   /v1/cache/purge
```

Required API behavior:

- JSON request/response schemas documented in OpenAPI or `docs/api.md`.
- Validation errors use `400` with field-level details.
- Not found uses `404`.
- Version conflict, if optimistic version is supplied, uses `409`.
- Not leader uses `307/308`, `503` with leader metadata, or transparent internal forwarding. Choose one and keep CLI support simple.
- All successful writes return resulting config version.
- Admin API uses authentication when not in explicit development mode.

## 11.2 CLI: REQUIRED V1

Required commands should include equivalents of:

```text
edgemeshctl status
edgemeshctl raft status
edgemeshctl nodes
edgemeshctl routes list
edgemeshctl routes get <id>
edgemeshctl routes apply -f route.yaml
edgemeshctl routes delete <id>
edgemeshctl origins list
edgemeshctl origins apply -f origin.yaml
edgemeshctl cache purge --route <id>
edgemeshctl cache purge --key <key>
```

Output should support human-readable tables and `--json` for machine-readable output.

# 12. Health, Readiness, and Graceful Lifecycle

## 12.1 Standard endpoints

Every process must expose:

- `/healthz`: process is alive; should not depend on external systems.
- `/readyz`: process can accept intended traffic.
- `/metrics`: Prometheus scrape endpoint.

Optional debug endpoints must be on a separate admin listener or disabled by default in production mode.

## 12.2 Edge readiness

Ready when:

- public listener initialized.
- at least one valid routing snapshot is loaded.
- essential internal subsystems are initialized.

Control-plane disconnection alone must not make a previously configured edge unready.

## 12.3 Control-plane readiness

A control node is ready for general cluster service when:

- stable storage opened successfully.
- Raft loop running.
- transport listener running.

Admin writes should additionally communicate leader availability.

## 12.4 Shutdown

On SIGTERM:

1. Stop admitting new admin work / mark readiness false where appropriate.
2. Edge stops accepting new public connections after load balancer grace period.
3. Drain in-flight requests up to configured deadline.
4. Cancel background health checks/streams.
5. Flush telemetry best effort.
6. Close storage and listeners.

No goroutine leaks in shutdown tests.

# 13. Observability Requirements

## 13.1 Structured logging: REQUIRED V1

Use structured `slog` records.

Common fields:

```text
service
node_id
region
request_id
trace_id
route_id
origin_id
peer_id
raft_term
raft_role
config_version
error_class
```

Do not log authorization headers, cookies, raw secrets, or full request bodies.

Log levels:

- DEBUG: detailed state transitions, disabled by default.
- INFO: startup, leadership change, membership change, config apply, clean shutdown.
- WARN: degraded peer path, control disconnect, circuit transitions, repeated origin failure.
- ERROR: invariant failure, persistence error, unrecoverable config rejection.

## 13.2 Prometheus metrics: REQUIRED V1

Follow low-cardinality metric design. Never put raw URLs, request IDs, cache keys, or arbitrary hostnames into metric labels.

Minimum edge metrics:

```text
edgemesh_http_requests_total{route,method,status_class}
edgemesh_http_request_duration_seconds{route}
edgemesh_http_inflight_requests{route}
edgemesh_origin_requests_total{origin,result}
edgemesh_origin_request_duration_seconds{origin}
edgemesh_origin_retries_total{origin,reason}
edgemesh_circuit_breaker_transitions_total{origin,to_state}
edgemesh_rate_limit_denied_total{route}
edgemesh_cache_requests_total{tier,outcome}
edgemesh_cache_objects{tier}
edgemesh_cache_bytes{tier}
edgemesh_cache_evictions_total{tier,reason}
edgemesh_cache_fill_duration_seconds
edgemesh_peer_requests_total{operation,result}
edgemesh_peer_request_duration_seconds{operation}
edgemesh_coalesced_waiters
edgemesh_config_version_applied
edgemesh_control_plane_connected
edgemesh_ring_nodes
```

Minimum control-plane/Raft metrics:

```text
edgemesh_raft_role{node}
edgemesh_raft_term
edgemesh_raft_commit_index
edgemesh_raft_last_applied
edgemesh_raft_log_entries
edgemesh_raft_elections_total
edgemesh_raft_leadership_changes_total
edgemesh_raft_append_entries_total{result}
edgemesh_raft_request_vote_total{result}
edgemesh_raft_replication_lag_entries{peer}
edgemesh_config_version
edgemesh_edge_nodes{state}
edgemesh_config_stream_clients
```

Avoid `node` label on very high-cardinality deployments; acceptable in the small reference cluster. Document cardinality trade-offs.

## 13.3 OpenTelemetry tracing: REQUIRED V1

Trace meaningful spans:

```text
edge.request
  route.match
  cache.l1.lookup
  cache.l2.lookup_or_fill
    peer.rpc
    origin.request
    cache.replicate
  response.write
```

Control spans:

```text
admin.write
  raft.propose
  raft.replicate
  state_machine.apply
  config.broadcast
```

Required attributes should use semantic conventions where applicable and bounded custom attributes otherwise.

Trace propagation must work across ingress -> peer -> origin when origin propagation is enabled.

## 13.4 Grafana: REQUIRED V1

Ship provisioned dashboards showing at minimum:

1. Request rate / errors / p50-p95-p99 latency.
2. L1/L2 cache hit ratio and bytes.
3. Origin latency/retries/circuit state.
4. Edge membership and control connectivity.
5. Raft leader/term/commit index/replication lag.
6. Go runtime CPU/memory/goroutines/GC.

A screenshot of these dashboards under load should be included in README assets after real execution.

# 14. Security and Threat Model

## 14.1 Internal transport security: REQUIRED V1

Support mTLS for:

- Raft RPC.
- edge-control RPC.
- peer-cache RPC.

Development mode may use generated local CA certificates. Production-mode configuration must fail closed when required certificates are missing.

## 14.2 Admin authentication

At minimum:

- Development mode can use a static bearer token supplied by environment/file.
- Token must never be hard-coded in repository defaults.
- Constant-time comparison where applicable.
- Reject missing/invalid auth before expensive parsing.

P1: mTLS admin client auth.

## 14.3 SSRF/origin restrictions

Because administrators configure origins, the system must still avoid accidental dangerous defaults.

Production-mode default:

- Reject loopback, link-local, multicast, and cloud instance-metadata ranges unless explicitly allowlisted.
- Resolve hostnames carefully and defend against obvious DNS rebinding in configuration validation/connection policy where feasible.
- Require `http` or `https` scheme only.
- No `file://`, Unix socket, or arbitrary scheme support in V1.

Development config may explicitly allow loopback/container-network origins.

## 14.4 HTTP proxy hardening

Required:

- Strip hop-by-hop headers and headers nominated by `Connection`.
- Validate header sizes through server limits.
- Set sane read/write/idle timeouts.
- Do not trust client-supplied `X-Forwarded-For` by default; append only after trusted-proxy evaluation or replace in simple mode.
- Prevent response splitting through standard-library APIs and validation.
- Do not cache authenticated/private responses by default.
- Bound cached-object size.
- Stream large uncached responses.

## 14.5 Secrets

Repository rules:

- `.env.example` contains names only, never real secrets.
- Generated dev certificates are ignored or generated at runtime.
- Terraform variables accept secrets through environment/secret managers, not checked-in `.tfvars`.
- CI uses repository secrets only if cloud integration is later enabled.

## 14.6 Security documentation

Create `docs/threat-model.md` with:

- assets.
- trust boundaries.
- threat actors.
- primary abuse cases.
- mitigations.
- accepted residual risks.

Create `SECURITY.md` describing responsible reporting and clearly stating the project is educational/production-inspired.

# 15. Configuration Model

## 15.1 Bootstrap config

Bootstrap config is local node configuration, not Raft-replicated application config.

Example control config:

```yaml
node:
  id: cp-1
  raft_address: 0.0.0.0:7000
  admin_address: 0.0.0.0:7100
  telemetry_address: 0.0.0.0:9100

raft:
  peers:
    - id: cp-1
      address: cp-1:7000
    - id: cp-2
      address: cp-2:7000
    - id: cp-3
      address: cp-3:7000
  election_timeout_min: 500ms
  election_timeout_max: 900ms
  heartbeat_interval: 150ms
  snapshot_entries: 1000
  data_dir: /var/lib/edgemesh

security:
  mtls_enabled: true
  ca_file: /certs/ca.crt
  cert_file: /certs/cp-1.crt
  key_file: /certs/cp-1.key
```

Example edge config:

```yaml
node:
  id: edge-1
  region: local-a
  zone: local-a1
  public_address: 0.0.0.0:8080
  peer_address: 0.0.0.0:7200
  telemetry_address: 0.0.0.0:9200

control_plane:
  endpoints:
    - cp-1:7300
    - cp-2:7300
    - cp-3:7300
  heartbeat_interval: 2s

cache:
  l1_max_bytes: 268435456
  l2_max_bytes: 536870912
  max_object_bytes: 8388608
  shards: 64
  replication_factor: 2

proxy:
  request_timeout: 15s
  idle_timeout: 60s
  max_header_bytes: 1048576

security:
  mtls_enabled: true
  ca_file: /certs/ca.crt
  cert_file: /certs/edge-1.crt
  key_file: /certs/edge-1.key
```

All config must validate on startup with actionable errors.

# 16. Demo Origin Requirements

The bundled origin service must expose deterministic endpoints:

```text
GET /static/{id}          # cacheable 200, stable body, configurable max-age
GET /dynamic/{id}         # no-store response
GET /delay/{milliseconds} # intentional latency
GET /status/{code}        # intentional status
GET /bytes/{n}            # deterministic body size up to safe max
GET /counter/{id}         # increments origin-hit counter to prove cache behavior
GET /healthz              # health check
```

Include response headers identifying origin instance so load balancing can be demonstrated.

# 17. Local Developer Experience

## 17.1 Required commands

The repository must provide a predictable Makefile or task runner.

Target interface:

```text
make bootstrap       # install/generate local prerequisites where safe
make proto           # generate protobuf code
make fmt
make lint
make test
make test-race
make test-integration
make test-e2e
make fuzz-smoke
make benchmark
make build
make images
make dev-certs
make compose-up
make compose-down
make kind-up
make kind-down
make demo
make chaos-leader
make chaos-edge
make dashboards
make terraform-validate
```

Targets must not perform Git mutations.

`make bootstrap` must not install system packages silently with elevated privileges. It may print missing prerequisites and safe install instructions.

## 17.2 Local topology

`make compose-up` should start approximately:

- cp-1, cp-2, cp-3
- edge-1, edge-2, edge-3
- origin-1, origin-2
- OpenTelemetry Collector
- Prometheus
- Grafana
- Toxiproxy if used for fault injection

Expose stable ports documented in README.

## 17.3 `make demo`

The demo should be scripted and repeatable:

1. Wait for cluster health.
2. Create an origin pool through `edgemeshctl`.
3. Create a route for `demo.edgemesh.local`.
4. Send first request and print `MISS` evidence.
5. Send second request and print L1/L2 `HIT` evidence.
6. Show origin hit counter.
7. Print current Raft leader.
8. Kill/stop current leader through a separate chaos target or provide human instruction.
9. Wait for new leader and show term change.
10. Prove existing edge traffic continues throughout.
11. Create/update a route after election and show propagation.
12. Kill an L2 primary edge and show replica/origin degraded path.
13. Print links/ports for Prometheus/Grafana/trace UI.

The demo script should never claim success without checking exit codes and expected responses.

# 18. Kubernetes and Helm

## 18.1 Required Kubernetes objects

Control plane:

- StatefulSet with 3 replicas.
- Headless service for stable peer addressing.
- PersistentVolumeClaims for Raft storage.
- PodDisruptionBudget.
- readiness/liveness/startup probes.
- anti-affinity or topology-spread constraints.

Edge plane:

- Deployment for stateless/ephemeral edge instances in V1.
- Service for public ingress.
- Service for peer RPC if required by cluster discovery.
- readiness/liveness/startup probes.
- resource requests/limits.
- HPA optional in initial V1; P1 for custom metrics.

Observability:

- Helm can optionally deploy or integrate with Prometheus/Grafana/OTel Collector.
- Keep observability subchart optional to avoid forcing one stack in every environment.

## 18.2 Helm requirements

`deploy/helm/edgemesh` must support values for:

- image repositories/tags.
- control replica count fixed/default 3 with warning if changed incompatibly.
- edge replica count.
- resource requests/limits.
- TLS secrets.
- service types.
- storage class/size.
- observability endpoints.
- region/zone metadata.
- cache sizes.

`helm lint` must pass in CI.

# 19. Terraform / AWS EKS

## 19.1 Required Terraform scope

Provide code under `deploy/terraform/aws` that can provision a reasonable demonstration environment:

- VPC.
- public/private subnets as appropriate.
- EKS cluster.
- managed node group.
- security groups/IAM required by the cluster.
- outputs for cluster name/region.

Prefer established Terraform modules when they improve safety and reduce irrelevant infrastructure code. The portfolio-defining work is EdgeMesh, not hand-written VPC boilerplate.

## 19.2 Cost guardrails

Include a prominent cost notice.

Required rules:

- No CI workflow runs `terraform apply`.
- No script runs `terraform apply` implicitly.
- `terraform plan` and `terraform validate` are safe automation targets.
- README includes explicit `terraform destroy` instructions.
- Default variables choose a small demonstration footprint.
- Do not create NAT gateways or expensive observability services by default unless clearly justified and documented.

## 19.3 Cloud demo claims

If the project is actually deployed and benchmarked on EKS, document exact region, node types, cluster size, test duration, and cost estimate. Otherwise, state only that Terraform/Helm deployment artifacts are provided and validated.

# 20. Testing Strategy

## 20.1 Unit tests: REQUIRED V1

High-value packages require deterministic unit tests:

- ring ownership/distribution/key movement.
- route matching/precedence.
- cache key normalization.
- TTL/admission rules.
- LRU/shard accounting.
- token bucket behavior using injectable clock.
- circuit-breaker transitions using injectable clock.
- origin selection.
- config validation.
- Raft log operations.
- RequestVote/AppendEntries rules.
- snapshot serialization/restoration.

Avoid real sleeps in core unit tests. Inject clocks/timers where feasible.

## 20.2 Race tests: REQUIRED V1

At minimum run `go test -race ./...` in CI or a curated package set if integration timing makes full race execution impractical.

Special concurrency tests:

- cache read/write/eviction.
- ring snapshot replacement during traffic.
- config snapshot replacement.
- request coalescing.
- Raft election/replication state.
- shutdown during in-flight work.

## 20.3 Fuzzing: REQUIRED V1

Add Go fuzz tests for:

- cache-key generation.
- route/path normalization.
- HTTP header normalization/allowlist logic.
- Raft log decoder / persisted record parser if custom binary framing exists.
- configuration decoding/validation where useful.

CI can run short fuzz smoke windows; longer fuzzing is manual/nightly if later configured.

## 20.4 Integration tests: REQUIRED V1

Spin multiple processes/containers and verify:

- route creation propagates to all edges.
- proxy reaches origin.
- cache miss becomes peer/L1 hit.
- replica receives object.
- exact/route purge propagates.
- unhealthy origin is excluded.
- retries work only when allowed.
- rate limiting returns 429.
- control leader failure elects replacement.
- control follower restart catches up.
- edge primary failure uses replica/degraded path.
- control outage does not stop existing edge routes.

## 20.5 End-to-end tests: REQUIRED V1

Run against Compose or kind with real network listeners.

At least one test should model:

```text
create route -> generate traffic -> verify cache -> kill leader -> continue traffic -> new leader -> update route -> verify update
```

## 20.6 Property/invariant testing

For the Raft simulation and ring, assert invariants instead of only exact output examples.

Raft examples:

- at most one leader per term in the simulated history.
- committed log prefixes never diverge between healthy nodes.
- applied indexes never exceed commit index.
- terms never decrease.

Ring examples:

- ownership always references current membership.
- adding/removing nodes preserves deterministic mapping for unaffected keys.

# 21. Failure and Chaos Test Matrix

Create `docs/failure-model.md` and scripts for the following.

| Scenario | Injection | Expected behavior | Proof |
|---|---|---|---|
| Control leader crash | stop leader container | new leader elected; committed config preserved | leader/term metrics + API read |
| Control minority partition | isolate 1 node | majority remains writable | successful config write |
| Control majority unavailable | stop 2/3 nodes | admin writes fail safely; edges continue old config | 503/admin failure + data traffic success |
| Stale leader returns | restart old leader | steps down/catches up | role + term logs |
| Edge node crash | stop edge | membership removes node after timeout; ring changes | node/ring metrics |
| L2 primary crash | stop key owner | replica or origin path serves request | cache outcome + successful response |
| Origin 500s | demo endpoint/policy | retries/circuit breaker per policy | metrics/logs |
| Origin hangs | delay/Toxiproxy | timeout; no goroutine leak | pprof/goroutine baseline + timeout metric |
| Peer latency | Toxiproxy | request deadline respected; fallback if configured | trace waterfall |
| Packet loss | Toxiproxy/netem | degraded latency/errors visible | load-test report |
| Cache churn | add 4th edge | bounded key movement | ring benchmark |
| Cache full | tiny byte budget | eviction without OOM | eviction metrics |
| Bad config | invalid admin request | rejected before Raft proposal if deterministic validation allows | 400 + no version change |
| Restart follower after compaction | stop follower, advance log, restart | snapshot install/catch-up | Raft metrics/logs |

Chaos scripts must clean up their own temporary proxy/fault state when possible.

# 22. Performance and Benchmark Requirements

## 22.1 Benchmark philosophy

Performance numbers are not release constants; they depend on hardware. The repository must instead provide reproducible methodology and publish measured results from named hardware.

Every published benchmark report must include:

- CPU model / vCPU count.
- RAM.
- OS/kernel.
- Go version.
- commit/revision identifier supplied manually by human when publishing; agent must not commit.
- topology.
- request size/response size.
- cache state.
- concurrency.
- duration/warmup.
- generator command.
- p50/p95/p99 latency.
- requests/sec.
- error rate.
- CPU/RSS where available.

## 22.2 Required benchmark experiments

### A. Direct-origin baseline vs EdgeMesh pass-through

Measure overhead when caching is disabled.

### B. L1-hit throughput

Warm local cache then load one edge.

### C. L2-peer-hit throughput

Force ingress node L1 miss while primary peer has object.

### D. Cold miss / origin fill

Measure latency cost and coalescing under many same-key concurrent misses.

### E. Horizontal scaling

Compare 1, 2, and 3 edge instances under a workload with enough distinct keys.

Target evidence: clear aggregate throughput scaling, while explaining any non-linear limits. A useful success threshold is at least ~2.2x aggregate throughput from 1 to 3 edges under a workload that can distribute, if local hardware permits.

### F. Ring churn

Map at least 100k keys before/after 3 -> 4 node change and compare with modulo hashing.

### G. Leader failover

Measure time from leader termination until new leader accepts a committed admin write. Target under 2 seconds in the local demo with configured election timeouts, while prioritizing correctness over an artificially low number.

### H. Cache-policy comparison: P1

Compare LRU and TinyLFU-inspired policy on uniform and Zipf workloads.

## 22.3 No fake targets in README

Do not pre-fill README badges or bullets with invented `50k req/s`, `99.99%`, or `0 data loss` style claims. Use placeholders in planning docs until benchmark/failure tests produce evidence.

# 23. CI/CD Requirements

## 23.1 Pull-request CI

GitHub Actions should run:

1. `gofmt` check.
2. `go vet`.
3. `golangci-lint`.
4. unit tests.
5. race tests or a defined race suite.
6. protobuf generation consistency check.
7. integration smoke tests.
8. short fuzz smoke tests.
9. `govulncheck`.
10. Docker builds.
11. `helm lint`.
12. `terraform fmt -check`.
13. `terraform validate`.
14. optional CodeQL workflow.

Use caching thoughtfully but never at the cost of stale generated artifacts.

## 23.2 Generated code

Generated protobuf Go code may be committed by the human owner if desired, but the implementation agent must only generate/update files and leave them unstaged.

CI should fail if `.proto` definitions and generated code are inconsistent when the chosen workflow requires checked-in generated code.

# 24. Error Model and Reliability Semantics

## 24.1 Error classes

Internally classify errors rather than string matching:

```text
ErrValidation
ErrNotLeader
ErrUnavailable
ErrTimeout
ErrRateLimited
ErrCircuitOpen
ErrOriginFailure
ErrPeerFailure
ErrCacheMiss
ErrStorage
ErrProtocol
ErrUnauthorized
```

Wrap errors with context while preserving classification via Go error wrapping.

## 24.2 Client-facing proxy errors

Return small, consistent error bodies without internal stack traces.

Example JSON when JSON is appropriate:

```json
{
  "error": "origin_unavailable",
  "request_id": "..."
}
```

For ordinary proxied traffic, a simple text/body response is acceptable; diagnostics must remain bounded and non-sensitive.

## 24.3 Degraded modes

Document and instrument these states:

- control plane disconnected, cached config active.
- peer cache unavailable, origin fallback active.
- one origin unhealthy, alternate active.
- all origins unavailable.
- cache over budget / evicting.
- Raft has no leader.

# 25. Concurrency and Backpressure Design

## 25.1 Context propagation

Every request path must derive from a context and respect cancellation:

```text
client request context
  -> route/cache lookup
  -> peer RPC
  -> origin request
  -> replication work (bounded derived context)
```

Async replication may outlive the client briefly but must have its own strict timeout and worker bound.

## 25.2 Bounded worker pools

Use bounded queues/pools for:

- async replica writes.
- origin active health checks if large pools are configured.
- purge fan-out if necessary.
- telemetry export handled by SDK batching.

Queue-full behavior must be explicit. For cache replication, dropping a replica write with a metric is preferable to blocking user responses indefinitely because cache data is derivative.

## 25.3 Goroutine discipline

No goroutine-per-unbounded-event design. Every long-lived goroutine must have:

- owner lifecycle.
- cancellation path.
- shutdown wait group or equivalent.
- panic containment policy where necessary.

Add a leak-oriented integration test around repeated startup/shutdown or use a library only if justified.

# 26. Data Structures and Internal Interfaces

The code should expose small interfaces that enable testing without over-abstracting.

Examples:

```go
type Ring interface {
    Owner(key string) (Node, bool)
    Owners(key string, n int) []Node
}

type Cache interface {
    Get(ctx context.Context, key string) (Object, bool, error)
    Put(ctx context.Context, key string, obj Object) error
    Delete(ctx context.Context, key string) error
}

type OriginFetcher interface {
    Fetch(ctx context.Context, req FetchRequest) (*FetchResponse, error)
}

type StableStore interface {
    Load() (PersistentState, error)
    SaveTermVote(term uint64, votedFor string) error
    Append(entries []LogEntry) error
    TruncateSuffix(from uint64) error
    SaveSnapshot(Snapshot) error
}
```

Do not create interfaces solely for style; create them where alternate implementations or deterministic tests benefit.

# 27. API/Protocol Compatibility

All protobuf packages must use a versioned namespace such as `edgemesh.v1`.

Rules:

- Never reuse protobuf field numbers.
- Reserve removed fields.
- Additive changes preferred.
- Include protocol version/capabilities in node registration if useful.
- Reject incompatible peers with actionable logs.

Public admin API should also be namespaced `/v1`.

# 28. Documentation Deliverables

## 28.1 README: REQUIRED V1

The README should be visually strong but technically honest.

Recommended top structure:

1. Logo/project name + one-line description.
2. Badges for CI, Go version, license; no vanity fake performance badges.
3. 10-20 second animated demo or architecture image.
4. **Engineering Highlights** with 5-7 bullets.
5. Architecture diagram.
6. Demo results panel showing measured failover/cache behavior after tests.
7. Quick start.
8. Failure demo.
9. Benchmark summary with link to methodology.
10. Deep-dive docs.
11. What EdgeMesh implements directly vs dependencies.
12. Limitations/non-goals.

Suggested engineering-highlight phrasing **after implementation proves it**:

- Custom three-node Raft control plane with persistent logs, snapshots, leader election, and deterministic failure tests.
- Consistent-hash L2 cache with virtual nodes, replica fallback, and measured key movement under membership changes.
- Two-tier cache with request coalescing, bounded memory, TTL/admission rules, and reproducible hit-ratio benchmarks.
- Reverse proxy with pooled origin transports, retries, circuit breaking, rate limiting, health checks, and backpressure.
- OpenTelemetry traces + Prometheus/Grafana dashboards covering request, cache, peer, origin, and Raft paths.
- Docker/kind local topology plus Helm/Terraform deployment artifacts for EKS.

## 28.2 `docs/architecture.md`

Must explain:

- control vs data plane.
- consistency model.
- request path.
- control write path.
- cache fill path.
- failure boundaries.
- why Raft is not in the request hot path.

## 28.3 `docs/raft.md`

Must explain:

- implemented subset.
- persistence.
- election timings.
- log replication.
- commit rule.
- snapshotting.
- known limitations (static membership).
- failure-test matrix.

## 28.4 `docs/cache.md`

Must explain:

- L1 vs L2.
- cache key.
- HTTP cache eligibility.
- consistent hashing.
- replication.
- coalescing.
- purge semantics.
- eventual consistency trade-offs.

## 28.5 ADRs

Create concise ADRs such as:

```text
ADR-001: Go as primary implementation language
ADR-002: Strong control plane / eventual cache consistency
ADR-003: Custom Raft instead of embedded consensus library
ADR-004: gRPC for internal node protocols
ADR-005: Immutable route/ring snapshots on hot path
ADR-006: Static Raft membership for V1
ADR-007: Local L1 + consistent-hash L2 caching
ADR-008: Terraform/Helm cloud packaging without automatic apply
```

# 29. Recruiter and Interview Presentation Requirements

## 29.1 30-second repository scan

A reviewer should see, without scrolling through source:

```text
Go
Distributed Systems
Networking / Reverse Proxy
Raft Consensus
Consistent Hashing
Caching
Concurrency
Fault Tolerance
OpenTelemetry / Prometheus
Kubernetes / Terraform / AWS
Benchmarks
Chaos Testing
Security
```

Use README headings and repository topics to surface these exact concepts naturally.

## 29.2 "What I built" boundary

Create a README section called **Implemented in EdgeMesh** that explicitly distinguishes project code from commodity libraries.

Example:

```text
Implemented directly:
- Raft state machine and replication algorithm
- consistent hash ring and replica selection
- L1 cache and eviction/accounting
- route matcher
- request coalescing behavior
- circuit breaker and rate-limiter logic
- control/data-plane orchestration

Libraries used for commodity primitives:
- gRPC/protobuf transport serialization
- OpenTelemetry SDK/export
- Prometheus client
- bbolt persistence primitive
- xxHash primitive
```

This prevents the project from being dismissed as integration glue.

## 29.3 Engineering write-up

Create a long-form `docs/engineering-notes.md` or blog post after implementation covering three problems:

1. Why config uses Raft while cache does not.
2. How consistent hashing changed key movement during node churn.
3. How profiling/benchmarking found and fixed one real bottleneck.

Do not manufacture the bottleneck; profile first, then document the actual finding.

# 30. Target Demo Story

The ideal 3-5 minute engineer/recruiter demo:

1. Show architecture diagram.
2. `docker compose up` or use already-running local cluster.
3. `edgemeshctl raft status` shows leader + two followers.
4. `edgemeshctl nodes` shows three healthy edges.
5. First curl to `/counter/demo` prints `X-EdgeMesh-Cache: MISS` and origin counter `1`.
6. Second curl prints `HIT`; origin counter remains `1`.
7. Show a trace spanning ingress -> L2 peer -> origin.
8. Kill the current control leader.
9. Continue sending proxy traffic successfully.
10. Show new leader election and term increment.
11. Apply a route change through the new leader.
12. Kill the L2 primary for a selected key; show replica/origin fallback.
13. Open Grafana and show request latency, cache ratio, edge membership, and Raft panels.
14. Show benchmark report and failure matrix.

This demo should require no manual database editing and minimal narration.

# 31. Acceptance Criteria / Definition of Done

EdgeMesh REQUIRED V1 is done only when all of the following are true.

## Core proxy

- [ ] Three edge nodes can proxy configured host/path routes to demo origins.
- [ ] Route precedence is deterministic and tested.
- [ ] Connection pooling, timeouts, retries, circuit breaker, health checks, and rate limiting work.
- [ ] Public request bodies/responses stream rather than being unbounded-buffered.
- [ ] Graceful shutdown drains in-flight requests.

## Cache

- [ ] L1 cache is bounded, sharded, TTL-aware, and race-tested.
- [ ] Consistent hash ring is project-implemented with virtual nodes.
- [ ] L2 ownership and replica selection work across three edges.
- [ ] Same-key cold misses are coalesced at the owner.
- [ ] Primary failure falls back to replica/origin without making the cache a hard dependency.
- [ ] Cache eligibility follows conservative HTTP/auth/cookie rules.
- [ ] Exact and route-wide purge propagate.

## Control plane

- [ ] Three static control nodes elect one leader.
- [ ] Admin writes replicate through custom Raft.
- [ ] Committed configuration survives leader crash/restart.
- [ ] Minority cannot commit.
- [ ] Follower catches up after downtime.
- [ ] Snapshots/log compaction work and a stale follower can recover.
- [ ] State-machine apply is deterministic.
- [ ] Admin reads/writes have clear leader semantics.

## Edge/control interaction

- [ ] Edges register and heartbeat.
- [ ] Membership changes update ring snapshots.
- [ ] Configuration streams to edges with monotonic versions.
- [ ] Edges continue serving last valid configuration during control outage.

## Observability

- [ ] Structured logs contain request/route/node/term context without secrets.
- [ ] Required Prometheus metrics are scraped.
- [ ] OpenTelemetry traces cross edge/peer/origin boundaries.
- [ ] Grafana dashboards are provisioned.
- [ ] Leadership and membership changes are visible in telemetry.

## Testing/reliability

- [ ] `go test ./...` passes.
- [ ] race suite passes.
- [ ] integration and e2e suites pass.
- [ ] required Raft failure scenarios pass.
- [ ] required cache/ring invariants pass.
- [ ] fuzz smoke tests pass.
- [ ] chaos scripts demonstrate documented degraded modes.

## Deployment

- [ ] Docker images build.
- [ ] Compose local topology works.
- [ ] kind deployment works.
- [ ] Helm chart lints and deploys locally.
- [ ] Terraform formats and validates for EKS.
- [ ] No automated `terraform apply` exists.

## Portfolio quality

- [ ] README contains architecture, highlights, quick start, failure demo, measured benchmarks, and limitations.
- [ ] `docs/architecture.md`, `docs/raft.md`, `docs/cache.md`, `docs/failure-model.md`, `docs/threat-model.md`, and ADRs exist.
- [ ] Public claims are backed by reproducible output.
- [ ] "Implemented directly vs libraries" is explicit.
- [ ] No secrets, generated junk, large benchmark binaries, or local certificate private keys are tracked.

# 32. Suggested Implementation Order

The coding agent should implement in this dependency order even if executing in one larger pass.

## Phase 0: Foundation

1. Create Go module and directory structure.
2. Add Makefile, lint/test/build skeleton.
3. Add config loader/validator.
4. Add observability bootstrap (`slog`, Prometheus, OTel).
5. Define protobuf APIs and codegen.
6. Add demo origin.

Exit: binaries compile, health/metrics endpoints work, tests/CI skeleton exists.

## Phase 1: Single-edge proxy

1. Route model + matcher.
2. Origin pool/health checks.
3. Reverse proxy transport.
4. Timeouts/retries.
5. Circuit breaker.
6. Rate limiter.
7. L1 cache + key/admission policy.
8. Request coalescing.

Exit: one edge can serve routes reliably and cache locally.

## Phase 2: Peer caching and ring

1. Consistent hash implementation.
2. Edge node model and immutable ring snapshot.
3. Peer-cache gRPC.
4. `GetOrFetch` owner behavior.
5. Replica writes/fallback.
6. Ring/cache metrics and traces.

Exit: three manually configured edges perform L2 caching and tolerate one peer loss.

## Phase 3: Raft control plane

1. Raft core state/timers.
2. RequestVote.
3. AppendEntries.
4. stable persistence.
5. replicated state machine.
6. admin API leader semantics.
7. snapshots/InstallSnapshot.
8. deterministic Raft tests.

Exit: three control nodes replicate configuration and survive leader failure.

## Phase 4: Dynamic edge/control integration

1. edge registration.
2. heartbeat/liveness.
3. config stream.
4. membership snapshot/ring versioning.
5. purge events.
6. control outage behavior.

Exit: full architecture works without static route/ring files on edges.

## Phase 5: Reliability and observability

1. end-to-end traces.
2. complete metrics.
3. Grafana dashboards.
4. failure injection.
5. race/fuzz coverage.
6. shutdown/leak tests.

Exit: failure matrix is reproducible and visible.

## Phase 6: Packaging

1. Docker hardening.
2. Compose demo.
3. kind manifests.
4. Helm.
5. Terraform EKS.
6. CI validation.

Exit: local and deployment artifacts are reproducible.

## Phase 7: Portfolio polish

1. Architecture graphics.
2. Recorded demo/GIF.
3. Real benchmarks.
4. Engineering notes.
5. README recruiter pass.
6. Security/limitations review.

Exit: repository tells the engineering story without requiring source-code archaeology.

# 33. Quality Bar for Code

Required code practices:

- `gofmt` clean.
- Context-aware APIs for network/blocking operations.
- Error wrapping with `%w`.
- No ignored errors without explicit rationale.
- No package-global mutable state for request path unless safely encapsulated.
- No data races under `-race`.
- No unbounded maps for client-keyed rate limits or request metadata.
- No `time.Sleep` synchronization in correctness-sensitive tests when signals/clocks can be used.
- Prefer table-driven tests for policy logic.
- Comments explain invariants and "why", not obvious syntax.
- Exported identifiers have useful GoDoc when part of internal reusable boundaries.
- Avoid premature generic abstractions.
- Avoid reflection in hot paths unless justified.
- Benchmark before micro-optimizing.
- Profile before claiming bottlenecks.

# 34. Performance-Engineering Checklist

When optimizing, investigate in this order:

1. Establish load-test reproducibility.
2. Record CPU/RSS/goroutine baselines.
3. Capture CPU and allocation profiles.
4. Check transport connection reuse.
5. Check cache lock contention.
6. Check ring lookup allocation behavior.
7. Check object-copy count and buffering.
8. Check trace/metric label overhead.
9. Check peer gRPC message sizing/chunking.
10. Change one thing at a time and re-run.

Potential improvements only after profiling:

- More cache shards.
- Buffer pooling for bounded object chunks.
- Immutable route/ring snapshots.
- Reduced allocations in cache-key construction.
- gRPC stream chunk sizing.
- Separate telemetry sampling strategy under load.

# 35. Resume Bullet Templates: DO NOT USE UNTIL VERIFIED

These are **templates only**. Replace bracketed values with measured facts after implementation; never publish placeholders or invented values.

- Built **EdgeMesh**, a distributed edge proxy in Go with a custom 3-node Raft control plane, consistent-hash peer caching, bounded L1/L2 caches, and gRPC configuration/replication protocols.
- Sustained **[X] requests/sec at [Y] ms p99** on **[hardware/topology]**, improving aggregate throughput **[Z]x** from 1 to 3 edge nodes while documenting CPU/memory bottlenecks with reproducible profiles.
- Implemented Raft leader election, log replication, persistence, snapshots, and failover; recovered from leader loss in **[N] ms** with **zero committed configuration entries lost** in the documented fault test.
- Reduced cache remapping during 3 -> 4 node membership changes from **[modulo %]** to **[ring %]** using virtual-node consistent hashing across **[key count]** synthetic keys.
- Instrumented request/cache/origin/Raft paths with OpenTelemetry, Prometheus, and Grafana; deployed locally with Docker/kind and packaged AWS EKS infrastructure using Helm and Terraform.

# 36. Interview Story Bank

The finished project should let the owner answer these questions with evidence:

1. **Why did you use Raft for configuration but not cache replication?**
   - Strong correctness vs derivative data, hot-path cost, availability trade-off.

2. **How do you avoid a thundering herd on a cold object?**
   - L2 owner + per-key coalescing + bounded waiters/deadlines.

3. **What happens when the cache owner dies?**
   - Replica lookup, then origin fallback; cache is not a hard dependency.

4. **How much data moves when a node joins?**
   - Show consistent-hash experiment vs modulo.

5. **What prevents a stale Raft leader from accepting writes?**
   - Terms, majority replication, step-down rules, leader semantics.

6. **What happens if two control nodes disappear?**
   - No majority -> no config writes; existing edge traffic continues last valid snapshot.

7. **How did you design for backpressure?**
   - bounded replication queue, contexts, size caps, rate limits, connection limits.

8. **How did you know what to optimize?**
   - load test + pprof/profile evidence; describe one measured bottleneck.

9. **How do you avoid dangerous caching?**
   - auth/cookie defaults, Cache-Control rules, Vary, object limits, purge versioning.

10. **Why Go?**
    - networking standard library, concurrency model, small services/binaries, cloud-native ecosystem; plus personal portfolio breadth.

# 37. Optional P1: TinyLFU-Inspired Cache Admission

If implemented, keep the design explainable:

- Maintain a small frequency sketch (e.g. Count-Min Sketch).
- Maintain a small admission window.
- Candidate entering main cache competes with a victim based on estimated frequency.
- Periodically age counters.
- Keep total metadata bounded.

Benchmark on:

- uniform random keys.
- Zipfian keys.
- scan workload that pollutes ordinary LRU.

Document where LRU wins on simplicity/overhead and where frequency-aware admission improves hit rate.

# 38. Optional STRETCH: HTTP/3 / QUIC Experiment

Use `quic-go`; do not implement QUIC cryptography from scratch.

Experiment:

- Same cached object workload over HTTP/2 and HTTP/3.
- Inject 0%, 1%, and 5% packet loss plus fixed RTT.
- Compare throughput and p95/p99 latency.
- Explain head-of-line blocking differences at a high level and what the actual test showed.

This feature should live behind a flag and must not destabilize required HTTP/1.1/2 behavior.

# 39. Reference Documentation

Use official documentation as primary references when implementing integrations:

- Go 1.27 release notes: https://go.dev/doc/go1.27
- Go release history: https://go.dev/doc/devel/release
- gRPC Go documentation: https://grpc.io/docs/languages/go/
- OpenTelemetry Go: https://opentelemetry.io/docs/languages/go/
- Prometheus instrumentation practices: https://prometheus.io/docs/practices/instrumentation/
- Kubernetes Horizontal Pod Autoscaling: https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/
- Terraform EKS tutorial: https://developer.hashicorp.com/terraform/tutorials/kubernetes/eks

For Raft algorithm details, implementation should be based on the original Raft paper and cross-checked against reputable implementations, but code must remain original to EdgeMesh for the portfolio-defining consensus subsystem.

# Appendix A: One-Shot Build Instruction for a Coding Agent

Copy this appendix together with the PRD when delegating implementation:

> Build EdgeMesh according to this PRD. Treat all **REQUIRED V1** requirements and the Definition of Done as binding. Implement the project end-to-end in the stated dependency order, running formatters, builds, unit tests, race tests, integration tests, and local validation continuously. Prefer correctness and a working complete vertical slice over unfinished optional features. Do not fabricate benchmark results; create benchmark tooling and leave results clearly marked as unmeasured until you actually run them. Do not deploy paid cloud infrastructure; generate and validate Terraform/Helm only. Do not delete unrelated existing files or overwrite user work without necessity. Keep dependencies minimal and implement the custom Raft algorithm, consistent hash ring, cache logic, routing, rate limiter, circuit breaker, and orchestration in project code as specified. Document architectural trade-offs and limitations. **Under no circumstances run `git add`, `git commit`, `git push`, `git pull`, `git fetch`, `git merge`, `git rebase`, `git reset`, `git clean`, `git checkout`, `git switch`, tag creation, branch mutation, or any command that changes Git staging/history. Do not modify `.git/`. Leave every change unstaged for human review.** Read-only `git status`, `git diff`, `git log`, and `git show` are allowed only when helpful.

# Appendix B: Example Public README Architecture Block

```text
                    EdgeMesh Control Plane
              +---------+---------+---------+
              |  cp-1   |  cp-2   |  cp-3   |
              | follower| LEADER  | follower|
              +----+----+----+----+----+----+
                   |  custom Raft   |
                   +--------+--------+
                            |
                      config stream
                            |
       +--------------------+--------------------+
       |                    |                    |
   +---v----+           +---v----+           +---v----+
   | edge-1 |           | edge-2 |           | edge-3 |
   | L1/L2  |<--------->| L1/L2  |<--------->| L1/L2  |
   +---+----+ consistent +---+----+ hash      +---+----+
       |                    |                    |
       +--------------------+--------------------+
                            |
                         origins
```

# Appendix C: Final Pre-Publication Checklist

Before making the repository a centerpiece of internship applications:

- [ ] Replace every benchmark placeholder with actual measured data or remove the claim.
- [ ] Record a short demo showing cache hit, leader failure, continued traffic, new leader, and Grafana.
- [ ] Add a clean architecture diagram rendered as SVG/PNG.
- [ ] Confirm README explains which components are original.
- [ ] Confirm all CI workflows are green.
- [ ] Run `govulncheck` and dependency review.
- [ ] Run secret scanning and verify generated cert private keys are ignored.
- [ ] Run `go test -race` on the concurrency-critical packages.
- [ ] Re-run the complete failure matrix.
- [ ] Ensure Terraform examples cannot accidentally deploy from CI.
- [ ] Ensure cloud cost warnings are obvious.
- [ ] Add GitHub topics: `go`, `distributed-systems`, `raft`, `reverse-proxy`, `consistent-hashing`, `caching`, `grpc`, `opentelemetry`, `prometheus`, `kubernetes`, `terraform`, `aws`, `chaos-engineering`.
- [ ] Pin the best architecture/demo image near the top of README.
- [ ] Make the repository description and first README paragraph understandable without specialized context.
- [ ] Ask at least one experienced engineer to review `docs/architecture.md` and the benchmark methodology before publishing performance claims.

---

**End of EdgeMesh PRD v1.0**
