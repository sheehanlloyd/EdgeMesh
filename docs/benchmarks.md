# Benchmarking EdgeMesh

## The rule

**No performance number appears in this repository without the hardware,
command, and conditions that produced it.**

A throughput figure without its machine is not a measurement, and a README that
claims "50k req/s" without saying on what is advertising, not engineering. The
tables below are therefore split into two kinds: results that are checked in
because a test produces them deterministically, and templates that are blank
until someone runs them.

## What every published result must record

| Field | Why |
|---|---|
| CPU model and core count | Dominates every throughput number |
| RAM | Determines cache behaviour |
| OS and kernel | Affects networking and scheduling |
| Go version | Compiler and runtime differences are material |
| Commit | The code that was measured |
| Topology | Edge count, origin count, deployment mode |
| Request and response size | Changes what is being measured |
| Cache state | Cold or warm changes results by an order of magnitude |
| Concurrency | Meaningless without it |
| Duration and warm-up | Short runs measure start-up |
| Generator command | Reproducibility |
| p50 / p95 / p99, throughput, error rate | The result |

`scripts/loadtest/run.sh` prints the environment block before every run for
exactly this reason.

## Deterministic results

These come from Go tests and are reproducible on any machine, because they
measure *ratios and counts* rather than machine-dependent rates.

### Consistent hash key movement

`go test ./internal/ring/ -run TestRingKeyMovementBeatsModulo -v`

100,000 synthetic keys, 3 → 4 nodes, 128 virtual nodes:

| Scheme | Keys remapped |
|---|---|
| Consistent hash ring | 25.9% |
| `hash(key) % N` | 75.1% |

### Ring distribution

`go test ./internal/ring/ -run TestRingDistributionBalance -v`

100,000 keys over 3 nodes:

| Virtual nodes | Worst-case deviation |
|---|---|
| 128 | ~16% |
| 2048 | ~2.6% |

### Cache policy comparison

`go test ./internal/cache/l1/ -run 'TinyLFU|Policy' -v`

Scan resistance, with a 200-key working set followed by 5,000 unique keys:

| Policy | Working set retained |
|---|---|
| LRU | 0% |
| TinyLFU | 93.5% |

Zipf (s=1.07, 20,000 keys, 200,000 requests, cache ≈1500 objects):

| Policy | Hit ratio |
|---|---|
| LRU | 0.786 |
| TinyLFU | 0.816 |

### Request coalescing

`go test ./test/integration/ -run TestConcurrentColdMissesAreCoalesced -v`

30 concurrent cold requests for one key across 3 edges → **1 origin fetch**.

### Failure-domain isolation

`go test ./test/integration/ -run 'ControlPlaneOutage|LeaderFailover' -v`

| Scenario | Data-plane requests | Failures |
|---|---|---|
| Entire control plane stopped | 30 | **0** |
| Leader killed mid-traffic | every request issued during the election | **0** |

## Microbenchmarks

```bash
make benchmark
go test -run '^$' -bench BenchmarkRing -benchmem ./internal/ring/
```

Covering: ring lookup and construction, cache get/put, cache-key construction,
route matching, rate limiting, breaker evaluation, Raft log operations, state
machine apply and serialization, and TinyLFU sketch operations.

Record these against a named machine before quoting them.

## Load-test scenarios

Each isolates one variable; mixing them produces numbers that cannot be
attributed.

| Scenario | What it measures | Command |
|---|---|---|
| `l1_hit` | Local cache hit path | `./scripts/loadtest/run.sh l1_hit 50 30s` |
| `l2_hit` | Peer-cache path | `./scripts/loadtest/run.sh l2_hit 50 30s` |
| `cold_miss` | Origin fill cost | `./scripts/loadtest/run.sh cold_miss 20 30s` |
| `coalesce` | Coalescing under a stampede | `./scripts/loadtest/run.sh coalesce 200 20s` |
| `passthrough` | Raw proxy overhead | `./scripts/loadtest/run.sh passthrough 50 30s` |
| `mixed` | Zipf-like realistic mix | `./scripts/loadtest/run.sh mixed 100 60s` |

### Result templates

Not filled in. Run the scenario and record what you measure.

#### A. Direct origin vs EdgeMesh pass-through

Isolates proxy overhead with caching disabled.

| Path | req/s | p50 | p95 | p99 |
|---|---|---|---|---|
| Direct to origin | | | | |
| Through EdgeMesh | | | | |
| **Overhead** | | | | |

#### B. L1 hit throughput

| Metric | Value |
|---|---|
| Throughput | |
| p50 / p95 / p99 | |
| Hit ratio | |

#### C. L2 peer hit throughput

Force an ingress miss with the object resident on a peer.

| Metric | Value |
|---|---|
| Throughput | |
| p50 / p95 / p99 | |
| Peer RPC p99 | |

#### D. Cold miss and coalescing

| Metric | Value |
|---|---|
| Throughput | |
| Origin fetches | |
| Coalesced waiters (peak) | |

#### E. Horizontal scaling

| Edges | req/s | Scaling factor |
|---|---|---|
| 1 | | 1.00× |
| 2 | | |
| 3 | | |

A useful success threshold is ≥2.2× aggregate throughput from 1 to 3 edges on a
workload with enough distinct keys to distribute. Explain any non-linearity
rather than omitting it. On a single laptop, all three edges share the same
cores, so perfect scaling is not expected and saying so is more honest than
quietly reporting the number.

#### F. Leader failover latency

| Metric | Value |
|---|---|
| Election timeout configured | |
| Time to a new leader | |
| Time to first committed write | |
| Data-plane requests failed | |

`TestLeaderFailoverLatency` reports the first three for the in-process cluster.

## Profiling

pprof is exposed on the telemetry listener in development mode:

```bash
go tool pprof -http=: http://localhost:9201/debug/pprof/profile?seconds=30  # CPU
go tool pprof -http=: http://localhost:9201/debug/pprof/heap                # heap
curl -s localhost:9201/debug/pprof/goroutine?debug=1 | head -40             # goroutines
```

Investigate in this order, changing one thing at a time and re-measuring:

1. Establish a reproducible load
2. Record CPU, RSS, and goroutine baselines
3. Capture CPU and allocation profiles
4. Check origin connection reuse
5. Check cache lock contention
6. Check ring lookup allocation
7. Check object copies and buffering
8. Check metric and trace label overhead
9. Check peer gRPC chunk sizing

Profile before claiming a bottleneck. The performance section of the README
stays empty until someone does.
