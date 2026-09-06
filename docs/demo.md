# Running the EdgeMesh Demo

## Fastest path

```bash
make build
make run-local     # 3 control nodes, 3 edges, 2 origins as local processes
make demo          # scripted walkthrough, every step asserted
make stop-local
```

No Docker required. Logs land in `.run/logs/`.

## With the full observability stack

```bash
make compose-up    # adds Prometheus, Grafana, Jaeger, OTel Collector, Toxiproxy
make demo
make compose-down
```

| Service | URL |
|---|---|
| edge-1 / 2 / 3 | http://localhost:8081 / 8082 / 8083 |
| Admin API | http://localhost:7101 / 7102 / 7103 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Traces | http://localhost:16686 |
| Origins | http://localhost:9001 / 9002 |

## What the demo asserts

`scripts/demo.sh` verifies every step and exits non-zero on failure. A demo that
cannot fail proves nothing.

1. The cluster is healthy and a leader is elected
2. An origin pool and a caching route are applied through the CLI
3. A first request is a `MISS` **and the origin counter increments**
4. A second request is a `HIT` **and the counter does not**
5. Requests to the other edges are served from the distributed cache **with no
   further origin fetches**
6. The Raft leader is killed while traffic flows, with **zero request failures**
7. A new leader is elected **in a higher term**
8. A configuration change through the new leader **reaches every edge**
9. A purge propagates and **the next request refetches**

## Doing it by hand

```bash
export EDGEMESH_SERVER=127.0.0.1:7101

./bin/edgemeshctl status
./bin/edgemeshctl raft status
./bin/edgemeshctl nodes
```

Configure a route:

```bash
cat > /tmp/pool.yaml <<'EOF'
id: demo-pool
load_balancing: LOAD_BALANCING_ROUND_ROBIN
health_interval_ms: 2000
unhealthy_threshold: 2
healthy_threshold: 1
origins:
  - {id: origin-1, scheme: http, host: 127.0.0.1, port: 9001, health_path: /healthz, expected_statuses: [200]}
  - {id: origin-2, scheme: http, host: 127.0.0.1, port: 9002, health_path: /healthz, expected_statuses: [200]}
EOF

cat > /tmp/route.yaml <<'EOF'
id: demo-route
hostname: demo.edgemesh.local
path_prefix: /
origin_pool_id: demo-pool
enabled: true
cache_policy: {enabled: true, default_ttl_seconds: 120}
retry_policy: {enabled: true, max_retries: 2, backoff_base_ms: 20, backoff_max_ms: 200}
header_policy: {diagnostic_headers: true}
EOF

./bin/edgemeshctl origins apply -f /tmp/pool.yaml
./bin/edgemeshctl routes apply -f /tmp/route.yaml
```

Watch the cache work:

```bash
# MISS, then HIT on the same edge, then HIT or PEER_HIT on the others
# depending on where the key lands on the ring. Exactly one origin fetch
# across all of them either way.
for port in 8081 8081 8082 8083; do
  curl -sD- -o /dev/null -H 'Host: demo.edgemesh.local' \
    http://127.0.0.1:$port/counter/demo | grep -i x-edgemesh-cache
done

curl -s http://127.0.0.1:9001/stats; curl -s http://127.0.0.1:9002/stats
```

Watch the failure domains stay separate:

```bash
# Kill the leader and keep sending traffic. Every request should succeed.
make chaos-leader &
for i in $(seq 1 50); do
  curl -so /dev/null -w '%{http_code} ' -H 'Host: demo.edgemesh.local' \
    http://127.0.0.1:8081/counter/demo
done; echo
```

## Demo origin endpoints

| Endpoint | Behaviour |
|---|---|
| `/static/{id}` | Cacheable, byte-stable body (`?max-age=N`) |
| `/dynamic/{id}` | `Cache-Control: no-store` |
| `/delay/{ms}` | Intentional latency |
| `/status/{code}` | Intentional status |
| `/bytes/{n}` | Deterministic body of n bytes |
| `/counter/{id}` | Increments an origin-hit counter, which is how a cache hit is *proven* |
| `/vary` | `Vary: Accept-Encoding` |
| `/private` | `Cache-Control: private` |
| `/setcookie` | Carries `Set-Cookie` |
| `/stats` | Request and counter totals |

`/counter/{id}` is the important one: if the counter does not advance, the
origin was genuinely not contacted.

## Grafana

Five provisioned dashboards: **Request Overview**, **Cache**, **Origin &
Reliability**, **Control Plane & Raft**, and **Go Runtime**. Generate load with
`make loadtest` and watch the cache hit ratio climb and the Raft panels react to
`make chaos-leader`.

## Traces

Jaeger at http://localhost:16686. A cold request produces a trace spanning the
ingress edge, the peer-cache RPC to the owner, and the origin request: three
processes in one trace, which is what the OpenTelemetry propagation exists for.

## Verification status

Everything here has been run in this repository:

| Path | Status |
|---|---|
| `make run-local` + `make demo` | **Verified**. All 11 steps pass every assertion. Run four times consecutively from a clean state, with cp-1, cp-2, and cp-3 each landing as the leader that step 8 kills |
| Unit, race, integration, deploy, fixture suites | **Verified**. All green under `-race`, including repeated runs of the Raft failure matrix |
| `make test-e2e` (real processes) | **Verified**. 8 scenarios against real binaries, stable across repeated runs |
| mTLS across Raft, edge-control, and peer-cache | **Verified**. A full cluster ran with mutual TLS enabled |
| Chaos scripts | **Verified**. `kill-leader`, `kill-edge`, and `fail-origin` each pass their own assertions, including `kill-edge` against a cluster whose control node is already down. `inject-latency` needs Toxiproxy from the Compose topology and says so plainly when it is absent |
| `helm lint` and `helm template` | **Verified**. Output is fed through the real config loader |
| `terraform fmt` / `init` / `validate` | **Verified** |
| `make fuzz-smoke` | **Verified**. All nine fuzz targets discovered and run, 10s each |
| `golangci-lint`, `govulncheck` | **Verified**. Clean |
| Container image builds | **Built by CI** on every push, all three images |
| kind deployment | Chart renders and its output passes the real config loader; `make kind-up` needs a local kind install |

The last two rows depend on tooling outside the Go toolchain. Images are built by
the `images` job on every push, and `make kind-up` wants kind on your machine;
`make bootstrap` reports whether you have it.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `no control node answered` | Cluster not running | `make run-local` or `make compose-up` |
| `no stable leader` | Election in progress | Wait a few seconds |
| `404` from an edge | Route not applied, or wrong `Host` | Check `edgemeshctl routes list` |
| Always `MISS` | Response not cacheable | Check `Cache-Control`; see [cache.md](cache.md) |
| `503` from an edge | No healthy origin | Check origins are running |
| Edges missing from `nodes` | Not yet registered | Wait one heartbeat interval |
