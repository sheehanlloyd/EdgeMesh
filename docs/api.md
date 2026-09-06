# EdgeMesh Admin API

JSON over HTTP under `/v1`. The schema is the protobuf schema in
[`api/proto/edgemesh/v1/control.proto`](../api/proto/edgemesh/v1/control.proto),
rendered with `protojson`, so there are no hand-maintained DTOs to drift from it.

## Leader semantics

Writes must reach the Raft leader. A follower does not proxy: it answers `503`
with the leader's address in the body, and the client retries there. Internal
forwarding would hide which node actually served a write, which is exactly what
an operator debugging a control-plane problem needs to see.

`edgemeshctl` follows the hint automatically, so point it at any control node.

```json
{
  "error": "not_leader",
  "message": "not the raft leader; leader is cp-2 at cp-2:7100",
  "leader_id": "cp-2",
  "leader_address": "cp-2:7100"
}
```

Reads that depend on leader-only state (`/v1/nodes`) redirect the same way.
Reads of replicated state are served by any node, from its applied state
machine. EdgeMesh does **not** claim linearizable follower reads.

## Authentication

When enabled, every endpoint requires `Authorization: Bearer <token>`.
Authentication runs before routing and before any body is read, so an
unauthenticated caller cannot make the process do parsing work. Comparison is
constant-time.

```bash
export EDGEMESH_TOKEN='...'
curl -H "Authorization: Bearer $EDGEMESH_TOKEN" http://cp-1:7100/v1/status
```

## Number encoding

Protobuf's JSON mapping encodes `int64` and `uint64` as **strings**, because
JavaScript numbers cannot represent the full 64-bit range exactly. Fields from
the state machine (`version`, `created_at_unix_ms`) therefore arrive quoted;
fields the API constructs itself (`config_version` in list responses) arrive as
numbers. Clients should accept both.

## Endpoints

### `GET /v1/status`

Cluster summary.

```json
{
  "node_id": "cp-1", "version": "0.1.0", "role": "leader", "term": 3,
  "leader_id": "cp-1", "is_leader": true, "config_version": 12,
  "routes": 2, "origin_pools": 1, "edge_nodes": 3,
  "commit_index": 15, "last_applied": 15
}
```

### `GET /v1/raft`

Consensus detail, including per-follower match index.

### `GET /v1/nodes`

Registered edge nodes and liveness. **Leader only**, because a follower owns no
liveness state, and returning an empty list would look like "no edges are
running".

Each node carries a `stats` object holding the last reading that edge reported on
its heartbeat: `inflight_requests`, `cache_objects`, `cache_bytes`, and
`requests_per_second`. It is absent for a node that has not heartbeated since the
current leader took over. `edgemeshctl nodes` renders that as `-` rather than as
a zero, because an unknown occupancy and an empty cache are not the same claim.

These values are observability only. They are deliberately **not** part of the
membership snapshot that defines cache-ring ownership: that snapshot has to stay
byte-stable for a given version so two edges building a ring from the same
version derive identical ownership, and occupancy changes on every heartbeat.

```json
{
  "nodes": [
    {
      "id": "edge-1",
      "region": "local-a",
      "peer_address": "127.0.0.1:7301",
      "state": "NODE_STATE_HEALTHY",
      "config_version_applied": "7",
      "stats": {
        "inflight_requests": "0",
        "cache_objects": "128",
        "cache_bytes": "1048576",
        "requests_per_second": 12.5
      }
    }
  ],
  "membership_version": "3",
  "is_leader": true
}
```

### Routes

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/routes` | List |
| `POST` | `/v1/routes` | Create; `409` if it exists |
| `GET` | `/v1/routes/{id}` | Fetch one |
| `PUT` | `/v1/routes/{id}` | Update; the path id wins over the body |
| `DELETE` | `/v1/routes/{id}` | Delete |

```bash
curl -X POST http://cp-1:7100/v1/routes -H 'Content-Type: application/json' -d '{
  "id": "demo-route",
  "hostname": "demo.edgemesh.local",
  "path_prefix": "/",
  "origin_pool_id": "demo-pool",
  "enabled": true,
  "cache_policy": {"enabled": true, "default_ttl_seconds": 60, "max_ttl_seconds": 3600},
  "retry_policy": {"enabled": true, "max_retries": 2, "backoff_base_ms": 20, "backoff_max_ms": 200},
  "rate_limit_policy": {"enabled": true, "rate_per_second": 100, "burst": 200,
                        "key_strategy": "KEY_STRATEGY_CLIENT_IP"},
  "header_policy": {"diagnostic_headers": true, "preserve_host": false}
}'
```

Validation runs **before** the Raft proposal, so a malformed route costs a `400`
rather than a consensus round and a permanent log entry.

Matching rules and every policy field are documented in
[architecture.md](architecture.md#request-path) and [cache.md](cache.md).

### Origin pools

| Method | Path |
|---|---|
| `GET` | `/v1/origin-pools` |
| `POST` | `/v1/origin-pools` |
| `GET` | `/v1/origin-pools/{id}` |
| `PUT` | `/v1/origin-pools/{id}` |
| `DELETE` | `/v1/origin-pools/{id}` |

A pool still referenced by a route cannot be deleted: doing so would leave
routes pointing at nothing and turn every matching request into a `503` with no
way to see why from the route alone.

### Settings

`GET` and `PUT` on `/v1/settings` for replication factor, ring virtual nodes,
and the maximum object size.

### Cache purge

```bash
curl -X POST http://cp-1:7100/v1/cache/purge -d '{"scope": "route", "route_id": "demo-route"}'
curl -X POST http://cp-1:7100/v1/cache/purge -d '{"scope": "key", "cache_key": "..."}'
curl -X POST http://cp-1:7100/v1/cache/purge -d '{"scope": "all"}'
```

A purge is Raft-committed, so it has a monotonic version, survives leadership
changes, and can be applied idempotently by edges that receive events out of
order.

## Optimistic concurrency

Any write accepts `?version=N`. The write is rejected with `409` unless the
object is at exactly that version, which prevents a lost update when two
operators edit concurrently.

```bash
curl -X PUT 'http://cp-1:7100/v1/routes/demo-route?version=7' -d @route.json
```

## Status codes

| Code | Meaning |
|---|---|
| `200` / `201` | Success |
| `400` | Validation failure, rejected before consensus |
| `401` | Missing or invalid credentials |
| `404` | Object does not exist |
| `409` | Already exists, version conflict, or still referenced |
| `413` | Request body over the 4 MiB bound |
| `503` | Not the leader (with a hint), or no leader elected |
| `504` | The proposal did not commit before the deadline |

`503` rather than a redirect for the not-leader case is deliberate: a `307`
would make a curl user silently re-POST their body to a different host.

## CLI equivalents

```bash
edgemeshctl status
edgemeshctl raft status
edgemeshctl nodes
edgemeshctl routes list | get <id> | apply -f route.yaml | delete <id>
edgemeshctl origins list | get <id> | apply -f pool.yaml | delete <id>
edgemeshctl settings get | apply -f settings.yaml
edgemeshctl cache purge --route <id> | --key <key> | --all
```

`apply` chooses `POST` or `PUT` based on whether the object exists, so running
it repeatedly is idempotent rather than failing with `409` on the second run.
Add `--json` for machine-readable output.
