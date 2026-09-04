# Example configurations

Replicated configuration objects, applied through the admin API or the CLI.
These are *not* bootstrap configuration. For that see [`configs/local/`](../local/).

| File | Purpose |
|---|---|
| [`origin-pool.yaml`](origin-pool.yaml) | An origin pool with health checking |
| [`route-basic.yaml`](route-basic.yaml) | A minimal caching route |
| [`route-advanced.yaml`](route-advanced.yaml) | Every policy EdgeMesh supports |
| [`settings.yaml`](settings.yaml) | Cluster-wide settings |

```bash
edgemeshctl origins apply -f configs/examples/origin-pool.yaml
edgemeshctl routes apply  -f configs/examples/route-basic.yaml
```

`apply` chooses create or update based on whether the object already exists, so
running it twice is idempotent rather than failing with a conflict.

Every field is validated before the write is proposed to Raft: a malformed
document costs a `400`, never a consensus round.
