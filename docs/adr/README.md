# Architecture Decision Records

Each ADR records one decision, the alternatives considered, and the consequences
accepted. They are written to be readable by someone who was not in the room.

| ADR | Decision |
|---|---|
| [001](001-go-as-implementation-language.md) | Go as the implementation language |
| [002](002-strong-config-eventual-cache.md) | Strong configuration, eventual cache |
| [003](003-custom-raft.md) | Implement Raft rather than embed a library |
| [004](004-grpc-for-internal-protocols.md) | gRPC for internal protocols |
| [005](005-immutable-hot-path-snapshots.md) | Immutable snapshots on the hot path |
| [006](006-static-raft-membership.md) | Static Raft membership for V1 |
| [007](007-two-tier-cache.md) | Local L1 plus consistent-hash L2 |
| [008](008-no-automatic-terraform-apply.md) | No automatic `terraform apply` |
| [009](009-custom-coalescer.md) | A custom coalescer instead of singleflight |
| [010](010-statefulset-for-edges.md) | StatefulSet rather than Deployment for edges |
