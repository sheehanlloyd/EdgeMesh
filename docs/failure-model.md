# EdgeMesh Failure Model

Every scenario below has an expected behaviour, a way to reproduce it, and a way
to observe the outcome. Where a scenario is covered by an automated test, the
test is named; those run on every commit.

## The organizing principle

The control plane and the data plane are separate failure domains. A
control-plane failure must never stop traffic that is already configured; a
data-plane failure must never corrupt configuration. Most rows below are an
instance of that split.

## Control-plane failures

| Scenario | Expected behaviour | Automated test | Manual reproduction |
|---|---|---|---|
| One control node stops | Majority survives; cluster stays writable | `TestLeaderCrashElectsNewLeader` | `docker stop edgemesh-cp-2` |
| Leader stops | New leader within ~1 election timeout; committed config preserved | `TestCommittedEntrySurvivesLeaderCrash` | `make chaos-leader` |
| Leader is partitioned | Isolated leader cannot commit; majority elects a replacement | `TestIsolatedMinorityCannotCommit` | Toxiproxy or `docker network disconnect` |
| Stale leader returns | Steps down on seeing a higher term; discards orphaned entries | `TestStaleLeaderStepsDownOnReturn`, `TestConflictingUncommittedEntriesAreRepaired` | Stop, wait for a new leader, restart |
| **Two of three control nodes stop** | **Admin writes fail; data plane unaffected** | `TestEdgesKeepServingDuringAControlPlaneOutage` | `docker stop edgemesh-cp-2 edgemesh-cp-3` |
| All control nodes stop | Same: edges serve last valid configuration | `TestEdgesKeepServingDuringAControlPlaneOutage` | `docker stop edgemesh-cp-{1,2,3}` |
| Follower restarts after downtime | Catches up from the leader's log | `TestFollowerRestartCatchesUp` | `docker restart edgemesh-cp-3` |
| Follower restarts after compaction | Recovers via InstallSnapshot | `TestInstallSnapshotRecoversStaleFollower` | Stop, generate >snapshot_entries writes, restart |
| Repeated election churn | Never two leaders in one term | `TestRepeatedElectionInstabilityKeepsSafety` | Repeatedly partition the leader |

**Measured:** stopping the entire control plane, 30/30 data-plane requests
succeeded. Stopping the leader mid-traffic, every request issued during the
election succeeded, 0 failures.

### Observing it

```bash
edgemeshctl raft status                              # role, term, commit index, lag
curl -s localhost:9101/metrics | grep edgemesh_raft  # the same as metrics
```

Key signals: `edgemesh_raft_role`, `edgemesh_raft_term`,
`edgemesh_raft_leadership_changes_total`, `edgemesh_raft_replication_lag_entries`.

## Edge-plane failures

| Scenario | Expected behaviour | Automated test | Manual reproduction |
|---|---|---|---|
| One edge stops | Removed from the ring after the dead threshold; its keys remap | `TestEdgeDepartureUpdatesTheRing` | `make chaos-edge` |
| The L2 owner of a key stops | Replica serves it, else origin fallback; request succeeds | `TestL2PrimaryFailureFallsBack` | Stop the owning edge, request the key |
| An edge briefly misses heartbeats | Marked *suspect*, stays on the ring | `TestSweepTransitionsThroughSuspectToDead` | Pause the container briefly |
| A new edge joins | ~1/N of keys remap; the rest keep their owner | `TestRingRemovalOnlyRemapsOrphanedKeys` | Scale the StatefulSet up |
| Edge loses the control plane | Keeps serving; `control_plane_connected` drops to 0 | `TestEdgesKeepServingDuringAControlPlaneOutage` | Stop all control nodes |
| Edge cache exceeds its budget | LRU eviction, no OOM | `TestByteBudgetIsEnforced` | Set a tiny budget and drive load |
| Edge receives an invalid config | Rejects the whole snapshot, keeps the previous one | `configRegistry.ApplySnapshot` | Propose a conflicting route set |

The suspect/dead distinction is deliberate: removing a node on the first missed
heartbeat would reshuffle cache ownership for a transient blip. A suspect node
keeps serving and keeps its keys; only a dead one leaves the ring.

## Origin failures

| Scenario | Expected behaviour | Automated test | Manual reproduction |
|---|---|---|---|
| One origin stops | Retries mask it; health checks then exclude it | `TestUnhealthyOriginIsExcluded` | `make chaos-origin` |
| All origins stop | Fast `503`; breaker opens | `TestSelectFailsFastWhenAllUnhealthy` | Stop every origin |
| Origin returns 5xx | Bounded retries, then the status is returned | `TestOpensOnConsecutiveFailures` | `curl .../status/503` |
| Origin hangs | Request times out; no goroutine leak | `TestCheckerStartStopHasNoLeak` | `curl .../delay/60000` |
| Origin recovers | Breaker half-opens, probes, closes | `TestHalfOpenClosesAfterSuccessThreshold` | Restart the origin |

**Measured:** with one of two origins killed, retries produced 15/15 successes
immediately, and 20/20 after health checks converged.

The documented V1 policy when *every* origin is unhealthy is **fail fast with
`503`**, not "try the least-recently-failed anyway". A fast failure lets the
client and the breaker react correctly; a slow one just burns the client's
deadline.

## Network degradation

| Scenario | Expected behaviour | Reproduction |
|---|---|---|
| High origin latency | Request deadline respected; `503`/`504` past it | `./scripts/chaos/inject-latency.sh 500` |
| High peer latency | Peer RPC times out; falls back to a replica or origin | Toxiproxy on port 7200 |
| Packet loss | Degraded latency, no correctness change | Toxiproxy `limit_data`/`timeout` toxics |
| Slow config-stream client | Backlog replaced by one snapshot; broadcaster never blocks | `TestSlowClientDoesNotStallTheBroadcaster` |

The slow-client row is the one worth dwelling on: a single edge that stops
reading must never stall configuration delivery to every other edge. The server
bounds each client's queue and, on overflow, replaces the backlog with a fresh
snapshot, which loses nothing, since an edge only needs the newest version.

## Degraded modes

Each of these is a named state with a metric, not a silent condition:

| Mode | Signal | What still works |
|---|---|---|
| Control plane disconnected | `edgemesh_control_plane_connected = 0` | All existing routes and cached objects |
| Peer cache unavailable | `cache_requests_total{outcome="DEGRADED"}` | Everything, via origin fallback |
| One origin unhealthy | `edgemesh_origin_healthy{origin=...} = 0` | Everything, via the healthy origins |
| All origins unavailable | `circuit_breaker_state = 2` | Nothing for that route; fast `503` |
| Cache over budget | `cache_evictions_total{reason="capacity"}` rising | Everything, at a lower hit ratio |
| No Raft leader | `edgemesh_raft_role{role="leader"} = 0` everywhere | All data-plane traffic |

## Running the chaos scripts

```bash
make compose-up            # start the topology
make demo                  # scripted walkthrough with assertions

make chaos-leader          # stop the Raft leader, observe failover
make chaos-edge            # stop an edge, observe ring convergence
make chaos-origin          # stop an origin, observe exclusion
./scripts/chaos/inject-latency.sh 500 100
```

Each script cleans up its own fault state where it can and prints how to undo
what it cannot. None of them claims success without checking: a chaos script
that always prints "OK" proves nothing.

## What is not covered

- **Byzantine faults.** Raft assumes crash-stop, not malicious peers. A
  compromised control node can propose arbitrary configuration.
- **Disk corruption.** bbolt provides transactional integrity, but EdgeMesh does
  not checksum the log against silent bit rot.
- **Clock skew.** Raft here does not depend on synchronized clocks, but cache
  TTLs do: badly skewed clocks across edges produce inconsistent expiry.
- **Split-brain across regions.** Membership is a single flat set; there is no
  region-aware quorum.
