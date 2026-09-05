# Raft in EdgeMesh

EdgeMesh implements Raft directly rather than embedding etcd, HashiCorp Raft, or
Consul. The implementation lives in [`internal/raft`](../internal/raft/) and is
the portfolio-defining subsystem of this project.

## What is implemented

| Feature | Status | Where |
|---|---|---|
| Leader election with randomized timeouts | ✅ | [`node/election.go`](../internal/raft/node/election.go) |
| Log replication with fast backup | ✅ | [`node/replication.go`](../internal/raft/node/replication.go) |
| Durable term, vote, and log | ✅ | [`storage/`](../internal/raft/storage/) |
| Snapshotting and log compaction | ✅ | [`node/snapshot.go`](../internal/raft/node/snapshot.go) |
| InstallSnapshot for lagging followers | ✅ | [`node/snapshot.go`](../internal/raft/node/snapshot.go) |
| Pre-vote | ✅ | [`node/election.go`](../internal/raft/node/election.go) |
| Deterministic state machine | ✅ | [`statemachine/`](../internal/raft/statemachine/) |
| Dynamic membership / joint consensus | ❌ out of scope for V1 | n/a |
| Read-index / lease reads | ❌ not claimed | n/a |

Cluster membership is **static**: three configured members, fixed at bootstrap.
Adding a fourth requires a configuration change and a restart. This is stated
plainly rather than glossed over: joint consensus is the single largest piece of
Raft that is not here.

## Durability rules

Raft's safety argument rests on three things surviving a crash. Each is written
before the action that depends on it, never after:

**Term and vote, before responding to RequestVote.** A node that responds
"granted" and then crashes before persisting could vote again for a different
candidate in the same term after restarting, electing two leaders. In
[`HandleRequestVote`](../internal/raft/node/election.go) the vote is written to
bbolt and only then does the response go out; a storage failure produces an
error rather than a vote.

**Log entries, before acknowledging them.** A follower that acknowledges an
entry it has not written lets the leader believe the entry is replicated when it
is not, which can commit an entry that a majority does not actually hold.
`appendLocalLocked` writes to disk first, in-memory second.

**Snapshots, before compacting the log.** Compacting first would open a window
where a crash loses both the entries and the snapshot meant to replace them.

## The commit rule

A leader may only mark an entry committed by counting replicas when the entry is
**from the leader's current term** (Raft paper §5.4.2, figure 8). Counting
replicas of an older entry can commit something a future leader legally
overwrites.

EdgeMesh enforces this in `advanceCommitLocked`, and a newly elected leader
immediately appends a no-op entry in its own term. Committing that no-op
indirectly commits everything before it, which is both safe and what makes a new
leader's state readable within one replication round instead of waiting for the
next client write.

## The election restriction

A candidate wins only if its log is at least as up to date as the voter's:
higher last term, or equal term and at least as long. This is what guarantees
a new leader holds every committed entry. `Log.IsUpToDate` implements it and
[`TestStaleLogCandidateCannotWin`](../internal/raft/node/raft_test.go) asserts
that a node kept out of the loop cannot win an election.

## Pre-vote, and a bug it caused

Pre-vote adds a probing round before incrementing the term, so a node returning
from a partition cannot force a healthy leader to step down merely by having
advanced its term while isolated.

The first implementation refused a pre-vote when the voter's own election
deadline had not elapsed. That was wrong, and the failure mode is instructive: a
node **resets its own deadline whenever it campaigns**, so after a real leader
crash every survivor looked "recently served" to every other one, no pre-vote
was ever granted, and the cluster sat leaderless indefinitely. The unit tests
missed it because they defaulted to pre-vote off; a live three-node cluster
found it in seconds.

The fix tracks `lastLeaderContact` separately, refreshed only by an actual
leader. [`TestPreVoteStillAllowsFailoverAfterLeaderCrash`](../internal/raft/node/raft_test.go)
is the regression test.

## Timing

| Parameter | Default | Why |
|---|---|---|
| Heartbeat interval | 150 ms | Must be well below the minimum election timeout |
| Election timeout | 500-900 ms | Randomized per node to break split votes |
| RPC timeout | 2 s | Bounds a stuck peer without tripping on a slow one |
| Snapshot threshold | 1000 entries | Bounds restart replay time and disk |

Configuration validation refuses a heartbeat interval above one third of the
minimum election timeout: a healthy leader would otherwise be displaced by
ordinary scheduling jitter. Randomization is essential: with identical
timeouts, every node times out together, all become candidates, split the vote,
and repeat.

## Testing approach

Consensus is tested against an in-memory transport with **explicit, controllable
partitions** rather than containers. Every scenario becomes fast and
deterministic, so the whole failure matrix runs on every commit.

The harness ([`testcluster_test.go`](../internal/raft/node/testcluster_test.go))
provides directional partitions, crash and revive, and restart-with-the-same-data-directory
so recovery exercises real disk state.

### The required failure matrix

| # | Scenario | Test |
|---|---|---|
| 1 | Three nodes elect exactly one leader | `TestThreeNodesElectOneLeader` |
| 2 | Leader crash triggers a new election | `TestLeaderCrashElectsNewLeader` |
| 3 | Committed entries survive a leader crash | `TestCommittedEntrySurvivesLeaderCrash` |
| 4 | An isolated minority cannot commit | `TestIsolatedMinorityCannotCommit` |
| 5 | A stale leader steps down on return | `TestStaleLeaderStepsDownOnReturn` |
| 6 | Conflicting uncommitted entries are repaired | `TestConflictingUncommittedEntriesAreRepaired` |
| 7 | A restarted follower catches up | `TestFollowerRestartCatchesUp` |
| 8 | Snapshot plus replay restores state | `TestRestartRestoresCommittedState` |
| 9 | InstallSnapshot recovers a stale follower | `TestInstallSnapshotRecoversStaleFollower` |
| 10 | Election churn never elects two leaders in a term | `TestRepeatedElectionInstabilityKeepsSafety` |

Plus: the election restriction, read-your-write on the leader, follower
rejection with a leader hint, concurrent proposals, pre-vote behaviour, stale
leader-hint clearing, and failover latency.

### Invariants, not just examples

Two properties are asserted across the whole observed history rather than at a
single instant:

- **At most one leader per term.** Every leadership change is recorded by an
  observer, and `assertAtMostOneLeaderPerTerm` checks the whole history.
- **Committed prefixes never diverge.** `assertLogsConsistent` compares every
  pair of live nodes index by index up to the lower commit index.

Both run at the end of the chaos-heavy scenarios, where they are most likely to
catch something.

## Reproducing

```bash
make test-race                                   # includes the Raft suite
go test -race ./internal/raft/... -v             # with per-scenario detail
go test ./internal/raft/node/ -run Failover -v   # failover timing
```

`TestLeaderFailoverLatency` reports measured election and first-committed-write
times for the configured timeouts. It asserts only a generous ceiling: the point
is to catch a pathological regression, not to publish a number that varies with
the machine.

## Known limitations

1. **Static membership.** No joint consensus; changing the member set requires
   reconfiguration and restart.
2. **No lease reads.** Admin reads go to the leader after its normal heartbeat
   cycle; linearizable follower reads are not claimed.
3. **Snapshots are sent whole.** Chunked on the wire, but not incremental: a
   very large state machine would transfer more than strictly necessary.
4. **One state machine.** No multi-Raft or sharding; the configuration for the
   whole cluster is one replicated group.

Each is a deliberate V1 scope decision, not an oversight.
