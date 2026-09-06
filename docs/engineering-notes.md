# Engineering Notes

Seven things worth writing down, because they are where the design met reality.

---

## 1. Why configuration gets consensus and the cache does not

The easy version of this decision is "config is important, cache is not." That
is wrong, and reasoning from it produces a worse system.

Both are important. The real distinction is **what it costs to be wrong, and
what it costs to be sure.**

### The cost of divergence

If two edges disagree about a route, identical requests go to different origins.
No error is raised. No metric moves. A client gets a response from the wrong
backend and nothing in the system knows. Detecting it requires noticing that
traffic is distributed oddly and then working backwards.

If two edges disagree about a cached object, one serves slightly older content
than the other, which is the *normal* behaviour of every cache ever built, and
is already bounded by the TTL the origin chose.

Divergence is silent and unbounded in one case, visible and bounded in the other.

### The cost of certainty

Making configuration strongly consistent costs one Raft round per admin write.
Writes happen when an operator changes something: a few times a day at most.

Making cached content strongly consistent would cost a quorum round trip per
*fill*, and cache invalidation would have to be linearizable with respect to
reads. On a system whose entire purpose is to avoid a network round trip, that
would put a *coordinated* network round trip in front of the one being avoided.

So: the state that changes rarely and diverges silently pays for coordination.
The state that changes constantly and diverges harmlessly does not.

### Where the line got tested

Edge membership sits uncomfortably between the two. It feels like configuration
because it determines cache ownership, but it changes on a two-second heartbeat.

It is deliberately **not** replicated. Committing every heartbeat would dominate
the Raft log and force snapshots purely to discard liveness churn. Liveness is
leader-local ephemeral state; when leadership moves, the new leader rebuilds its
roster from the heartbeats it then receives.

That choice caused a real bug. When a node becomes leader its roster is empty,
and an edge whose config stream was *already attached to that node* never
re-registers, because registration happens when a stream opens. The edge stayed
invisible to the ring indefinitely.

The fix was to make the heartbeat response's `reregister` flag actually do
something: it now forces the config stream to restart, which re-registers the
edge. The general lesson is that "rebuild from the next heartbeat" needs a path
for clients that are already connected, not only for ones that reconnect.

---

## 2. Consistent hashing, measured

The claim is that consistent hashing moves less data than modulo hashing when
membership changes. Here is what it actually does, over 100,000 synthetic keys
going from three edges to four:

| Scheme | Keys remapped |
|---|---|
| Consistent hash ring, 128 vnodes | **25.9%** |
| `hash(key) % N` | **75.1%** |

Theoretical ideal for 3→4 is 25%. Modulo throws away three quarters of the cache
to add one node's worth of capacity; the ring gives up only the share the new
node takes on.

### The part that is easy to get wrong

Virtual nodes are not an optimization, they are what makes the scheme work. With
one ring position per node, three nodes carve the keyspace into three arbitrary
arcs whose sizes depend entirely on where three hashes happen to land. The
result can be wildly uneven and there is nothing to average it out.

With V positions per node, load is the sum of V independent arcs, so the
standard deviation of a node's share falls as roughly `1/√V`:

| Virtual nodes | Worst-case deviation over 100k keys, 3 nodes |
|---|---|
| 128 (default) | ~16% |
| 2048 | ~2.6% |

The first test asserting this was written to expect 15% at 128 vnodes and
failed at 15.99%. The instinct was to bump the threshold. That would have been
testing nothing.

What the test asserts now is the bound that actually holds at the default *and*
that raising the vnode count monotonically improves the distribution. The second
assertion is the one with teeth: it fails if the placement function stops being
statistically sound, which a loosened threshold would silently permit.

---

## 3. Two bugs that only a running cluster could find

The unit and integration suites were green. Three nodes on a laptop were not.

### The pre-vote deadlock

Pre-vote exists so a node returning from a partition cannot disrupt a healthy
leader by having advanced its term while isolated. A voter refuses a pre-vote if
it is still hearing from a leader.

The first implementation expressed "still hearing from a leader" as *my election
deadline has not elapsed*. That reads correctly and is wrong, because **a node
resets its own election deadline whenever it campaigns.**

After a genuine leader crash:

- both survivors time out and start pre-vote rounds,
- each resets its own deadline in doing so,
- so each looks "recently served" to the other,
- so each refuses the other's pre-vote,
- forever.

The cluster sat leaderless indefinitely. Two survivors, a perfectly good
majority, no leader.

The unit tests missed it because they defaulted to pre-vote off, so the feature
was tested in isolation (a partitioned node must not inflate its term, which it
did correctly) but never in combination with the failure it interacts with.

The fix tracks `lastLeaderContact` separately, refreshed only by an actual
AppendEntries from a leader. A node campaigning cannot refresh it, so it means
what it says.

### The event-reordering window

Symptom: two of three edges stuck at membership version 2 while the leader was
at version 3. Intermittent, roughly one run in three.

The config stream sends each new client an opening snapshot, then streams
subsequent changes through a bounded queue. The snapshot is captured at send
time. So:

1. edge-1 connects and is added as a client,
2. edge-2 registers; a membership event is queued for edge-1,
3. edge-1's snapshot is captured, now at v3,
4. edge-1 receives the v3 snapshot,
5. edge-1 then drains its queue and receives the **v2** event,
6. edge-1 rebuilds its ring from v2 and stays there.

The real client was protected by a version guard. The test harness was not, but
the harness was mirroring the real client, and the fact that the protocol
*required* a defensive client was itself the problem.

Two fixes, deliberately:

- **The server drains a client's queue before capturing the opening snapshot**,
  so nothing older than the snapshot can arrive after it. This removes the
  window at the source.
- **The harness got the version guard the real client already had**, so the
  contract is enforced on both sides.

### What both have in common

Each was a *combination* failure. Pre-vote worked; failover worked; pre-vote
during failover deadlocked. Snapshots worked; broadcasts worked; a broadcast
landing between `addClient` and the snapshot capture reordered them.

Unit tests are excellent at "does this component do what it says." They are
structurally poor at "do these components do the right thing when they interact
at an unlucky moment." That is what the in-process integration harness is for:
real transports, real Raft, real caches, process boundaries removed so it runs
in seconds and can be executed on every commit. It found both bugs in the first
few runs, and the flakiness that revealed the second one was a feature: a test
that fails one time in three is telling you about a race.

The instinct on seeing a flaky test is to add a retry or extend a timeout. Both
of these would have been silenced by a longer timeout, and both were real bugs
that would have shown up in production as "the cluster occasionally doesn't
elect a leader" and "edges occasionally have the wrong ring."

---

## 4. Two more the tests were not asking about

The two above were found by running a cluster. These two were found by going back
to tests that were already passing and asking what they actually assert.

### The leader that voted to depose itself

`TestPreVotePreventsTermInflation` passed on every run. Read closely, it captured
the isolated node's term into a variable named `inflated` and then only ever
printed it. The one property pre-vote exists to provide was never asserted.

Adding the assertion, and slowing the election timers so ordinary machine load
could not cause a spurious election, turned up something the original test could
not have caught: the returning node was not inflating its term at all, and it was
still taking leadership.

The pre-vote guard refused only in one case:

```go
if n.role == RoleFollower && n.leaderID != "" &&
    n.clk.Now().Sub(n.lastLeaderContact) < n.cfg.ElectionTimeoutMin {
    return false
}
```

A leader is not a follower, so a leader fell through and granted. In a three-node
cluster the returning node needs exactly one vote besides its own. The healthy
follower refuses, because it is being heartbeated. The leader hands over the
deciding vote, the pre-vote round succeeds, and a real election follows that the
returning node wins on an equal log. The mechanism built to protect the leader was
supplying the vote that removed it.

It is worse at five nodes, where two partitioned nodes plus an obliging leader
are a majority on their own.

The fix is one clause: a leader refuses. It is, by definition, still hearing from
a leader. That does not reintroduce the deadlock above, because the deadlock case
is two *followers* with stale leader contact, and they still grant each other.

### The snapshot install that gave up half-way

`InstallSnapshot` on the receiving side does four things: persist the snapshot,
restore the state machine from it, reclaim the log prefix it replaces, and move
the in-memory Raft position to match. The third step returned its error to the
caller.

That ordering means a failure to reclaim the prefix aborted the install *after*
the state machine had already been restored. The node was left holding snapshot
state while its Raft log still reported the old position, so it would re-apply
entries the snapshot already covered and advertise to the leader a position its
own state machine had passed.

Reclaiming the prefix is housekeeping. The snapshot is already durable and
recovery reads it before any surviving entry, so the failure is now logged and
the install finishes.

The same handler had a second problem, which is what put it under suspicion in
the first place: it dropped the consensus lock across all of that slow work. The
apply loop writes to the same state machine, also outside the lock, and the
local snapshot path reads it through `Serialize`. Nothing stopped a leader's
snapshot being restored underneath an entry that was part-way through being
applied, or a local snapshot pairing a serialized image of the restored state
with the index it held before the restore.

The state machine and its durable snapshot are now one unit behind their own
mutex, ordered before the consensus lock, so applying and installing exclude each
other without either of them holding up an election.

### What these two have in common

Neither needed a cluster. Both needed a test that was asking the right question.
The first test asserted something adjacent to the property and passed. The second
path had no test at all for its error branch, and the error branch was where the
damage was.

Both now have regression tests that fail against the original code: a unit test
that a leader refuses a pre-vote, and one that drives a snapshot install through a
store whose prefix reclamation always fails and checks that the node still adopts
the snapshot.

---

## 5. The demo is a test, and it found a bug in the product

`scripts/demo.sh` asserts every step. It does not print a walkthrough and hope;
it checks the cache header, reads the origin's own hit counter, and fails loudly
when the two disagree. Running it is what surfaced the next one.

### PEER_HIT that had just been to the origin

Step 5 asks the first edge for a cold object and expects `MISS`. It got
`PEER_HIT`, while the origin's counter went from 0 to 1 in the same request. The
header said the cluster already had the object. The origin said otherwise, and
the origin was right.

The peer protocol has two ways to answer a `GetOrFetch`. The owner serves the
object from its own tier, or the owner does not have it either and fetches it
from the origin on the caller's behalf. Both come back down the same stream, and
the ingress edge labelled both `PEER_HIT`:

```go
obj, err := c.opts.Peers.GetOrFetch(ctx, addr, req)
if err == nil && obj != nil {
    return obj, cache.OutcomePeerHit, nil
}
```

The owner was already reporting which of the two had happened. `ObjectMetadata`
carries an outcome field, the server fills it in from `GetOrFill`, and the client
parsed the metadata and then dropped that field on the floor.

So the first request for any object owned by another edge reported a cache hit.
Not an inflated number in a dashboard: the diagnostic header the README teaches
people to read, and the metric behind the L2 hit ratio, both counting an origin
fetch as a hit. The measurement that the whole caching story rests on was
flattering itself in exactly the case where it mattered.

The fix is to carry the owner's answer through instead of assuming it. An unset
outcome now reads as a miss, because the conservative direction for a cache
statistic is to under-claim.

### The demo's own blind spot

Step 9 then hung. The cluster was healthy, a new leader had been elected in
600 ms, and the script waited 30 seconds and gave up.

`leader_id` was reading from `first_of "$EDGEMESH_ADMIN"`, which is always the
first control node. The list has three entries precisely so the script survives
one of them dying. Step 8 kills the current leader, and that leader is cp-1 about
a third of the time, so a third of runs reported a working cluster as gone.

It is the same shape as the harness bug in section 3: the tooling had a defect
that only appeared in combination with the failure it was written to observe.
This one fails towards a false alarm rather than a false pass, which is the
better direction, but only until someone believes it and goes looking for an
outage that never happened.

Both helpers now ask every node, and `leader_id` keeps asking until one names a
leader, because a node that has not yet learned the election result answers with
an empty string and that is not the same as no leader.

---

## 6. A fuzz target nobody was running

`make fuzz-smoke` iterated a hardcoded list of five packages. The repository has
nine fuzz targets across six. `internal/security`'s `FuzzAuthenticate` was
written and then never executed, by the smoke target or by CI, which runs the
same target.

Replacing the list with discovery over `go list ./...` found it, and it failed
inside a second:

```
credential "beArer correct-horse-battery-staple" was accepted
```

The implementation was right. RFC 7235 makes the auth-scheme case-insensitive
and `Authenticate` compares it with `strings.EqualFold`, so `beArer` is a valid
spelling. The test was wrong: it had enumerated the three spellings someone
thought of, and treated everything else as an attack.

```go
if header != "Bearer correct-horse-battery-staple" &&
    header != "bearer correct-horse-battery-staple" &&
    header != "BEARER correct-horse-battery-staple" {
    t.Fatalf("credential %q was accepted", header)
}
```

That is a list of examples wearing the costume of a property. `EqualFold` accepts
sixty-four spellings of the scheme; the test knew about three. The rewrite states
the rule instead and derives the expected answer from it, so it now checks both
directions: nothing outside the rule is accepted, and nothing inside it is
rejected. It survives three million executions.

The lesson is not about bearer tokens. A fuzz target that nothing runs is a file,
not a test, and the runner that decides what gets run deserves the same suspicion
as the code under it. A hardcoded list of packages will drift the moment someone
adds a target somewhere new, and it will drift silently, because the job it feeds
goes on passing.

---

## 7. A unit test that was right about the wrong thing

`edgemeshctl` takes its global flags before the subcommand as well as after it,
because `edgemeshctl --server X status` is what a person actually types. Leading
flags are lifted out by `hoistGlobalFlags` and re-appended after the subcommand,
so each subcommand's own `FlagSet` parses them and every flag is defined once.

`TestHoistGlobalFlags` covers that helper thoroughly. Eight cases: the equals
form, the single-dash form, an unknown leading flag, a dangling value flag. All
green. The helper was correct.

The caller was not:

```go
args, leading := hoistGlobalFlags(args)
if len(leading) > 0 && len(args) > 1 {
    args = append(args, leading...)
}
```

After hoisting, `--server X status` leaves `args` holding one element. `len(args)
> 1` is false, so the hoisted flags went on the floor. Every subcommand taking
no further arguments ignored `--server`, `--token`, `--json`, and `--timeout`.
That is `status` and `nodes`, the two an operator runs most.

Ignored them *silently*, which is the part that matters. A dropped `--server`
raised no error. It fell back to the default endpoint and reported that node as
unreachable, so the message named an address nobody had asked for:

```
$ edgemeshctl --server http://127.0.0.1:7102 status
edgemeshctl: no control node reachable: Get "http://127.0.0.1:7101/v1/status"...
```

A dropped `--token` was worse: an unexplained 401 against a cluster whose token
was fine.

It reached further than the CLI. `scripts/chaos/kill-edge.sh` opens with `ctl
nodes`, and `lib.sh` hands every configured admin endpoint to `--server` exactly
so a scenario that has just killed the leader still has somewhere to ask. The
flag was discarded, the default endpoint was the node the previous scenario had
killed, and the script died on its first line. The comment directly above
`admin_get` in that same file warns about this hazard and guards against it
properly. The bug was in the helper next to it.

A unit test proves a function, not a feature. `hoistGlobalFlags` had excellent
coverage while the behaviour it exists to provide was entirely broken, because
nothing tested the two lines that call it. The regression test now targets
`normalizeArgs`, the seam that decides what a subcommand actually receives.

And falling back is not the same as failing. Had the dropped flag produced an
error, this would have surfaced the first time anyone typed the command. Quietly
substituting a default turned a wrong invocation into a plausible wrong answer,
and pushed the cost onto whoever read the misleading message later.

---

## What is deliberately not here

A section claiming a profiling-driven optimization. The benchmark tooling and
methodology are in place ([benchmarks.md](benchmarks.md)), the pprof endpoints
are wired, and the microbenchmarks run, but no load test has been run on
documented hardware yet, so there is no measured bottleneck to write about.

Inventing one would be worse than leaving this section short.
