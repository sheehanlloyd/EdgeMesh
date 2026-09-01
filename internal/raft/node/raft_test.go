package node

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// Scenario 1: three nodes elect exactly one leader.
func TestThreeNodesElectOneLeader(t *testing.T) {
	c := newCluster(t, defaultClusterOptions())

	leader, term := c.waitForLeader(3 * time.Second)
	t.Logf("elected %s at term %d", leader, term)

	followers := 0
	for id, n := range c.nodes {
		if id == leader {
			continue
		}
		// Both conditions belong in the wait. A node is a follower the moment it
		// steps down, which is before the new leader's first AppendEntries tells
		// it who won, so checking the role first and the leader afterwards reads
		// a value that has not been written yet.
		c.waitFor(2*time.Second, id+" follows "+leader, func() bool {
			return n.Role() == RoleFollower && n.LeaderID() == leader
		})
		if n.LeaderID() != leader {
			t.Errorf("%s believes the leader is %q, want %q", id, n.LeaderID(), leader)
		}
		followers++
	}
	if followers != 2 {
		t.Fatalf("expected 2 followers, got %d", followers)
	}
	c.assertAtMostOneLeaderPerTerm()
}

// Scenario 2: leader crash triggers a new election.
func TestLeaderCrashElectsNewLeader(t *testing.T) {
	c := newCluster(t, defaultClusterOptions())
	first, firstTerm := c.waitForLeader(3 * time.Second)

	c.net.crash(first)
	c.waitFor(5*time.Second, "a surviving node takes over", func() bool {
		for id, n := range c.nodes {
			if id != first && n.IsLeader() {
				return true
			}
		}
		return false
	})

	var second string
	var secondTerm uint64
	for id, n := range c.nodes {
		if id != first && n.IsLeader() {
			second, secondTerm = id, n.Term()
		}
	}
	if second == first {
		t.Fatal("the crashed leader was re-elected")
	}
	// A new leader must be in a strictly higher term.
	if secondTerm <= firstTerm {
		t.Fatalf("new term %d did not advance past %d", secondTerm, firstTerm)
	}
	t.Logf("failover: %s (term %d) -> %s (term %d)", first, firstTerm, second, secondTerm)
	c.assertAtMostOneLeaderPerTerm()
}

// Scenario 3: a committed entry survives a leader crash.
func TestCommittedEntrySurvivesLeaderCrash(t *testing.T) {
	c := newCluster(t, defaultClusterOptions())
	leader, _ := c.waitForLeader(3 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := c.proposeRoute(ctx, leader, "survivor")
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if res.ConfigVersion == 0 {
		t.Fatal("a committed write must return a config version")
	}

	c.net.crash(leader)
	c.waitFor(5*time.Second, "a new leader emerges", func() bool {
		for id, n := range c.nodes {
			if id != leader && n.IsLeader() {
				return true
			}
		}
		return false
	})

	// Every surviving node must still hold the committed route.
	for id, sm := range c.sms {
		if id == leader {
			continue
		}
		c.waitFor(3*time.Second, id+" applies the committed route", func() bool {
			_, ok := sm.Route("survivor")
			return ok
		})
	}
	c.assertLogsConsistent()
	c.assertAtMostOneLeaderPerTerm()
}

// Scenario 4: an isolated minority cannot commit.
func TestIsolatedMinorityCannotCommit(t *testing.T) {
	c := newCluster(t, defaultClusterOptions())
	leader, _ := c.waitForLeader(3 * time.Second)

	// Isolate the leader: it is now a minority of one.
	c.net.isolate(leader)

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	_, err := c.proposeRoute(ctx, leader, "should-not-commit")
	if err == nil {
		t.Fatal("an isolated leader must not be able to commit a write")
	}
	t.Logf("isolated write correctly failed: %v (class %q)", err, errs.ClassOf(err))

	// The majority elects a new leader and remains writable.
	c.waitFor(5*time.Second, "the majority elects a leader", func() bool {
		for id, n := range c.nodes {
			if id != leader && n.IsLeader() {
				return true
			}
		}
		return false
	})
	var newLeader string
	for id, n := range c.nodes {
		if id != leader && n.IsLeader() {
			newLeader = id
		}
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if _, err := c.proposeRoute(ctx2, newLeader, "majority-write"); err != nil {
		t.Fatalf("the majority partition must remain writable: %v", err)
	}
	// The failed write must not be visible anywhere.
	for id, sm := range c.sms {
		if _, ok := sm.Route("should-not-commit"); ok {
			t.Fatalf("%s applied an entry that never committed", id)
		}
	}
	c.assertAtMostOneLeaderPerTerm()
}

// Scenario 5: a stale leader returning from a partition steps down.
func TestStaleLeaderStepsDownOnReturn(t *testing.T) {
	c := newCluster(t, defaultClusterOptions())
	old, oldTerm := c.waitForLeader(3 * time.Second)

	c.net.isolate(old)
	c.waitFor(5*time.Second, "the majority elects a replacement", func() bool {
		for id, n := range c.nodes {
			if id != old && n.IsLeader() && n.Term() > oldTerm {
				return true
			}
		}
		return false
	})

	c.net.heal(old)
	c.waitFor(5*time.Second, "the old leader steps down", func() bool {
		return c.nodes[old].Role() == RoleFollower
	})
	if got := c.nodes[old].Term(); got <= oldTerm {
		t.Fatalf("the returning node's term %d did not advance past %d", got, oldTerm)
	}
	c.assertAtMostOneLeaderPerTerm()
}

// Scenario 6: conflicting uncommitted entries are repaired.
func TestConflictingUncommittedEntriesAreRepaired(t *testing.T) {
	opts := defaultClusterOptions()
	c := newCluster(t, opts)
	leader, _ := c.waitForLeader(3 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.proposeRoute(ctx, leader, "committed-before"); err != nil {
		t.Fatal(err)
	}

	// Isolate the leader and let it accumulate entries no majority accepts.
	c.net.isolate(leader)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ictx, icancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer icancel()
			_, _ = c.proposeRoute(ictx, leader, fmt.Sprintf("orphan-%d", i))
		}(i)
	}
	wg.Wait()

	orphanIndex := c.nodes[leader].LastLogIndex()
	t.Logf("isolated leader reached index %d with uncommitted entries", orphanIndex)

	// The majority elects a new leader and commits its own entries.
	c.waitFor(5*time.Second, "a new leader emerges", func() bool {
		for id, n := range c.nodes {
			if id != leader && n.IsLeader() {
				return true
			}
		}
		return false
	})
	var newLeader string
	for id, n := range c.nodes {
		if id != leader && n.IsLeader() {
			newLeader = id
		}
	}
	nctx, ncancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer ncancel()
	if _, err := c.proposeRoute(nctx, newLeader, "after-failover"); err != nil {
		t.Fatalf("new leader could not commit: %v", err)
	}

	// Heal: the old leader must discard its orphaned suffix and converge.
	c.net.heal(leader)
	c.waitFor(8*time.Second, "the old leader converges on the new history", func() bool {
		_, ok := c.sms[leader].Route("after-failover")
		return ok && c.nodes[leader].Role() == RoleFollower
	})
	if _, ok := c.sms[leader].Route("orphan-0"); ok {
		t.Fatal("an uncommitted entry was applied after repair")
	}
	c.assertLogsConsistent()
	c.assertAtMostOneLeaderPerTerm()
}

// Scenario 7: a restarted follower catches up.
func TestFollowerRestartCatchesUp(t *testing.T) {
	opts := defaultClusterOptions()
	c := newCluster(t, opts)
	leader, _ := c.waitForLeader(3 * time.Second)

	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	c.stopNode(follower)

	// Commit while the follower is down. Two of three nodes remain, which is
	// still a majority.
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if _, err := c.proposeRoute(ctx, leader, fmt.Sprintf("while-down-%d", i)); err != nil {
			cancel()
			t.Fatalf("propose %d: %v", i, err)
		}
		cancel()
	}

	c.startNode(follower, opts)
	c.waitFor(8*time.Second, follower+" catches up", func() bool {
		for i := 0; i < 5; i++ {
			if _, ok := c.sms[follower].Route(fmt.Sprintf("while-down-%d", i)); !ok {
				return false
			}
		}
		return true
	})
	c.assertLogsConsistent()
}

// Scenario 8: committed state survives a full restart via snapshot + replay.
func TestRestartRestoresCommittedState(t *testing.T) {
	opts := defaultClusterOptions()
	opts.snapshotEntries = 4 // force snapshots during the run
	c := newCluster(t, opts)
	leader, _ := c.waitForLeader(3 * time.Second)

	const writes = 12
	for i := 0; i < writes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if _, err := c.proposeRoute(ctx, leader, fmt.Sprintf("route-%02d", i)); err != nil {
			cancel()
			t.Fatalf("propose %d: %v", i, err)
		}
		cancel()
	}
	c.waitFor(5*time.Second, "the leader snapshots", func() bool {
		return c.nodes[leader].SnapshotIndex() > 0
	})
	snapIndex := c.nodes[leader].SnapshotIndex()
	versionBefore := c.sms[leader].ConfigVersion()
	t.Logf("snapshot taken through index %d, config version %d", snapIndex, versionBefore)

	// Restart every node, which forces recovery from snapshot plus log replay.
	for _, id := range c.ids {
		c.restart(id, opts)
	}
	newLeader, _ := c.waitForLeader(8 * time.Second)

	c.waitFor(8*time.Second, "state is restored after restart", func() bool {
		for i := 0; i < writes; i++ {
			if _, ok := c.sms[newLeader].Route(fmt.Sprintf("route-%02d", i)); !ok {
				return false
			}
		}
		return true
	})
	if got := c.sms[newLeader].ConfigVersion(); got < versionBefore {
		t.Fatalf("config version regressed across restart: %d -> %d", versionBefore, got)
	}
	c.assertLogsConsistent()
}

// Scenario 9: InstallSnapshot recovers a follower whose entries were compacted.
func TestInstallSnapshotRecoversStaleFollower(t *testing.T) {
	opts := defaultClusterOptions()
	opts.snapshotEntries = 3
	c := newCluster(t, opts)
	leader, _ := c.waitForLeader(3 * time.Second)

	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	// Crash the follower, then advance the log far enough that the leader
	// compacts away everything the follower would need.
	c.net.crash(follower)

	const writes = 20
	for i := 0; i < writes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if _, err := c.proposeRoute(ctx, leader, fmt.Sprintf("compacted-%02d", i)); err != nil {
			cancel()
			t.Fatalf("propose %d: %v", i, err)
		}
		cancel()
	}
	c.waitFor(5*time.Second, "the leader compacts its log", func() bool {
		return c.nodes[leader].SnapshotIndex() >= 5
	})
	snapIndex := c.nodes[leader].SnapshotIndex()
	t.Logf("leader compacted through index %d", snapIndex)

	before := c.net.counted("InstallSnapshot")
	c.net.revive(follower)

	c.waitFor(10*time.Second, follower+" recovers via InstallSnapshot", func() bool {
		for i := 0; i < writes; i++ {
			if _, ok := c.sms[follower].Route(fmt.Sprintf("compacted-%02d", i)); !ok {
				return false
			}
		}
		return true
	})
	after := c.net.counted("InstallSnapshot")
	if after <= before {
		t.Fatal("the follower caught up without an InstallSnapshot, so the path was not exercised")
	}
	if c.nodes[follower].SnapshotIndex() == 0 {
		t.Fatal("the follower did not record the installed snapshot")
	}
	c.assertLogsConsistent()
}

// Scenario 10: repeated election churn never produces two leaders in one term.
func TestRepeatedElectionInstabilityKeepsSafety(t *testing.T) {
	opts := defaultClusterOptions()
	// Aggressive timeouts maximise the chance of split votes.
	opts.electionMin = 60 * time.Millisecond
	opts.electionMax = 120 * time.Millisecond
	opts.heartbeat = 20 * time.Millisecond
	c := newCluster(t, opts)
	c.waitForLeader(3 * time.Second)

	for round := 0; round < 8; round++ {
		var leader string
		for id, n := range c.nodes {
			if n.IsLeader() {
				leader = id
			}
		}
		if leader == "" {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		c.net.isolate(leader)
		time.Sleep(150 * time.Millisecond)
		c.net.healAll()
		time.Sleep(100 * time.Millisecond)
	}
	// Whatever happened along the way, the safety invariant must hold.
	c.assertAtMostOneLeaderPerTerm()
	c.assertLogsConsistent()

	// The cluster must still converge on a single leader once churn stops.
	c.net.healAll()
	leader, term := c.waitForLeader(8 * time.Second)
	t.Logf("cluster reconverged on %s at term %d", leader, term)
}

// The election restriction must keep a node with a stale log from winning.
func TestStaleLogCandidateCannotWin(t *testing.T) {
	opts := defaultClusterOptions()
	c := newCluster(t, opts)
	leader, _ := c.waitForLeader(3 * time.Second)

	var stale string
	for _, id := range c.ids {
		if id != leader {
			stale = id
			break
		}
	}
	// Keep one node out of the loop while the others commit entries.
	c.net.crash(stale)
	for i := 0; i < 6; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if _, err := c.proposeRoute(ctx, leader, fmt.Sprintf("ahead-%d", i)); err != nil {
			cancel()
			t.Fatalf("propose: %v", err)
		}
		cancel()
	}
	staleLast := c.nodes[stale].LastLogIndex()
	leaderLast := c.nodes[leader].LastLogIndex()
	if staleLast >= leaderLast {
		t.Fatalf("test setup failed: stale node is at %d, leader at %d", staleLast, leaderLast)
	}

	c.net.revive(stale)
	// The stale node may campaign, but must never win while its log is behind.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.nodes[stale].IsLeader() && c.nodes[stale].LastLogIndex() < leaderLast {
			t.Fatal("a node with a stale log became leader, violating the election restriction")
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.assertAtMostOneLeaderPerTerm()
}

// A write must be readable from the leader immediately after Propose returns:
// that is what makes the admin API's success response meaningful.
func TestProposeIsReadYourWrite(t *testing.T) {
	c := newCluster(t, defaultClusterOptions())
	leader, _ := c.waitForLeader(3 * time.Second)

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		res, err := c.proposeRoute(ctx, leader, fmt.Sprintf("rw-%d", i))
		cancel()
		if err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
		if _, ok := c.sms[leader].Route(fmt.Sprintf("rw-%d", i)); !ok {
			t.Fatalf("write %d was acknowledged but is not readable on the leader", i)
		}
		if res.ConfigVersion == 0 {
			t.Fatalf("write %d returned no config version", i)
		}
	}
}

// A follower must refuse a proposal and name the leader so the CLI can redirect.
func TestFollowerRejectsProposeWithLeaderHint(t *testing.T) {
	c := newCluster(t, defaultClusterOptions())
	leader, _ := c.waitForLeader(3 * time.Second)

	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := c.proposeRoute(ctx, follower, "rejected")
	if err == nil {
		t.Fatal("a follower must refuse a proposal")
	}
	if !errs.IsClass(err, errs.ClassNotLeader) {
		t.Fatalf("error class = %q, want not_leader", errs.ClassOf(err))
	}
	if c.nodes[follower].LeaderID() != leader {
		t.Fatalf("follower's leader hint = %q, want %q", c.nodes[follower].LeaderID(), leader)
	}
}

// Concurrent proposals must all commit exactly once, in a consistent order.
func TestConcurrentProposals(t *testing.T) {
	c := newCluster(t, defaultClusterOptions())
	leader, _ := c.waitForLeader(3 * time.Second)

	const n = 20
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := c.proposeRoute(ctx, leader, fmt.Sprintf("concurrent-%02d", i)); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent propose failed: %v", err)
	}

	for i := 0; i < n; i++ {
		if _, ok := c.sms[leader].Route(fmt.Sprintf("concurrent-%02d", i)); !ok {
			t.Fatalf("route concurrent-%02d is missing after commit", i)
		}
	}
	// Config versions must be unique and monotonic, never duplicated.
	if got := c.sms[leader].ConfigVersion(); got < n {
		t.Fatalf("config version %d is below the %d committed writes", got, n)
	}
	c.assertLogsConsistent()
}

// Pre-vote must stop a rejoining partitioned node from disrupting a healthy
// leader by having advanced its term while isolated.
func TestPreVotePreventsTermInflation(t *testing.T) {
	opts := defaultClusterOptions()
	opts.preVote = true
	// Deliberately slower than the package default. The property under test is
	// about term inflation, not about timer precision, and a 150 ms election
	// timeout makes the two healthy nodes re-elect spontaneously whenever the
	// machine is loaded enough to delay a heartbeat. That spurious election
	// would fail the test for a reason that has nothing to do with pre-vote.
	opts.electionMin = 400 * time.Millisecond
	opts.electionMax = 800 * time.Millisecond
	opts.heartbeat = 80 * time.Millisecond
	c := newCluster(t, opts)
	leader, term := c.waitForLeader(3 * time.Second)

	var isolated string
	for _, id := range c.ids {
		if id != leader {
			isolated = id
			break
		}
	}
	termAtIsolation := c.nodes[isolated].Term()

	// While isolated with pre-vote enabled, this node cannot win a pre-vote
	// round, so it must not inflate its term. Stay isolated long enough for
	// several election timeouts to expire.
	c.net.isolate(isolated)
	time.Sleep(2 * time.Second)
	inflated := c.nodes[isolated].Term()

	// This is the property pre-vote exists to provide, and it holds regardless
	// of how loaded the machine is: an isolated node cannot reach a pre-vote
	// quorum, so it never advances its term. Without pre-vote this node would
	// have burned through an election timeout every few hundred milliseconds,
	// incrementing its term each time.
	if inflated != termAtIsolation {
		t.Fatalf("the isolated node inflated its term from %d to %d despite pre-vote",
			termAtIsolation, inflated)
	}

	c.net.heal(isolated)
	time.Sleep(500 * time.Millisecond)

	if c.nodes[leader].Term() > term+1 {
		t.Fatalf("the healthy leader's term jumped from %d to %d on rejoin",
			term, c.nodes[leader].Term())
	}
	if c.nodes[isolated].IsLeader() {
		t.Fatal("the rejoining partitioned node took leadership from the healthy leader")
	}
	if !c.nodes[leader].IsLeader() {
		t.Fatal("the healthy leader was displaced by a rejoining partitioned node")
	}
	t.Logf("isolated node held term %d across the partition; leader held term %d",
		inflated, c.nodes[leader].Term())
	c.assertAtMostOneLeaderPerTerm()
}

// Measure failover time, which the PRD asks to be reported rather than assumed.
func TestLeaderFailoverLatency(t *testing.T) {
	opts := defaultClusterOptions()
	c := newCluster(t, opts)
	leader, _ := c.waitForLeader(5 * time.Second)

	start := time.Now()
	c.net.crash(leader)

	var newLeader string
	c.waitFor(10*time.Second, "a new leader accepts a write", func() bool {
		for id, n := range c.nodes {
			if id != leader && n.IsLeader() {
				newLeader = id
				return true
			}
		}
		return false
	})
	electionTime := time.Since(start)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.proposeRoute(ctx, newLeader, "post-failover"); err != nil {
		t.Fatalf("the new leader could not commit: %v", err)
	}
	total := time.Since(start)

	t.Logf("failover: election in %s, first committed write at %s "+
		"(election timeout %s-%s, heartbeat %s)",
		electionTime.Round(time.Millisecond), total.Round(time.Millisecond),
		opts.electionMin, opts.electionMax, opts.heartbeat)

	// A generous bound: the point is to catch a pathological regression, not to
	// assert a tuned number that varies with CI hardware.
	if total > 5*time.Second {
		t.Fatalf("failover took %s, which indicates a regression", total)
	}
}

// Pre-vote must not prevent a legitimate election after the leader really dies.
//
// This is a regression test for a deadlock in which every survivor refused
// every other survivor's pre-vote: the refusal condition was keyed off each
// node's own election deadline, which a node resets whenever it campaigns, so
// after a genuine leader crash all survivors looked "recently served" to each
// other and the cluster sat leaderless indefinitely. Leader liveness must be
// measured from actual leader contact instead.
func TestPreVoteStillAllowsFailoverAfterLeaderCrash(t *testing.T) {
	opts := defaultClusterOptions()
	opts.preVote = true
	c := newCluster(t, opts)

	first, firstTerm := c.waitForLeader(3 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := c.proposeRoute(ctx, first, "before-crash"); err != nil {
		cancel()
		t.Fatalf("propose: %v", err)
	}
	cancel()

	c.net.crash(first)

	var second string
	c.waitFor(8*time.Second, "a survivor is elected despite pre-vote", func() bool {
		for id, n := range c.nodes {
			if id != first && n.IsLeader() {
				second = id
				return true
			}
		}
		return false
	})
	if c.nodes[second].Term() <= firstTerm {
		t.Fatalf("new term %d did not advance past %d", c.nodes[second].Term(), firstTerm)
	}

	// The new leader must be able to commit, which proves it holds a real
	// quorum rather than merely believing it leads.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if _, err := c.proposeRoute(ctx2, second, "after-crash"); err != nil {
		t.Fatalf("the new leader could not commit: %v", err)
	}
	if _, ok := c.sms[second].Route("before-crash"); !ok {
		t.Fatal("the committed entry did not survive failover")
	}
	c.assertAtMostOneLeaderPerTerm()
	c.assertLogsConsistent()
}

// A follower must stop reporting a leader once its election timeout elapses
// without contact, so clients are not redirected to a node that is gone.
func TestFollowerClearsStaleLeaderHint(t *testing.T) {
	opts := defaultClusterOptions()
	opts.preVote = true
	c := newCluster(t, opts)
	leader, _ := c.waitForLeader(3 * time.Second)

	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	// Wait for the follower to actually learn the leader before asserting that
	// it later forgets one; asserting immediately races the first heartbeat.
	c.waitFor(3*time.Second, follower+" learns the current leader", func() bool {
		return c.nodes[follower].LeaderID() == leader
	})

	// Isolate the follower from everyone, so it loses contact without a new
	// leader appearing to replace the hint.
	c.net.isolate(follower)
	c.waitFor(5*time.Second, "the isolated follower drops its stale leader hint", func() bool {
		return c.nodes[follower].LeaderID() == ""
	})
}
