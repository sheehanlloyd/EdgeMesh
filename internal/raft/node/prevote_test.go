package node

import (
	"testing"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/clock"
	raftlog "github.com/sheehanlloyd/edgemesh/internal/raft/log"
)

// A leader must never grant a pre-vote.
//
// Pre-vote exists so a node that was partitioned away cannot disturb a working
// cluster when it comes back. The healthy followers refuse it because they are
// still being heartbeated. If the leader answers the same probe with a yes, the
// mechanism collapses: in a three-node cluster the returning node needs exactly
// one vote besides its own, so the leader alone is enough to send it into a real
// election that it then wins on an equal log, deposing a leader that never
// stopped working.
func TestLeaderRefusesPreVote(t *testing.T) {
	n := &Node{
		cfg:  Config{ID: "cp-1", ElectionTimeoutMin: 150 * time.Millisecond},
		clk:  clock.New(),
		rlog: raftlog.New(),
		role: RoleLeader,
	}
	req := &edgemeshv1.RequestVoteRequest{
		Term: n.currentTerm, CandidateId: "cp-3", PreVote: true,
		LastLogIndex: 0, LastLogTerm: 0,
	}
	if n.wouldGrantPreVoteLocked(req) {
		t.Fatal("a leader granted a pre-vote, which hands a returning node the vote it needs to depose it")
	}
}

// A follower that has stopped hearing from its leader must grant pre-votes,
// otherwise a genuine leader crash leaves the cluster unable to elect anyone.
func TestFollowerWithNoLeaderContactGrantsPreVote(t *testing.T) {
	clk := clock.New()
	n := &Node{
		cfg:               Config{ID: "cp-1", ElectionTimeoutMin: 150 * time.Millisecond},
		clk:               clk,
		rlog:              raftlog.New(),
		role:              RoleFollower,
		leaderID:          "cp-2",
		lastLeaderContact: clk.Now().Add(-2 * time.Second),
	}
	req := &edgemeshv1.RequestVoteRequest{
		Term: n.currentTerm, CandidateId: "cp-3", PreVote: true,
	}
	if !n.wouldGrantPreVoteLocked(req) {
		t.Fatal("a follower that has not heard from its leader refused a pre-vote, which would leave the cluster leaderless")
	}
}

// A follower that is still being heartbeated must refuse.
func TestFollowerHearingFromLeaderRefusesPreVote(t *testing.T) {
	clk := clock.New()
	n := &Node{
		cfg:               Config{ID: "cp-1", ElectionTimeoutMin: 150 * time.Millisecond},
		clk:               clk,
		rlog:              raftlog.New(),
		role:              RoleFollower,
		leaderID:          "cp-2",
		lastLeaderContact: clk.Now(),
	}
	req := &edgemeshv1.RequestVoteRequest{
		Term: n.currentTerm, CandidateId: "cp-3", PreVote: true,
	}
	if n.wouldGrantPreVoteLocked(req) {
		t.Fatal("a follower being heartbeated granted a pre-vote to a returning node")
	}
}
