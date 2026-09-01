package node

import (
	"context"
	"testing"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
	"github.com/sheehanlloyd/edgemesh/internal/raft/storage"
)

// failingTruncatePrefixStore is a Store whose prefix reclamation always fails.
// Everything else behaves normally.
type failingTruncatePrefixStore struct {
	Store
}

func (s failingTruncatePrefixStore) TruncatePrefix(uint64) error {
	return errs.New(errs.ClassStorage, "synthetic prefix reclamation failure")
}

// A failed log-prefix reclamation must not abandon a snapshot install midway.
//
// The install restores the state machine before it reclaims the prefix. Treating
// the reclamation failure as fatal used to return early, which left the node
// holding snapshot state in its state machine while its Raft log still reported
// the old position. The node would then re-apply entries the snapshot already
// covered and advertise a position its state machine had passed. Reclamation is
// housekeeping: the snapshot is already durable and recovery reads it first.
func TestInstallSnapshotAdoptsEvenWhenPrefixReclamationFails(t *testing.T) {
	dir := t.TempDir()
	inner, err := storage.Open(storage.Options{Dir: dir, NoSync: true})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = inner.Close() })

	sm := statemachine.New()
	n, err := New(Config{
		ID:                  "cp-1",
		Peers:               []string{"cp-1", "cp-2", "cp-3"},
		ElectionTimeoutMin:  10 * time.Second,
		ElectionTimeoutMax:  20 * time.Second,
		HeartbeatInterval:   1 * time.Second,
		MaxEntriesPerAppend: 64,
		RPCTimeout:          200 * time.Millisecond,
		Store:               failingTruncatePrefixStore{Store: inner},
		Transport:           unreachableTransport{},
		StateMachine:        sm,
		Logger:              testLogger(t),
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	// Build a snapshot payload the follower has never seen.
	source := statemachine.New()
	if _, err := source.Apply(1, &statemachine.Command{
		Type:            statemachine.CommandCreateRoute,
		TimestampUnixMs: time.Now().UnixMilli(),
		Route: &edgemeshv1.Route{
			Id: "r-1", Hostname: "r-1.local", PathPrefix: "/",
			OriginPoolId: "pool-1", Enabled: true,
		},
	}); err != nil {
		t.Fatalf("seed source state machine: %v", err)
	}
	data, err := source.Serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	const lastIncluded = 12
	resp, err := n.HandleInstallSnapshot(context.Background(), &edgemeshv1.InstallSnapshotRequest{
		Term:              1,
		LeaderId:          "cp-2",
		LastIncludedIndex: lastIncluded,
		LastIncludedTerm:  1,
		Data:              data,
		Done:              true,
	})
	if err != nil {
		t.Fatalf("HandleInstallSnapshot returned an error for a reclamation failure: %v", err)
	}
	if resp.GetBytesStored() != uint64(len(data)) {
		t.Errorf("BytesStored = %d, want %d", resp.GetBytesStored(), len(data))
	}

	// The state machine moved, so the Raft position must have moved with it.
	if _, ok := sm.Route("r-1"); !ok {
		t.Fatal("the state machine was not restored from the installed snapshot")
	}
	if got := n.SnapshotIndex(); got != lastIncluded {
		t.Errorf("SnapshotIndex = %d, want %d: the log still reports a position the state machine has passed",
			got, lastIncluded)
	}
	if got := n.LastApplied(); got != lastIncluded {
		t.Errorf("LastApplied = %d, want %d: the node would re-apply covered entries", got, lastIncluded)
	}
	if got := n.CommitIndex(); got != lastIncluded {
		t.Errorf("CommitIndex = %d, want %d", got, lastIncluded)
	}
}

// unreachableTransport stands in for peers this node never contacts.
type unreachableTransport struct{}

func (unreachableTransport) RequestVote(context.Context, string, *edgemeshv1.RequestVoteRequest) (*edgemeshv1.RequestVoteResponse, error) {
	return nil, errs.New(errs.ClassUnavailable, "no transport in this test")
}

func (unreachableTransport) AppendEntries(context.Context, string, *edgemeshv1.AppendEntriesRequest) (*edgemeshv1.AppendEntriesResponse, error) {
	return nil, errs.New(errs.ClassUnavailable, "no transport in this test")
}

func (unreachableTransport) InstallSnapshot(context.Context, string, *edgemeshv1.InstallSnapshotRequest) (*edgemeshv1.InstallSnapshotResponse, error) {
	return nil, errs.New(errs.ClassUnavailable, "no transport in this test")
}
