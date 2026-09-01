package configstream

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/control/membership"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
)

// fakeStream captures events a server sends to one client.
//
// blockAfter lets a test model a client that stops reading, which is how the
// slow-client backpressure behaviour is exercised.
type fakeStream struct {
	ctx    context.Context
	mu     sync.Mutex
	events []*edgemeshv1.ConfigEvent
	// gate, when non-nil, blocks Send until closed.
	gate chan struct{}
	grpc.ServerStream
}

func newFakeStream(ctx context.Context) *fakeStream {
	return &fakeStream{ctx: ctx}
}

func (f *fakeStream) Send(e *edgemeshv1.ConfigEvent) error {
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-f.ctx.Done():
			return f.ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
	return nil
}

func (f *fakeStream) Context() context.Context     { return f.ctx }
func (f *fakeStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeStream) SetTrailer(metadata.MD)       {}

func (f *fakeStream) captured() []*edgemeshv1.ConfigEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*edgemeshv1.ConfigEvent, len(f.events))
	copy(out, f.events)
	return out
}

// staticLeader reports a fixed leadership view.
type staticLeader struct {
	isLeader bool
	id       string
	addr     string
}

func (s staticLeader) LeaderInfo() LeaderInfo {
	return LeaderInfo{LeaderID: s.id, EdgeAddress: s.addr, IsLocalLeader: s.isLeader}
}

func newServer(t *testing.T, isLeader bool) (*Server, *statemachine.StateMachine, *membership.Tracker) {
	t.Helper()
	sm := statemachine.New()
	members := membership.New(membership.Options{})
	members.SetLeader(isLeader)

	s, err := NewServer(Options{
		StateMachine: sm, Membership: members,
		Leader: staticLeader{isLeader: isLeader, id: "cp-1", addr: "cp-1:7300"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, sm, members
}

func registration(id string) *edgemeshv1.EdgeNode {
	return &edgemeshv1.EdgeNode{Id: id, PeerAddress: id + ":7200", Weight: 1}
}

// Every stream opens with a full snapshot, which is what closes any version gap
// without needing a delta protocol or unbounded retained history.
func TestStreamOpensWithAFullSnapshot(t *testing.T) {
	s, _, _ := newServer(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakeStream(ctx)

	done := make(chan error, 1)
	go func() {
		done <- s.WatchConfig(&edgemeshv1.WatchConfigRequest{
			NodeId: "edge-1", Registration: registration("edge-1"),
		}, stream)
	}()

	waitFor(t, time.Second, func() bool { return len(stream.captured()) > 0 })
	first := stream.captured()[0]
	if first.GetFullSnapshot() == nil {
		t.Fatalf("first event was not a full snapshot: %T", first.GetEvent())
	}
	// The snapshot must carry membership, or the edge cannot build a ring.
	if first.GetFullSnapshot().GetMembership() == nil {
		t.Fatal("the opening snapshot carried no membership")
	}
	cancel()
	<-done
}

// A follower cannot serve an authoritative membership view, so it redirects.
func TestFollowerRedirectsWithALeaderNotice(t *testing.T) {
	s, _, _ := newServer(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newFakeStream(ctx)

	err := s.WatchConfig(&edgemeshv1.WatchConfigRequest{NodeId: "edge-1"}, stream)
	if err == nil {
		t.Fatal("a follower must refuse to serve a config stream")
	}
	if !errs.IsClass(err, errs.ClassNotLeader) {
		t.Fatalf("error class = %q, want not_leader", errs.ClassOf(err))
	}
	events := stream.captured()
	if len(events) != 1 || events[0].GetLeaderNotice() == nil {
		t.Fatalf("expected a leader notice, got %v", events)
	}
	// The notice must carry a dialable address or the edge cannot redirect.
	if events[0].GetLeaderNotice().GetLeaderControlAddress() != "cp-1:7300" {
		t.Fatalf("leader notice = %+v", events[0].GetLeaderNotice())
	}
}

func TestWatchRequiresANodeID(t *testing.T) {
	s, _, _ := newServer(t, true)
	err := s.WatchConfig(&edgemeshv1.WatchConfigRequest{}, newFakeStream(context.Background()))
	if err == nil || !errs.IsClass(err, errs.ClassValidation) {
		t.Fatalf("err = %v, want a validation error", err)
	}
}

func TestBroadcastReachesEveryClient(t *testing.T) {
	s, _, _ := newServer(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streams := make([]*fakeStream, 3)
	for i := range streams {
		streams[i] = newFakeStream(ctx)
		id := string(rune('a' + i))
		go func(st *fakeStream, id string) {
			_ = s.WatchConfig(&edgemeshv1.WatchConfigRequest{
				NodeId: "edge-" + id, Registration: registration("edge-" + id),
			}, st)
		}(streams[i], id)
	}
	waitFor(t, 2*time.Second, func() bool { return s.ClientCount() == 3 })

	s.BroadcastRoute(&edgemeshv1.Route{Id: "r1"}, 7)
	for i, st := range streams {
		waitFor(t, 2*time.Second, func() bool {
			for _, e := range st.captured() {
				if e.GetRouteChanged() != nil {
					return true
				}
			}
			return false
		})
		_ = i
	}
}

// A slow client must never stall the broadcaster; its backlog is replaced by a
// single snapshot that supersedes everything it missed.
func TestSlowClientDoesNotStallTheBroadcaster(t *testing.T) {
	s, _, _ := newServer(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slow := newFakeStream(ctx)
	slow.gate = make(chan struct{})
	go func() {
		_ = s.WatchConfig(&edgemeshv1.WatchConfigRequest{
			NodeId: "edge-slow", Registration: registration("edge-slow"),
		}, slow)
	}()
	waitFor(t, 2*time.Second, func() bool { return s.ClientCount() == 1 })

	// Far more events than the queue can hold. Broadcast must not block.
	broadcastDone := make(chan struct{})
	go func() {
		for i := 0; i < queueDepth*10; i++ {
			s.BroadcastRoute(&edgemeshv1.Route{Id: "r"}, uint64(i))
		}
		close(broadcastDone)
	}()
	select {
	case <-broadcastDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Broadcast blocked on a slow client")
	}

	// Once the client reads again it receives a snapshot rather than a backlog.
	close(slow.gate)
	waitFor(t, 3*time.Second, func() bool {
		for _, e := range slow.captured() {
			if e.GetFullSnapshot() != nil {
				return true
			}
		}
		return false
	})
}

// A reconnecting edge replaces its old stream, so a half-dead connection cannot
// hold a queue slot forever.
func TestReconnectReplacesThePreviousStream(t *testing.T) {
	s, _, _ := newServer(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := newFakeStream(ctx)
	firstDone := make(chan struct{})
	go func() {
		_ = s.WatchConfig(&edgemeshv1.WatchConfigRequest{
			NodeId: "edge-1", Registration: registration("edge-1"),
		}, first)
		close(firstDone)
	}()
	waitFor(t, 2*time.Second, func() bool { return s.ClientCount() == 1 })

	second := newFakeStream(ctx)
	go func() {
		_ = s.WatchConfig(&edgemeshv1.WatchConfigRequest{
			NodeId: "edge-1", Registration: registration("edge-1"),
		}, second)
	}()

	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the superseded stream was not closed")
	}
	waitFor(t, 2*time.Second, func() bool { return s.ClientCount() == 1 })
}

func TestHeartbeat(t *testing.T) {
	s, sm, members := newServer(t, true)
	if err := members.Register(registration("edge-1")); err != nil {
		t.Fatal(err)
	}

	resp, err := s.Heartbeat(context.Background(), &edgemeshv1.HeartbeatRequest{
		NodeId: "edge-1", ConfigVersionApplied: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetAccepted() || resp.GetReregister() {
		t.Fatalf("heartbeat response = %+v", resp)
	}
	if resp.GetLeaderId() != "cp-1" {
		t.Fatalf("leader id = %q", resp.GetLeaderId())
	}
	_ = sm

	// An unknown node is asked to re-register rather than being rejected.
	resp, err = s.Heartbeat(context.Background(), &edgemeshv1.HeartbeatRequest{NodeId: "edge-unknown"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetReregister() {
		t.Fatal("an unknown node must be asked to re-register")
	}
}

// A heartbeat to a follower is not an error; the edge is told where to go.
func TestHeartbeatOnAFollowerRedirects(t *testing.T) {
	s, _, _ := newServer(t, false)
	resp, err := s.Heartbeat(context.Background(), &edgemeshv1.HeartbeatRequest{NodeId: "edge-1"})
	if err != nil {
		t.Fatalf("a heartbeat to a follower must not error: %v", err)
	}
	if resp.GetAccepted() {
		t.Fatal("a follower must not accept a heartbeat")
	}
	if resp.GetLeaderControlAddress() != "cp-1:7300" {
		t.Fatalf("no usable leader address: %+v", resp)
	}
}

// Losing leadership must send edges to the new leader promptly rather than
// leaving them attached to a node that can no longer answer authoritatively.
func TestDisconnectAllClosesEveryStream(t *testing.T) {
	s, _, _ := newServer(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		id := "edge-" + string(rune('a'+i))
		go func(id string) {
			_ = s.WatchConfig(&edgemeshv1.WatchConfigRequest{
				NodeId: id, Registration: registration(id),
			}, newFakeStream(ctx))
			done <- struct{}{}
		}(id)
	}
	waitFor(t, 2*time.Second, func() bool { return s.ClientCount() == 2 })

	s.DisconnectAll()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("a stream survived DisconnectAll")
		}
	}
	if s.ClientCount() != 0 {
		t.Fatalf("client count = %d after DisconnectAll", s.ClientCount())
	}
}

func TestNewServerValidatesOptions(t *testing.T) {
	if _, err := NewServer(Options{}); err == nil {
		t.Fatal("a server without a state machine must be rejected")
	}
	if _, err := NewServer(Options{StateMachine: statemachine.New()}); err == nil {
		t.Fatal("a server without a membership tracker must be rejected")
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition never held within %s", timeout)
}
