// Package transport carries Raft RPCs between control-plane nodes over gRPC.
//
// The transport owns connection lifetime and nothing else: it must never
// interpret consensus state. Keeping that boundary sharp is what allows the
// node package's tests to substitute an in-memory network and exercise every
// partition scenario without containers.
package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// Dialer opens a connection to a peer. It is a field rather than a hard-coded
// call so tests and the mTLS wiring can supply their own dial options.
type Dialer func(ctx context.Context, target string) (*grpc.ClientConn, error)

// GRPCTransport implements node.Transport over gRPC.
type GRPCTransport struct {
	// addresses maps peer ID to dial target. Membership is static in V1, so
	// this map is fixed at construction and never mutated.
	addresses map[string]string
	dial      Dialer
	log       *slog.Logger

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	// closed stops new dials once the process is shutting down.
	closed bool

	// snapshotChunkBytes bounds one InstallSnapshot stream frame.
	snapshotChunkBytes int
}

// Options configures a GRPCTransport.
type Options struct {
	Peers  map[string]string // peer ID -> host:port
	Dialer Dialer
	Logger *slog.Logger
	// SnapshotChunkBytes bounds one streamed snapshot frame. A whole snapshot
	// in one message would exceed gRPC's default limit and allocate the entire
	// payload twice.
	SnapshotChunkBytes int
}

// New builds a transport.
func New(o Options) (*GRPCTransport, error) {
	if o.Dialer == nil {
		return nil, errs.New(errs.ClassValidation, "raft transport: a dialer is required")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.SnapshotChunkBytes <= 0 {
		o.SnapshotChunkBytes = 256 << 10
	}
	return &GRPCTransport{
		addresses:          o.Peers,
		dial:               o.Dialer,
		log:                o.Logger,
		conns:              make(map[string]*grpc.ClientConn, len(o.Peers)),
		snapshotChunkBytes: o.SnapshotChunkBytes,
	}, nil
}

// DefaultDialOptions returns the dial options every internal EdgeMesh gRPC
// client uses.
//
// Keepalives matter here specifically: a Raft peer behind a silently dropped
// TCP connection would otherwise look reachable until a write finally times
// out, which delays failure detection well past the election timeout.
func DefaultDialOptions(creds grpc.DialOption) []grpc.DialOption {
	return []grpc.DialOption{
		creds,
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// Must not be more frequent than the server's enforcement minimum
			// (see DefaultServerOptions), or the server sends GOAWAY
			// "too_many_pings" and tears the connection down, which looks
			// exactly like a network failure and causes reconnect churn.
			Time:                15 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
}

// DefaultServerOptions returns the options every internal EdgeMesh gRPC server
// uses.
//
// The enforcement policy has to admit the client keepalive above. gRPC's
// default server policy rejects any ping more often than every five minutes on
// an idle connection, so a client configured for fast failure detection gets
// disconnected for "too many pings" unless the server agrees to the same
// cadence.
func DefaultServerOptions(creds grpc.ServerOption) []grpc.ServerOption {
	return []grpc.ServerOption{
		creds,
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    20 * time.Second,
			Timeout: 5 * time.Second,
			// Config streams are long-lived by design, so no maximum age is
			// set: forcibly cycling them would cause avoidable resyncs.
		}),
	}
}

// conn returns a live connection to peer, dialing lazily.
//
// gRPC connections reconnect on their own, so a dialed connection is cached for
// the process lifetime rather than being torn down on every RPC failure. A peer
// that is down simply produces failing RPCs until it returns, which is exactly
// what the consensus layer expects.
func (t *GRPCTransport) conn(ctx context.Context, peer string) (*grpc.ClientConn, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, errs.New(errs.ClassUnavailable, "raft transport is closed")
	}
	if c, ok := t.conns[peer]; ok {
		t.mu.Unlock()
		return c, nil
	}
	addr, ok := t.addresses[peer]
	if !ok {
		t.mu.Unlock()
		return nil, errs.New(errs.ClassValidation, "raft transport: unknown peer %q", peer)
	}
	t.mu.Unlock()

	// Dial outside the lock: a slow dial must not block RPCs to other peers.
	c, err := t.dial(ctx, addr)
	if err != nil {
		return nil, errs.Wrap(errs.ClassUnavailable, err, "dial raft peer %q at %q", peer, addr)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		_ = c.Close()
		return nil, errs.New(errs.ClassUnavailable, "raft transport is closed")
	}
	// Another goroutine may have won the race; keep the first connection.
	if existing, ok := t.conns[peer]; ok {
		_ = c.Close()
		return existing, nil
	}
	t.conns[peer] = c
	return c, nil
}

// RequestVote sends a RequestVote RPC.
func (t *GRPCTransport) RequestVote(ctx context.Context, peer string, req *edgemeshv1.RequestVoteRequest) (*edgemeshv1.RequestVoteResponse, error) {
	c, err := t.conn(ctx, peer)
	if err != nil {
		return nil, err
	}
	resp, err := edgemeshv1.NewRaftTransportClient(c).RequestVote(ctx, req)
	if err != nil {
		return nil, errs.Wrap(errs.ClassUnavailable, err, "RequestVote to %q", peer)
	}
	return resp, nil
}

// AppendEntries sends an AppendEntries RPC.
func (t *GRPCTransport) AppendEntries(ctx context.Context, peer string, req *edgemeshv1.AppendEntriesRequest) (*edgemeshv1.AppendEntriesResponse, error) {
	c, err := t.conn(ctx, peer)
	if err != nil {
		return nil, err
	}
	resp, err := edgemeshv1.NewRaftTransportClient(c).AppendEntries(ctx, req)
	if err != nil {
		return nil, errs.Wrap(errs.ClassUnavailable, err, "AppendEntries to %q", peer)
	}
	return resp, nil
}

// InstallSnapshot streams a snapshot to a peer in bounded chunks.
func (t *GRPCTransport) InstallSnapshot(ctx context.Context, peer string, req *edgemeshv1.InstallSnapshotRequest) (*edgemeshv1.InstallSnapshotResponse, error) {
	c, err := t.conn(ctx, peer)
	if err != nil {
		return nil, err
	}
	stream, err := edgemeshv1.NewRaftTransportClient(c).InstallSnapshot(ctx)
	if err != nil {
		return nil, errs.Wrap(errs.ClassUnavailable, err, "open InstallSnapshot stream to %q", peer)
	}

	data := req.GetData()
	// An empty snapshot still needs one frame so the receiver sees Done.
	if len(data) == 0 {
		if err := stream.Send(&edgemeshv1.InstallSnapshotRequest{
			Term: req.GetTerm(), LeaderId: req.GetLeaderId(),
			LastIncludedIndex: req.GetLastIncludedIndex(),
			LastIncludedTerm:  req.GetLastIncludedTerm(),
			Offset:            0, Data: nil, Done: true,
		}); err != nil {
			return nil, errs.Wrap(errs.ClassUnavailable, err, "send empty snapshot to %q", peer)
		}
	}
	for off := 0; off < len(data); off += t.snapshotChunkBytes {
		end := off + t.snapshotChunkBytes
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&edgemeshv1.InstallSnapshotRequest{
			Term:              req.GetTerm(),
			LeaderId:          req.GetLeaderId(),
			LastIncludedIndex: req.GetLastIncludedIndex(),
			LastIncludedTerm:  req.GetLastIncludedTerm(),
			Offset:            uint64(off),
			Data:              data[off:end],
			Done:              end == len(data),
		}); err != nil {
			return nil, errs.Wrap(errs.ClassUnavailable, err,
				"send snapshot chunk at offset %d to %q", off, peer)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, errs.Wrap(errs.ClassUnavailable, err, "complete InstallSnapshot to %q", peer)
	}
	return resp, nil
}

// Close releases every connection.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	var firstErr error
	for peer, c := range t.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = errs.Wrap(errs.ClassUnavailable, err, "close connection to %q", peer)
		}
		delete(t.conns, peer)
	}
	return firstErr
}

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

// RaftNode is the subset of the consensus node the gRPC server calls into.
type RaftNode interface {
	HandleRequestVote(context.Context, *edgemeshv1.RequestVoteRequest) (*edgemeshv1.RequestVoteResponse, error)
	HandleAppendEntries(context.Context, *edgemeshv1.AppendEntriesRequest) (*edgemeshv1.AppendEntriesResponse, error)
	HandleInstallSnapshot(context.Context, *edgemeshv1.InstallSnapshotRequest) (*edgemeshv1.InstallSnapshotResponse, error)
}

// CommandForwarder relays a follower's admin write to the leader.
type CommandForwarder interface {
	ForwardCommand(ctx context.Context, encoded []byte) (configVersion, raftIndex uint64, result []byte, err error)
}

// Server implements the RaftTransport gRPC service.
type Server struct {
	edgemeshv1.UnimplementedRaftTransportServer
	node      RaftNode
	forwarder CommandForwarder
	log       *slog.Logger
	// maxSnapshotBytes bounds a reassembled snapshot so a hostile or broken
	// peer cannot drive this process out of memory with an endless stream.
	maxSnapshotBytes int
}

// NewServer builds the gRPC service.
func NewServer(n RaftNode, f CommandForwarder, log *slog.Logger, maxSnapshotBytes int) *Server {
	if log == nil {
		log = slog.Default()
	}
	if maxSnapshotBytes <= 0 {
		maxSnapshotBytes = 256 << 20
	}
	return &Server{node: n, forwarder: f, log: log, maxSnapshotBytes: maxSnapshotBytes}
}

// RequestVote handles an inbound vote request.
func (s *Server) RequestVote(ctx context.Context, req *edgemeshv1.RequestVoteRequest) (*edgemeshv1.RequestVoteResponse, error) {
	return s.node.HandleRequestVote(ctx, req)
}

// AppendEntries handles an inbound replication request.
func (s *Server) AppendEntries(ctx context.Context, req *edgemeshv1.AppendEntriesRequest) (*edgemeshv1.AppendEntriesResponse, error) {
	return s.node.HandleAppendEntries(ctx, req)
}

// InstallSnapshot reassembles a streamed snapshot and hands it to the node.
func (s *Server) InstallSnapshot(stream edgemeshv1.RaftTransport_InstallSnapshotServer) error {
	var (
		buf    []byte
		header *edgemeshv1.InstallSnapshotRequest
	)
	for {
		chunk, err := stream.Recv()
		// errors.Is, not ==: gRPC wraps io.EOF, and an equality check would
		// fall through to the generic error path with a misleading message.
		if errors.Is(err, io.EOF) {
			// The sender closed without a Done frame, so the snapshot is
			// incomplete and must not be applied.
			return errs.New(errs.ClassProtocol, "snapshot stream ended before the final chunk")
		}
		if err != nil {
			return errs.Wrap(errs.ClassUnavailable, err, "receive snapshot chunk")
		}
		if header == nil {
			header = chunk
		}
		// Offsets must arrive in order and contiguously; a gap would silently
		// produce a corrupt snapshot.
		if chunk.GetOffset() != uint64(len(buf)) {
			return errs.New(errs.ClassProtocol,
				"snapshot chunk at offset %d does not continue %d bytes already received",
				chunk.GetOffset(), len(buf))
		}
		if len(buf)+len(chunk.GetData()) > s.maxSnapshotBytes {
			return errs.New(errs.ClassTooLarge,
				"snapshot exceeds the %d byte limit", s.maxSnapshotBytes)
		}
		buf = append(buf, chunk.GetData()...)

		if chunk.GetDone() {
			resp, err := s.node.HandleInstallSnapshot(stream.Context(), &edgemeshv1.InstallSnapshotRequest{
				Term:              chunk.GetTerm(),
				LeaderId:          chunk.GetLeaderId(),
				LastIncludedIndex: chunk.GetLastIncludedIndex(),
				LastIncludedTerm:  chunk.GetLastIncludedTerm(),
				Data:              buf,
				Done:              true,
			})
			if err != nil {
				return err
			}
			return stream.SendAndClose(resp)
		}
	}
}

// ForwardCommand relays a follower's write to the leader.
//
// This is what lets the CLI talk to any control node: a follower forwards
// internally rather than making every client implement leader discovery.
func (s *Server) ForwardCommand(ctx context.Context, req *edgemeshv1.ForwardCommandRequest) (*edgemeshv1.ForwardCommandResponse, error) {
	if s.forwarder == nil {
		return nil, errs.New(errs.ClassUnavailable, "command forwarding is not configured on this node")
	}
	version, index, result, err := s.forwarder.ForwardCommand(ctx, req.GetCommand())
	if err != nil {
		return nil, err
	}
	return &edgemeshv1.ForwardCommandResponse{
		ConfigVersion: version, RaftIndex: index, Result: result,
	}, nil
}

// ForwardToLeader sends an encoded command to the leader over the transport.
func (t *GRPCTransport) ForwardToLeader(ctx context.Context, leaderID string, encoded []byte) (*edgemeshv1.ForwardCommandResponse, error) {
	c, err := t.conn(ctx, leaderID)
	if err != nil {
		return nil, err
	}
	resp, err := edgemeshv1.NewRaftTransportClient(c).ForwardCommand(ctx,
		&edgemeshv1.ForwardCommandRequest{Command: encoded})
	if err != nil {
		return nil, errs.Wrap(errs.ClassUnavailable, err, "forward command to leader %q", leaderID)
	}
	return resp, nil
}
