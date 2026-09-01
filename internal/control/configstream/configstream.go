// Package configstream pushes configuration from the control plane to edges.
//
// # Delivery model
//
// Each connected edge gets a bounded per-client queue. When a client cannot
// keep up, its queue fills and the server replaces the backlog with a single
// full-snapshot event rather than blocking the broadcaster or growing memory.
// This is the right failure mode for configuration: an edge does not need every
// intermediate version, only the newest one, so collapsing a backlog loses
// nothing an edge would have acted on.
//
// A slow client must never be able to stall the control plane. That is the
// invariant this package exists to protect.
package configstream

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/control/membership"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
)

// queueDepth bounds one client's pending events. Small on purpose: an edge more
// than a handful of events behind is better served by one fresh snapshot than
// by replaying a backlog.
const queueDepth = 32

// LeaderInfo describes the current leader for a redirect hint.
type LeaderInfo struct {
	LeaderID      string
	EdgeAddress   string
	IsLocalLeader bool
}

// LeaderProvider reports the current leader.
type LeaderProvider interface {
	LeaderInfo() LeaderInfo
}

// Server implements the EdgeControl gRPC service.
type Server struct {
	edgemeshv1.UnimplementedEdgeControlServer

	sm      *statemachine.StateMachine
	members *membership.Tracker
	leader  LeaderProvider
	log     *slog.Logger

	mu      sync.RWMutex
	clients map[string]*client
	// clientCount is read by metrics without taking the lock.
	clientCount atomic.Int64
}

type client struct {
	nodeID string
	events chan *edgemeshv1.ConfigEvent
	// resync is set when the client's queue overflowed and it must be sent a
	// fresh snapshot instead of the dropped increments.
	resync    atomic.Bool
	done      chan struct{}
	closeOnce sync.Once
}

func (c *client) close() {
	c.closeOnce.Do(func() { close(c.done) })
}

// Options configures the server.
type Options struct {
	StateMachine *statemachine.StateMachine
	Membership   *membership.Tracker
	Leader       LeaderProvider
	Logger       *slog.Logger
}

// NewServer builds the config-stream service.
func NewServer(o Options) (*Server, error) {
	if o.StateMachine == nil {
		return nil, errs.New(errs.ClassValidation, "configstream: a state machine is required")
	}
	if o.Membership == nil {
		return nil, errs.New(errs.ClassValidation, "configstream: a membership tracker is required")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Server{
		sm:      o.StateMachine,
		members: o.Membership,
		leader:  o.Leader,
		log:     o.Logger,
		clients: make(map[string]*client),
	}, nil
}

// WatchConfig streams configuration events to one edge.
func (s *Server) WatchConfig(req *edgemeshv1.WatchConfigRequest, stream edgemeshv1.EdgeControl_WatchConfigServer) error {
	nodeID := req.GetNodeId()
	if nodeID == "" {
		return errs.New(errs.ClassValidation, "watch_config requires a node id")
	}

	// Only the leader owns liveness and can answer with an authoritative
	// membership view. A follower redirects rather than serving a stale ring.
	if s.leader != nil {
		info := s.leader.LeaderInfo()
		if !info.IsLocalLeader {
			// Send a leader notice so the edge reconnects to the right node
			// instead of retrying blindly.
			_ = stream.Send(&edgemeshv1.ConfigEvent{
				Event: &edgemeshv1.ConfigEvent_LeaderNotice{
					LeaderNotice: &edgemeshv1.LeaderNotice{
						LeaderId:             info.LeaderID,
						LeaderControlAddress: info.EdgeAddress,
						IsLeader:             false,
					},
				},
			})
			return errs.New(errs.ClassNotLeader,
				"this control node is not the leader; leader is %q at %q", info.LeaderID, info.EdgeAddress)
		}
	}

	if reg := req.GetRegistration(); reg != nil {
		if err := s.members.Register(reg); err != nil {
			return err
		}
	}

	c := &client{
		nodeID: nodeID,
		events: make(chan *edgemeshv1.ConfigEvent, queueDepth),
		done:   make(chan struct{}),
	}
	s.addClient(c)
	defer s.removeClient(c)

	s.log.Info("edge config stream opened",
		slog.String("edge_node_id", nodeID),
		slog.Uint64("known_config_version", req.GetKnownConfigVersion()))

	// Every stream opens with a full snapshot. Attempting to compute a delta
	// from the client's claimed version would require retaining unbounded
	// history; a snapshot is simpler and always correct.
	//
	// The queue is drained first. A broadcast can land between addClient and
	// the snapshot being captured, and delivering that older event *after* a
	// newer snapshot would move the client backwards: an edge would rebuild
	// its ring from stale membership and stay there until the next change.
	// Draining first means everything the client subsequently receives is at
	// least as new as its snapshot.
	drain(c.events)
	if err := stream.Send(s.snapshotEvent()); err != nil {
		return errs.Wrap(errs.ClassUnavailable, err, "send initial snapshot to %q", nodeID)
	}

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			s.log.Info("edge config stream closed",
				slog.String("edge_node_id", nodeID))
			return nil
		case <-c.done:
			return nil
		case ev := <-c.events:
			// A client whose queue overflowed gets one snapshot that supersedes
			// everything it missed. The queue is drained before the snapshot is
			// captured, for the same ordering reason as the opening snapshot.
			if c.resync.Swap(false) {
				drain(c.events)
				ev = s.snapshotEvent()
			}
			if err := stream.Send(ev); err != nil {
				return errs.Wrap(errs.ClassUnavailable, err, "send config event to %q", nodeID)
			}
		}
	}
}

// Heartbeat records an edge's liveness.
func (s *Server) Heartbeat(_ context.Context, req *edgemeshv1.HeartbeatRequest) (*edgemeshv1.HeartbeatResponse, error) {
	resp := &edgemeshv1.HeartbeatResponse{ConfigVersion: s.sm.ConfigVersion()}

	if s.leader != nil {
		info := s.leader.LeaderInfo()
		resp.LeaderId = info.LeaderID
		resp.LeaderControlAddress = info.EdgeAddress
		if !info.IsLocalLeader {
			// A heartbeat to a follower is not an error; the edge is told where
			// the leader is and redirects itself.
			resp.Accepted = false
			return resp, nil
		}
	}

	reregister, err := s.members.Heartbeat(req.GetNodeId(), req.GetConfigVersionApplied(), req.GetStats())
	if err != nil {
		return nil, err
	}
	resp.Accepted = !reregister
	resp.Reregister = reregister
	return resp, nil
}

// drain empties a client's pending event queue without blocking.
func drain(ch chan *edgemeshv1.ConfigEvent) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// snapshotEvent builds a full-snapshot event including current membership.
func (s *Server) snapshotEvent() *edgemeshv1.ConfigEvent {
	snap := s.sm.Snapshot()
	snap.Membership = s.members.Snapshot()
	return &edgemeshv1.ConfigEvent{
		Event:         &edgemeshv1.ConfigEvent_FullSnapshot{FullSnapshot: snap},
		ConfigVersion: snap.GetConfigVersion(),
	}
}

func (s *Server) addClient(c *client) {
	s.mu.Lock()
	// A reconnecting edge replaces its previous stream; the old one is closed
	// so a half-dead connection cannot hold a queue forever.
	if prev, ok := s.clients[c.nodeID]; ok {
		prev.close()
	}
	s.clients[c.nodeID] = c
	s.mu.Unlock()
	s.clientCount.Store(int64(s.ClientCount()))
}

func (s *Server) removeClient(c *client) {
	s.mu.Lock()
	if cur, ok := s.clients[c.nodeID]; ok && cur == c {
		delete(s.clients, c.nodeID)
	}
	s.mu.Unlock()
	c.close()
	s.clientCount.Store(int64(s.ClientCount()))
}

// ClientCount reports connected edges.
func (s *Server) ClientCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clients)
}

// Broadcast fans an event out to every connected edge.
//
// It never blocks. A client whose queue is full is flagged for resync and its
// backlog is superseded by a snapshot, because stalling the broadcaster on one
// slow edge would delay configuration for every other edge.
func (s *Server) Broadcast(ev *edgemeshv1.ConfigEvent) {
	s.mu.RLock()
	clients := make([]*client, 0, len(s.clients))
	for _, c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.RUnlock()

	for _, c := range clients {
		select {
		case c.events <- ev:
		default:
			if !c.resync.Swap(true) {
				s.log.Warn("edge config queue overflowed; scheduling a full resync",
					slog.String("edge_node_id", c.nodeID),
					slog.Int("queue_depth", queueDepth))
			}
			// Wake the stream so it notices the resync flag. A second failure
			// here means the stream is already pending work, which is enough.
			select {
			case c.events <- ev:
			default:
			}
		}
	}
}

// BroadcastSnapshot pushes the full configuration to every edge.
func (s *Server) BroadcastSnapshot() { s.Broadcast(s.snapshotEvent()) }

// BroadcastMembership pushes a new membership/ring version.
func (s *Server) BroadcastMembership(m *edgemeshv1.Membership) {
	s.Broadcast(&edgemeshv1.ConfigEvent{
		Event:         &edgemeshv1.ConfigEvent_MembershipChanged{MembershipChanged: m},
		ConfigVersion: s.sm.ConfigVersion(),
	})
}

// BroadcastRoute pushes a route change.
func (s *Server) BroadcastRoute(r *edgemeshv1.Route, version uint64) {
	s.Broadcast(&edgemeshv1.ConfigEvent{
		Event:         &edgemeshv1.ConfigEvent_RouteChanged{RouteChanged: r},
		ConfigVersion: version,
	})
}

// BroadcastRouteDeleted pushes a route deletion.
func (s *Server) BroadcastRouteDeleted(id string, version uint64) {
	s.Broadcast(&edgemeshv1.ConfigEvent{
		Event:         &edgemeshv1.ConfigEvent_RouteDeleted{RouteDeleted: id},
		ConfigVersion: version,
	})
}

// BroadcastOriginPool pushes an origin-pool change.
func (s *Server) BroadcastOriginPool(p *edgemeshv1.OriginPool, version uint64) {
	s.Broadcast(&edgemeshv1.ConfigEvent{
		Event:         &edgemeshv1.ConfigEvent_OriginPoolChanged{OriginPoolChanged: p},
		ConfigVersion: version,
	})
}

// BroadcastOriginPoolDeleted pushes an origin-pool deletion.
func (s *Server) BroadcastOriginPoolDeleted(id string, version uint64) {
	s.Broadcast(&edgemeshv1.ConfigEvent{
		Event:         &edgemeshv1.ConfigEvent_OriginPoolDeleted{OriginPoolDeleted: id},
		ConfigVersion: version,
	})
}

// BroadcastPurge pushes a cache invalidation.
func (s *Server) BroadcastPurge(p *edgemeshv1.PurgeDirective) {
	s.Broadcast(&edgemeshv1.ConfigEvent{
		Event:         &edgemeshv1.ConfigEvent_Purge{Purge: p},
		ConfigVersion: p.GetVersion(),
	})
}

// BroadcastSettings pushes a global-settings change.
func (s *Server) BroadcastSettings(g *edgemeshv1.GlobalSettings, version uint64) {
	s.Broadcast(&edgemeshv1.ConfigEvent{
		Event:         &edgemeshv1.ConfigEvent_SettingsChanged{SettingsChanged: g},
		ConfigVersion: version,
	})
}

// DisconnectAll closes every stream, used when this node loses leadership so
// edges reconnect to the new leader promptly rather than waiting for a timeout.
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	clients := make([]*client, 0, len(s.clients))
	for _, c := range s.clients {
		clients = append(clients, c)
	}
	s.clients = make(map[string]*client)
	s.mu.Unlock()

	for _, c := range clients {
		c.close()
	}
	if len(clients) > 0 {
		s.log.Info("closed edge config streams after losing leadership",
			slog.Int("stream_count", len(clients)))
	}
	s.clientCount.Store(0)
}
