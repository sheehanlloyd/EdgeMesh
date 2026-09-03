package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/cache/l2"
	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/observability"
	"github.com/sheehanlloyd/edgemesh/internal/raft/transport"
	"github.com/sheehanlloyd/edgemesh/internal/ring"
)

// controlClient maintains the edge's connection to the control plane.
//
// # The core availability property
//
// This client is allowed to fail. When the control plane is unreachable the
// edge keeps its last applied configuration and keeps serving; only the ability
// to *receive* new configuration is lost. Nothing in this file may make the
// data plane's health depend on the control plane's.
//
// What is lost during an outage: new routes, membership updates, and purges.
// What is retained: every route, origin pool, and cached object the edge
// already had. The control_plane_connected metric goes to 0 and a warning is
// logged, so the degraded state is visible rather than silent.
type controlClient struct {
	cfg         *config.EdgeConfig
	registry    *configRegistry
	ring        *ring.Holder
	coordinator *l2.Coordinator
	log         *slog.Logger
	metrics     *observability.Metrics
	creds       grpc.DialOption

	// endpointIndex rotates through the configured control endpoints when no
	// leader is known, so a reconnect does not always retry the same node.
	endpointIndex atomic.Uint64
	// leaderHint is the edge-facing address of the control node that most
	// recently identified itself as leader. Following it is what makes a
	// reconnect after a leadership change land on the first attempt; rotating
	// blindly through the endpoint list can take several rounds to find the
	// leader, and during that time the edge receives no configuration.
	leaderHint atomic.Pointer[string]
	// connected drives the control_plane_connected metric and the warning log.
	connected atomic.Bool
	// appliedVersion is the highest configuration version applied, used to
	// reject stale events after a reconnect.
	appliedVersion atomic.Uint64
	// membershipVersion guards ring updates against reordering.
	membershipVersion atomic.Uint64

	// reconnect forces the config stream to restart. It is signalled when the
	// leader reports that it has no record of this node, which happens after a
	// leadership change: the new leader's roster is empty, and an edge whose
	// stream is already attached to it would otherwise never re-register and
	// would sit invisible to the ring indefinitely.
	reconnect chan struct{}

	// hb caches the heartbeat connection. Heartbeats run on a two-second timer
	// forever, and dialling per beat paid a TCP and full mTLS handshake every
	// time for a single small unary call. The connection is replaced only when
	// the target endpoint changes, which is what happens on leader movement.
	hb heartbeatConn

	// inflight and requests sample the data plane for the stats an edge reports
	// to the leader. Both are optional: a nil func simply omits that reading.
	inflight func() int64
	requests func() uint64

	rng *rand.Rand
}

// heartbeatConn is the reused client connection behind the heartbeat loop.
type heartbeatConn struct {
	mu       sync.Mutex
	endpoint string
	conn     *grpc.ClientConn
}

type controlClientOptions struct {
	Config      *config.EdgeConfig
	Registry    *configRegistry
	Ring        *ring.Holder
	Coordinator *l2.Coordinator
	Logger      *slog.Logger
	Metrics     *observability.Metrics
	Creds       grpc.DialOption
	// Inflight and Requests sample the data plane for heartbeat stats. They are
	// optional so a control client can be built in a test without a handler.
	Inflight func() int64
	Requests func() uint64
}

func newControlClient(o controlClientOptions) (*controlClient, error) {
	if len(o.Config.ControlPlane.Endpoints) == 0 {
		return nil, errs.New(errs.ClassValidation, "no control-plane endpoints are configured")
	}
	return &controlClient{
		cfg: o.Config, registry: o.Registry, ring: o.Ring,
		coordinator: o.Coordinator, log: o.Logger, metrics: o.Metrics, creds: o.Creds,
		inflight:  o.Inflight,
		requests:  o.Requests,
		reconnect: make(chan struct{}, 1),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Run maintains the config stream and heartbeat until ctx is canceled.
func (c *controlClient) Run(ctx context.Context) {
	go c.heartbeatLoop(ctx)

	backoff := c.cfg.ControlPlane.ReconnectBackoffMin
	for {
		if ctx.Err() != nil {
			return
		}
		endpoint := c.nextEndpoint()
		err := c.watch(ctx, endpoint)
		if ctx.Err() != nil {
			return
		}

		c.setConnected(false)
		if err != nil {
			// A hint that produced a failure is stale; fall back to rotating
			// the configured endpoints so a dead leader is not retried forever.
			if hint := c.leaderHint.Load(); hint != nil && *hint == endpoint {
				c.clearLeaderHint()
			}
			c.log.Warn("control-plane stream ended; continuing to serve the last applied configuration",
				slog.String("endpoint", endpoint),
				slog.Uint64(observability.FieldConfigVersion, c.appliedVersion.Load()),
				slog.String("error", err.Error()),
				slog.String(observability.FieldErrorClass, string(errs.ClassOf(err))))
		}

		// Exponential backoff with jitter. Without jitter every edge would
		// reconnect at the same instant after a leader change and stampede the
		// new leader.
		jitter := time.Duration(c.rng.Int63n(int64(backoff/2) + 1))
		wait := backoff + jitter
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		backoff *= 2
		if backoff > c.cfg.ControlPlane.ReconnectBackoffMax {
			backoff = c.cfg.ControlPlane.ReconnectBackoffMax
		}
		// A successful connection resets the backoff; that is handled inside
		// watch, which sets it back through the connected flag below.
		if c.connected.Load() {
			backoff = c.cfg.ControlPlane.ReconnectBackoffMin
		}
	}
}

// watch opens and services one config stream.
func (c *controlClient) watch(parent context.Context, endpoint string) error {
	conn, err := grpc.NewClient(endpoint, transport.DefaultDialOptions(c.creds)...)
	if err != nil {
		return errs.Wrap(errs.ClassUnavailable, err, "dial control plane %q", endpoint)
	}
	defer conn.Close() //nolint:errcheck // best-effort close on a failed stream

	// Discard any re-register signal raised before this stream existed. Such a
	// signal refers to the stream that already ended; acting on it here would
	// cancel this stream before it can register, which makes the leader ask for
	// re-registration again, a self-perpetuating loop that leaves the edge
	// permanently unregistered while its data plane keeps working.
	select {
	case <-c.reconnect:
	default:
	}

	// The stream runs under its own context so a re-register request can end it
	// without tearing down the whole client.
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
		case <-c.reconnect:
			cancel()
		}
	}()

	stream, err := edgemeshv1.NewEdgeControlClient(conn).WatchConfig(ctx, &edgemeshv1.WatchConfigRequest{
		NodeId:             c.cfg.Node.ID,
		KnownConfigVersion: c.appliedVersion.Load(),
		Registration:       c.registration(),
	})
	if err != nil {
		return errs.Wrap(errs.ClassUnavailable, err, "open config stream to %q", endpoint)
	}

	for {
		event, err := stream.Recv()
		// errors.Is, not ==: gRPC wraps io.EOF, so an equality check
		// misclassifies a cleanly closed stream as a transport failure and
		// logs a warning for what is normal shutdown.
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return errs.Wrap(errs.ClassUnavailable, err, "receive config event")
		}
		if !c.connected.Load() {
			c.setConnected(true)
			c.log.Info("connected to the control plane", slog.String("endpoint", endpoint))
		}
		c.apply(ctx, event)
	}
}

// apply handles one configuration event.
//
// Events are applied in version order and stale versions are ignored. A stream
// reconnect always begins with a full snapshot, so a version gap is closed
// without needing to request one explicitly.
func (c *controlClient) apply(ctx context.Context, event *edgemeshv1.ConfigEvent) {
	version := event.GetConfigVersion()

	switch e := event.GetEvent().(type) {
	case *edgemeshv1.ConfigEvent_FullSnapshot:
		snap := e.FullSnapshot
		if err := c.registry.ApplySnapshot(ctx, snap); err != nil {
			return
		}
		c.storeVersion(snap.GetConfigVersion())
		c.applyMembership(snap.GetMembership())
		for _, p := range snap.GetRecentPurges() {
			c.applyPurge(p)
		}

	case *edgemeshv1.ConfigEvent_RouteChanged:
		if c.isStale(version) {
			return
		}
		if err := c.registry.ApplyRoute(e.RouteChanged, version); err == nil {
			c.storeVersion(version)
		}

	case *edgemeshv1.ConfigEvent_RouteDeleted:
		if c.isStale(version) {
			return
		}
		c.registry.DeleteRoute(e.RouteDeleted, version)
		c.storeVersion(version)

	case *edgemeshv1.ConfigEvent_OriginPoolChanged:
		if c.isStale(version) {
			return
		}
		if err := c.registry.ApplyOriginPool(ctx, e.OriginPoolChanged, version); err == nil {
			c.storeVersion(version)
		}

	case *edgemeshv1.ConfigEvent_OriginPoolDeleted:
		if c.isStale(version) {
			return
		}
		c.registry.DeleteOriginPool(e.OriginPoolDeleted, version)
		c.storeVersion(version)

	case *edgemeshv1.ConfigEvent_MembershipChanged:
		c.applyMembership(e.MembershipChanged)

	case *edgemeshv1.ConfigEvent_Purge:
		c.applyPurge(e.Purge)

	case *edgemeshv1.ConfigEvent_SettingsChanged:
		c.applySettings(e.SettingsChanged)
		c.storeVersion(version)

	case *edgemeshv1.ConfigEvent_LeaderNotice:
		if addr := e.LeaderNotice.GetLeaderControlAddress(); addr != "" && !e.LeaderNotice.GetIsLeader() {
			c.setLeaderHint(addr)
			c.log.Info("redirected to the control-plane leader",
				slog.String("leader_id", e.LeaderNotice.GetLeaderId()),
				slog.String("leader_address", addr))
		}
	}
}

// applyMembership rebuilds the consistent hash ring.
//
// The ring is replaced atomically, so requests in flight see either the old
// ring or the new one and never a partially built one. A stale membership
// version is ignored: applying it would move key ownership backwards and cause
// avoidable cache misses.
func (c *controlClient) applyMembership(m *edgemeshv1.Membership) {
	if m == nil {
		return
	}
	for {
		cur := c.membershipVersion.Load()
		if m.GetVersion() != 0 && m.GetVersion() <= cur {
			return
		}
		if c.membershipVersion.CompareAndSwap(cur, m.GetVersion()) {
			break
		}
	}

	nodes := make([]ring.Node, 0, len(m.GetNodes()))
	for _, n := range m.GetNodes() {
		// A dead node must never receive ownership: routing keys to it would
		// send every request for those keys into a timeout.
		if n.GetState() == edgemeshv1.NodeState_NODE_STATE_DEAD {
			continue
		}
		if n.GetPeerAddress() == "" {
			continue
		}
		nodes = append(nodes, ring.Node{
			ID: n.GetId(), PeerAddress: n.GetPeerAddress(),
			Region: n.GetRegion(), Zone: n.GetZone(), Weight: n.GetWeight(),
		})
	}

	vnodes := ring.DefaultVirtualNodes
	newRing := ring.New(nodes, vnodes).WithVersion(m.GetVersion())
	c.ring.Store(newRing)
	c.metrics.RingNodes.Set(float64(newRing.Len()))
	c.metrics.RingVersion.Set(float64(m.GetVersion()))

	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID)
	}
	c.log.Info("ring membership updated",
		slog.Uint64("membership_version", m.GetVersion()),
		slog.Int("ring_nodes", newRing.Len()),
		slog.Any("node_ids", ids))
}

func (c *controlClient) applyPurge(p *edgemeshv1.PurgeDirective) {
	if p == nil {
		return
	}
	c.coordinator.Purge(p.GetScope(), p.GetRouteId(), p.GetCacheKey(), p.GetVersion())
}

func (c *controlClient) applySettings(g *edgemeshv1.GlobalSettings) {
	if g == nil {
		return
	}
	c.log.Info("applied global settings",
		slog.Uint64("replication_factor", uint64(g.GetReplicationFactor())),
		slog.Uint64("ring_virtual_nodes", uint64(g.GetRingVirtualNodes())))
}

// heartbeatLoop reports liveness to the control-plane leader.
//
// A failed heartbeat is logged at debug, not warn: during a leader election
// heartbeats fail for a second or two by design, and warning on each one would
// bury the genuinely interesting log lines.
func (c *controlClient) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.ControlPlane.HeartbeatInterval)
	defer t.Stop()
	// The cached connection belongs to this loop, so it is released here rather
	// than left for process exit to reclaim.
	defer c.closeHeartbeatConn()

	// Request rate is derived from two samples of the handler's monotonic
	// counter rather than kept as a windowed estimate in the data plane, which
	// would put bookkeeping on the hot path for a number only the admin API
	// reads.
	var lastRequests uint64
	if c.requests != nil {
		lastRequests = c.requests()
	}
	lastSample := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now()
			var rps float64
			if c.requests != nil {
				cur := c.requests()
				// The counter is monotonic, but guard the subtraction anyway:
				// an unsigned wrap would turn into an absurd rate rather than a
				// negative one, and reporting nonsense is worse than reporting
				// nothing.
				if elapsed := now.Sub(lastSample).Seconds(); elapsed > 0 && cur >= lastRequests {
					rps = float64(cur-lastRequests) / elapsed
				}
				lastRequests = cur
			}
			lastSample = now
			c.sendHeartbeat(ctx, rps)
		}
	}
}

// heartbeatClient returns a client on the cached connection, redialling only
// when the target endpoint has changed.
func (c *controlClient) heartbeatClient(endpoint string) (edgemeshv1.EdgeControlClient, error) {
	c.hb.mu.Lock()
	defer c.hb.mu.Unlock()

	if c.hb.conn != nil && c.hb.endpoint == endpoint {
		return edgemeshv1.NewEdgeControlClient(c.hb.conn), nil
	}
	if c.hb.conn != nil {
		_ = c.hb.conn.Close()
		c.hb.conn, c.hb.endpoint = nil, ""
	}
	conn, err := grpc.NewClient(endpoint, transport.DefaultDialOptions(c.creds)...)
	if err != nil {
		return nil, errs.Wrap(errs.ClassUnavailable, err, "dial control plane %q", endpoint)
	}
	c.hb.conn, c.hb.endpoint = conn, endpoint
	return edgemeshv1.NewEdgeControlClient(conn), nil
}

// closeHeartbeatConn releases the cached heartbeat connection.
func (c *controlClient) closeHeartbeatConn() {
	c.hb.mu.Lock()
	defer c.hb.mu.Unlock()
	if c.hb.conn != nil {
		_ = c.hb.conn.Close()
		c.hb.conn, c.hb.endpoint = nil, ""
	}
}

func (c *controlClient) sendHeartbeat(ctx context.Context, rps float64) {
	endpoint := c.currentEndpoint()
	client, err := c.heartbeatClient(endpoint)
	if err != nil {
		c.log.Debug("heartbeat dial failed",
			slog.String("endpoint", endpoint), slog.String("error", err.Error()))
		return
	}

	hctx, cancel := context.WithTimeout(ctx, c.cfg.ControlPlane.HeartbeatInterval)
	defer cancel()

	objects, bytes := c.coordinator.Stats()
	stats := &edgemeshv1.EdgeStats{
		CacheObjects:      objects,
		CacheBytes:        bytes,
		RequestsPerSecond: rps,
	}
	if c.inflight != nil {
		if n := c.inflight(); n > 0 {
			stats.InflightRequests = uint64(n)
		}
	}
	resp, err := client.Heartbeat(hctx, &edgemeshv1.HeartbeatRequest{
		NodeId:               c.cfg.Node.ID,
		Node:                 c.registration(),
		ConfigVersionApplied: c.appliedVersion.Load(),
		Stats:                stats,
	})
	if err != nil {
		c.log.Debug("heartbeat failed",
			slog.String("endpoint", endpoint), slog.String("error", err.Error()))
		// A failed call may have been the cached connection going stale against
		// a node that has gone away. Dropping it forces a fresh dial on the next
		// beat instead of retrying a dead connection forever.
		c.closeHeartbeatConn()
		return
	}
	if resp.GetLeaderControlAddress() != "" {
		// Whether or not this heartbeat was accepted, the response names the
		// leader. Recording it points the next stream reconnect straight at the
		// right node instead of rotating through the endpoint list.
		c.setLeaderHint(resp.GetLeaderControlAddress())
	}
	if resp.GetReregister() {
		// The leader has no record of this node. Registration happens when a
		// config stream opens, so the stream has to be restarted for this edge
		// to reappear on the ring.
		c.log.Info("control plane requested re-registration; restarting the config stream",
			slog.String("endpoint", endpoint))
		select {
		case c.reconnect <- struct{}{}:
		default: // a restart is already pending
		}
	}
}

// registration describes this edge to the control plane.
func (c *controlClient) registration() *edgemeshv1.EdgeNode {
	return &edgemeshv1.EdgeNode{
		Id:                   c.cfg.Node.ID,
		Region:               c.cfg.Node.Region,
		Zone:                 c.cfg.Node.Zone,
		PeerAddress:          c.cfg.Node.AdvertisePeerAddress,
		PublicAddress:        c.cfg.Node.AdvertisePublicAddress,
		Weight:               c.cfg.Node.Weight,
		ConfigVersionApplied: c.appliedVersion.Load(),
		Version:              version,
	}
}

// nextEndpoint chooses where to open the next config stream.
//
// A known leader wins outright: only the leader can serve an authoritative
// membership view, so connecting anywhere else just produces another redirect.
// Without a hint the configured endpoints are rotated.
func (c *controlClient) nextEndpoint() string {
	if hint := c.leaderHint.Load(); hint != nil && *hint != "" {
		return *hint
	}
	eps := c.cfg.ControlPlane.Endpoints
	i := c.endpointIndex.Add(1)
	return eps[int(i)%len(eps)]
}

// currentEndpoint chooses where to send the next heartbeat.
func (c *controlClient) currentEndpoint() string {
	if hint := c.leaderHint.Load(); hint != nil && *hint != "" {
		return *hint
	}
	eps := c.cfg.ControlPlane.Endpoints
	return eps[int(c.endpointIndex.Load())%len(eps)]
}

// setLeaderHint records the leader's edge-facing address.
func (c *controlClient) setLeaderHint(addr string) {
	if cur := c.leaderHint.Load(); cur != nil && *cur == addr {
		return
	}
	c.leaderHint.Store(&addr)
}

// clearLeaderHint drops a hint that failed, so the next attempt falls back to
// rotating the configured endpoints rather than retrying a dead leader forever.
func (c *controlClient) clearLeaderHint() { c.leaderHint.Store(nil) }

// isStale reports whether an incremental event predates what is already applied.
func (c *controlClient) isStale(version uint64) bool {
	return version != 0 && version <= c.appliedVersion.Load()
}

func (c *controlClient) storeVersion(v uint64) {
	for {
		cur := c.appliedVersion.Load()
		if v <= cur {
			return
		}
		if c.appliedVersion.CompareAndSwap(cur, v) {
			return
		}
	}
}

func (c *controlClient) setConnected(v bool) {
	was := c.connected.Swap(v)
	if v {
		c.metrics.ControlPlaneConnected.Set(1)
		c.coordinator.SetReady(true)
		return
	}
	c.metrics.ControlPlaneConnected.Set(0)
	if was {
		// The edge stays ready and keeps serving; only new configuration is
		// unavailable. This is the degraded mode the architecture is designed
		// around, so it is stated explicitly in the log.
		c.log.Warn("control plane disconnected; serving the last applied configuration",
			slog.Uint64(observability.FieldConfigVersion, c.appliedVersion.Load()))
	}
}
