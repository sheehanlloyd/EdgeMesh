// Package integration spins up a real EdgeMesh cluster in one process and
// exercises the behaviours that only emerge when the pieces run together.
//
// The nodes here are the production types wired exactly as the binaries wire
// them, communicating over real gRPC on loopback ports. Only the process
// boundary is removed, which keeps the tests fast enough to run on every commit
// while still exercising the real transports, the real Raft, and the real
// cache.
package integration

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/breaker"
	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/cache/l1"
	"github.com/sheehanlloyd/edgemesh/internal/cache/l2"
	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/control/configstream"
	"github.com/sheehanlloyd/edgemesh/internal/control/membership"
	"github.com/sheehanlloyd/edgemesh/internal/origin"
	"github.com/sheehanlloyd/edgemesh/internal/peer"
	"github.com/sheehanlloyd/edgemesh/internal/proxy"
	raftnode "github.com/sheehanlloyd/edgemesh/internal/raft/node"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
	"github.com/sheehanlloyd/edgemesh/internal/raft/storage"
	"github.com/sheehanlloyd/edgemesh/internal/raft/transport"
	"github.com/sheehanlloyd/edgemesh/internal/ratelimit"
	"github.com/sheehanlloyd/edgemesh/internal/ring"
	"github.com/sheehanlloyd/edgemesh/internal/routing"
)

// ---------------------------------------------------------------------------
// Demo origin
// ---------------------------------------------------------------------------

// testOrigin counts hits per path so a cache hit can be proven rather than
// assumed: if the counter does not advance, the origin was genuinely not asked.
type testOrigin struct {
	*httptest.Server
	mu       sync.Mutex
	hits     map[string]int
	failing  atomic.Bool
	delay    atomic.Int64 // nanoseconds
	instance string
}

func newTestOrigin(t *testing.T, instance string) *testOrigin {
	t.Helper()
	o := &testOrigin{hits: map[string]int{}, instance: instance}
	o.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if d := time.Duration(o.delay.Load()); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("X-Origin-Instance", o.instance)
		if r.URL.Path == "/healthz" {
			if o.failing.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		o.mu.Lock()
		o.hits[r.URL.Path]++
		n := o.hits[r.URL.Path]
		o.mu.Unlock()

		if o.failing.Load() {
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "path=%s hits=%d instance=%s", r.URL.Path, n, o.instance)
	}))
	t.Cleanup(o.Close)
	return o
}

func (o *testOrigin) hitsFor(path string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.hits[path]
}

func (o *testOrigin) hostPort(t *testing.T) (string, uint32) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(o.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var port uint32
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatal(err)
	}
	return host, port
}

// ---------------------------------------------------------------------------
// Control plane
// ---------------------------------------------------------------------------

type controlNode struct {
	id      string
	node    *raftnode.Node
	sm      *statemachine.StateMachine
	members *membership.Tracker
	stream  *configstream.Server
	raftLn  net.Listener
	edgeLn  net.Listener
	raftSrv *grpc.Server
	edgeSrv *grpc.Server
	cancel  context.CancelFunc
	dir     string
}

func (c *controlNode) edgeAddr() string { return c.edgeLn.Addr().String() }

type controlCluster struct {
	t     *testing.T
	nodes map[string]*controlNode
	ids   []string
}

// newControlCluster starts a three-node control plane on loopback ports.
func newControlCluster(t *testing.T) *controlCluster {
	t.Helper()
	ids := []string{"cp-1", "cp-2", "cp-3"}
	cc := &controlCluster{t: t, nodes: map[string]*controlNode{}, ids: ids}

	// Listeners are bound first so every node knows every peer's address before
	// any of them starts campaigning.
	raftLns := map[string]net.Listener{}
	edgeLns := map[string]net.Listener{}
	addrs := map[string]string{}
	for _, id := range ids {
		rl, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		el, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		raftLns[id], edgeLns[id] = rl, el
		addrs[id] = rl.Addr().String()
	}

	for _, id := range ids {
		cc.start(id, ids, addrs, raftLns[id], edgeLns[id], t.TempDir())
	}
	t.Cleanup(cc.stop)
	return cc
}

func (cc *controlCluster) start(id string, ids []string, addrs map[string]string,
	raftLn, edgeLn net.Listener, dir string) {
	cc.t.Helper()

	store, err := storage.Open(storage.Options{Dir: dir, NoSync: true})
	if err != nil {
		cc.t.Fatal(err)
	}
	sm := statemachine.New()

	rpcTransport, err := transport.New(transport.Options{
		Peers: addrs,
		Dialer: func(_ context.Context, target string) (*grpc.ClientConn, error) {
			return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
	})
	if err != nil {
		cc.t.Fatal(err)
	}

	cn := &controlNode{id: id, sm: sm, raftLn: raftLn, edgeLn: edgeLn, dir: dir}

	cn.members = membership.New(membership.Options{
		HeartbeatInterval:  200 * time.Millisecond,
		SuspectAfterMissed: 2, DeadAfterMissed: 3,
		OnChange: func(m *edgemeshv1.Membership) {
			if cn.stream != nil {
				cn.stream.BroadcastMembership(m)
			}
		},
	})

	node, err := raftnode.New(raftnode.Config{
		ID: id, Peers: ids,
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		HeartbeatInterval:   40 * time.Millisecond,
		MaxEntriesPerAppend: 64,
		RPCTimeout:          500 * time.Millisecond,
		Store:               store, Transport: rpcTransport, StateMachine: sm,
		Logger:   quietLogger(),
		Observer: &clusterObserver{node: cn},
	})
	if err != nil {
		cc.t.Fatal(err)
	}
	cn.node = node

	cn.stream, err = configstream.NewServer(configstream.Options{
		StateMachine: sm, Membership: cn.members,
		Leader: &leaderInfo{cluster: cc, node: cn},
		Logger: quietLogger(),
	})
	if err != nil {
		cc.t.Fatal(err)
	}

	cn.raftSrv = grpc.NewServer()
	edgemeshv1.RegisterRaftTransportServer(cn.raftSrv, transport.NewServer(node, nil, quietLogger(), 1<<26))
	go func() { _ = cn.raftSrv.Serve(raftLn) }()

	cn.edgeSrv = grpc.NewServer()
	edgemeshv1.RegisterEdgeControlServer(cn.edgeSrv, cn.stream)
	go func() { _ = cn.edgeSrv.Serve(edgeLn) }()

	ctx, cancel := context.WithCancel(context.Background())
	cn.cancel = cancel
	if err := node.Start(ctx); err != nil {
		cc.t.Fatal(err)
	}

	// The membership sweep is what eventually removes a dead edge from the ring.
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if node.IsLeader() {
					cn.members.Sweep()
				}
			}
		}
	}()

	cc.nodes[id] = cn
}

type clusterObserver struct{ node *controlNode }

func (o *clusterObserver) OnRoleChange(from, to raftnode.Role, _ uint64) {
	isLeader := to == raftnode.RoleLeader
	o.node.members.SetLeader(isLeader)
	if o.node.stream == nil {
		return
	}
	if isLeader {
		o.node.stream.BroadcastSnapshot()
	} else if from == raftnode.RoleLeader {
		o.node.stream.DisconnectAll()
	}
}
func (o *clusterObserver) OnCommit(uint64) {}
func (o *clusterObserver) OnApply(uint64, *statemachine.Command, statemachine.Result, time.Duration, error) {
}
func (o *clusterObserver) OnLeaderChange(string, uint64) {}

type leaderInfo struct {
	cluster *controlCluster
	node    *controlNode
}

func (l *leaderInfo) LeaderInfo() configstream.LeaderInfo {
	leaderID := l.node.node.LeaderID()
	addr := ""
	if n, ok := l.cluster.nodes[leaderID]; ok {
		addr = n.edgeAddr()
	}
	return configstream.LeaderInfo{
		LeaderID: leaderID, EdgeAddress: addr,
		IsLocalLeader: l.node.node.IsLeader(),
	}
}

func (cc *controlCluster) stop() {
	for _, n := range cc.nodes {
		cc.stopNode(n.id)
	}
}

func (cc *controlCluster) stopNode(id string) {
	n, ok := cc.nodes[id]
	if !ok {
		return
	}
	n.cancel()
	n.node.Stop()
	n.raftSrv.Stop()
	n.edgeSrv.Stop()
	delete(cc.nodes, id)
}

// leader blocks until exactly one node leads and returns it.
func (cc *controlCluster) leader(timeout time.Duration) *controlNode {
	cc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var found *controlNode
		count := 0
		for _, n := range cc.nodes {
			if n.node.IsLeader() {
				found, count = n, count+1
			}
		}
		if count == 1 {
			return found
		}
		time.Sleep(5 * time.Millisecond)
	}
	cc.t.Fatalf("no leader elected within %s", timeout)
	return nil
}

// edgeEndpoints returns every control node's edge-facing address.
func (cc *controlCluster) edgeEndpoints() []string {
	out := make([]string, 0, len(cc.nodes))
	for _, id := range cc.ids {
		if n, ok := cc.nodes[id]; ok {
			out = append(out, n.edgeAddr())
		}
	}
	return out
}

// propose commits a command through the current leader and broadcasts it, which
// is what the admin API does in the real control binary.
func (cc *controlCluster) propose(cmd *statemachine.Command) statemachine.Result {
	cc.t.Helper()
	leader := cc.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := leader.node.Propose(ctx, cmd)
	if err != nil {
		cc.t.Fatalf("propose %s: %v", cmd.Type, err)
	}
	switch cmd.Type {
	case statemachine.CommandCreateRoute, statemachine.CommandUpdateRoute:
		leader.stream.BroadcastRoute(res.Route, res.ConfigVersion)
	case statemachine.CommandDeleteRoute:
		leader.stream.BroadcastRouteDeleted(cmd.ID, res.ConfigVersion)
	case statemachine.CommandCreateOriginPool, statemachine.CommandUpdateOriginPool:
		leader.stream.BroadcastOriginPool(res.OriginPool, res.ConfigVersion)
	case statemachine.CommandPurgeCache:
		leader.stream.BroadcastPurge(res.Purge)
	}
	return res
}

// ---------------------------------------------------------------------------
// Edge plane
// ---------------------------------------------------------------------------

type edgeNode struct {
	id                string
	handler           *proxy.Handler
	server            *httptest.Server
	coord             *l2.Coordinator
	l1                *l1.Cache
	l2                *l1.Cache
	routes            *routing.Holder
	ring              *ring.Holder
	pools             *poolRegistry
	peerLn            net.Listener
	peerSrv           *grpc.Server
	cancel            context.CancelFunc
	applied           atomic.Uint64
	membershipVersion atomic.Uint64
	connected         atomic.Bool
	limiters          *limiterRegistry
	checker           *origin.Checker

	// hint is the leader address this edge was last redirected to. It is a
	// field rather than package state because every test builds its own cluster
	// with the same edge IDs, and a leftover hint pointing at a closed listener
	// would silently break the next test.
	hintMu sync.Mutex
	hint   string
	// reconnect forces the config stream to restart when the leader reports it
	// has no record of this node, mirroring the edge binary.
	reconnect chan struct{}
}

func (e *edgeNode) URL() string      { return e.server.URL }
func (e *edgeNode) peerAddr() string { return e.peerLn.Addr().String() }

// poolRegistry holds built origin pools for an edge.
type poolRegistry struct {
	mu      sync.RWMutex
	pools   map[string]*origin.Pool
	guard   *origin.Guard
	checker *origin.Checker
}

func (p *poolRegistry) Pool(id string) (*origin.Pool, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pool, ok := p.pools[id]
	return pool, ok
}

func (p *poolRegistry) set(id string, pool *origin.Pool) {
	p.mu.Lock()
	p.pools[id] = pool
	// The checker gets a snapshot of the whole set, since SetPools replaces it.
	snapshot := make(map[string]*origin.Pool, len(p.pools))
	for k, v := range p.pools {
		snapshot[k] = v
	}
	checker := p.checker
	p.mu.Unlock()

	if checker != nil {
		checker.SetPools(snapshot)
	}
}

// limiterRegistry builds one rate limiter per route on demand, mirroring the
// edge binary. Without it the proxy's rate-limit path is never taken.
type limiterRegistry struct {
	mu       sync.Mutex
	limiters map[string]*ratelimit.Limiter
}

func (l *limiterRegistry) Limiter(route *edgemeshv1.Route) (*ratelimit.Limiter, bool) {
	p := route.GetRateLimitPolicy()
	if !p.GetEnabled() {
		return nil, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if lim, ok := l.limiters[route.GetId()]; ok {
		return lim, true
	}
	lim, err := ratelimit.New(ratelimit.Options{
		Rate: p.GetRatePerSecond(), Burst: int(p.GetBurst()),
	})
	if err != nil {
		return nil, false
	}
	l.limiters[route.GetId()] = lim
	return lim, true
}

// newEdge starts one edge node wired exactly as the edge binary wires it.
func newEdge(t *testing.T, id string, controlEndpoints []string) *edgeNode {
	t.Helper()

	l1Cache, err := l1.New(l1.Options{MaxBytes: 1 << 22, Shards: 8, MaxObjectBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	l2Tier, err := l1.New(l1.Options{MaxBytes: 1 << 22, Shards: 8, MaxObjectBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}

	guard, err := origin.NewGuard(config.OriginSecurity{AllowLoopback: true, AllowPrivateNetworks: true})
	if err != nil {
		t.Fatal(err)
	}

	e := &edgeNode{
		id:        id,
		l1:        l1Cache,
		l2:        l2Tier,
		routes:    routing.NewHolder(),
		ring:      ring.NewHolder(),
		pools:     &poolRegistry{pools: map[string]*origin.Pool{}, guard: guard},
		limiters:  &limiterRegistry{limiters: map[string]*ratelimit.Limiter{}},
		reconnect: make(chan struct{}, 1),
	}

	proxyCfg := config.ProxyConfig{}
	proxyCfg.RequestTimeout = 5 * time.Second
	proxyCfg.OriginDialTimeout = time.Second
	proxyCfg.OriginKeepAlive = 30 * time.Second
	proxyCfg.OriginMaxIdleConns = 64
	proxyCfg.OriginMaxIdleConnsPerHost = 32
	proxyCfg.OriginIdleConnTimeout = 30 * time.Second
	proxyCfg.OriginResponseHeaderTimeout = 3 * time.Second

	originTransport := proxy.NewOriginTransport(proxyCfg, guard)
	breakers := breaker.NewGroup(breaker.Options{
		FailureThreshold: 3, Window: 5 * time.Second, Cooldown: 500 * time.Millisecond,
		HalfOpenProbes: 1, SuccessesToClose: 1,
	})
	originClient := proxy.NewOriginClient(originTransport, breakers)

	peerClient, err := peer.NewClient(peer.ClientOptions{
		Timeout: 3 * time.Second,
		Dialer: func(_ context.Context, target string) (*grpc.ClientConn, error) {
			return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	fetcher, err := proxy.NewFetcher(proxy.FetcherOptions{
		NodeID: id, Routes: e.routes, Pools: e.pools, Origin: originClient,
		Logger: quietLogger(), MaxObjectBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel

	e.coord, err = l2.New(ctx, l2.Options{
		NodeID: id, Local: l2Tier, FrontTier: l1Cache, Ring: e.ring,
		Peers: peerClient, Origin: fetcher, Logger: quietLogger(),
		ReplicationFactor: 2, ReplicationWorkers: 2, ReplicationQueue: 64,
		ReplicationTimeout: 2 * time.Second, MaxObjectBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.coord.SetReady(true)

	peerLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e.peerLn = peerLn
	e.peerSrv = grpc.NewServer()
	edgemeshv1.RegisterPeerCacheServer(e.peerSrv, peer.NewServer(e.coord, quietLogger()))
	go func() { _ = e.peerSrv.Serve(peerLn) }()

	e.handler, err = proxy.NewHandler(proxy.Options{
		NodeID: id, Routes: e.routes, Pools: e.pools,
		Cache:          &tieredCache{l1: l1Cache, l2: e.coord},
		Origin:         originClient,
		Limiters:       e.limiters,
		Logger:         quietLogger(),
		MaxObjectBytes: 1 << 20, RequestTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.server = httptest.NewServer(e.handler)

	// Active health checks run exactly as they do in the binary; without them
	// a dead origin would stay eligible for traffic forever.
	e.checker = origin.NewChecker(origin.CheckerOptions{
		Client:        &http.Client{Transport: originTransport},
		Logger:        quietLogger(),
		MaxConcurrent: 8,
	})
	e.pools.checker = e.checker
	e.checker.Start(ctx, 100*time.Millisecond)

	// The control-plane client runs exactly as it does in the binary.
	go e.watchConfig(ctx, controlEndpoints)
	go e.heartbeat(ctx, controlEndpoints, id)

	t.Cleanup(func() {
		cancel()
		e.checker.Stop()
		e.server.Close()
		e.peerSrv.Stop()
		_ = peerClient.Close()
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		_ = e.coord.Close(sctx)
		_ = l1Cache.Close(sctx)
		_ = l2Tier.Close(sctx)
	})
	return e
}

// tieredCache checks L1 before the distributed tier, mirroring the binary.
type tieredCache struct {
	l1 *l1.Cache
	l2 *l2.Coordinator
}

func (t *tieredCache) Get(ctx context.Context, req peer.FetchRequest) (*cache.Object, cache.Outcome, error) {
	if obj, ok := t.l1.Get(req.CacheKey); ok {
		return obj, cache.OutcomeHit, nil
	}
	obj, outcome, err := t.l2.Get(ctx, req)
	if err != nil || obj == nil {
		return obj, outcome, err
	}
	if !obj.ExpiresAt.IsZero() {
		_ = t.l1.Put(req.CacheKey, obj)
	}
	return obj, outcome, nil
}

// watchConfig maintains the edge's config stream, following leader hints.
func (e *edgeNode) watchConfig(ctx context.Context, endpoints []string) {
	var hint string
	idx := 0
	for ctx.Err() == nil {
		target := hint
		if target == "" {
			target = endpoints[idx%len(endpoints)]
			idx++
		}
		err := e.streamOnce(ctx, target)
		e.connected.Store(false)
		if err != nil && target == hint {
			hint = ""
		}
		if h := e.takeHint(); h != "" {
			hint = h
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (e *edgeNode) streamOnce(parent context.Context, target string) error {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck

	// Discard a re-register signal raised before this stream existed, for the
	// same reason as the edge binary: acting on a stale one cancels this stream
	// before it can register, and the loop never breaks.
	select {
	case <-e.reconnect:
	default:
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
		case <-e.reconnect:
			cancel()
		}
	}()

	stream, err := edgemeshv1.NewEdgeControlClient(conn).WatchConfig(ctx, &edgemeshv1.WatchConfigRequest{
		NodeId:             e.id,
		KnownConfigVersion: e.applied.Load(),
		Registration: &edgemeshv1.EdgeNode{
			Id: e.id, Region: "test", PeerAddress: e.peerAddr(), Weight: 1,
		},
	})
	if err != nil {
		return err
	}
	for {
		event, err := stream.Recv()
		if err != nil {
			return err
		}
		e.connected.Store(true)
		e.applyEvent(event)
	}
}

// setHint records the leader address this edge was redirected to. It is a
// field rather than package state because every test builds its own cluster
// with the same edge IDs, and a leftover hint pointing at a closed listener
// would silently break the next test.
func (e *edgeNode) setHint(addr string) {
	e.hintMu.Lock()
	e.hint = addr
	e.hintMu.Unlock()
}

func (e *edgeNode) takeHint() string {
	e.hintMu.Lock()
	defer e.hintMu.Unlock()
	h := e.hint
	e.hint = ""
	return h
}

func (e *edgeNode) applyEvent(event *edgemeshv1.ConfigEvent) {
	switch ev := event.GetEvent().(type) {
	case *edgemeshv1.ConfigEvent_FullSnapshot:
		snap := ev.FullSnapshot
		e.applyRoutes(snap.GetRoutes(), snap.GetConfigVersion())
		for _, p := range snap.GetOriginPools() {
			e.applyPool(p)
		}
		e.applyMembership(snap.GetMembership())
		for _, p := range snap.GetRecentPurges() {
			e.coord.Purge(p.GetScope(), p.GetRouteId(), p.GetCacheKey(), p.GetVersion())
		}
		e.applied.Store(snap.GetConfigVersion())

	case *edgemeshv1.ConfigEvent_RouteChanged:
		current := allRoutes(e.routes.Load())
		next := make([]*edgemeshv1.Route, 0, len(current)+1)
		for _, r := range current {
			if r.GetId() != ev.RouteChanged.GetId() {
				next = append(next, r)
			}
		}
		next = append(next, ev.RouteChanged)
		e.applyRoutes(next, event.GetConfigVersion())
		e.applied.Store(event.GetConfigVersion())

	case *edgemeshv1.ConfigEvent_RouteDeleted:
		current := allRoutes(e.routes.Load())
		next := make([]*edgemeshv1.Route, 0, len(current))
		for _, r := range current {
			if r.GetId() != ev.RouteDeleted {
				next = append(next, r)
			}
		}
		e.applyRoutes(next, event.GetConfigVersion())
		e.applied.Store(event.GetConfigVersion())

	case *edgemeshv1.ConfigEvent_OriginPoolChanged:
		e.applyPool(ev.OriginPoolChanged)
		e.applied.Store(event.GetConfigVersion())

	case *edgemeshv1.ConfigEvent_MembershipChanged:
		e.applyMembership(ev.MembershipChanged)

	case *edgemeshv1.ConfigEvent_Purge:
		p := ev.Purge
		e.coord.Purge(p.GetScope(), p.GetRouteId(), p.GetCacheKey(), p.GetVersion())

	case *edgemeshv1.ConfigEvent_LeaderNotice:
		if addr := ev.LeaderNotice.GetLeaderControlAddress(); addr != "" && !ev.LeaderNotice.GetIsLeader() {
			e.setHint(addr)
		}
	}
}

func (e *edgeNode) applyRoutes(routes []*edgemeshv1.Route, version uint64) {
	if err := routing.ValidateSet(routes); err != nil {
		return
	}
	e.routes.Store(routing.NewTable(routes, version))
}

func (e *edgeNode) applyPool(p *edgemeshv1.OriginPool) {
	prev, _ := e.pools.Pool(p.GetId())
	built, err := origin.BuildPool(context.Background(), p, prev, e.pools.guard)
	if err != nil {
		return
	}
	e.pools.set(p.GetId(), built)
}

func (e *edgeNode) applyMembership(m *edgemeshv1.Membership) {
	if m == nil {
		return
	}
	// Reject stale membership, mirroring the edge binary. Config events can
	// arrive out of order across a reconnect, and rebuilding the ring from an
	// older version would move key ownership backwards.
	for {
		cur := e.membershipVersion.Load()
		if m.GetVersion() != 0 && m.GetVersion() <= cur {
			return
		}
		if e.membershipVersion.CompareAndSwap(cur, m.GetVersion()) {
			break
		}
	}
	nodes := make([]ring.Node, 0, len(m.GetNodes()))
	for _, n := range m.GetNodes() {
		if n.GetState() == edgemeshv1.NodeState_NODE_STATE_DEAD || n.GetPeerAddress() == "" {
			continue
		}
		nodes = append(nodes, ring.Node{ID: n.GetId(), PeerAddress: n.GetPeerAddress(), Weight: 1})
	}
	e.ring.Store(ring.New(nodes, 128).WithVersion(m.GetVersion()))
}

// heartbeat reports liveness to the leader, following the leader hint exactly
// as the edge binary does.
//
// Blind endpoint rotation is not good enough: with three control nodes a
// rotating heartbeat reaches the leader only one tick in three, and the leader
// would mark a perfectly healthy edge dead for missing heartbeats it did in
// fact send.
func (e *edgeNode) heartbeat(ctx context.Context, endpoints []string, id string) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()

	var leaderAddr string
	idx := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			target := leaderAddr
			if target == "" {
				target = endpoints[idx%len(endpoints)]
				idx++
			}
			conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				leaderAddr = ""
				continue
			}
			hctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			resp, err := edgemeshv1.NewEdgeControlClient(conn).Heartbeat(hctx, &edgemeshv1.HeartbeatRequest{
				NodeId: id, ConfigVersionApplied: e.applied.Load(),
				Node: &edgemeshv1.EdgeNode{
					Id: id, Region: "test", PeerAddress: e.peerAddr(), Weight: 1,
				},
			})
			cancel()
			_ = conn.Close()

			if err != nil {
				leaderAddr = ""
				continue
			}
			if addr := resp.GetLeaderControlAddress(); addr != "" {
				leaderAddr = addr
			}
			if resp.GetReregister() {
				// The leader has no record of this edge, which happens after a
				// leadership change. Registration happens when a config stream
				// opens, so the stream must restart for this edge to reappear.
				if leaderAddr != "" {
					e.setHint(leaderAddr)
				}
				select {
				case e.reconnect <- struct{}{}:
				default:
				}
			}
		}
	}
}

func allRoutes(t *routing.Table) []*edgemeshv1.Route {
	ids := t.AllIDs()
	out := make([]*edgemeshv1.Route, 0, len(ids))
	for _, id := range ids {
		if r, ok := t.ByID(id); ok {
			out = append(out, r)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// cluster is a full EdgeMesh deployment: control plane, edges, and origins.
type cluster struct {
	t       *testing.T
	control *controlCluster
	edges   []*edgeNode
	origins []*testOrigin
}

func newCluster(t *testing.T, edgeCount, originCount int) *cluster {
	t.Helper()
	c := &cluster{t: t, control: newControlCluster(t)}
	c.control.leader(5 * time.Second)

	for i := 0; i < originCount; i++ {
		c.origins = append(c.origins, newTestOrigin(t, fmt.Sprintf("origin-%d", i+1)))
	}
	endpoints := c.control.edgeEndpoints()
	for i := 0; i < edgeCount; i++ {
		c.edges = append(c.edges, newEdge(t, fmt.Sprintf("edge-%d", i+1), endpoints))
	}
	return c
}

// configure installs a pool over every origin plus one caching route.
func (c *cluster) configure(routeID, hostname string) {
	c.t.Helper()
	origins := make([]*edgemeshv1.Origin, 0, len(c.origins))
	for i, o := range c.origins {
		host, port := o.hostPort(c.t)
		origins = append(origins, &edgemeshv1.Origin{
			Id: fmt.Sprintf("origin-%d", i+1), Scheme: "http", Host: host, Port: port,
			Weight: 1, HealthPath: "/healthz", ExpectedStatuses: []int32{200},
		})
	}
	c.control.propose(&statemachine.Command{
		Type: statemachine.CommandCreateOriginPool, TimestampUnixMs: 1,
		OriginPool: &edgemeshv1.OriginPool{
			Id: "pool-1", Origins: origins,
			LoadBalancing:      edgemeshv1.LoadBalancing_LOAD_BALANCING_ROUND_ROBIN,
			HealthIntervalMs:   500,
			HealthTimeoutMs:    500,
			UnhealthyThreshold: 2, HealthyThreshold: 1,
		},
	})
	c.control.propose(&statemachine.Command{
		Type: statemachine.CommandCreateRoute, TimestampUnixMs: 1,
		Route: &edgemeshv1.Route{
			Id: routeID, Hostname: hostname, PathPrefix: "/",
			OriginPoolId: "pool-1", Enabled: true,
			CachePolicy: &edgemeshv1.CachePolicy{
				Enabled: true, DefaultTtlSeconds: 60, MaxTtlSeconds: 3600,
			},
			RetryPolicy: &edgemeshv1.RetryPolicy{
				Enabled: true, MaxRetries: 2, BackoffBaseMs: 5, BackoffMaxMs: 50,
			},
			HeaderPolicy: &edgemeshv1.HeaderPolicy{DiagnosticHeaders: true},
		},
	})
	c.waitForConfig()
}

// waitForConfig blocks until every edge has applied the current config version.
func (c *cluster) waitForConfig() {
	c.t.Helper()
	want := c.control.leader(5 * time.Second).sm.ConfigVersion()
	waitFor(c.t, 10*time.Second, "every edge applies the current config", func() bool {
		for _, e := range c.edges {
			if e.applied.Load() < want {
				return false
			}
		}
		return true
	})
}

// waitForRing blocks until every edge sees n nodes on its ring.
func (c *cluster) waitForRing(n int) {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, e := range c.edges {
			if e.ring.Load().Len() != n {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Report enough state to diagnose the disagreement rather than just the
	// timeout.
	for _, e := range c.edges {
		r := e.ring.Load()
		ids := make([]string, 0, r.Len())
		for _, node := range r.Nodes() {
			ids = append(ids, node.ID)
		}
		c.t.Logf("  %s: ring=%v version=%d applied_config=%d connected=%v",
			e.id, ids, r.Version(), e.applied.Load(), e.connected.Load())
	}
	for _, id := range c.control.ids {
		if n, ok := c.control.nodes[id]; ok {
			snap := n.members.Snapshot()
			ids := make([]string, 0, len(snap.GetNodes()))
			for _, node := range snap.GetNodes() {
				ids = append(ids, node.GetId()+":"+node.GetState().String())
			}
			c.t.Logf("  %s: leader=%v roster=%v version=%d",
				id, n.node.IsLeader(), ids, snap.GetVersion())
		}
	}
	c.t.Fatalf("not every edge saw %d ring nodes within 15s", n)
}

// get issues a request to one edge and returns the response.
func (c *cluster) get(edge *edgeNode, host, path string) (*http.Response, string) {
	c.t.Helper()
	req, err := http.NewRequest(http.MethodGet, edge.URL()+path, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	req.Host = host

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("GET %s via %s: %v", path, edge.id, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	body := make([]byte, 0, 512)
	buf := make([]byte, 512)
	for {
		n, err := resp.Body.Read(buf)
		body = append(body, buf[:n]...)
		if err != nil {
			break
		}
	}
	return resp, string(body)
}

// totalOriginHits sums a path's hits across every origin.
func (c *cluster) totalOriginHits(path string) int {
	n := 0
	for _, o := range c.origins {
		n += o.hitsFor(path)
	}
	return n
}

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition never held within %s: %s", timeout, desc)
}
