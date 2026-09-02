// Package l2 coordinates EdgeMesh's distributed peer cache.
//
// # Request flow
//
//	ingress edge
//	  L1 lookup ......................... HIT -> serve
//	  MISS
//	    ring.Owners(key, R)
//	      owner is local ................ GetOrFill locally (coalesced)
//	      owner is remote ............... peer GetOrFetch
//	        peer unreachable ............ try replicas in order
//	          all unreachable ........... fetch from origin (degraded)
//	    store the result in L1
//
// # Why the owner fills
//
// Centralizing the fill for a key on its ring owner means a cold object is
// fetched from origin once cluster-wide rather than once per edge. Combined
// with per-key coalescing at the owner, N concurrent misses across M edges
// produce exactly one origin request.
//
// # The cache is never a hard dependency
//
// Every peer failure has an origin fallback. A request is failed only when the
// origin itself cannot serve it. This is what stops one dead edge from taking
// out the keys it happened to own.
package l2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/cache/l1"
	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/peer"
	"github.com/sheehanlloyd/edgemesh/internal/ring"
)

// OriginFetcher fills an object from origin. The proxy supplies this so the L2
// coordinator never needs to know how routes, retries, or breakers work.
type OriginFetcher interface {
	Fetch(ctx context.Context, req peer.FetchRequest) (*cache.Object, error)
}

// PeerClient is the subset of peer.Client the coordinator uses.
type PeerClient interface {
	GetOrFetch(ctx context.Context, addr string, req peer.FetchRequest) (*cache.Object, cache.Outcome, error)
	Replicate(ctx context.Context, addr, sourceNodeID string, obj *cache.Object) error
}

// Metrics receives coordinator events. Every method must be non-blocking.
type Metrics struct {
	CacheRequest       func(tier cache.Tier, outcome cache.Outcome)
	ReplicationQueued  func(depth int)
	ReplicationDropped func(reason string)
	CoalescedWaiters   func(n int64)
	CoalescedFill      func()
	FillDuration       func(d time.Duration)
}

// Options configures a Coordinator.
type Options struct {
	NodeID string
	// Local is this node's share of the distributed tier.
	Local *l1.Cache
	// FrontTier is the node-local L1 cache sitting in front of the distributed
	// tier. It is purged alongside Local: an invalidation that cleared only L2
	// would leave the stale object serving from L1 indefinitely, which is the
	// difference between a purge that works and one that silently does nothing.
	FrontTier *l1.Cache
	Ring      *ring.Holder
	Peers     PeerClient
	Origin    OriginFetcher
	Clock     clock.Clock
	Logger    *slog.Logger
	Metrics   Metrics

	// ReplicationFactor is the owner plus successor count.
	ReplicationFactor int
	// ReplicationQueue bounds outstanding async replica writes.
	ReplicationQueue int
	// ReplicationWorkers bounds concurrent replica writes.
	ReplicationWorkers int
	// ReplicationTimeout bounds one replica write.
	ReplicationTimeout time.Duration
	// MaxObjectBytes bounds an accepted object.
	MaxObjectBytes uint64
	// PeerAddressFor resolves a ring node to a dialable peer address.
	PeerAddressFor func(n ring.Node) string
}

func (o *Options) applyDefaults() {
	if o.Clock == nil {
		o.Clock = clock.New()
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.ReplicationFactor < 1 {
		o.ReplicationFactor = 2
	}
	if o.ReplicationQueue <= 0 {
		o.ReplicationQueue = 1024
	}
	if o.ReplicationWorkers <= 0 {
		o.ReplicationWorkers = 4
	}
	if o.ReplicationTimeout <= 0 {
		o.ReplicationTimeout = 5 * time.Second
	}
	if o.MaxObjectBytes == 0 {
		o.MaxObjectBytes = 8 << 20
	}
	if o.PeerAddressFor == nil {
		o.PeerAddressFor = func(n ring.Node) string { return n.PeerAddress }
	}
}

// replicaJob is one queued asynchronous replica write.
type replicaJob struct {
	addr string
	obj  *cache.Object
}

// Coordinator owns this node's participation in the distributed cache.
type Coordinator struct {
	opts      Options
	coalescer *cache.Coalescer
	replicaCh chan replicaJob
	configVer atomic.Uint64
	ready     atomic.Bool
	// purgeVersion guards against a reordered purge event undoing a newer one.
	purgeVersion atomic.Uint64

	stopOnce sync.Once
	stopped  chan struct{}
	wg       sync.WaitGroup
}

// New builds the coordinator and starts its replication workers.
func New(ctx context.Context, o Options) (*Coordinator, error) {
	o.applyDefaults()
	if o.Local == nil {
		return nil, errs.New(errs.ClassValidation, "l2: a local cache tier is required")
	}
	if o.Ring == nil {
		return nil, errs.New(errs.ClassValidation, "l2: a ring holder is required")
	}
	if o.NodeID == "" {
		return nil, errs.New(errs.ClassValidation, "l2: a node id is required")
	}

	c := &Coordinator{
		opts:      o,
		coalescer: cache.NewCoalescer(ctx),
		replicaCh: make(chan replicaJob, o.ReplicationQueue),
		stopped:   make(chan struct{}),
	}
	for i := 0; i < o.ReplicationWorkers; i++ {
		c.wg.Add(1)
		go c.replicaWorker(ctx)
	}
	return c, nil
}

// SetReady marks whether this node can serve peer traffic.
func (c *Coordinator) SetReady(v bool) { c.ready.Store(v) }

// SetConfigVersion records the applied configuration version.
func (c *Coordinator) SetConfigVersion(v uint64) { c.configVer.Store(v) }

// NodeID identifies this edge.
func (c *Coordinator) NodeID() string { return c.opts.NodeID }

// Ready reports peer-serving readiness.
func (c *Coordinator) Ready() bool { return c.ready.Load() }

// ConfigVersion reports the applied configuration version.
func (c *Coordinator) ConfigVersion() uint64 { return c.configVer.Load() }

// MaxObjectBytes bounds an accepted object.
func (c *Coordinator) MaxObjectBytes() uint64 { return c.opts.MaxObjectBytes }

// Stats reports the local tier's occupancy.
func (c *Coordinator) Stats() (objects, bytes uint64) {
	s := c.opts.Local.Stats()
	return uint64(s.Objects), uint64(s.Bytes)
}

// Coalescer exposes fill statistics for metrics.
func (c *Coordinator) Coalescer() *cache.Coalescer { return c.coalescer }

// Get resolves a cacheable request through the distributed tier.
//
// It returns the object and the outcome that should be reported to the client
// in the diagnostic header and in metrics.
func (c *Coordinator) Get(ctx context.Context, req peer.FetchRequest) (*cache.Object, cache.Outcome, error) {
	r := c.opts.Ring.Load()

	// With no ring membership there is no distributed tier to consult; the
	// only correct behaviour is to fill locally rather than fail.
	if r.Empty() {
		obj, err := c.fillLocal(ctx, req)
		return obj, cache.OutcomeMiss, err
	}

	owners := r.Owners(req.CacheKey, c.opts.ReplicationFactor)
	if len(owners) == 0 {
		obj, err := c.fillLocal(ctx, req)
		return obj, cache.OutcomeMiss, err
	}

	// This node owns the key: fill locally, coalescing concurrent misses.
	if owners[0].ID == c.opts.NodeID {
		obj, outcome, err := c.GetOrFill(ctx, req)
		if err == nil && obj != nil {
			c.enqueueReplication(obj, owners)
		}
		return obj, outcome, err
	}

	// Ask the owner, then each replica in turn.
	var lastErr error
	for i, o := range owners {
		if o.ID == c.opts.NodeID {
			// This node is a replica: check its local tier before going remote.
			if obj, ok := c.opts.Local.Get(req.CacheKey); ok {
				c.recordCache(cache.OutcomeHit)
				return obj, cache.OutcomePeerHit, nil
			}
			continue
		}
		addr := c.opts.PeerAddressFor(o)
		if addr == "" {
			continue
		}
		// Not timed here: peer.Client times every RPC it makes and reports it
		// through OnResult, which is both closer to the wire and already wired
		// to peer_request_duration_seconds. A second measurement at this layer
		// would double-count.
		obj, peerOutcome, err := c.opts.Peers.GetOrFetch(ctx, addr, req)
		if err == nil && obj != nil {
			// PEER_HIT has to mean the object was already in the cluster. The
			// owner also fills from origin on this caller's behalf, and
			// reporting that as a hit would credit the cache with an origin
			// fetch it did not save. The owner tells us which it was.
			if peerOutcome == cache.OutcomeMiss {
				c.recordCache(cache.OutcomeMiss)
				return obj, cache.OutcomeMiss, nil
			}
			c.recordCache(cache.OutcomeHit)
			return obj, cache.OutcomePeerHit, nil
		}
		lastErr = err
		// A request whose own context ended must stop immediately rather than
		// walking every replica after the client has already gone.
		if ctx.Err() != nil {
			return nil, cache.OutcomeMiss, errs.Wrap(errs.ClassTimeout, ctx.Err(),
				"peer lookup abandoned")
		}
		c.opts.Logger.Warn("peer cache lookup failed, trying the next owner",
			slog.String("peer_id", o.ID),
			slog.String("peer_address", addr),
			slog.Int("owner_rank", i),
			slog.String("error", err.Error()),
			slog.String("error_class", string(errs.ClassOf(err))))
	}

	// Every peer that owns this key is unreachable. Falling back to origin is
	// what keeps the cache from being a hard dependency.
	c.recordCache(cache.OutcomeDegraded)
	c.opts.Logger.Warn("all peer owners unreachable, falling back to origin",
		slog.String("route_id", req.RouteID),
		slog.Int("owners_tried", len(owners)),
		slog.String("last_error", errText(lastErr)))

	obj, err := c.fillLocal(ctx, req)
	if err != nil {
		return nil, cache.OutcomeDegraded, err
	}
	return obj, cache.OutcomeDegraded, nil
}

// GetOrFill serves the key from this node's tier, filling from origin on a miss.
//
// This is both the local-owner path and the method the peer server exposes to
// other edges, which is why the coalescing lives here: one fill per key per
// process regardless of whether the callers are local or remote.
func (c *Coordinator) GetOrFill(ctx context.Context, req peer.FetchRequest) (*cache.Object, cache.Outcome, error) {
	if obj, ok := c.opts.Local.Get(req.CacheKey); ok {
		c.recordCache(cache.OutcomeHit)
		return obj, cache.OutcomeHit, nil
	}
	c.recordCache(cache.OutcomeMiss)

	start := c.opts.Clock.Now()
	obj, owner, err := c.coalescer.Do(ctx, req.CacheKey, func(fillCtx context.Context) (*cache.Object, error) {
		return c.fetchAndStore(fillCtx, req)
	})
	if c.opts.Metrics.CoalescedWaiters != nil {
		c.opts.Metrics.CoalescedWaiters(c.coalescer.Waiters())
	}
	if err != nil {
		return nil, cache.OutcomeMiss, err
	}
	if owner {
		if c.opts.Metrics.CoalescedFill != nil {
			c.opts.Metrics.CoalescedFill()
		}
		if c.opts.Metrics.FillDuration != nil {
			c.opts.Metrics.FillDuration(c.opts.Clock.Now().Sub(start))
		}
	}
	return obj, cache.OutcomeMiss, nil
}

// fetchAndStore fills from origin and admits the result to the local tier.
func (c *Coordinator) fetchAndStore(ctx context.Context, req peer.FetchRequest) (*cache.Object, error) {
	obj, err := c.opts.Origin.Fetch(ctx, req)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, errs.New(errs.ClassOriginFailure, "origin fetch produced no object")
	}
	obj.OriginNodeID = c.opts.NodeID
	if obj.ChecksumSHA256 == "" {
		sum := sha256.Sum256(obj.Body)
		obj.ChecksumSHA256 = hex.EncodeToString(sum[:])
	}
	// A non-cacheable response is returned to the caller but not stored: the
	// proxy already decided admissibility, and storing it anyway would be the
	// cache overriding policy.
	if !obj.ExpiresAt.IsZero() && obj.ExpiresAt.After(c.opts.Clock.Now()) {
		if err := c.opts.Local.Put(req.CacheKey, obj); err != nil {
			// A rejected admission is normal (too large, over budget); the
			// object is still served, just not remembered.
			c.opts.Logger.Debug("object not admitted to the local tier",
				slog.String("route_id", req.RouteID),
				slog.String("reason", err.Error()))
		}
	}
	return obj, nil
}

// fillLocal fetches from origin without consulting peers, used when the ring is
// empty or every peer owner is unreachable.
func (c *Coordinator) fillLocal(ctx context.Context, req peer.FetchRequest) (*cache.Object, error) {
	obj, _, err := c.coalescer.Do(ctx, req.CacheKey, func(fillCtx context.Context) (*cache.Object, error) {
		return c.fetchAndStore(fillCtx, req)
	})
	return obj, err
}

// StoreReplica accepts an object replicated from another edge.
func (c *Coordinator) StoreReplica(obj *cache.Object) error {
	if obj == nil {
		return errs.New(errs.ClassValidation, "nil replica")
	}
	if uint64(len(obj.Body)) > c.opts.MaxObjectBytes {
		return errs.New(errs.ClassTooLarge,
			"replica of %d bytes exceeds this node's %d byte limit", len(obj.Body), c.opts.MaxObjectBytes)
	}
	// An already-expired replica is dropped rather than stored: admitting it
	// would occupy budget that a live object could use.
	if obj.Expired(c.opts.Clock.Now()) {
		return errs.New(errs.ClassValidation, "replica is already expired")
	}
	return c.opts.Local.Put(obj.Key, obj)
}

// enqueueReplication schedules asynchronous replica writes to the successors.
//
// Replication is asynchronous and droppable by design. Cached content is
// derivative: a lost replica costs at worst one extra origin fetch after a node
// failure, whereas blocking a client response on a slow peer costs the client
// its latency budget for no correctness gain.
// The owners slice already carries everything replication needs, so the ring
// itself is not a parameter: passing it invited a second, possibly newer, view
// of ownership into a decision that must match the one the caller just made.
func (c *Coordinator) enqueueReplication(obj *cache.Object, owners []ring.Node) {
	if len(owners) < 2 || obj == nil {
		return
	}
	for _, o := range owners[1:] {
		if o.ID == c.opts.NodeID {
			continue
		}
		addr := c.opts.PeerAddressFor(o)
		if addr == "" {
			continue
		}
		select {
		case c.replicaCh <- replicaJob{addr: addr, obj: obj}:
			if c.opts.Metrics.ReplicationQueued != nil {
				c.opts.Metrics.ReplicationQueued(len(c.replicaCh))
			}
		default:
			// The queue is full. Dropping with a metric is the correct
			// backpressure response here; blocking would stall a client
			// response for a write nobody is waiting on.
			if c.opts.Metrics.ReplicationDropped != nil {
				c.opts.Metrics.ReplicationDropped("queue_full")
			}
		}
	}
}

// replicaWorker performs queued replica writes.
func (c *Coordinator) replicaWorker(ctx context.Context) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopped:
			return
		case job := <-c.replicaCh:
			// The replica write gets its own bounded context. It may briefly
			// outlive the client request that produced it, but never
			// indefinitely.
			wctx, cancel := context.WithTimeout(ctx, c.opts.ReplicationTimeout)
			err := c.opts.Peers.Replicate(wctx, job.addr, c.opts.NodeID, job.obj)
			cancel()
			if err != nil {
				if c.opts.Metrics.ReplicationDropped != nil {
					c.opts.Metrics.ReplicationDropped(string(errs.ClassOf(err)))
				}
				c.opts.Logger.Debug("replica write failed",
					slog.String("peer_address", job.addr),
					slog.String("error", err.Error()))
			}
		}
	}
}

// Purge drops matching objects from the local tier.
//
// Version checks make purge idempotent under reordering: config events can
// arrive out of order after a reconnect, and re-applying an older purge would
// needlessly evict objects fetched since.
func (c *Coordinator) Purge(scope edgemeshv1.PurgeScope, routeID, cacheKey string, version uint64) uint64 {
	if version > 0 {
		for {
			cur := c.purgeVersion.Load()
			if version <= cur {
				// Already applied this or a newer purge.
				return 0
			}
			if c.purgeVersion.CompareAndSwap(cur, version) {
				break
			}
		}
	}

	// Both tiers are purged. The front tier is purged first so a concurrent
	// request cannot repopulate L1 from an L2 entry that is about to be
	// removed.
	tiers := make([]*l1.Cache, 0, 2)
	if c.opts.FrontTier != nil {
		tiers = append(tiers, c.opts.FrontTier)
	}
	tiers = append(tiers, c.opts.Local)

	var n int
	for _, tier := range tiers {
		switch scope {
		case edgemeshv1.PurgeScope_PURGE_SCOPE_KEY:
			if tier.Delete(cacheKey) {
				n++
			}
		case edgemeshv1.PurgeScope_PURGE_SCOPE_ROUTE:
			n += tier.DeleteRoute(routeID)
		case edgemeshv1.PurgeScope_PURGE_SCOPE_ALL:
			n += tier.Clear()
		}
	}
	if n > 0 {
		c.opts.Logger.Info("cache purge applied",
			slog.String("scope", scope.String()),
			slog.String("route_id", routeID),
			slog.Uint64("purge_version", version),
			slog.Int("objects_purged", n))
	}
	return uint64(n)
}

// ReplicationQueueDepth reports pending replica writes, for metrics.
func (c *Coordinator) ReplicationQueueDepth() int { return len(c.replicaCh) }

// Close stops the replication workers and waits for them.
func (c *Coordinator) Close(ctx context.Context) error {
	c.stopOnce.Do(func() { close(c.stopped) })
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errs.Wrap(errs.ClassTimeout, ctx.Err(), "l2: replication workers did not stop in time")
	}
}

// recordCache reports a distributed-tier outcome. The coordinator only ever
// speaks for L2; the front tier's own hits are recorded by the proxy handler.
func (c *Coordinator) recordCache(outcome cache.Outcome) {
	if c.opts.Metrics.CacheRequest != nil {
		c.opts.Metrics.CacheRequest(cache.TierL2, outcome)
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
