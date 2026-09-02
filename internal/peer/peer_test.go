package peer

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// fakeOwner implements Owner over an in-memory map.
type fakeOwner struct {
	mu       sync.Mutex
	objects  map[string]*cache.Object
	fillErr  error
	replicas []*cache.Object
	maxBytes uint64
	// lastHop records the hop counter of the most recent GetOrFill.
	lastHop uint32
}

func newFakeOwner() *fakeOwner {
	return &fakeOwner{objects: map[string]*cache.Object{}, maxBytes: 1 << 20}
}

func (f *fakeOwner) GetOrFill(_ context.Context, req FetchRequest) (*cache.Object, cache.Outcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastHop = req.Hop
	if f.fillErr != nil {
		return nil, cache.OutcomeMiss, f.fillErr
	}
	if obj, ok := f.objects[req.CacheKey]; ok {
		return obj, cache.OutcomeHit, nil
	}
	now := time.Now()
	obj := &cache.Object{
		Key: req.CacheKey, RouteID: req.RouteID, Status: 200,
		Header:   http.Header{"Content-Type": []string{"text/plain"}},
		Body:     []byte(strings.Repeat("filled-", 20000)), // spans several chunks
		StoredAt: now, ExpiresAt: now.Add(time.Hour), StaleUntil: now.Add(time.Hour),
		OriginNodeID: "edge-owner",
	}
	f.objects[req.CacheKey] = obj
	return obj, cache.OutcomeMiss, nil
}

func (f *fakeOwner) StoreReplica(obj *cache.Object) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if uint64(len(obj.Body)) > f.maxBytes {
		return errs.New(errs.ClassTooLarge, "replica too large")
	}
	f.replicas = append(f.replicas, obj)
	f.objects[obj.Key] = obj
	return nil
}

func (f *fakeOwner) Purge(scope edgemeshv1.PurgeScope, routeID, cacheKey string, _ uint64) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := uint64(0)
	switch scope {
	case edgemeshv1.PurgeScope_PURGE_SCOPE_KEY:
		if _, ok := f.objects[cacheKey]; ok {
			delete(f.objects, cacheKey)
			n = 1
		}
	case edgemeshv1.PurgeScope_PURGE_SCOPE_ALL:
		n = uint64(len(f.objects))
		f.objects = map[string]*cache.Object{}
	case edgemeshv1.PurgeScope_PURGE_SCOPE_ROUTE:
		for k, o := range f.objects {
			if o.RouteID == routeID {
				delete(f.objects, k)
				n++
			}
		}
	}
	return n
}

func (f *fakeOwner) Stats() (uint64, uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return uint64(len(f.objects)), 4096
}

func (f *fakeOwner) NodeID() string         { return "edge-owner" }
func (f *fakeOwner) Ready() bool            { return true }
func (f *fakeOwner) ConfigVersion() uint64  { return 42 }
func (f *fakeOwner) MaxObjectBytes() uint64 { return f.maxBytes }

// startServer runs a real gRPC peer server on a loopback port, so the streaming
// framing, chunking, and checksum paths are exercised rather than mocked.
func startServer(t *testing.T, owner Owner) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	edgemeshv1.RegisterPeerCacheServer(srv, NewServer(owner, nil))
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), func() { srv.GracefulStop() }
}

func newTestClient(t *testing.T) *Client {
	t.Helper()
	c, err := NewClient(ClientOptions{
		Timeout: 5 * time.Second,
		Dialer: func(_ context.Context, target string) (*grpc.ClientConn, error) {
			return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestGetOrFetchRoundTrip(t *testing.T) {
	owner := newFakeOwner()
	addr, stop := startServer(t, owner)
	defer stop()
	c := newTestClient(t)

	obj, _, err := c.GetOrFetch(context.Background(), addr, FetchRequest{
		RouteID: "route-a", CacheKey: "k1", Method: "GET", Path: "/x",
		Header: http.Header{"Accept-Encoding": []string{"gzip"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if obj.Status != 200 || obj.RouteID != "route-a" || obj.Key != "k1" {
		t.Fatalf("object metadata lost: %+v", obj)
	}
	// A body spanning multiple chunks must reassemble exactly.
	if len(obj.Body) != len(strings.Repeat("filled-", 20000)) {
		t.Fatalf("body length = %d, want %d", len(obj.Body), len(strings.Repeat("filled-", 20000)))
	}
	if obj.Header.Get("Content-Type") != "text/plain" {
		t.Fatalf("headers lost: %v", obj.Header)
	}
	if obj.ChecksumSHA256 == "" {
		t.Fatal("no checksum was transmitted")
	}
}

// A peer must never forward to a third peer, or a ring disagreement could
// bounce a request until the deadline expires.
func TestHopCounterIsIncremented(t *testing.T) {
	owner := newFakeOwner()
	addr, stop := startServer(t, owner)
	defer stop()
	c := newTestClient(t)

	if _, _, err := c.GetOrFetch(context.Background(), addr, FetchRequest{
		RouteID: "route-a", CacheKey: "k1", Method: "GET", Path: "/x", Hop: 0,
	}); err != nil {
		t.Fatal(err)
	}
	owner.mu.Lock()
	hop := owner.lastHop
	owner.mu.Unlock()
	if hop != 1 {
		t.Fatalf("owner saw hop = %d, want 1", hop)
	}
}

// The header allowlist is enforced on the receiving side too: trusting the
// sender's filtering would make it a client-side check, which is no check.
func TestHeaderAllowlistIsEnforcedOnBothSides(t *testing.T) {
	sent := filterHeaders(http.Header{
		"Accept-Encoding": []string{"gzip"},
		"Authorization":   []string{"Bearer secret"},
		"Cookie":          []string{"sid=1"},
		"X-Internal-Auth": []string{"internal"},
	})
	for _, h := range sent {
		if strings.EqualFold(h.GetName(), "Authorization") ||
			strings.EqualFold(h.GetName(), "Cookie") ||
			strings.EqualFold(h.GetName(), "X-Internal-Auth") {
			t.Fatalf("sensitive header %q was sent to a peer", h.GetName())
		}
	}

	// Even if a peer sends a disallowed header, the receiver drops it.
	got := HeadersToHTTP([]*edgemeshv1.Header{
		{Name: "Accept-Encoding", Values: []string{"gzip"}},
		{Name: "Authorization", Values: []string{"Bearer secret"}},
		{Name: "X-Internal-Auth", Values: []string{"internal"}},
	})
	if got.Get("Authorization") != "" || got.Get("X-Internal-Auth") != "" {
		t.Fatalf("a disallowed header survived receipt: %v", got)
	}
	if got.Get("Accept-Encoding") != "gzip" {
		t.Fatal("an allowlisted header was dropped")
	}
}

func TestPutReplicaRoundTrip(t *testing.T) {
	owner := newFakeOwner()
	addr, stop := startServer(t, owner)
	defer stop()
	c := newTestClient(t)

	now := time.Now()
	obj := &cache.Object{
		Key: "k-replica", RouteID: "route-a", Status: 200,
		Header:   http.Header{"Content-Type": []string{"application/json"}},
		Body:     []byte(strings.Repeat("replica-", 30000)),
		StoredAt: now, ExpiresAt: now.Add(time.Hour), StaleUntil: now.Add(time.Hour),
	}
	if err := c.Replicate(context.Background(), addr, "edge-1", obj); err != nil {
		t.Fatal(err)
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if len(owner.replicas) != 1 {
		t.Fatalf("stored %d replicas, want 1", len(owner.replicas))
	}
	got := owner.replicas[0]
	if got.Key != obj.Key || len(got.Body) != len(obj.Body) {
		t.Fatalf("replica corrupted in transit: key=%q len=%d", got.Key, len(got.Body))
	}
}

// A peer that lies about content length must not make this node buffer without
// limit.
func TestOversizedReplicaIsRefused(t *testing.T) {
	owner := newFakeOwner()
	owner.maxBytes = 1024
	addr, stop := startServer(t, owner)
	defer stop()
	c := newTestClient(t)

	now := time.Now()
	err := c.Replicate(context.Background(), addr, "edge-1", &cache.Object{
		Key: "big", RouteID: "route-a", Status: 200, Header: http.Header{},
		Body: make([]byte, 8192), StoredAt: now,
		ExpiresAt: now.Add(time.Hour), StaleUntil: now.Add(time.Hour),
	})
	if err == nil {
		t.Fatal("an oversized replica must be refused")
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if len(owner.replicas) != 0 {
		t.Fatal("an oversized replica was stored")
	}
}

func TestPurgeAndHealthRPCs(t *testing.T) {
	owner := newFakeOwner()
	addr, stop := startServer(t, owner)
	defer stop()
	c := newTestClient(t)

	if _, _, err := c.GetOrFetch(context.Background(), addr, FetchRequest{
		RouteID: "route-a", CacheKey: "k1", Method: "GET", Path: "/x",
	}); err != nil {
		t.Fatal(err)
	}
	n, err := c.Purge(context.Background(), addr, &edgemeshv1.PurgeRequest{
		Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_KEY, CacheKey: "k1", Version: 1,
	})
	if err != nil || n != 1 {
		t.Fatalf("purge = %d, %v", n, err)
	}

	health, err := c.Health(context.Background(), addr, "edge-1")
	if err != nil {
		t.Fatal(err)
	}
	if health.GetNodeId() != "edge-owner" || !health.GetReady() || health.GetConfigVersion() != 42 {
		t.Fatalf("health = %+v", health)
	}
}

func TestUnreachablePeerIsClassified(t *testing.T) {
	c := newTestClient(t)
	// Port 1 on loopback has nothing listening.
	_, _, err := c.GetOrFetch(context.Background(), "127.0.0.1:1", FetchRequest{CacheKey: "k"})
	if err == nil {
		t.Fatal("an unreachable peer must produce an error")
	}
	if !errs.IsClass(err, errs.ClassPeerFailure) {
		t.Fatalf("error class = %q, want peer_failure", errs.ClassOf(err))
	}
}

func TestFillErrorPropagates(t *testing.T) {
	owner := newFakeOwner()
	owner.fillErr = errs.New(errs.ClassOriginFailure, "origin exploded")
	addr, stop := startServer(t, owner)
	defer stop()
	c := newTestClient(t)

	if _, _, err := c.GetOrFetch(context.Background(), addr, FetchRequest{CacheKey: "k"}); err == nil {
		t.Fatal("an owner-side fill failure must reach the caller")
	}
}

func TestGetOrFetchRequiresACacheKey(t *testing.T) {
	addr, stop := startServer(t, newFakeOwner())
	defer stop()
	c := newTestClient(t)
	if _, _, err := c.GetOrFetch(context.Background(), addr, FetchRequest{}); err == nil {
		t.Fatal("a request with no cache key must be rejected")
	}
}

func TestClientCloseIsIdempotent(t *testing.T) {
	c := newTestClient(t)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// A closed client must refuse new work rather than panicking.
	if _, _, err := c.GetOrFetch(context.Background(), "127.0.0.1:1", FetchRequest{CacheKey: "k"}); err == nil {
		t.Fatal("a closed client accepted a request")
	}
}

func TestObjectToHTTPRecomputesAge(t *testing.T) {
	now := time.Now()
	obj := &cache.Object{
		Status: 200, Header: http.Header{"Content-Type": []string{"text/plain"}},
		Body: []byte("hello"), StoredAt: now.Add(-30 * time.Second),
		ExpiresAt: now.Add(time.Hour), StaleUntil: now.Add(time.Hour),
	}
	w := &recorder{header: http.Header{}}
	n, err := ObjectToHTTP(w, obj, now)
	if err != nil || n != 5 {
		t.Fatalf("wrote %d bytes: %v", n, err)
	}
	// Age must reflect how stale the object is *now*, not when it was stored.
	if w.header.Get("Age") != "30" {
		t.Fatalf("Age = %q, want 30", w.header.Get("Age"))
	}
	if w.status != 200 {
		t.Fatalf("status = %d", w.status)
	}
}

type recorder struct {
	header http.Header
	status int
	body   []byte
}

func (r *recorder) Header() http.Header { return r.header }
func (r *recorder) WriteHeader(s int)   { r.status = s }
func (r *recorder) Write(b []byte) (int, error) {
	r.body = append(r.body, b...)
	return len(b), nil
}

// Run with -race.
func TestPeerConcurrency(t *testing.T) {
	owner := newFakeOwner()
	addr, stop := startServer(t, owner)
	defer stop()
	c := newTestClient(t)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				key := "k" + string(rune('a'+i%5))
				switch i % 3 {
				case 0:
					_, _, _ = c.GetOrFetch(context.Background(), addr, FetchRequest{
						RouteID: "route-a", CacheKey: key, Method: "GET", Path: "/x",
					})
				case 1:
					_, _ = c.Purge(context.Background(), addr, &edgemeshv1.PurgeRequest{
						Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_KEY, CacheKey: key,
					})
				case 2:
					_, _ = c.Health(context.Background(), addr, "edge-1")
				}
			}
		}(w)
	}
	wg.Wait()
}
