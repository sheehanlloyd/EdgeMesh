// Package peer implements the L2 distributed cache protocol between edges.
//
// # Ownership
//
// Every cache key has a primary owner chosen by the consistent hash ring, plus
// R-1 clockwise successors as replicas. The ingress edge, the node that
// received the client request, is usually not the owner, so on an L1 miss it
// asks the owner through GetOrFetch.
//
// Centralizing the fill on the owner is the point: without it, three edges
// receiving the same cold key would each fetch from origin. With it, exactly
// one fetch happens per key cluster-wide, and the other edges stream the result.
//
// # The cache is never a hard dependency
//
// If the owner is unreachable, the ingress edge tries the replicas, and failing
// that fetches from origin itself. A request is never failed because a peer is
// down when the origin is reachable. That property is what keeps the L2 cache
// from converting a single node failure into a cluster-wide outage.
package peer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// chunkBytes bounds one streamed body frame. Streaming rather than sending one
// large message keeps peak memory proportional to the chunk size instead of the
// object size, and stays well inside gRPC's default message limit.
const chunkBytes = 64 << 10

// allowlistedRequestHeaders are the only client headers forwarded to a peer.
//
// An allowlist rather than a denylist: forwarding an unknown header is how an
// internal or admin header leaks across a trust boundary, and a denylist has to
// be updated every time a new sensitive header exists.
var allowlistedRequestHeaders = []string{
	"Accept",
	"Accept-Encoding",
	"Accept-Language",
	"If-None-Match",
	"If-Modified-Since",
	"User-Agent",
}

// Dialer opens a connection to a peer edge.
type Dialer func(ctx context.Context, target string) (*grpc.ClientConn, error)

// Client dials peer edges for L2 operations.
type Client struct {
	dial    Dialer
	timeout time.Duration
	log     *slog.Logger

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	closed bool

	// OnResult records an RPC outcome for metrics.
	OnResult func(operation, result string, d time.Duration)
}

// ClientOptions configures a peer client.
type ClientOptions struct {
	Dialer  Dialer
	Timeout time.Duration
	Logger  *slog.Logger
}

// NewClient builds a peer client.
func NewClient(o ClientOptions) (*Client, error) {
	if o.Dialer == nil {
		return nil, errs.New(errs.ClassValidation, "peer client: a dialer is required")
	}
	if o.Timeout <= 0 {
		o.Timeout = 10 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Client{
		dial:    o.Dialer,
		timeout: o.Timeout,
		log:     o.Logger,
		conns:   make(map[string]*grpc.ClientConn),
	}, nil
}

// conn returns a cached connection, dialing lazily.
func (c *Client) conn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errs.New(errs.ClassUnavailable, "peer client is closed")
	}
	if cc, ok := c.conns[addr]; ok {
		c.mu.Unlock()
		return cc, nil
	}
	c.mu.Unlock()

	cc, err := c.dial(ctx, addr)
	if err != nil {
		return nil, errs.Wrap(errs.ClassPeerFailure, err, "dial peer %q", addr)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = cc.Close()
		return nil, errs.New(errs.ClassUnavailable, "peer client is closed")
	}
	if existing, ok := c.conns[addr]; ok {
		_ = cc.Close()
		return existing, nil
	}
	c.conns[addr] = cc
	return cc, nil
}

// FetchRequest describes what the ingress edge wants from an owner.
type FetchRequest struct {
	RouteID    string
	CacheKey   string
	Method     string
	Path       string
	RawQuery   string
	Header     http.Header
	ClientHost string
	// Hop guards against a peer forwarding to another peer, which could form a
	// cycle. An owner receiving hop > 0 fetches from origin itself.
	Hop uint32
}

// GetOrFetch asks a peer for an object, filling from origin at the peer if
// necessary. The returned object is fully buffered: L2 objects are bounded by
// max_object_bytes, so buffering is bounded too, and a buffered object can be
// stored in L1 without a second read.
// The returned outcome is what the *owner* reported: whether it served the
// object from its own tier or had to fetch it from the origin on this caller's
// behalf. The caller needs that distinction to label the response honestly,
// because an object that arrived over the peer protocol has not necessarily
// saved anyone an origin fetch.
func (c *Client) GetOrFetch(ctx context.Context, addr string, req FetchRequest) (*cache.Object, cache.Outcome, error) {
	start := time.Now()
	obj, outcome, err := c.getOrFetch(ctx, addr, req)
	c.record("get_or_fetch", err, time.Since(start))
	return obj, outcome, err
}

func (c *Client) getOrFetch(ctx context.Context, addr string, req FetchRequest) (*cache.Object, cache.Outcome, error) {
	cc, err := c.conn(ctx, addr)
	if err != nil {
		return nil, cache.OutcomeMiss, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	stream, err := edgemeshv1.NewPeerCacheClient(cc).GetOrFetch(ctx, &edgemeshv1.GetOrFetchRequest{
		RouteId:    req.RouteID,
		CacheKey:   req.CacheKey,
		Method:     req.Method,
		Path:       req.Path,
		RawQuery:   req.RawQuery,
		Headers:    filterHeaders(req.Header),
		ClientHost: req.ClientHost,
		Hop:        req.Hop,
	})
	if err != nil {
		return nil, cache.OutcomeMiss, errs.Wrap(errs.ClassPeerFailure, err, "open GetOrFetch stream to %q", addr)
	}

	var (
		meta *edgemeshv1.ObjectMetadata
		body []byte
	)
	for {
		chunk, err := stream.Recv()
		// errors.Is, not ==: gRPC wraps io.EOF, and an equality check would
		// turn the normal end of an object stream into a peer failure.
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, cache.OutcomeMiss, errs.Wrap(errs.ClassPeerFailure, err, "receive object from %q", addr)
		}
		if m := chunk.GetMetadata(); m != nil {
			meta = m
			if m.GetContentLength() > 0 {
				body = make([]byte, 0, m.GetContentLength())
			}
			continue
		}
		body = append(body, chunk.GetBody()...)
	}
	if meta == nil {
		return nil, cache.OutcomeMiss, errs.New(errs.ClassProtocol, "peer %q sent an object with no metadata", addr)
	}
	// A checksum mismatch means the bytes on the wire are not the bytes the
	// peer stored. Serving them would propagate corruption into this node's L1.
	if sum := meta.GetChecksumSha256(); sum != "" {
		got := sha256.Sum256(body)
		if hex.EncodeToString(got[:]) != sum {
			return nil, cache.OutcomeMiss, errs.New(errs.ClassProtocol,
				"object from %q failed its checksum; %d bytes discarded", addr, len(body))
		}
	}
	return objectFromProto(req.CacheKey, req.RouteID, meta, body), outcomeFromProto(meta.GetOutcome()), nil
}

// Replicate stores a copy of an object on a peer.
func (c *Client) Replicate(ctx context.Context, addr, sourceNodeID string, obj *cache.Object) error {
	start := time.Now()
	err := c.replicate(ctx, addr, sourceNodeID, obj)
	c.record("put_replica", err, time.Since(start))
	return err
}

func (c *Client) replicate(ctx context.Context, addr, sourceNodeID string, obj *cache.Object) error {
	cc, err := c.conn(ctx, addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	stream, err := edgemeshv1.NewPeerCacheClient(cc).PutReplica(ctx)
	if err != nil {
		return errs.Wrap(errs.ClassPeerFailure, err, "open PutReplica stream to %q", addr)
	}
	if err := stream.Send(&edgemeshv1.ReplicaChunk{
		Payload: &edgemeshv1.ReplicaChunk_Header{
			Header: &edgemeshv1.ReplicaHeader{
				CacheKey:     obj.Key,
				RouteId:      obj.RouteID,
				Metadata:     metadataFromObject(obj, cache.OutcomeHit),
				SourceNodeId: sourceNodeID,
			},
		},
	}); err != nil {
		return errs.Wrap(errs.ClassPeerFailure, err, "send replica header to %q", addr)
	}
	for off := 0; off < len(obj.Body); off += chunkBytes {
		end := off + chunkBytes
		if end > len(obj.Body) {
			end = len(obj.Body)
		}
		if err := stream.Send(&edgemeshv1.ReplicaChunk{
			Payload: &edgemeshv1.ReplicaChunk_Body{Body: obj.Body[off:end]},
		}); err != nil {
			return errs.Wrap(errs.ClassPeerFailure, err, "send replica body to %q", addr)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return errs.Wrap(errs.ClassPeerFailure, err, "complete PutReplica to %q", addr)
	}
	if !resp.GetStored() {
		// A refused replica is not an error: the peer may legitimately reject an
		// object that exceeds its budget. It is reported so it can be counted.
		return errs.New(errs.ClassPeerFailure, "peer %q refused the replica: %s", addr, resp.GetReason())
	}
	return nil
}

// Purge asks a peer to drop cached objects.
func (c *Client) Purge(ctx context.Context, addr string, req *edgemeshv1.PurgeRequest) (uint64, error) {
	start := time.Now()
	cc, err := c.conn(ctx, addr)
	if err != nil {
		c.record("purge", err, time.Since(start))
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	resp, err := edgemeshv1.NewPeerCacheClient(cc).Purge(ctx, req)
	c.record("purge", err, time.Since(start))
	if err != nil {
		return 0, errs.Wrap(errs.ClassPeerFailure, err, "purge on %q", addr)
	}
	return resp.GetPurgedObjects(), nil
}

// Health probes a peer.
func (c *Client) Health(ctx context.Context, addr, fromNodeID string) (*edgemeshv1.PeerHealthResponse, error) {
	cc, err := c.conn(ctx, addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	resp, err := edgemeshv1.NewPeerCacheClient(cc).Health(ctx,
		&edgemeshv1.PeerHealthRequest{FromNodeId: fromNodeID})
	if err != nil {
		return nil, errs.Wrap(errs.ClassPeerFailure, err, "health check on %q", addr)
	}
	return resp, nil
}

func (c *Client) record(op string, err error, d time.Duration) {
	if c.OnResult == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = string(errs.ClassOf(err))
	}
	c.OnResult(op, result, d)
}

// Close releases every peer connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var firstErr error
	for addr, cc := range c.conns {
		if err := cc.Close(); err != nil && firstErr == nil {
			firstErr = errs.Wrap(errs.ClassPeerFailure, err, "close peer connection to %q", addr)
		}
		delete(c.conns, addr)
	}
	return firstErr
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

// filterHeaders keeps only the allowlisted request headers.
func filterHeaders(h http.Header) []*edgemeshv1.Header {
	if len(h) == 0 {
		return nil
	}
	out := make([]*edgemeshv1.Header, 0, len(allowlistedRequestHeaders))
	for _, name := range allowlistedRequestHeaders {
		if v := h.Values(name); len(v) > 0 {
			out = append(out, &edgemeshv1.Header{Name: name, Values: v})
		}
	}
	return out
}

// HeadersToHTTP converts wire headers into an http.Header, applying the same
// allowlist on the receiving side. Trusting the sender's filtering would make
// the allowlist a client-side check, which is no check at all.
func HeadersToHTTP(in []*edgemeshv1.Header) http.Header {
	allowed := make(map[string]struct{}, len(allowlistedRequestHeaders))
	for _, n := range allowlistedRequestHeaders {
		allowed[http.CanonicalHeaderKey(n)] = struct{}{}
	}
	out := http.Header{}
	for _, h := range in {
		name := http.CanonicalHeaderKey(h.GetName())
		if _, ok := allowed[name]; !ok {
			continue
		}
		for _, v := range h.GetValues() {
			out.Add(name, v)
		}
	}
	return out
}

// metadataFromObject renders an object's metadata for the wire.
func metadataFromObject(obj *cache.Object, outcome cache.Outcome) *edgemeshv1.ObjectMetadata {
	headers := make([]*edgemeshv1.Header, 0, len(obj.Header))
	for name, values := range obj.Header {
		headers = append(headers, &edgemeshv1.Header{Name: name, Values: values})
	}
	sum := obj.ChecksumSHA256
	if sum == "" {
		s := sha256.Sum256(obj.Body)
		sum = hex.EncodeToString(s[:])
	}
	return &edgemeshv1.ObjectMetadata{
		Status:          int32(obj.Status),
		Headers:         headers,
		ContentLength:   uint64(len(obj.Body)),
		StoredAtUnixMs:  obj.StoredAt.UnixMilli(),
		ExpiresAtUnixMs: obj.ExpiresAt.UnixMilli(),
		AgeSeconds:      uint64(time.Since(obj.StoredAt).Seconds()),
		Outcome:         outcomeToProto(outcome),
		ChecksumSha256:  sum,
		ServedBy:        obj.OriginNodeID,
		Cacheable:       true,
	}
}

// objectFromProto rebuilds an object from the wire.
func objectFromProto(key, routeID string, meta *edgemeshv1.ObjectMetadata, body []byte) *cache.Object {
	h := http.Header{}
	for _, hdr := range meta.GetHeaders() {
		for _, v := range hdr.GetValues() {
			h.Add(hdr.GetName(), v)
		}
	}
	expires := time.UnixMilli(meta.GetExpiresAtUnixMs())
	return &cache.Object{
		Key:            key,
		RouteID:        routeID,
		Status:         int(meta.GetStatus()),
		Header:         h,
		Body:           body,
		StoredAt:       time.UnixMilli(meta.GetStoredAtUnixMs()),
		ExpiresAt:      expires,
		StaleUntil:     expires,
		ChecksumSHA256: meta.GetChecksumSha256(),
		OriginNodeID:   meta.GetServedBy(),
	}
}

// outcomeFromProto reads what a peer reported about how it answered.
//
// Only the hit/miss distinction survives the round trip, which is all the caller
// needs: did this object already exist somewhere in the cluster, or did the
// owner fetch it from the origin just now. An unset outcome is read as a miss,
// the conservative direction, since claiming a hit that did not happen would
// overstate the cache.
func outcomeFromProto(o edgemeshv1.CacheOutcome) cache.Outcome {
	switch o {
	case edgemeshv1.CacheOutcome_CACHE_OUTCOME_HIT:
		return cache.OutcomeHit
	case edgemeshv1.CacheOutcome_CACHE_OUTCOME_STALE:
		return cache.OutcomeStale
	case edgemeshv1.CacheOutcome_CACHE_OUTCOME_REVALIDATED:
		return cache.OutcomeRevalidated
	default:
		return cache.OutcomeMiss
	}
}

func outcomeToProto(o cache.Outcome) edgemeshv1.CacheOutcome {
	switch o {
	case cache.OutcomeHit, cache.OutcomePeerHit:
		return edgemeshv1.CacheOutcome_CACHE_OUTCOME_HIT
	case cache.OutcomeMiss, cache.OutcomeDegraded:
		return edgemeshv1.CacheOutcome_CACHE_OUTCOME_MISS
	case cache.OutcomeBypass:
		return edgemeshv1.CacheOutcome_CACHE_OUTCOME_BYPASS
	case cache.OutcomeStale:
		return edgemeshv1.CacheOutcome_CACHE_OUTCOME_STALE
	case cache.OutcomeRevalidated:
		return edgemeshv1.CacheOutcome_CACHE_OUTCOME_REVALIDATED
	default:
		return edgemeshv1.CacheOutcome_CACHE_OUTCOME_UNSPECIFIED
	}
}
