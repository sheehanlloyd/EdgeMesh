package peer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// Owner is the local L2 tier this server exposes to peers.
//
// It is an interface so that the peer transport can be tested without the whole
// edge stack, and so the L2 coordinator does not have to know about gRPC.
type Owner interface {
	// GetOrFill returns an object for the key, filling from origin when the
	// local tier misses. hop is the peer-forwarding depth.
	GetOrFill(ctx context.Context, req FetchRequest) (*cache.Object, cache.Outcome, error)
	// StoreReplica accepts a replica from another edge.
	StoreReplica(obj *cache.Object) error
	// Purge drops objects matching the directive and reports the count.
	Purge(scope edgemeshv1.PurgeScope, routeID, cacheKey string, version uint64) uint64
	// Stats reports the local tier's size for health responses.
	Stats() (objects, bytes uint64)
	// NodeID identifies this edge.
	NodeID() string
	// Ready reports whether this edge can serve peer traffic.
	Ready() bool
	// ConfigVersion reports the applied configuration version.
	ConfigVersion() uint64
	// MaxObjectBytes bounds an accepted replica.
	MaxObjectBytes() uint64
}

// Server implements the PeerCache gRPC service.
type Server struct {
	edgemeshv1.UnimplementedPeerCacheServer
	owner Owner
	log   *slog.Logger

	// OnRequest records an inbound RPC outcome for metrics.
	OnRequest func(operation, result string, d time.Duration)
}

// NewServer builds the peer service.
func NewServer(owner Owner, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{owner: owner, log: log}
}

// GetOrFetch serves an object to a peer, filling from origin if needed.
func (s *Server) GetOrFetch(req *edgemeshv1.GetOrFetchRequest, stream edgemeshv1.PeerCache_GetOrFetchServer) error {
	start := time.Now()
	err := s.getOrFetch(req, stream)
	s.record("get_or_fetch", err, time.Since(start))
	return err
}

func (s *Server) getOrFetch(req *edgemeshv1.GetOrFetchRequest, stream edgemeshv1.PeerCache_GetOrFetchServer) error {
	if req.GetCacheKey() == "" {
		return errs.New(errs.ClassValidation, "GetOrFetch requires a cache key")
	}
	// A peer request must never be forwarded to a third peer. Without this
	// bound, a transient ring disagreement between two nodes could bounce a
	// request between them until the deadline expires.
	if req.GetHop() > 0 {
		s.log.Debug("serving a forwarded peer request from origin directly",
			slog.String("cache_key_route", req.GetRouteId()),
			slog.Uint64("hop", uint64(req.GetHop())))
	}

	obj, outcome, err := s.owner.GetOrFill(stream.Context(), FetchRequest{
		RouteID:    req.GetRouteId(),
		CacheKey:   req.GetCacheKey(),
		Method:     req.GetMethod(),
		Path:       req.GetPath(),
		RawQuery:   req.GetRawQuery(),
		Header:     HeadersToHTTP(req.GetHeaders()),
		ClientHost: req.GetClientHost(),
		Hop:        req.GetHop() + 1,
	})
	if err != nil {
		return err
	}
	if obj == nil {
		return errs.New(errs.ClassCacheMiss, "no object available for the requested key")
	}

	if err := stream.Send(&edgemeshv1.ObjectChunk{
		Payload: &edgemeshv1.ObjectChunk_Metadata{Metadata: metadataFromObject(obj, outcome)},
	}); err != nil {
		return errs.Wrap(errs.ClassPeerFailure, err, "send object metadata")
	}
	for off := 0; off < len(obj.Body); off += chunkBytes {
		end := off + chunkBytes
		if end > len(obj.Body) {
			end = len(obj.Body)
		}
		if err := stream.Send(&edgemeshv1.ObjectChunk{
			Payload: &edgemeshv1.ObjectChunk_Body{Body: obj.Body[off:end]},
		}); err != nil {
			return errs.Wrap(errs.ClassPeerFailure, err, "send object body")
		}
	}
	return nil
}

// PutReplica accepts a replicated object from another edge.
func (s *Server) PutReplica(stream edgemeshv1.PeerCache_PutReplicaServer) error {
	start := time.Now()
	err := s.putReplica(stream)
	s.record("put_replica", err, time.Since(start))
	return err
}

func (s *Server) putReplica(stream edgemeshv1.PeerCache_PutReplicaServer) error {
	var (
		header *edgemeshv1.ReplicaHeader
		body   []byte
	)
	maxBytes := s.owner.MaxObjectBytes()

	for {
		chunk, err := stream.Recv()
		if err != nil {
			// A clean EOF ends the stream; anything else is a transport failure.
			if errors.Is(err, io.EOF) {
				break
			}
			return errs.Wrap(errs.ClassPeerFailure, err, "receive replica chunk")
		}
		if h := chunk.GetHeader(); h != nil {
			header = h
			if n := h.GetMetadata().GetContentLength(); n > 0 {
				if n > maxBytes {
					return stream.SendAndClose(&edgemeshv1.PutReplicaResponse{
						Stored: false,
						Reason: "object exceeds this node's maximum object size",
					})
				}
				body = make([]byte, 0, n)
			}
			continue
		}
		// The size bound is enforced while receiving, not only from the
		// declared length: a peer that lies about content length must not be
		// able to make this node buffer without limit.
		if uint64(len(body))+uint64(len(chunk.GetBody())) > maxBytes {
			return stream.SendAndClose(&edgemeshv1.PutReplicaResponse{
				Stored: false,
				Reason: "replica body exceeded the declared maximum object size",
			})
		}
		body = append(body, chunk.GetBody()...)
	}

	if header == nil {
		return errs.New(errs.ClassProtocol, "replica stream carried no header")
	}
	obj := objectFromProto(header.GetCacheKey(), header.GetRouteId(), header.GetMetadata(), body)
	if err := s.owner.StoreReplica(obj); err != nil {
		return stream.SendAndClose(&edgemeshv1.PutReplicaResponse{
			Stored: false, Reason: err.Error(),
		})
	}
	return stream.SendAndClose(&edgemeshv1.PutReplicaResponse{Stored: true})
}

// Purge drops matching objects from the local tier.
func (s *Server) Purge(_ context.Context, req *edgemeshv1.PurgeRequest) (*edgemeshv1.PurgeResponse, error) {
	start := time.Now()
	n := s.owner.Purge(req.GetScope(), req.GetRouteId(), req.GetCacheKey(), req.GetVersion())
	s.record("purge", nil, time.Since(start))
	return &edgemeshv1.PurgeResponse{PurgedObjects: n}, nil
}

// Health answers a peer's liveness probe.
func (s *Server) Health(_ context.Context, req *edgemeshv1.PeerHealthRequest) (*edgemeshv1.PeerHealthResponse, error) {
	objects, bytes := s.owner.Stats()
	return &edgemeshv1.PeerHealthResponse{
		NodeId:        s.owner.NodeID(),
		Ready:         s.owner.Ready(),
		ConfigVersion: s.owner.ConfigVersion(),
		CacheObjects:  objects,
		CacheBytes:    bytes,
	}, nil
}

func (s *Server) record(op string, err error, d time.Duration) {
	if s.OnRequest == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = string(errs.ClassOf(err))
	}
	s.OnRequest(op, result, d)
}

// ObjectToHTTP renders a cached object into an http.ResponseWriter. It lives
// here so both the proxy and any peer-facing debug surface format a cached
// response identically.
func ObjectToHTTP(w http.ResponseWriter, obj *cache.Object, now time.Time) (int64, error) {
	for name, values := range obj.Header {
		for _, v := range values {
			w.Header().Add(name, v)
		}
	}
	// Age is recomputed on every serve: a cached response must tell the client
	// how stale it is, and the stored value was correct only when it was stored.
	w.Header().Set("Age", formatSeconds(obj.Age(now)))
	w.WriteHeader(obj.Status)
	n, err := w.Write(obj.Body)
	return int64(n), err
}

func formatSeconds(d time.Duration) string {
	s := int64(d.Seconds())
	if s < 0 {
		s = 0
	}
	return itoa(s)
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
