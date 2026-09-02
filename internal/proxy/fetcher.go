package proxy

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/cache/policy"
	"github.com/sheehanlloyd/edgemesh/internal/clock"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/peer"
	"github.com/sheehanlloyd/edgemesh/internal/routing"
)

// Fetcher fills cache objects from origin on behalf of the L2 coordinator.
//
// It exists so the coordinator never needs to know about routes, retries, or
// header policy, and so admission decisions are made in exactly one place,
// here, rather than being duplicated between the local fill path and the
// peer-served fill path.
type Fetcher struct {
	routes   *routing.Holder
	pools    PoolResolver
	origin   *OriginClient
	trusted  *TrustedProxies
	clk      clock.Clock
	log      *slog.Logger
	nodeID   string
	maxBytes uint64

	// OnAdmission records an admission decision for metrics.
	OnAdmission func(tier cache.Tier, reason string)
}

// FetcherOptions configures a Fetcher.
type FetcherOptions struct {
	NodeID         string
	Routes         *routing.Holder
	Pools          PoolResolver
	Origin         *OriginClient
	Trusted        *TrustedProxies
	Clock          clock.Clock
	Logger         *slog.Logger
	MaxObjectBytes uint64
	OnAdmission    func(tier cache.Tier, reason string)
}

// NewFetcher builds an origin fetcher.
func NewFetcher(o FetcherOptions) (*Fetcher, error) {
	if o.Routes == nil {
		return nil, errs.New(errs.ClassValidation, "fetcher: a route holder is required")
	}
	if o.Pools == nil {
		return nil, errs.New(errs.ClassValidation, "fetcher: a pool resolver is required")
	}
	if o.Origin == nil {
		return nil, errs.New(errs.ClassValidation, "fetcher: an origin client is required")
	}
	if o.Clock == nil {
		o.Clock = clock.New()
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.MaxObjectBytes == 0 {
		o.MaxObjectBytes = 8 << 20
	}
	return &Fetcher{
		routes:      o.Routes,
		pools:       o.Pools,
		origin:      o.Origin,
		trusted:     o.Trusted,
		clk:         o.Clock,
		log:         o.Logger,
		nodeID:      o.NodeID,
		maxBytes:    o.MaxObjectBytes,
		OnAdmission: o.OnAdmission,
	}, nil
}

// Fetch performs an origin request and returns a cache object.
//
// The returned object carries an expiry only when the response is admissible.
// A non-admissible response comes back with a zero expiry, which tells the
// coordinator to serve it without storing it. Signalling admissibility this way
// keeps a single source of truth for the policy decision.
func (f *Fetcher) Fetch(ctx context.Context, req peer.FetchRequest) (*cache.Object, error) {
	route, ok := f.routes.Load().ByID(req.RouteID)
	if !ok {
		return nil, errs.New(errs.ClassNotFound,
			"route %q is not present in this edge's configuration", req.RouteID)
	}
	pool, ok := f.pools.Pool(route.GetOriginPoolId())
	if !ok {
		return nil, errs.New(errs.ClassUnavailable,
			"origin pool %q referenced by route %q is not configured",
			route.GetOriginPoolId(), req.RouteID)
	}

	header := req.Header.Clone()
	if header == nil {
		header = http.Header{}
	}
	stripHopByHop(header)

	resp, err := f.origin.Fetch(ctx, FetchOptions{
		Pool:         pool,
		Route:        route,
		Method:       req.Method,
		Path:         req.Path,
		RawQuery:     req.RawQuery,
		Header:       header,
		ClientHost:   req.ClientHost,
		MaxBodyBytes: int64(f.maxBytes),
	})
	if err != nil {
		return nil, err
	}

	now := f.clk.Now()
	size := int64(len(resp.Body))
	if resp.Truncated {
		// A body that exceeded the cap cannot be cached: storing the truncated
		// prefix would serve a corrupt response to every later requester.
		f.recordAdmission(string(policy.ReasonTooLarge))
		return &cache.Object{
			Key: req.CacheKey, RouteID: req.RouteID,
			Status: resp.Status, Header: resp.Header, Body: resp.Body,
			StoredAt: now, OriginNodeID: f.nodeID,
		}, nil
	}

	decision := policy.EvaluateResponse(resp.Status, resp.Header, size, f.maxBytes, route.GetCachePolicy(), now)
	f.recordAdmission(string(decision.Reason))

	obj := &cache.Object{
		Key:          req.CacheKey,
		RouteID:      req.RouteID,
		Status:       resp.Status,
		Header:       resp.Header,
		Body:         resp.Body,
		StoredAt:     now,
		OriginNodeID: f.nodeID,
		Negative:     decision.Negative,
	}
	if !decision.Admit {
		// A zero expiry means "serve but do not store".
		return obj, nil
	}
	obj.ExpiresAt = now.Add(decision.TTL)
	obj.StaleUntil = obj.ExpiresAt.Add(decision.StaleWhileRevalidate)
	obj.Vary = decision.Vary
	return obj, nil
}

func (f *Fetcher) recordAdmission(reason string) {
	if f.OnAdmission != nil {
		f.OnAdmission(cache.TierL2, reason)
	}
}
