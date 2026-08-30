package observability

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// Span attribute keys. Custom attributes are bounded and named consistently so
// a trace backend can group them; anything unbounded (URLs, cache keys) is
// deliberately absent.
const (
	AttrRouteID       = attribute.Key("edgemesh.route_id")
	AttrNodeID        = attribute.Key("edgemesh.node_id")
	AttrPeerID        = attribute.Key("edgemesh.peer_id")
	AttrOriginID      = attribute.Key("edgemesh.origin_id")
	AttrCacheTier     = attribute.Key("edgemesh.cache.tier")
	AttrCacheOutcome  = attribute.Key("edgemesh.cache.outcome")
	AttrCacheKeyHash  = attribute.Key("edgemesh.cache.key_hash")
	AttrConfigVersion = attribute.Key("edgemesh.config_version")
	AttrRaftTerm      = attribute.Key("edgemesh.raft.term")
	AttrRaftIndex     = attribute.Key("edgemesh.raft.index")
	AttrRaftRole      = attribute.Key("edgemesh.raft.role")
	AttrErrorClass    = attribute.Key("edgemesh.error_class")
	AttrDegraded      = attribute.Key("edgemesh.degraded")
	AttrCoalesced     = attribute.Key("edgemesh.coalesced")
	AttrRetryCount    = attribute.Key("edgemesh.retry_count")
)

// TracingOptions configures the tracer provider.
type TracingOptions struct {
	ServiceName    string
	ServiceVersion string
	Namespace      string
	NodeID         string
	Region         string
	Zone           string
	// OTLPEndpoint is the collector target. Empty installs a no-op exporter so
	// instrumentation still runs and can be exercised by tests without a
	// collector present.
	OTLPEndpoint string
	Insecure     bool
	SampleRatio  float64
}

// Tracing owns the tracer provider's lifecycle.
type Tracing struct {
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
}

// InitTracing installs the global tracer provider and W3C propagators.
//
// The propagator set is trace context plus baggage, which is what makes a trace
// span the ingress edge, a peer-cache RPC, and the origin request as one trace
// rather than three disconnected ones.
func InitTracing(ctx context.Context, o TracingOptions) (*Tracing, error) {
	if o.SampleRatio <= 0 {
		o.SampleRatio = 1
	}
	attrs := []attribute.KeyValue{
		semconv.ServiceName(o.ServiceName),
		semconv.ServiceVersion(o.ServiceVersion),
		semconv.ServiceNamespace(o.Namespace),
	}
	if o.NodeID != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(o.NodeID), AttrNodeID.String(o.NodeID))
	}
	if o.Region != "" {
		attrs = append(attrs, attribute.String("edgemesh.region", o.Region))
	}
	if o.Zone != "" {
		attrs = append(attrs, attribute.String("edgemesh.zone", o.Zone))
	}

	res, err := sdkresource.Merge(sdkresource.Default(), sdkresource.NewSchemaless(attrs...))
	if err != nil {
		return nil, errs.Wrap(errs.ClassValidation, err, "build telemetry resource")
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		// ParentBased keeps a sampling decision consistent across a whole
		// trace: a sampled ingress request stays sampled through the peer and
		// origin spans instead of producing a trace with holes in it.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(o.SampleRatio))),
	}

	if o.OTLPEndpoint != "" {
		expOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(o.OTLPEndpoint)}
		if o.Insecure {
			expOpts = append(expOpts, otlptracegrpc.WithInsecure())
		}
		exp, err := otlptracegrpc.New(ctx, expOpts...)
		if err != nil {
			return nil, errs.Wrap(errs.ClassUnavailable, err, "create OTLP trace exporter for %q", o.OTLPEndpoint)
		}
		// Batching is what keeps export off the request path.
		opts = append(opts, sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithMaxQueueSize(4096),
			sdktrace.WithMaxExportBatchSize(512),
		))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Tracing{provider: tp, tracer: tp.Tracer("github.com/sheehanlloyd/edgemesh")}, nil
}

// Tracer returns the process tracer.
func (t *Tracing) Tracer() trace.Tracer {
	if t == nil {
		return otel.Tracer("github.com/sheehanlloyd/edgemesh")
	}
	return t.tracer
}

// Shutdown flushes pending spans on a best-effort basis. Telemetry loss during
// shutdown must never block process exit, so the caller supplies a bounded
// context.
func (t *Tracing) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	if err := t.provider.Shutdown(ctx); err != nil {
		return errs.Wrap(errs.ClassUnavailable, err, "shut down tracer provider")
	}
	return nil
}

// RecordError annotates a span with a classified error. Recording the class as
// a bounded attribute is what makes traces filterable by failure mode without
// putting free-text messages into attribute cardinality.
func RecordError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetAttributes(AttrErrorClass.String(string(errs.ClassOf(err))))
}
