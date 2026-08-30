// Package observability bootstraps structured logging, Prometheus metrics, and
// OpenTelemetry tracing for every EdgeMesh process.
//
// Observability is treated as part of the design rather than an afterthought:
// each subsystem gets metrics, logs, and spans alongside its code, and this
// package owns the shared conventions so the field names stay consistent
// across the control plane and the data plane.
package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// Field names shared by every EdgeMesh log record. They are constants so a
// typo cannot silently split a field across two names in a dashboard query.
const (
	FieldService       = "service"
	FieldNodeID        = "node_id"
	FieldRegion        = "region"
	FieldZone          = "zone"
	FieldRequestID     = "request_id"
	FieldTraceID       = "trace_id"
	FieldSpanID        = "span_id"
	FieldRouteID       = "route_id"
	FieldOriginID      = "origin_id"
	FieldPeerID        = "peer_id"
	FieldRaftTerm      = "raft_term"
	FieldRaftRole      = "raft_role"
	FieldRaftIndex     = "raft_index"
	FieldConfigVersion = "config_version"
	FieldErrorClass    = "error_class"
	FieldCacheOutcome  = "cache_outcome"
	FieldStatus        = "status"
	FieldDurationMS    = "duration_ms"
)

// redactedHeaders never reach a log record. Logging any of these would put a
// credential in a log aggregator, which is the most common way secrets leak.
var redactedHeaders = map[string]struct{}{
	"authorization":       {},
	"proxy-authorization": {},
	"cookie":              {},
	"set-cookie":          {},
	"x-api-key":           {},
	"x-edgemesh-token":    {},
}

// RedactHeader returns a loggable value for a header, replacing the value of
// any sensitive header with a fixed marker.
func RedactHeader(name, value string) string {
	if _, ok := redactedHeaders[strings.ToLower(name)]; ok {
		return "[redacted]"
	}
	return value
}

// LoggerOptions configures the process logger.
type LoggerOptions struct {
	Level   string // debug | info | warn | error
	Format  string // json | text
	Service string
	NodeID  string
	Region  string
	Zone    string
}

// NewLogger builds the process logger with the standard identity fields
// pre-attached, so every record carries them without a per-call-site reminder.
func NewLogger(o LoggerOptions) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(o.Level) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler
	if strings.EqualFold(o.Format, "text") {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}

	attrs := []slog.Attr{slog.String(FieldService, o.Service)}
	if o.NodeID != "" {
		attrs = append(attrs, slog.String(FieldNodeID, o.NodeID))
	}
	if o.Region != "" {
		attrs = append(attrs, slog.String(FieldRegion, o.Region))
	}
	if o.Zone != "" {
		attrs = append(attrs, slog.String(FieldZone, o.Zone))
	}
	return slog.New(h.WithAttrs(attrs))
}

// WithTrace attaches the active trace and span IDs to a logger so a log record
// can be correlated with the trace that produced it. It is a no-op when the
// context carries no recording span.
func WithTrace(ctx context.Context, l *slog.Logger) *slog.Logger {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return l
	}
	return l.With(
		slog.String(FieldTraceID, sc.TraceID().String()),
		slog.String(FieldSpanID, sc.SpanID().String()),
	)
}
