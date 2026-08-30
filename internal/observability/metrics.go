package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds every EdgeMesh collector.
//
// Cardinality discipline is deliberate: no metric label ever carries a raw URL,
// request ID, cache key, or client-supplied hostname. Route and origin IDs are
// administrator-defined and therefore bounded; status is bucketed into a class
// rather than an exact code. A metrics endpoint that a hostile request can
// grow without bound is a denial-of-service surface, not observability.
type Metrics struct {
	reg *prometheus.Registry

	// ---- Edge data plane ----
	HTTPRequests          *prometheus.CounterVec   // route, method, status_class
	HTTPDuration          *prometheus.HistogramVec // route
	HTTPInflight          *prometheus.GaugeVec     // route
	HTTPBodyBytes         *prometheus.HistogramVec // route, direction
	OriginRequests        *prometheus.CounterVec   // origin, result
	OriginDuration        *prometheus.HistogramVec // origin
	OriginRetries         *prometheus.CounterVec   // origin, reason
	OriginHealth          *prometheus.GaugeVec     // pool, origin
	CircuitTransitions    *prometheus.CounterVec   // origin, to_state
	CircuitState          *prometheus.GaugeVec     // origin
	RateLimitDenied       *prometheus.CounterVec   // route
	CacheRequests         *prometheus.CounterVec   // tier, outcome
	CacheObjects          *prometheus.GaugeVec     // tier
	CacheBytes            *prometheus.GaugeVec     // tier
	CacheEvictions        *prometheus.CounterVec   // tier, reason
	CacheAdmissions       *prometheus.CounterVec   // tier, reason
	CacheFillDuration     prometheus.Histogram
	PeerRequests          *prometheus.CounterVec   // operation, result
	PeerDuration          *prometheus.HistogramVec // operation
	ReplicationQueued     prometheus.Gauge
	ReplicationDropped    *prometheus.CounterVec // reason
	CoalescedWaiters      prometheus.Gauge
	CoalescedFills        prometheus.Counter
	ConfigVersionApplied  prometheus.Gauge
	ControlPlaneConnected prometheus.Gauge
	RingNodes             prometheus.Gauge
	RingVersion           prometheus.Gauge

	// ---- Control plane ----
	RaftRole              *prometheus.GaugeVec // role
	RaftTerm              prometheus.Gauge
	RaftCommitIndex       prometheus.Gauge
	RaftLastApplied       prometheus.Gauge
	RaftLogEntries        prometheus.Gauge
	RaftSnapshotIndex     prometheus.Gauge
	RaftElections         prometheus.Counter
	RaftLeadershipChanges prometheus.Counter
	RaftAppendEntries     *prometheus.CounterVec // result
	RaftRequestVote       *prometheus.CounterVec // result
	RaftInstallSnapshots  *prometheus.CounterVec // result
	RaftReplicationLag    *prometheus.GaugeVec   // peer
	RaftApplyDuration     prometheus.Histogram
	RaftProposeDuration   *prometheus.HistogramVec // result
	ConfigVersion         prometheus.Gauge
	EdgeNodes             *prometheus.GaugeVec // state
	ConfigStreamClients   prometheus.Gauge
	AdminRequests         *prometheus.CounterVec // endpoint, result
}

// latencyBuckets spans sub-millisecond cache hits through multi-second origin
// timeouts. Default Prometheus buckets start at 5ms, which would put every L1
// hit in one bucket and make p50 meaningless.
var latencyBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
}

// sizeBuckets spans a small JSON response through the 8 MiB object cap.
var sizeBuckets = prometheus.ExponentialBuckets(256, 4, 9)

// NewMetrics registers every collector on a fresh registry.
//
// A dedicated registry rather than the default one keeps a test's metrics
// isolated and stops a transitively imported library from publishing into the
// EdgeMesh namespace by accident.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg}
	f := promauto{reg}

	const ns = "edgemesh"

	m.HTTPRequests = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "http_requests_total",
		Help: "Public HTTP requests handled by the edge proxy.",
	}, []string{"route", "method", "status_class"})

	m.HTTPDuration = f.histogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "http_request_duration_seconds",
		Help: "End-to-end public request duration.", Buckets: latencyBuckets,
	}, []string{"route"})

	m.HTTPInflight = f.gaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "http_inflight_requests",
		Help: "Public requests currently in flight.",
	}, []string{"route"})

	m.HTTPBodyBytes = f.histogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "http_body_bytes",
		Help: "Body sizes observed on the public listener.", Buckets: sizeBuckets,
	}, []string{"route", "direction"})

	m.OriginRequests = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "origin_requests_total",
		Help: "Requests forwarded to an origin.",
	}, []string{"origin", "result"})

	m.OriginDuration = f.histogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "origin_request_duration_seconds",
		Help: "Origin request duration.", Buckets: latencyBuckets,
	}, []string{"origin"})

	m.OriginRetries = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "origin_retries_total",
		Help: "Origin retries by reason.",
	}, []string{"origin", "reason"})

	m.OriginHealth = f.gaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "origin_healthy",
		Help: "1 when an origin passes health checks, 0 otherwise.",
	}, []string{"pool", "origin"})

	m.CircuitTransitions = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "circuit_breaker_transitions_total",
		Help: "Circuit breaker state transitions.",
	}, []string{"origin", "to_state"})

	m.CircuitState = f.gaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "circuit_breaker_state",
		Help: "Circuit breaker state: 0 closed, 1 half_open, 2 open.",
	}, []string{"origin"})

	m.RateLimitDenied = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "rate_limit_denied_total",
		Help: "Requests denied by the node-local rate limiter.",
	}, []string{"route"})

	m.CacheRequests = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "cache_requests_total",
		Help: "Cache lookups by tier and outcome.",
	}, []string{"tier", "outcome"})

	m.CacheObjects = f.gaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "cache_objects",
		Help: "Objects resident in a cache tier.",
	}, []string{"tier"})

	m.CacheBytes = f.gaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "cache_bytes",
		Help: "Bytes accounted in a cache tier.",
	}, []string{"tier"})

	m.CacheEvictions = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "cache_evictions_total",
		Help: "Objects removed from a cache tier by reason.",
	}, []string{"tier", "reason"})

	m.CacheAdmissions = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "cache_admissions_total",
		Help: "Cache admission decisions by reason.",
	}, []string{"tier", "reason"})

	m.CacheFillDuration = f.histogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "cache_fill_duration_seconds",
		Help: "Time to fill a cache entry from origin.", Buckets: latencyBuckets,
	})

	m.PeerRequests = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "peer_requests_total",
		Help: "Peer-cache RPCs by operation and result.",
	}, []string{"operation", "result"})

	m.PeerDuration = f.histogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "peer_request_duration_seconds",
		Help: "Peer-cache RPC duration.", Buckets: latencyBuckets,
	}, []string{"operation"})

	m.ReplicationQueued = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "replication_queue_depth",
		Help: "Asynchronous replica writes waiting for a worker.",
	})

	m.ReplicationDropped = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "replication_dropped_total",
		Help: "Replica writes dropped rather than blocking a client response.",
	}, []string{"reason"})

	m.CoalescedWaiters = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "coalesced_waiters",
		Help: "Requests currently waiting on an in-flight cache fill.",
	})

	m.CoalescedFills = f.counter(prometheus.CounterOpts{
		Namespace: ns, Name: "coalesced_fills_total",
		Help: "Origin fills performed on behalf of coalesced waiters.",
	})

	m.ConfigVersionApplied = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "config_version_applied",
		Help: "Configuration version currently applied on this edge.",
	})

	m.ControlPlaneConnected = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "control_plane_connected",
		Help: "1 when the edge holds a live config stream, 0 otherwise.",
	})

	m.RingNodes = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "ring_nodes",
		Help: "Edge nodes on the consistent hash ring.",
	})

	m.RingVersion = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "ring_version",
		Help: "Membership version of the current ring snapshot.",
	})

	m.RaftRole = f.gaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "raft_role",
		Help: "1 for the node's current Raft role, 0 for the others.",
	}, []string{"role"})

	m.RaftTerm = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "raft_term", Help: "Current Raft term."})
	m.RaftCommitIndex = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "raft_commit_index", Help: "Highest committed log index."})
	m.RaftLastApplied = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "raft_last_applied", Help: "Highest log index applied to the state machine."})
	m.RaftLogEntries = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "raft_log_entries", Help: "Entries retained in the Raft log."})
	m.RaftSnapshotIndex = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "raft_snapshot_index", Help: "Last index included in the newest snapshot."})
	m.RaftElections = f.counter(prometheus.CounterOpts{
		Namespace: ns, Name: "raft_elections_total", Help: "Elections this node has started."})
	m.RaftLeadershipChanges = f.counter(prometheus.CounterOpts{
		Namespace: ns, Name: "raft_leadership_changes_total", Help: "Times this node gained or lost leadership."})

	m.RaftAppendEntries = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "raft_append_entries_total", Help: "AppendEntries RPCs by result.",
	}, []string{"result"})
	m.RaftRequestVote = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "raft_request_vote_total", Help: "RequestVote RPCs by result.",
	}, []string{"result"})
	m.RaftInstallSnapshots = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "raft_install_snapshot_total", Help: "InstallSnapshot RPCs by result.",
	}, []string{"result"})

	m.RaftReplicationLag = f.gaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "raft_replication_lag_entries",
		Help: "Entries by which a follower trails the leader.",
	}, []string{"peer"})

	m.RaftApplyDuration = f.histogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "raft_apply_duration_seconds",
		Help: "State machine apply duration.", Buckets: latencyBuckets,
	})

	m.RaftProposeDuration = f.histogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "raft_propose_duration_seconds",
		Help: "Time from proposal to commit acknowledgement.", Buckets: latencyBuckets,
	}, []string{"result"})

	m.ConfigVersion = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "config_version",
		Help: "Replicated configuration version on this control node.",
	})

	m.EdgeNodes = f.gaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "edge_nodes",
		Help: "Registered edge nodes by liveness state.",
	}, []string{"state"})

	m.ConfigStreamClients = f.gauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "config_stream_clients",
		Help: "Edges holding an open config stream on this node.",
	})

	m.AdminRequests = f.counterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "admin_requests_total",
		Help: "Admin API requests by endpoint and result.",
	}, []string{"endpoint", "result"})

	// Go runtime and process collectors give the Grafana runtime row its data.
	reg.MustRegister(newGoCollector(), newProcessCollector())
	return m
}

// Registry exposes the registry for the /metrics handler and for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// StatusClass buckets an HTTP status into a low-cardinality label. Using the
// exact code would multiply every request series by the number of statuses an
// origin can emit.
func StatusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	case status >= 100:
		return "1xx"
	default:
		return "unknown"
	}
}

// promauto is a tiny local helper that registers and returns a collector,
// panicking on duplicate registration. A duplicate is a programming error
// caught on the first run, not a runtime condition to handle.
type promauto struct{ reg *prometheus.Registry }

func (p promauto) counter(o prometheus.CounterOpts) prometheus.Counter {
	c := prometheus.NewCounter(o)
	p.reg.MustRegister(c)
	return c
}

func (p promauto) counterVec(o prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(o, labels)
	p.reg.MustRegister(c)
	return c
}

func (p promauto) gauge(o prometheus.GaugeOpts) prometheus.Gauge {
	g := prometheus.NewGauge(o)
	p.reg.MustRegister(g)
	return g
}

func (p promauto) gaugeVec(o prometheus.GaugeOpts, labels []string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(o, labels)
	p.reg.MustRegister(g)
	return g
}

func (p promauto) histogram(o prometheus.HistogramOpts) prometheus.Histogram {
	h := prometheus.NewHistogram(o)
	p.reg.MustRegister(h)
	return h
}

func (p promauto) histogramVec(o prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(o, labels)
	p.reg.MustRegister(h)
	return h
}
