// Package config loads and validates bootstrap configuration.
//
// Bootstrap configuration is per-process local state: identity, listen
// addresses, storage paths, and TLS material. It is deliberately distinct from
// the *replicated* application configuration (routes, origin pools, policies)
// that lives in the Raft state machine. Nothing in this package is replicated.
//
// Every configuration is validated at startup and reports actionable errors:
// a process must never begin serving with a half-valid configuration.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// Mode selects the security posture of a process. Development relaxes
// SSRF-protection and admin-auth requirements so a laptop demo works;
// production fails closed.
type Mode string

const (
	ModeDevelopment Mode = "development"
	ModeProduction  Mode = "production"
)

// Valid reports whether m is a recognized mode.
func (m Mode) Valid() bool { return m == ModeDevelopment || m == ModeProduction }

// TLS holds mTLS material shared by every internal gRPC surface.
type TLS struct {
	Enabled  bool   `yaml:"enabled"`
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// ServerName overrides the name verified against a peer certificate. Left
	// empty the dial target's host is used.
	ServerName string `yaml:"server_name"`
	// SkipVerify is refused in production mode. It exists only so that a
	// developer can debug a certificate problem locally.
	SkipVerify bool `yaml:"skip_verify"`
}

func (t TLS) validate(mode Mode, field string) error {
	if !t.Enabled {
		if mode == ModeProduction {
			return errs.New(errs.ClassValidation, "%s.enabled must be true in production mode", field)
		}
		return nil
	}
	for name, path := range map[string]string{
		field + ".ca_file":   t.CAFile,
		field + ".cert_file": t.CertFile,
		field + ".key_file":  t.KeyFile,
	} {
		if strings.TrimSpace(path) == "" {
			return errs.New(errs.ClassValidation, "%s is required when TLS is enabled", name)
		}
	}
	if t.SkipVerify && mode == ModeProduction {
		return errs.New(errs.ClassValidation, "%s.skip_verify is forbidden in production mode", field)
	}
	return nil
}

// Telemetry configures the observability bootstrap for any process.
type Telemetry struct {
	// Address serves /metrics, /healthz and /readyz. Required.
	Address string `yaml:"address"`
	// OTLPEndpoint is the OpenTelemetry collector target. Empty disables trace
	// export; the tracer still runs so instrumentation code paths stay live.
	OTLPEndpoint string `yaml:"otlp_endpoint"`
	// OTLPInsecure permits a plaintext OTLP connection (local collector).
	OTLPInsecure bool `yaml:"otlp_insecure"`
	// TraceSampleRatio in [0,1]. 1 traces everything; use lower under load.
	TraceSampleRatio float64 `yaml:"trace_sample_ratio"`
	LogLevel         string  `yaml:"log_level"`
	LogFormat        string  `yaml:"log_format"` // json | text
	// ServiceNamespace tags exported telemetry.
	ServiceNamespace string `yaml:"service_namespace"`
	// EnablePprof exposes net/http/pprof on the telemetry listener. Refused in
	// production mode: the telemetry listener is not authenticated.
	EnablePprof bool `yaml:"enable_pprof"`
}

func (t *Telemetry) applyDefaults() {
	if t.TraceSampleRatio == 0 {
		t.TraceSampleRatio = 1
	}
	if t.LogLevel == "" {
		t.LogLevel = "info"
	}
	if t.LogFormat == "" {
		t.LogFormat = "json"
	}
	if t.ServiceNamespace == "" {
		t.ServiceNamespace = "edgemesh"
	}
}

func (t Telemetry) validate(mode Mode) error {
	if err := validateListenAddress("telemetry.address", t.Address); err != nil {
		return err
	}
	if t.TraceSampleRatio < 0 || t.TraceSampleRatio > 1 {
		return errs.New(errs.ClassValidation, "telemetry.trace_sample_ratio must be within [0,1], got %v", t.TraceSampleRatio)
	}
	switch t.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return errs.New(errs.ClassValidation, "telemetry.log_level must be one of debug|info|warn|error, got %q", t.LogLevel)
	}
	switch t.LogFormat {
	case "json", "text":
	default:
		return errs.New(errs.ClassValidation, "telemetry.log_format must be json or text, got %q", t.LogFormat)
	}
	if t.EnablePprof && mode == ModeProduction {
		return errs.New(errs.ClassValidation, "telemetry.enable_pprof is forbidden in production mode")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Control-plane configuration
// ---------------------------------------------------------------------------

// RaftPeer identifies one statically configured control-plane member.
type RaftPeer struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

// RaftConfig holds consensus timing and storage settings.
//
// The heartbeat interval must sit comfortably below the minimum election
// timeout, otherwise followers time out during healthy operation and the
// cluster churns leaders.
type RaftConfig struct {
	Peers              []RaftPeer    `yaml:"peers"`
	ElectionTimeoutMin time.Duration `yaml:"election_timeout_min"`
	ElectionTimeoutMax time.Duration `yaml:"election_timeout_max"`
	HeartbeatInterval  time.Duration `yaml:"heartbeat_interval"`
	// SnapshotEntries triggers a snapshot once the log grows this many entries
	// beyond the last snapshot. Zero disables count-based snapshotting.
	SnapshotEntries uint64 `yaml:"snapshot_entries"`
	// SnapshotBytes triggers a snapshot once the log exceeds this many bytes.
	SnapshotBytes uint64 `yaml:"snapshot_bytes"`
	DataDir       string `yaml:"data_dir"`
	// MaxEntriesPerAppend bounds a single AppendEntries payload.
	MaxEntriesPerAppend int `yaml:"max_entries_per_append"`
	// SnapshotChunkBytes bounds one InstallSnapshot stream chunk.
	SnapshotChunkBytes int `yaml:"snapshot_chunk_bytes"`
	// RPCTimeout bounds a single outbound Raft RPC.
	RPCTimeout time.Duration `yaml:"rpc_timeout"`
	// PreVote runs an extra probing round before incrementing the term, which
	// prevents a partitioned node from disrupting a healthy leader on rejoin.
	PreVote bool `yaml:"pre_vote"`
}

func (r *RaftConfig) applyDefaults() {
	if r.ElectionTimeoutMin == 0 {
		r.ElectionTimeoutMin = 500 * time.Millisecond
	}
	if r.ElectionTimeoutMax == 0 {
		r.ElectionTimeoutMax = 900 * time.Millisecond
	}
	if r.HeartbeatInterval == 0 {
		r.HeartbeatInterval = 150 * time.Millisecond
	}
	if r.SnapshotEntries == 0 {
		r.SnapshotEntries = 1000
	}
	if r.SnapshotBytes == 0 {
		r.SnapshotBytes = 8 << 20
	}
	if r.DataDir == "" {
		r.DataDir = "/var/lib/edgemesh"
	}
	if r.MaxEntriesPerAppend == 0 {
		r.MaxEntriesPerAppend = 64
	}
	if r.SnapshotChunkBytes == 0 {
		r.SnapshotChunkBytes = 256 << 10
	}
	if r.RPCTimeout == 0 {
		r.RPCTimeout = 2 * time.Second
	}
}

func (r RaftConfig) validate(selfID string) error {
	if len(r.Peers) < 3 {
		return errs.New(errs.ClassValidation, "raft.peers must contain at least 3 members, got %d", len(r.Peers))
	}
	if len(r.Peers)%2 == 0 {
		return errs.New(errs.ClassValidation, "raft.peers must be an odd count to form a majority, got %d", len(r.Peers))
	}
	seenID := make(map[string]bool, len(r.Peers))
	seenAddr := make(map[string]bool, len(r.Peers))
	foundSelf := false
	for i, p := range r.Peers {
		if strings.TrimSpace(p.ID) == "" {
			return errs.New(errs.ClassValidation, "raft.peers[%d].id is required", i)
		}
		if seenID[p.ID] {
			return errs.New(errs.ClassValidation, "raft.peers contains duplicate id %q", p.ID)
		}
		seenID[p.ID] = true
		if err := validateDialAddress(fmt.Sprintf("raft.peers[%d].address", i), p.Address); err != nil {
			return err
		}
		if seenAddr[p.Address] {
			return errs.New(errs.ClassValidation, "raft.peers contains duplicate address %q", p.Address)
		}
		seenAddr[p.Address] = true
		if p.ID == selfID {
			foundSelf = true
		}
	}
	if !foundSelf {
		return errs.New(errs.ClassValidation, "raft.peers must include this node's id %q", selfID)
	}
	if r.ElectionTimeoutMin <= 0 || r.ElectionTimeoutMax <= 0 || r.HeartbeatInterval <= 0 {
		return errs.New(errs.ClassValidation, "raft timers must be positive durations")
	}
	if r.ElectionTimeoutMin >= r.ElectionTimeoutMax {
		return errs.New(errs.ClassValidation,
			"raft.election_timeout_min (%s) must be strictly less than election_timeout_max (%s); "+
				"an identical timeout on every node causes repeated split votes",
			r.ElectionTimeoutMin, r.ElectionTimeoutMax)
	}
	// Raft safety needs broadcastTime << electionTimeout. A 3x margin keeps a
	// healthy leader from being displaced by ordinary scheduling jitter.
	if r.HeartbeatInterval*3 > r.ElectionTimeoutMin {
		return errs.New(errs.ClassValidation,
			"raft.heartbeat_interval (%s) must be at most one third of election_timeout_min (%s)",
			r.HeartbeatInterval, r.ElectionTimeoutMin)
	}
	if strings.TrimSpace(r.DataDir) == "" {
		return errs.New(errs.ClassValidation, "raft.data_dir is required")
	}
	if r.MaxEntriesPerAppend <= 0 {
		return errs.New(errs.ClassValidation, "raft.max_entries_per_append must be positive")
	}
	if r.SnapshotChunkBytes <= 0 {
		return errs.New(errs.ClassValidation, "raft.snapshot_chunk_bytes must be positive")
	}
	if r.RPCTimeout <= 0 {
		return errs.New(errs.ClassValidation, "raft.rpc_timeout must be positive")
	}
	return nil
}

// AdminAuth configures authentication on the admin HTTP API.
type AdminAuth struct {
	// Enabled must be true in production mode.
	Enabled bool `yaml:"enabled"`
	// TokenEnv names an environment variable holding the bearer token. Tokens
	// are never read from the configuration file itself so that configs stay
	// safe to commit.
	TokenEnv string `yaml:"token_env"`
	// TokenFile is an alternative source, e.g. a mounted Kubernetes secret.
	TokenFile string `yaml:"token_file"`
}

// ControlConfig is the bootstrap configuration of a control-plane process.
type ControlConfig struct {
	Mode Mode `yaml:"mode"`
	Node struct {
		ID string `yaml:"id"`
		// RaftAddress serves the inter-node consensus RPCs.
		RaftAddress string `yaml:"raft_address"`
		// AdminAddress serves the JSON admin API.
		AdminAddress string `yaml:"admin_address"`
		// EdgeAddress serves the edge config-stream/heartbeat gRPC API.
		EdgeAddress string `yaml:"edge_address"`
		// AdvertiseAdminAddress is what a leader hint points a client at. It
		// defaults to AdminAddress when that is not a wildcard bind.
		AdvertiseAdminAddress string `yaml:"advertise_admin_address"`
		// AdvertiseEdgeAddress is the edge-facing address published in leader
		// notices.
		AdvertiseEdgeAddress string `yaml:"advertise_edge_address"`
	} `yaml:"node"`
	Raft      RaftConfig `yaml:"raft"`
	Security  TLS        `yaml:"security"`
	AdminAuth AdminAuth  `yaml:"admin_auth"`
	Telemetry Telemetry  `yaml:"telemetry"`
	// Membership governs how edge liveness is tracked by the leader.
	Membership MembershipConfig `yaml:"membership"`
}

// MembershipConfig governs edge liveness tracking on the leader.
type MembershipConfig struct {
	// HeartbeatInterval is the cadence edges are told to use.
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	// MissedHeartbeats before a node is marked suspect.
	SuspectAfterMissed int `yaml:"suspect_after_missed"`
	// MissedHeartbeats before a node is removed from the ring entirely.
	DeadAfterMissed int `yaml:"dead_after_missed"`
	// SweepInterval is how often liveness is re-evaluated.
	SweepInterval time.Duration `yaml:"sweep_interval"`
}

func (m *MembershipConfig) applyDefaults() {
	if m.HeartbeatInterval == 0 {
		m.HeartbeatInterval = 2 * time.Second
	}
	if m.SuspectAfterMissed == 0 {
		m.SuspectAfterMissed = 2
	}
	if m.DeadAfterMissed == 0 {
		m.DeadAfterMissed = 3
	}
	if m.SweepInterval == 0 {
		m.SweepInterval = time.Second
	}
}

func (m MembershipConfig) validate() error {
	if m.HeartbeatInterval <= 0 || m.SweepInterval <= 0 {
		return errs.New(errs.ClassValidation, "membership intervals must be positive")
	}
	if m.SuspectAfterMissed < 1 {
		return errs.New(errs.ClassValidation, "membership.suspect_after_missed must be >= 1")
	}
	if m.DeadAfterMissed <= m.SuspectAfterMissed {
		return errs.New(errs.ClassValidation,
			"membership.dead_after_missed (%d) must exceed suspect_after_missed (%d)",
			m.DeadAfterMissed, m.SuspectAfterMissed)
	}
	return nil
}

func (c *ControlConfig) applyDefaults() {
	if c.Mode == "" {
		c.Mode = ModeDevelopment
	}
	c.Raft.applyDefaults()
	c.Telemetry.applyDefaults()
	c.Membership.applyDefaults()
	if c.Node.AdvertiseAdminAddress == "" {
		c.Node.AdvertiseAdminAddress = advertiseFor(c.Node.AdminAddress, c.Node.ID)
	}
	if c.Node.AdvertiseEdgeAddress == "" {
		c.Node.AdvertiseEdgeAddress = advertiseFor(c.Node.EdgeAddress, c.Node.ID)
	}
}

// Validate checks every invariant the process depends on. It is called by Load
// and is exported so tests and the CLI can validate a config without a file.
func (c *ControlConfig) Validate() error {
	if !c.Mode.Valid() {
		return errs.New(errs.ClassValidation, "mode must be development or production, got %q", c.Mode)
	}
	if err := validateNodeID("node.id", c.Node.ID); err != nil {
		return err
	}
	for field, addr := range map[string]string{
		"node.raft_address":  c.Node.RaftAddress,
		"node.admin_address": c.Node.AdminAddress,
		"node.edge_address":  c.Node.EdgeAddress,
	} {
		if err := validateListenAddress(field, addr); err != nil {
			return err
		}
	}
	if err := c.Raft.validate(c.Node.ID); err != nil {
		return err
	}
	if err := c.Security.validate(c.Mode, "security"); err != nil {
		return err
	}
	if err := c.Telemetry.validate(c.Mode); err != nil {
		return err
	}
	if err := c.Membership.validate(); err != nil {
		return err
	}
	if c.Mode == ModeProduction && !c.AdminAuth.Enabled {
		return errs.New(errs.ClassValidation, "admin_auth.enabled must be true in production mode")
	}
	if c.AdminAuth.Enabled && c.AdminAuth.TokenEnv == "" && c.AdminAuth.TokenFile == "" {
		return errs.New(errs.ClassValidation, "admin_auth requires token_env or token_file")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Edge configuration
// ---------------------------------------------------------------------------

// CacheConfig bounds both cache tiers on an edge node.
type CacheConfig struct {
	L1MaxBytes     uint64 `yaml:"l1_max_bytes"`
	L2MaxBytes     uint64 `yaml:"l2_max_bytes"`
	MaxObjectBytes uint64 `yaml:"max_object_bytes"`
	// Shards must be a power of two so shard selection is a mask, not a modulo.
	Shards int `yaml:"shards"`
	// ReplicationFactor is the owner plus successor count for an L2 object.
	ReplicationFactor int `yaml:"replication_factor"`
	// CleanupInterval drives bounded periodic expiry sweeps.
	CleanupInterval time.Duration `yaml:"cleanup_interval"`
	// Policy selects the eviction/admission policy: lru | tinylfu.
	Policy string `yaml:"policy"`
	// ReplicationQueue bounds outstanding asynchronous replica writes. When the
	// queue is full replica writes are dropped with a metric rather than
	// blocking a client response: cached data is derivative.
	ReplicationQueue int `yaml:"replication_queue"`
	// ReplicationWorkers bounds concurrent replica writes.
	ReplicationWorkers int `yaml:"replication_workers"`
	// ReplicationTimeout bounds one asynchronous replica write.
	ReplicationTimeout time.Duration `yaml:"replication_timeout"`
}

func (c *CacheConfig) applyDefaults() {
	if c.L1MaxBytes == 0 {
		c.L1MaxBytes = 256 << 20
	}
	if c.L2MaxBytes == 0 {
		c.L2MaxBytes = 512 << 20
	}
	if c.MaxObjectBytes == 0 {
		c.MaxObjectBytes = 8 << 20
	}
	if c.Shards == 0 {
		c.Shards = 64
	}
	if c.ReplicationFactor == 0 {
		c.ReplicationFactor = 2
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = 30 * time.Second
	}
	if c.Policy == "" {
		c.Policy = "lru"
	}
	if c.ReplicationQueue == 0 {
		c.ReplicationQueue = 1024
	}
	if c.ReplicationWorkers == 0 {
		c.ReplicationWorkers = 4
	}
	if c.ReplicationTimeout == 0 {
		c.ReplicationTimeout = 5 * time.Second
	}
}

func (c CacheConfig) validate() error {
	if c.L1MaxBytes == 0 || c.L2MaxBytes == 0 {
		return errs.New(errs.ClassValidation, "cache tier budgets must be positive")
	}
	if c.MaxObjectBytes == 0 {
		return errs.New(errs.ClassValidation, "cache.max_object_bytes must be positive")
	}
	if c.MaxObjectBytes > c.L1MaxBytes || c.MaxObjectBytes > c.L2MaxBytes {
		return errs.New(errs.ClassValidation,
			"cache.max_object_bytes (%d) must not exceed either tier budget (l1=%d l2=%d)",
			c.MaxObjectBytes, c.L1MaxBytes, c.L2MaxBytes)
	}
	if c.Shards <= 0 || c.Shards&(c.Shards-1) != 0 {
		return errs.New(errs.ClassValidation, "cache.shards must be a positive power of two, got %d", c.Shards)
	}
	if c.ReplicationFactor < 1 {
		return errs.New(errs.ClassValidation, "cache.replication_factor must be >= 1")
	}
	if c.CleanupInterval <= 0 {
		return errs.New(errs.ClassValidation, "cache.cleanup_interval must be positive")
	}
	switch c.Policy {
	case "lru", "tinylfu":
	default:
		return errs.New(errs.ClassValidation, "cache.policy must be lru or tinylfu, got %q", c.Policy)
	}
	if c.ReplicationQueue <= 0 || c.ReplicationWorkers <= 0 {
		return errs.New(errs.ClassValidation, "cache replication queue and worker counts must be positive")
	}
	if c.ReplicationTimeout <= 0 {
		return errs.New(errs.ClassValidation, "cache.replication_timeout must be positive")
	}
	return nil
}

// ProxyConfig bounds the public HTTP listener and origin transport.
type ProxyConfig struct {
	RequestTimeout    time.Duration `yaml:"request_timeout"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	MaxHeaderBytes    int           `yaml:"max_header_bytes"`
	DrainTimeout      time.Duration `yaml:"drain_timeout"`
	// MaxConcurrentRequests bounds in-flight public requests. Zero disables the
	// limiter, which is only appropriate in tests.
	MaxConcurrentRequests int `yaml:"max_concurrent_requests"`
	// TrustedProxyCIDRs lists networks whose X-Forwarded-For may be appended to
	// rather than replaced. Empty means trust nobody, which is the safe default.
	TrustedProxyCIDRs []string `yaml:"trusted_proxy_cidrs"`
	// TLS optionally terminates HTTPS (and thus HTTP/2) on the public listener.
	TLS struct {
		Enabled  bool   `yaml:"enabled"`
		CertFile string `yaml:"cert_file"`
		KeyFile  string `yaml:"key_file"`
	} `yaml:"tls"`
	// OriginDialTimeout and friends bound the shared origin transport.
	OriginDialTimeout           time.Duration `yaml:"origin_dial_timeout"`
	OriginKeepAlive             time.Duration `yaml:"origin_keep_alive"`
	OriginMaxIdleConns          int           `yaml:"origin_max_idle_conns"`
	OriginMaxIdleConnsPerHost   int           `yaml:"origin_max_idle_conns_per_host"`
	OriginIdleConnTimeout       time.Duration `yaml:"origin_idle_conn_timeout"`
	OriginResponseHeaderTimeout time.Duration `yaml:"origin_response_header_timeout"`
	// PropagateTraceToOrigin injects W3C trace context into origin requests.
	PropagateTraceToOrigin bool `yaml:"propagate_trace_to_origin"`
}

func (p *ProxyConfig) applyDefaults() {
	if p.RequestTimeout == 0 {
		p.RequestTimeout = 15 * time.Second
	}
	if p.ReadHeaderTimeout == 0 {
		p.ReadHeaderTimeout = 5 * time.Second
	}
	if p.IdleTimeout == 0 {
		p.IdleTimeout = 60 * time.Second
	}
	if p.MaxHeaderBytes == 0 {
		p.MaxHeaderBytes = 1 << 20
	}
	if p.DrainTimeout == 0 {
		p.DrainTimeout = 20 * time.Second
	}
	if p.MaxConcurrentRequests == 0 {
		p.MaxConcurrentRequests = 4096
	}
	if p.OriginDialTimeout == 0 {
		p.OriginDialTimeout = 3 * time.Second
	}
	if p.OriginKeepAlive == 0 {
		p.OriginKeepAlive = 30 * time.Second
	}
	if p.OriginMaxIdleConns == 0 {
		p.OriginMaxIdleConns = 512
	}
	if p.OriginMaxIdleConnsPerHost == 0 {
		p.OriginMaxIdleConnsPerHost = 128
	}
	if p.OriginIdleConnTimeout == 0 {
		p.OriginIdleConnTimeout = 90 * time.Second
	}
	if p.OriginResponseHeaderTimeout == 0 {
		p.OriginResponseHeaderTimeout = 10 * time.Second
	}
}

func (p ProxyConfig) validate() error {
	if p.RequestTimeout <= 0 || p.ReadHeaderTimeout <= 0 || p.IdleTimeout <= 0 {
		return errs.New(errs.ClassValidation, "proxy timeouts must be positive")
	}
	// WriteTimeout on a streaming proxy would cut long downloads; it is opt-in
	// and must exceed the request timeout when set.
	if p.WriteTimeout != 0 && p.WriteTimeout < p.RequestTimeout {
		return errs.New(errs.ClassValidation,
			"proxy.write_timeout (%s) must be zero or greater than request_timeout (%s)",
			p.WriteTimeout, p.RequestTimeout)
	}
	if p.MaxHeaderBytes <= 0 {
		return errs.New(errs.ClassValidation, "proxy.max_header_bytes must be positive")
	}
	if p.DrainTimeout <= 0 {
		return errs.New(errs.ClassValidation, "proxy.drain_timeout must be positive")
	}
	if p.MaxConcurrentRequests < 0 {
		return errs.New(errs.ClassValidation, "proxy.max_concurrent_requests must not be negative")
	}
	for i, c := range p.TrustedProxyCIDRs {
		if _, _, err := net.ParseCIDR(c); err != nil {
			return errs.Wrap(errs.ClassValidation, err, "proxy.trusted_proxy_cidrs[%d] %q is not a CIDR", i, c)
		}
	}
	if p.TLS.Enabled && (p.TLS.CertFile == "" || p.TLS.KeyFile == "") {
		return errs.New(errs.ClassValidation, "proxy.tls requires cert_file and key_file")
	}
	return nil
}

// OriginSecurity restricts which origin addresses an administrator may target.
// It exists so that a misconfigured route cannot turn the proxy into an SSRF
// gadget against link-local or instance-metadata addresses.
type OriginSecurity struct {
	// AllowPrivateNetworks permits RFC1918/ULA destinations. Required for the
	// container-network demo, refused by default in production mode.
	AllowPrivateNetworks bool `yaml:"allow_private_networks"`
	// AllowLoopback permits 127.0.0.0/8 and ::1 destinations.
	AllowLoopback bool `yaml:"allow_loopback"`
	// AllowedCIDRs explicitly allowlists destinations regardless of the above.
	AllowedCIDRs []string `yaml:"allowed_cidrs"`
	// DeniedCIDRs is checked first and always wins.
	DeniedCIDRs []string `yaml:"denied_cidrs"`
}

func (o OriginSecurity) validate() error {
	for _, group := range []struct {
		field string
		list  []string
	}{{"origin_security.allowed_cidrs", o.AllowedCIDRs}, {"origin_security.denied_cidrs", o.DeniedCIDRs}} {
		for i, c := range group.list {
			if _, _, err := net.ParseCIDR(c); err != nil {
				return errs.Wrap(errs.ClassValidation, err, "%s[%d] %q is not a CIDR", group.field, i, c)
			}
		}
	}
	return nil
}

// EdgeConfig is the bootstrap configuration of an edge process.
type EdgeConfig struct {
	Mode Mode `yaml:"mode"`
	Node struct {
		ID     string `yaml:"id"`
		Region string `yaml:"region"`
		Zone   string `yaml:"zone"`
		// PublicAddress is the client-facing HTTP listener.
		PublicAddress string `yaml:"public_address"`
		// PeerAddress is the L2 peer-cache gRPC listener.
		PeerAddress string `yaml:"peer_address"`
		// AdvertisePeerAddress is what peers dial. Required when PeerAddress
		// binds a wildcard, which it usually does in a container.
		AdvertisePeerAddress   string `yaml:"advertise_peer_address"`
		AdvertisePublicAddress string `yaml:"advertise_public_address"`
		// Weight is the ring weight of this node. Weighted rings are P1; V1
		// validates the field but treats every live node as weight 1.
		Weight uint32 `yaml:"weight"`
	} `yaml:"node"`
	ControlPlane struct {
		Endpoints []string `yaml:"endpoints"`
		// HeartbeatInterval overrides the leader-suggested cadence.
		HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
		// ReconnectBackoffMin/Max bound config-stream reconnection.
		ReconnectBackoffMin time.Duration `yaml:"reconnect_backoff_min"`
		ReconnectBackoffMax time.Duration `yaml:"reconnect_backoff_max"`
	} `yaml:"control_plane"`
	Cache          CacheConfig    `yaml:"cache"`
	Proxy          ProxyConfig    `yaml:"proxy"`
	Security       TLS            `yaml:"security"`
	OriginSecurity OriginSecurity `yaml:"origin_security"`
	Telemetry      Telemetry      `yaml:"telemetry"`
	// PeerRPCTimeout bounds a single peer-cache RPC.
	PeerRPCTimeout time.Duration `yaml:"peer_rpc_timeout"`
}

func (c *EdgeConfig) applyDefaults() {
	if c.Mode == "" {
		c.Mode = ModeDevelopment
	}
	if c.Node.Weight == 0 {
		c.Node.Weight = 1
	}
	if c.Node.AdvertisePeerAddress == "" {
		c.Node.AdvertisePeerAddress = advertiseFor(c.Node.PeerAddress, c.Node.ID)
	}
	if c.Node.AdvertisePublicAddress == "" {
		c.Node.AdvertisePublicAddress = advertiseFor(c.Node.PublicAddress, c.Node.ID)
	}
	if c.ControlPlane.HeartbeatInterval == 0 {
		c.ControlPlane.HeartbeatInterval = 2 * time.Second
	}
	if c.ControlPlane.ReconnectBackoffMin == 0 {
		c.ControlPlane.ReconnectBackoffMin = 250 * time.Millisecond
	}
	if c.ControlPlane.ReconnectBackoffMax == 0 {
		c.ControlPlane.ReconnectBackoffMax = 5 * time.Second
	}
	if c.PeerRPCTimeout == 0 {
		c.PeerRPCTimeout = 10 * time.Second
	}
	c.Cache.applyDefaults()
	c.Proxy.applyDefaults()
	c.Telemetry.applyDefaults()
	if c.Mode == ModeDevelopment {
		// A laptop/Compose demo necessarily talks to loopback and container
		// networks. Production keeps both closed unless explicitly opened.
		c.OriginSecurity.AllowLoopback = true
		c.OriginSecurity.AllowPrivateNetworks = true
	}
}

// Validate checks every invariant the edge process depends on.
func (c *EdgeConfig) Validate() error {
	if !c.Mode.Valid() {
		return errs.New(errs.ClassValidation, "mode must be development or production, got %q", c.Mode)
	}
	if err := validateNodeID("node.id", c.Node.ID); err != nil {
		return err
	}
	for field, addr := range map[string]string{
		"node.public_address": c.Node.PublicAddress,
		"node.peer_address":   c.Node.PeerAddress,
	} {
		if err := validateListenAddress(field, addr); err != nil {
			return err
		}
	}
	if err := validateDialAddress("node.advertise_peer_address", c.Node.AdvertisePeerAddress); err != nil {
		return err
	}
	if len(c.ControlPlane.Endpoints) == 0 {
		return errs.New(errs.ClassValidation, "control_plane.endpoints must list at least one control node")
	}
	for i, e := range c.ControlPlane.Endpoints {
		if err := validateDialAddress(fmt.Sprintf("control_plane.endpoints[%d]", i), e); err != nil {
			return err
		}
	}
	if c.ControlPlane.HeartbeatInterval <= 0 {
		return errs.New(errs.ClassValidation, "control_plane.heartbeat_interval must be positive")
	}
	if c.ControlPlane.ReconnectBackoffMin <= 0 ||
		c.ControlPlane.ReconnectBackoffMax < c.ControlPlane.ReconnectBackoffMin {
		return errs.New(errs.ClassValidation,
			"control_plane reconnect backoff must satisfy 0 < min (%s) <= max (%s)",
			c.ControlPlane.ReconnectBackoffMin, c.ControlPlane.ReconnectBackoffMax)
	}
	if c.PeerRPCTimeout <= 0 {
		return errs.New(errs.ClassValidation, "peer_rpc_timeout must be positive")
	}
	if err := c.Cache.validate(); err != nil {
		return err
	}
	if err := c.Proxy.validate(); err != nil {
		return err
	}
	if err := c.Security.validate(c.Mode, "security"); err != nil {
		return err
	}
	if err := c.OriginSecurity.validate(); err != nil {
		return err
	}
	if c.Mode == ModeProduction && (c.OriginSecurity.AllowLoopback || c.OriginSecurity.AllowPrivateNetworks) {
		// Not fatal on its own but must be a deliberate act: an operator has to
		// list the exact networks instead of opening whole classes.
		if len(c.OriginSecurity.AllowedCIDRs) == 0 {
			return errs.New(errs.ClassValidation,
				"origin_security: production mode requires explicit allowed_cidrs when loopback or private networks are permitted")
		}
	}
	if err := c.Telemetry.validate(c.Mode); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

// LoadControl reads, defaults, and validates a control-plane configuration.
func LoadControl(path string) (*ControlConfig, error) {
	var cfg ControlConfig
	if err := decodeFile(path, &cfg); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, errs.Wrap(errs.ClassValidation, err, "invalid control config %q", path)
	}
	return &cfg, nil
}

// LoadEdge reads, defaults, and validates an edge configuration.
func LoadEdge(path string) (*EdgeConfig, error) {
	var cfg EdgeConfig
	if err := decodeFile(path, &cfg); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, errs.Wrap(errs.ClassValidation, err, "invalid edge config %q", path)
	}
	return &cfg, nil
}

// decodeFile parses YAML strictly: an unknown field is a configuration error,
// not something to silently ignore, because silently ignoring a typo'd security
// setting is exactly the failure this project must not have.
func decodeFile(path string, out any) error {
	f, err := os.Open(path)
	if err != nil {
		return errs.Wrap(errs.ClassValidation, err, "open config %q", path)
	}
	defer f.Close() //nolint:errcheck // read-only handle

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil {
		return errs.Wrap(errs.ClassValidation, err, "parse config %q", path)
	}
	return nil
}

// ResolveAdminToken reads the admin bearer token from the configured source.
// Returning an empty token with a nil error is only possible when auth is
// disabled; every enabled path either yields a token or an error.
func (a AdminAuth) ResolveAdminToken() (string, error) {
	if !a.Enabled {
		return "", nil
	}
	if a.TokenEnv != "" {
		if v := strings.TrimSpace(os.Getenv(a.TokenEnv)); v != "" {
			return v, nil
		}
	}
	if a.TokenFile != "" {
		b, err := os.ReadFile(a.TokenFile)
		if err != nil {
			return "", errs.Wrap(errs.ClassValidation, err, "read admin token file %q", a.TokenFile)
		}
		if v := strings.TrimSpace(string(b)); v != "" {
			return v, nil
		}
	}
	return "", errs.New(errs.ClassValidation,
		"admin auth is enabled but no token was found in env %q or file %q", a.TokenEnv, a.TokenFile)
}

// ---------------------------------------------------------------------------
// Shared validation helpers
// ---------------------------------------------------------------------------

var errEmpty = errors.New("value is empty")

func validateNodeID(field, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errs.Wrap(errs.ClassValidation, errEmpty, "%s is required", field)
	}
	if len(id) > 63 {
		return errs.New(errs.ClassValidation, "%s must be at most 63 characters", field)
	}
	for _, r := range id {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
		if !ok {
			return errs.New(errs.ClassValidation,
				"%s may only contain letters, digits, '-', '_' and '.', got %q", field, id)
		}
	}
	return nil
}

// validateListenAddress accepts host:port where host may be empty or a wildcard.
func validateListenAddress(field, addr string) error {
	if strings.TrimSpace(addr) == "" {
		return errs.Wrap(errs.ClassValidation, errEmpty, "%s is required", field)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return errs.Wrap(errs.ClassValidation, err, "%s must be host:port, got %q", field, addr)
	}
	if err := validateListenHost(field, host); err != nil {
		return err
	}
	return validatePort(field, port)
}

// validateListenHost accepts an empty host, a wildcard, an IP literal, or a
// DNS name.
//
// An unvalidated host reached net.Listen unchecked, where a typo surfaced as a
// runtime bind failure after the process had already started rather than as a
// configuration error at load time. That is exactly the "half-valid
// configuration" this package exists to reject.
func validateListenHost(field, host string) error {
	// Empty and the wildcards mean "every interface", which is the common case
	// in a container.
	if host == "" || host == "0.0.0.0" || host == "::" || host == "*" {
		return nil
	}
	if net.ParseIP(host) != nil {
		return nil
	}
	if !isDNSName(host) {
		return errs.New(errs.ClassValidation,
			"%s host %q is neither an IP address nor a valid hostname", field, host)
	}
	return nil
}

// isDNSName reports whether s is a valid RFC 1123 hostname.
func isDNSName(s string) bool {
	if s == "" || len(s) > 253 {
		return false
	}
	// A single trailing dot denotes the root and is legal.
	s = strings.TrimSuffix(s, ".")
	if s == "" {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			switch {
			case c >= 'a' && c <= 'z',
				c >= 'A' && c <= 'Z',
				c >= '0' && c <= '9',
				c == '-':
			default:
				return false
			}
		}
	}
	return true
}

// validateDialAddress additionally requires a non-empty host: you cannot dial a
// wildcard. This catches the classic mistake of advertising 0.0.0.0:7200.
func validateDialAddress(field, addr string) error {
	if err := validateListenAddress(field, addr); err != nil {
		return err
	}
	host, _, _ := net.SplitHostPort(addr)
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		return errs.New(errs.ClassValidation,
			"%s must be a dialable host:port, not the wildcard %q; set an advertised address", field, addr)
	}
	return nil
}

func validatePort(field, port string) error {
	n, err := net.LookupPort("tcp", port)
	if err != nil {
		return errs.Wrap(errs.ClassValidation, err, "%s has an invalid port %q", field, port)
	}
	if n == 0 {
		return errs.New(errs.ClassValidation, "%s port must not be 0", field)
	}
	return nil
}

// advertiseFor derives a dialable address from a listen address by replacing a
// wildcard host with the node ID. In Compose and Kubernetes the node ID is the
// service/pod DNS name, which makes this the right default; anything else must
// set the advertise field explicitly and validation will catch it if not.
func advertiseFor(listen, nodeID string) string {
	if listen == "" || nodeID == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return ""
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = nodeID
	}
	return net.JoinHostPort(host, port)
}
