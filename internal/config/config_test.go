package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const minimalControl = `
mode: development
node:
  id: cp-1
  raft_address: 0.0.0.0:7000
  admin_address: 0.0.0.0:7100
  edge_address: 0.0.0.0:7300
raft:
  peers:
    - {id: cp-1, address: cp-1:7000}
    - {id: cp-2, address: cp-2:7000}
    - {id: cp-3, address: cp-3:7000}
  data_dir: /var/lib/edgemesh
security:
  enabled: false
telemetry:
  address: 0.0.0.0:9100
`

const minimalEdge = `
mode: development
node:
  id: edge-1
  region: local-a
  public_address: 0.0.0.0:8080
  peer_address: 0.0.0.0:7200
control_plane:
  endpoints: [cp-1:7300, cp-2:7300, cp-3:7300]
security:
  enabled: false
telemetry:
  address: 0.0.0.0:9200
`

func TestLoadControlAppliesDefaults(t *testing.T) {
	cfg, err := LoadControl(writeFile(t, "control.yaml", minimalControl))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Raft.ElectionTimeoutMin != 500*time.Millisecond ||
		cfg.Raft.HeartbeatInterval != 150*time.Millisecond {
		t.Fatalf("raft timer defaults not applied: %+v", cfg.Raft)
	}
	if cfg.Telemetry.LogLevel != "info" || cfg.Telemetry.LogFormat != "json" {
		t.Fatalf("telemetry defaults not applied: %+v", cfg.Telemetry)
	}
	// A wildcard listen address must yield a dialable advertise address, or
	// peers would try to connect to 0.0.0.0.
	if cfg.Node.AdvertiseAdminAddress != "cp-1:7100" {
		t.Fatalf("advertise admin address = %q, want cp-1:7100", cfg.Node.AdvertiseAdminAddress)
	}
	if cfg.Node.AdvertiseEdgeAddress != "cp-1:7300" {
		t.Fatalf("advertise edge address = %q", cfg.Node.AdvertiseEdgeAddress)
	}
}

func TestLoadEdgeAppliesDefaults(t *testing.T) {
	cfg, err := LoadEdge(writeFile(t, "edge.yaml", minimalEdge))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cache.Shards != 64 || cfg.Cache.ReplicationFactor != 2 {
		t.Fatalf("cache defaults not applied: %+v", cfg.Cache)
	}
	if cfg.Proxy.RequestTimeout != 15*time.Second {
		t.Fatalf("proxy defaults not applied: %+v", cfg.Proxy)
	}
	if cfg.Node.AdvertisePeerAddress != "edge-1:7200" {
		t.Fatalf("advertise peer address = %q", cfg.Node.AdvertisePeerAddress)
	}
	// Development mode opens loopback and private networks so the local demo
	// can reach containerized origins.
	if !cfg.OriginSecurity.AllowLoopback || !cfg.OriginSecurity.AllowPrivateNetworks {
		t.Fatal("development mode must permit loopback and private origins")
	}
}

// An unknown field is a configuration error, not something to ignore: silently
// dropping a misspelled security setting is the failure this must not have.
func TestUnknownFieldsAreRejected(t *testing.T) {
	path := writeFile(t, "control.yaml", minimalControl+"\nnot_a_real_field: true\n")
	if _, err := LoadControl(path); err == nil {
		t.Fatal("an unknown top-level field must be rejected")
	}
	path = writeFile(t, "edge.yaml", minimalEdge+"\ncache:\n  wrong_name: 1\n")
	if _, err := LoadEdge(path); err == nil {
		t.Fatal("an unknown nested field must be rejected")
	}
}

func TestMissingFileIsAnError(t *testing.T) {
	if _, err := LoadControl("/nonexistent/control.yaml"); err == nil {
		t.Fatal("a missing file must be an error")
	}
	if !errs.IsClass(mustErr(LoadEdge("/nonexistent/edge.yaml")), errs.ClassValidation) {
		t.Fatal("expected a validation-classified error")
	}
}

func mustErr(_ *EdgeConfig, err error) error { return err }

func TestControlValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ControlConfig)
	}{
		{"bad mode", func(c *ControlConfig) { c.Mode = "staging" }},
		{"missing node id", func(c *ControlConfig) { c.Node.ID = "" }},
		{"node id with a slash", func(c *ControlConfig) { c.Node.ID = "cp/1" }},
		{"missing raft address", func(c *ControlConfig) { c.Node.RaftAddress = "" }},
		{"raft address without a port", func(c *ControlConfig) { c.Node.RaftAddress = "0.0.0.0" }},
		{"too few peers", func(c *ControlConfig) { c.Raft.Peers = c.Raft.Peers[:2] }},
		{"even peer count", func(c *ControlConfig) {
			c.Raft.Peers = append(c.Raft.Peers, RaftPeer{ID: "cp-4", Address: "cp-4:7000"})
		}},
		{"duplicate peer id", func(c *ControlConfig) { c.Raft.Peers[1].ID = "cp-1" }},
		{"duplicate peer address", func(c *ControlConfig) { c.Raft.Peers[1].Address = "cp-1:7000" }},
		{"peers exclude self", func(c *ControlConfig) { c.Node.ID = "cp-9" }},
		{"peer address is a wildcard", func(c *ControlConfig) { c.Raft.Peers[0].Address = "0.0.0.0:7000" }},
		{"inverted election timeouts", func(c *ControlConfig) {
			c.Raft.ElectionTimeoutMin, c.Raft.ElectionTimeoutMax = time.Second, time.Millisecond
		}},
		{"heartbeat too close to the election timeout", func(c *ControlConfig) {
			c.Raft.HeartbeatInterval = 400 * time.Millisecond // min is 500ms
		}},
		{"bad log level", func(c *ControlConfig) { c.Telemetry.LogLevel = "verbose" }},
		{"trace ratio above one", func(c *ControlConfig) { c.Telemetry.TraceSampleRatio = 2 }},
		{"dead threshold below suspect", func(c *ControlConfig) {
			c.Membership.SuspectAfterMissed, c.Membership.DeadAfterMissed = 5, 2
		}},
		{"auth enabled without a token source", func(c *ControlConfig) {
			c.AdminAuth.Enabled = true
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadControl(writeFile(t, "c.yaml", minimalControl))
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected a validation error")
			} else if !errs.IsClass(err, errs.ClassValidation) {
				t.Fatalf("error class = %q, want validation", errs.ClassOf(err))
			}
		})
	}
}

// Production mode must fail closed on every security-relevant setting.
func TestProductionModeFailsClosed(t *testing.T) {
	base := func(t *testing.T) *ControlConfig {
		t.Helper()
		cfg, err := LoadControl(writeFile(t, "c.yaml", minimalControl))
		if err != nil {
			t.Fatal(err)
		}
		cfg.Mode = ModeProduction
		cfg.Security = TLS{Enabled: true, CAFile: "ca", CertFile: "crt", KeyFile: "key"}
		cfg.AdminAuth = AdminAuth{Enabled: true, TokenEnv: "TOKEN"}
		return cfg
	}

	if err := base(t).Validate(); err != nil {
		t.Fatalf("a correctly hardened production config was rejected: %v", err)
	}

	t.Run("mTLS is mandatory", func(t *testing.T) {
		cfg := base(t)
		cfg.Security.Enabled = false
		if err := cfg.Validate(); err == nil {
			t.Fatal("production must refuse plaintext internal RPC")
		}
	})
	t.Run("skip_verify is refused", func(t *testing.T) {
		cfg := base(t)
		cfg.Security.SkipVerify = true
		if err := cfg.Validate(); err == nil {
			t.Fatal("production must refuse skip_verify")
		}
	})
	t.Run("admin auth is mandatory", func(t *testing.T) {
		cfg := base(t)
		cfg.AdminAuth.Enabled = false
		if err := cfg.Validate(); err == nil {
			t.Fatal("production must refuse an unauthenticated admin API")
		}
	})
	t.Run("pprof is refused", func(t *testing.T) {
		cfg := base(t)
		cfg.Telemetry.EnablePprof = true
		if err := cfg.Validate(); err == nil {
			t.Fatal("production must refuse an unauthenticated pprof endpoint")
		}
	})
}

func TestEdgeValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*EdgeConfig)
	}{
		{"missing node id", func(c *EdgeConfig) { c.Node.ID = "" }},
		{"no control endpoints", func(c *EdgeConfig) { c.ControlPlane.Endpoints = nil }},
		{"wildcard control endpoint", func(c *EdgeConfig) { c.ControlPlane.Endpoints = []string{"0.0.0.0:7300"} }},
		{"non power of two shards", func(c *EdgeConfig) { c.Cache.Shards = 100 }},
		{"zero replication factor", func(c *EdgeConfig) { c.Cache.ReplicationFactor = 0 }},
		{"object larger than the tier", func(c *EdgeConfig) { c.Cache.MaxObjectBytes = c.Cache.L1MaxBytes + 1 }},
		{"bad cache policy", func(c *EdgeConfig) { c.Cache.Policy = "arc" }},
		{"invalid trusted CIDR", func(c *EdgeConfig) { c.Proxy.TrustedProxyCIDRs = []string{"nope"} }},
		{"tls without a cert", func(c *EdgeConfig) { c.Proxy.TLS.Enabled = true }},
		{"write timeout below request timeout", func(c *EdgeConfig) {
			c.Proxy.WriteTimeout = time.Second
		}},
		{"inverted reconnect backoff", func(c *EdgeConfig) {
			c.ControlPlane.ReconnectBackoffMin = time.Minute
			c.ControlPlane.ReconnectBackoffMax = time.Second
		}},
		{"invalid origin CIDR", func(c *EdgeConfig) { c.OriginSecurity.AllowedCIDRs = []string{"nope"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadEdge(writeFile(t, "e.yaml", minimalEdge))
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

// An edge in production mode that opens whole address classes must name the
// exact networks, so SSRF exposure is a deliberate act rather than a default.
func TestProductionEdgeRequiresExplicitOriginAllowlist(t *testing.T) {
	cfg, err := LoadEdge(writeFile(t, "e.yaml", minimalEdge))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Mode = ModeProduction
	cfg.Security = TLS{Enabled: true, CAFile: "ca", CertFile: "crt", KeyFile: "key"}
	cfg.OriginSecurity.AllowPrivateNetworks = true
	cfg.OriginSecurity.AllowedCIDRs = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("production must require explicit allowed_cidrs")
	}
	cfg.OriginSecurity.AllowedCIDRs = []string{"10.0.0.0/8"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("an explicit allowlist was rejected: %v", err)
	}
}

func TestResolveAdminToken(t *testing.T) {
	// Disabled auth yields no token and no error.
	tok, err := AdminAuth{}.ResolveAdminToken()
	if err != nil || tok != "" {
		t.Fatalf("disabled auth: %q %v", tok, err)
	}
	// Enabled auth with no source is an error rather than an empty token.
	if _, err := (AdminAuth{Enabled: true}).ResolveAdminToken(); err == nil {
		t.Fatal("enabled auth without a source must fail")
	}
	t.Setenv("EDGEMESH_TEST_TOKEN", "s3cret-token-value")
	tok, err = AdminAuth{Enabled: true, TokenEnv: "EDGEMESH_TEST_TOKEN"}.ResolveAdminToken()
	if err != nil || tok != "s3cret-token-value" {
		t.Fatalf("env token: %q %v", tok, err)
	}
	// A file source works and is trimmed.
	path := writeFile(t, "token", "  file-token-value  \n")
	tok, err = AdminAuth{Enabled: true, TokenFile: path}.ResolveAdminToken()
	if err != nil || tok != "file-token-value" {
		t.Fatalf("file token: %q %v", tok, err)
	}
}

// The shipped configurations must actually load, or the demo is broken before
// it starts.
func TestShippedLocalConfigsAreValid(t *testing.T) {
	for _, n := range []string{"1", "2", "3"} {
		if _, err := LoadControl(filepath.Join("..", "..", "configs", "local", "control-"+n+".yaml")); err != nil {
			t.Errorf("configs/local/control-%s.yaml: %v", n, err)
		}
		if _, err := LoadEdge(filepath.Join("..", "..", "configs", "local", "edge-"+n+".yaml")); err != nil {
			t.Errorf("configs/local/edge-%s.yaml: %v", n, err)
		}
	}
}

func FuzzLoadControl(f *testing.F) {
	f.Add(minimalControl)
	f.Add("")
	f.Add("mode: production")
	f.Fuzz(func(t *testing.T, content string) {
		path := filepath.Join(t.TempDir(), "c.yaml")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Skip()
		}
		cfg, err := LoadControl(path)
		if err != nil {
			return
		}
		// Anything that loads must satisfy its own invariants; a config that
		// validates but is internally inconsistent is the bug this guards.
		if err := cfg.Validate(); err != nil {
			t.Fatalf("LoadControl returned a config that fails Validate: %v", err)
		}
	})
}

// A listen address's host was split out and then ignored, so a typo became a
// runtime bind failure after startup instead of a configuration error at load.
func TestValidateListenAddress(t *testing.T) {
	valid := []string{
		":8080",             // every interface, the container default
		"0.0.0.0:8080",      // explicit IPv4 wildcard
		"[::]:8080",         // explicit IPv6 wildcard
		"127.0.0.1:8080",    // IPv4 literal
		"[::1]:8080",        // IPv6 literal
		"localhost:8080",    // short hostname
		"edge-1.svc:7300",   // Kubernetes-style service name
		"a.b.example.com:1", // fully qualified
		"edgemesh.local.:1", // trailing root dot is legal
	}
	for _, addr := range valid {
		t.Run("valid/"+addr, func(t *testing.T) {
			if err := validateListenAddress("listen", addr); err != nil {
				t.Errorf("validateListenAddress(%q) = %v, want nil", addr, err)
			}
		})
	}

	invalid := []string{
		"",                  // required
		"8080",              // no port separator
		"host with space:1", // spaces are not hostname characters
		"exa_mple.com:1",    // underscore is not an RFC 1123 hostname character
		"-leading.com:1",    // a label may not start with a hyphen
		"trailing-.com:1",   // nor end with one
		"host..com:1",       // an empty label
		"host:0",            // port 0 never listens usefully
		"host:notaport",     // non-numeric port
	}
	for _, addr := range invalid {
		t.Run("invalid/"+addr, func(t *testing.T) {
			if err := validateListenAddress("listen", addr); err == nil {
				t.Errorf("validateListenAddress(%q) = nil, want an error", addr)
			}
		})
	}
}
