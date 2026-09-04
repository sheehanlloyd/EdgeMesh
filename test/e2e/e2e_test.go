//go:build e2e

// Package e2e drives real EdgeMesh processes over real sockets.
//
// The integration suite removes the process boundary so it can run in seconds
// on every commit. This suite keeps it: separate OS processes, real signal
// handling, real graceful shutdown, and real crash semantics. That is the only
// way to test things the in-process harness structurally cannot: a SIGKILL
// mid-request, a SIGTERM drain, or a restart recovering durable state from disk.
//
// Build with the e2e tag:
//
//	make test-e2e
package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const demoHost = "demo.edgemesh.local"

// cluster manages a set of real EdgeMesh processes.
type cluster struct {
	t       *testing.T
	binDir  string
	dataDir string
	logDir  string

	mu    sync.Mutex
	procs map[string]*exec.Cmd

	controlAdmin []string // admin URLs, in node order
	edgeURLs     []string
	originURLs   []string
}

// ports are fixed rather than dynamic because the node configurations
// reference each other by address, and a config file cannot be written until
// every port is known. High ports avoid collisions with a developer's own
// services.
const (
	basePortRaft  = 17000
	basePortAdmin = 17100
	basePortEdge  = 17200
	basePortPeer  = 17300
	basePortPub   = 18080
	basePortTelem = 19100
	basePortOrig  = 19000
)

func newCluster(t *testing.T) *cluster {
	t.Helper()

	binDir := os.Getenv("EDGEMESH_BIN_DIR")
	if binDir == "" {
		binDir = filepath.Join("..", "..", "bin")
	}
	abs, err := filepath.Abs(binDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []string{"control", "edge", "edgemeshctl", "origin-demo"} {
		if _, err := os.Stat(filepath.Join(abs, b)); err != nil {
			t.Skipf("binaries not found in %s; run 'make build' first", abs)
		}
	}
	// A previous suite's teardown may still be releasing sockets, so this waits
	// rather than skipping: skipping would silently turn a port conflict into a
	// green run that tested nothing.
	waitForPortsFree(t, 30*time.Second)

	c := &cluster{
		t:       t,
		binDir:  abs,
		dataDir: t.TempDir(),
		logDir:  t.TempDir(),
		procs:   map[string]*exec.Cmd{},
	}
	t.Cleanup(c.stopAll)
	return c
}

// waitForPortsFree blocks until every port the cluster binds is available.
//
// Every port is checked, not a sample: a cluster that starts with one port
// still held produces a confusing timeout deep inside a scenario rather than a
// clear message here.
func waitForPortsFree(t *testing.T, timeout time.Duration) {
	t.Helper()

	var ports []int
	for i := 1; i <= 3; i++ {
		ports = append(ports,
			basePortRaft+i, basePortAdmin+i, basePortEdge+i,
			basePortPeer+i, basePortPub+i,
			basePortTelem+i, basePortTelem+10+i, basePortOrig+i)
	}

	deadline := time.Now().Add(timeout)
	for {
		busy := 0
		var firstBusy int
		for _, p := range ports {
			ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
			if err != nil {
				if busy == 0 {
					firstBusy = p
				}
				busy++
				continue
			}
			_ = ln.Close()
		}
		if busy == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of the %d ports this suite needs are still in use after %s "+
				"(first: %d); another EdgeMesh cluster may be running",
				busy, len(ports), timeout, firstBusy)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// writeConfigs renders the node configuration files.
func (c *cluster) writeConfigs(controlCount, edgeCount int) {
	c.t.Helper()

	var peers strings.Builder
	for i := 1; i <= controlCount; i++ {
		fmt.Fprintf(&peers, "    - {id: cp-%d, address: 127.0.0.1:%d}\n", i, basePortRaft+i)
	}
	var endpoints strings.Builder
	for i := 1; i <= controlCount; i++ {
		fmt.Fprintf(&endpoints, "    - 127.0.0.1:%d\n", basePortEdge+i)
	}

	for i := 1; i <= controlCount; i++ {
		cfg := fmt.Sprintf(`mode: development
node:
  id: cp-%d
  raft_address: 127.0.0.1:%d
  admin_address: 127.0.0.1:%d
  edge_address: 127.0.0.1:%d
  advertise_admin_address: 127.0.0.1:%d
  advertise_edge_address: 127.0.0.1:%d
raft:
  peers:
%s  election_timeout_min: 400ms
  election_timeout_max: 700ms
  heartbeat_interval: 100ms
  snapshot_entries: 100
  pre_vote: true
  data_dir: %s/cp-%d
security:
  enabled: false
admin_auth:
  enabled: false
membership:
  heartbeat_interval: 500ms
  suspect_after_missed: 2
  dead_after_missed: 3
  sweep_interval: 250ms
telemetry:
  address: 127.0.0.1:%d
  log_level: info
  log_format: json
`, i, basePortRaft+i, basePortAdmin+i, basePortEdge+i, basePortAdmin+i, basePortEdge+i,
			peers.String(), c.dataDir, i, basePortTelem+i)
		c.writeFile(fmt.Sprintf("control-%d.yaml", i), cfg)
		c.controlAdmin = append(c.controlAdmin, fmt.Sprintf("http://127.0.0.1:%d", basePortAdmin+i))
	}

	for i := 1; i <= edgeCount; i++ {
		cfg := fmt.Sprintf(`mode: development
node:
  id: edge-%d
  region: e2e
  zone: e2e-%d
  public_address: 127.0.0.1:%d
  peer_address: 127.0.0.1:%d
  advertise_peer_address: 127.0.0.1:%d
  advertise_public_address: 127.0.0.1:%d
control_plane:
  endpoints:
%s  heartbeat_interval: 500ms
  reconnect_backoff_min: 100ms
  reconnect_backoff_max: 1s
cache:
  l1_max_bytes: 33554432
  l2_max_bytes: 67108864
  max_object_bytes: 4194304
  shards: 16
  replication_factor: 2
  cleanup_interval: 5s
  policy: lru
proxy:
  request_timeout: 10s
  drain_timeout: 5s
security:
  enabled: false
origin_security:
  allow_loopback: true
  allow_private_networks: true
telemetry:
  address: 127.0.0.1:%d
  log_level: info
  log_format: json
`, i, i, basePortPub+i, basePortPeer+i, basePortPeer+i, basePortPub+i,
			endpoints.String(), basePortTelem+10+i)
		c.writeFile(fmt.Sprintf("edge-%d.yaml", i), cfg)
		c.edgeURLs = append(c.edgeURLs, fmt.Sprintf("http://127.0.0.1:%d", basePortPub+i))
	}
}

func (c *cluster) writeFile(name, content string) {
	c.t.Helper()
	if err := os.WriteFile(filepath.Join(c.dataDir, name), []byte(content), 0o600); err != nil {
		c.t.Fatal(err)
	}
}

// start launches one process and records it for cleanup.
func (c *cluster) start(name string, args ...string) {
	c.t.Helper()

	logFile, err := os.Create(filepath.Join(c.logDir, name+".log"))
	if err != nil {
		c.t.Fatal(err)
	}
	bin := strings.SplitN(name, "-", 2)[0]
	switch {
	case strings.HasPrefix(name, "cp-"):
		bin = "control"
	case strings.HasPrefix(name, "edge-"):
		bin = "edge"
	case strings.HasPrefix(name, "origin-"):
		bin = "origin-demo"
	}

	cmd := exec.Command(filepath.Join(c.binDir, bin), args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// A process group lets a crashed child's own children be cleaned up too.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		c.t.Fatalf("start %s: %v", name, err)
	}
	c.mu.Lock()
	c.procs[name] = cmd
	c.mu.Unlock()
}

// kill terminates a process abruptly, modelling a crash.
func (c *cluster) kill(name string) {
	c.t.Helper()
	c.mu.Lock()
	cmd, ok := c.procs[name]
	delete(c.procs, name)
	c.mu.Unlock()
	if !ok {
		c.t.Fatalf("no such process: %s", name)
	}
	if err := cmd.Process.Kill(); err != nil {
		c.t.Fatalf("kill %s: %v", name, err)
	}
	_, _ = cmd.Process.Wait()
}

// terminate sends SIGTERM, exercising the graceful shutdown path.
func (c *cluster) terminate(name string, timeout time.Duration) error {
	c.t.Helper()
	c.mu.Lock()
	cmd, ok := c.procs[name]
	delete(c.procs, name)
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("no such process: %s", name)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { _, err := cmd.Process.Wait(); done <- err }()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		return fmt.Errorf("%s did not exit within %s of SIGTERM", name, timeout)
	}
}

func (c *cluster) stopAll() {
	c.mu.Lock()
	names := make([]string, 0, len(c.procs))
	for n := range c.procs {
		names = append(names, n)
	}
	c.mu.Unlock()
	for _, n := range names {
		_ = c.terminate(n, 3*time.Second)
	}
	// Dump logs when a test failed, so a CI failure is diagnosable.
	if c.t.Failed() {
		entries, _ := os.ReadDir(c.logDir)
		for _, e := range entries {
			b, err := os.ReadFile(filepath.Join(c.logDir, e.Name()))
			if err != nil {
				continue
			}
			lines := strings.Split(strings.TrimSpace(string(b)), "\n")
			if len(lines) > 25 {
				lines = lines[len(lines)-25:]
			}
			c.t.Logf("=== %s (last %d lines) ===\n%s", e.Name(), len(lines), strings.Join(lines, "\n"))
		}
	}
}

// bootstrap starts the whole topology and waits for it to be usable.
func (c *cluster) bootstrap(controlCount, edgeCount, originCount int) {
	c.t.Helper()
	c.writeConfigs(controlCount, edgeCount)

	for i := 1; i <= originCount; i++ {
		c.start(fmt.Sprintf("origin-%d", i),
			"--addr", fmt.Sprintf("127.0.0.1:%d", basePortOrig+i),
			"--id", fmt.Sprintf("origin-%d", i))
		c.originURLs = append(c.originURLs, fmt.Sprintf("http://127.0.0.1:%d", basePortOrig+i))
	}
	for i := 1; i <= controlCount; i++ {
		c.start(fmt.Sprintf("cp-%d", i), "--config", filepath.Join(c.dataDir, fmt.Sprintf("control-%d.yaml", i)))
	}
	c.waitForLeader(30 * time.Second)

	for i := 1; i <= edgeCount; i++ {
		c.start(fmt.Sprintf("edge-%d", i), "--config", filepath.Join(c.dataDir, fmt.Sprintf("edge-%d.yaml", i)))
	}
	c.waitFor(30*time.Second, "every edge is ready", func() bool {
		for i := 1; i <= edgeCount; i++ {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", basePortTelem+10+i))
			if err != nil {
				return false
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return false
			}
		}
		return true
	})
}

type status struct {
	NodeID        string `json:"node_id"`
	Role          string `json:"role"`
	Term          uint64 `json:"term"`
	LeaderID      string `json:"leader_id"`
	IsLeader      bool   `json:"is_leader"`
	ConfigVersion uint64 `json:"config_version"`
	Routes        int    `json:"routes"`
	EdgeNodes     int    `json:"edge_nodes"`
}

// status queries one control node.
func (c *cluster) status(i int) (status, error) {
	var s status
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(c.controlAdmin[i] + "/v1/status")
	if err != nil {
		return s, err
	}
	defer resp.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return s, err
	}
	return s, json.Unmarshal(b, &s)
}

// anyStatus returns the status from whichever control node answers.
//
// Use this only for cluster-wide facts such as the leader's identity. Anything
// leader-only, edge membership being the obvious case, must come from leaderStatus,
// because a follower owns no liveness state and would report zero edges.
func (c *cluster) anyStatus() (status, bool) {
	for i := range c.controlAdmin {
		if s, err := c.status(i); err == nil && s.NodeID != "" {
			return s, true
		}
	}
	return status{}, false
}

// leaderStatus returns the status of the node that currently reports itself
// leader, which is the only node with an authoritative view of edge membership
// and the newest applied configuration.
func (c *cluster) leaderStatus() (status, bool) {
	for i := range c.controlAdmin {
		if s, err := c.status(i); err == nil && s.IsLeader {
			return s, true
		}
	}
	return status{}, false
}

func (c *cluster) waitForLeader(timeout time.Duration) string {
	c.t.Helper()
	var leader string
	c.waitFor(timeout, "a raft leader", func() bool {
		s, ok := c.anyStatus()
		if ok && s.LeaderID != "" {
			leader = s.LeaderID
			return true
		}
		return false
	})
	return leader
}

func (c *cluster) waitFor(timeout time.Duration, desc string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("condition never held within %s: %s", timeout, desc)
}

// ctl runs edgemeshctl against the cluster.
func (c *cluster) ctl(args ...string) (string, error) {
	full := append([]string{"--server", strings.Join(c.controlAdmin, ",")}, args...)
	out, err := exec.Command(filepath.Join(c.binDir, "edgemeshctl"), full...).CombinedOutput()
	return string(out), err
}

// configure applies a pool and a caching route through the CLI.
func (c *cluster) configure(originCount int) {
	c.t.Helper()

	var origins strings.Builder
	for i := 1; i <= originCount; i++ {
		fmt.Fprintf(&origins, `  - {id: origin-%d, scheme: http, host: 127.0.0.1, port: %d, weight: 1, health_path: /healthz, expected_statuses: [200]}
`, i, basePortOrig+i)
	}
	c.writeFile("pool.yaml", fmt.Sprintf(`id: e2e-pool
load_balancing: LOAD_BALANCING_ROUND_ROBIN
health_interval_ms: 1000
health_timeout_ms: 500
unhealthy_threshold: 2
healthy_threshold: 1
origins:
%s`, origins.String()))

	c.writeFile("route.yaml", `id: e2e-route
hostname: demo.edgemesh.local
path_prefix: /
origin_pool_id: e2e-pool
enabled: true
cache_policy: {enabled: true, default_ttl_seconds: 120, max_ttl_seconds: 3600}
retry_policy: {enabled: true, max_retries: 2, backoff_base_ms: 10, backoff_max_ms: 100}
header_policy: {diagnostic_headers: true}
`)

	if out, err := c.ctl("origins", "apply", "-f", filepath.Join(c.dataDir, "pool.yaml")); err != nil {
		c.t.Fatalf("apply pool: %v\nedgemeshctl output:\n%s\npool.yaml:\n%s",
			err, out, mustRead(c.t, filepath.Join(c.dataDir, "pool.yaml")))
	}
	if out, err := c.ctl("routes", "apply", "-f", filepath.Join(c.dataDir, "route.yaml")); err != nil {
		c.t.Fatalf("apply route: %v\n%s", err, out)
	}
	c.waitFor(20*time.Second, "the route reaches every edge", func() bool {
		for _, u := range c.edgeURLs {
			resp, _, err := c.get(u, "/static/warmup")
			if err != nil || resp.StatusCode != http.StatusOK {
				return false
			}
		}
		return true
	})
}

// mustRead reads a file for diagnostic output.
func mustRead(t *testing.T, path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return "<unreadable: " + err.Error() + ">"
	}
	return string(b)
}

// get issues a proxied request.
func (c *cluster) get(edgeURL, path string) (*http.Response, string, error) {
	req, err := http.NewRequest(http.MethodGet, edgeURL+path, nil)
	if err != nil {
		return nil, "", err
	}
	req.Host = demoHost
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(resp.Body)
	return resp, string(b), err
}

// originHits sums a counter across every origin.
func (c *cluster) originHits(counter string) int {
	total := 0
	for _, u := range c.originURLs {
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(u + "/stats")
		if err != nil {
			continue
		}
		var s struct {
			Counters map[string]int `json:"counters"`
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if json.Unmarshal(b, &s) == nil {
			total += s.Counters[counter]
		}
	}
	return total
}
