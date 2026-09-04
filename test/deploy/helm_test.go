// Package deploy validates that the deployment artifacts produce configuration
// EdgeMesh itself accepts.
//
// A Helm chart that lints but renders a configuration the binary rejects is a
// deployment that fails at pod start rather than at install time. These tests
// close that gap by feeding the chart's own output through the real config
// loader.
package deploy

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/sheehanlloyd/edgemesh/internal/config"
)

// chartPath is relative to this package.
const chartPath = "../../deploy/helm/edgemesh"

// render runs `helm template` with the given overrides.
func render(t *testing.T, args ...string) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed; skipping chart rendering tests")
	}
	full := append([]string{"template", "edgemesh", chartPath}, args...)
	out, err := exec.Command("helm", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %v failed: %v\n%s", args, err, out)
	}
	return string(out)
}

// configMaps extracts the node configurations embedded in the rendered chart,
// substituting the placeholders the init container fills in at runtime.
func configMaps(t *testing.T, rendered string) map[string]string {
	t.Helper()
	dec := yaml.NewDecoder(strings.NewReader(rendered))
	out := map[string]string{}
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			break
		}
		if doc == nil || doc["kind"] != "ConfigMap" {
			continue
		}
		data, _ := doc["data"].(map[string]any)
		for name, v := range data {
			s, _ := v.(string)
			out[name] = s
		}
	}
	if len(out) == 0 {
		t.Fatal("the chart rendered no node configuration")
	}
	return out
}

// substitute performs the same replacement the init container does.
func substitute(tmpl, nodeID, fqdn string) string {
	return strings.NewReplacer(
		"__NODE_ID__", nodeID,
		"__POD_FQDN__", fqdn,
		"__REGION__", "test-region",
		"__ZONE__", "test-zone",
	).Replace(tmpl)
}

// The configurations the chart produces must load and validate, or the pods
// crash-loop on a configuration the chart happily installed.
func TestChartRendersLoadableConfigs(t *testing.T) {
	rendered := render(t)
	cms := configMaps(t, rendered)

	dir := t.TempDir()

	controlTmpl, ok := cms["control.yaml.tmpl"]
	if !ok {
		t.Fatal("no control configuration in the rendered chart")
	}
	// Node 0 of the StatefulSet, whose id must appear in the peer list.
	controlPath := filepath.Join(dir, "control.yaml")
	if err := os.WriteFile(controlPath, []byte(substitute(controlTmpl,
		"edgemesh-control-0",
		"edgemesh-control-0.edgemesh-control-headless.default.svc.cluster.local")), 0o600); err != nil {
		t.Fatal(err)
	}
	cc, err := config.LoadControl(controlPath)
	if err != nil {
		t.Fatalf("the chart's control configuration is invalid: %v", err)
	}
	if len(cc.Raft.Peers) != 3 {
		t.Fatalf("rendered peer count = %d, want 3", len(cc.Raft.Peers))
	}
	// Every peer must be dialable: a wildcard or empty host would make Raft
	// unable to reach anyone.
	for _, p := range cc.Raft.Peers {
		if !strings.Contains(p.Address, ".svc.cluster.local:") {
			t.Errorf("peer %q has address %q, which is not stable pod DNS", p.ID, p.Address)
		}
	}

	edgeTmpl, ok := cms["edge.yaml.tmpl"]
	if !ok {
		t.Fatal("no edge configuration in the rendered chart")
	}
	edgePath := filepath.Join(dir, "edge.yaml")
	if err := os.WriteFile(edgePath, []byte(substitute(edgeTmpl,
		"edgemesh-edge-0",
		"edgemesh-edge-0.edgemesh-edge-headless.default.svc.cluster.local")), 0o600); err != nil {
		t.Fatal(err)
	}
	ec, err := config.LoadEdge(edgePath)
	if err != nil {
		t.Fatalf("the chart's edge configuration is invalid: %v", err)
	}
	if len(ec.ControlPlane.Endpoints) != 3 {
		t.Fatalf("rendered control endpoint count = %d, want 3", len(ec.ControlPlane.Endpoints))
	}
	// Peers dial this address directly, so it must be per-pod DNS rather than
	// the load-balanced service.
	if !strings.Contains(ec.Node.AdvertisePeerAddress, "edgemesh-edge-0.") {
		t.Fatalf("advertised peer address %q is not per-pod DNS", ec.Node.AdvertisePeerAddress)
	}
}

// A production-mode release must render a configuration that passes the
// binary's production checks, not merely the chart's own.
func TestProductionValuesRenderProductionConfigs(t *testing.T) {
	rendered := render(t,
		"--set", "mode=production",
		"--set", "security.mtls.enabled=true",
		"--set", "adminAuth.enabled=true",
		"--set", "adminAuth.existingSecret=edgemesh-admin",
		"--set", "edge.originSecurity.allowedCIDRs={10.0.0.0/8}",
	)
	cms := configMaps(t, rendered)
	dir := t.TempDir()

	controlPath := filepath.Join(dir, "control.yaml")
	if err := os.WriteFile(controlPath, []byte(substitute(cms["control.yaml.tmpl"],
		"edgemesh-control-0",
		"edgemesh-control-0.edgemesh-control-headless.default.svc.cluster.local")), 0o600); err != nil {
		t.Fatal(err)
	}
	cc, err := config.LoadControl(controlPath)
	if err != nil {
		t.Fatalf("the production control configuration is invalid: %v", err)
	}
	if cc.Mode != config.ModeProduction {
		t.Fatalf("mode = %q, want production", cc.Mode)
	}
	if !cc.Security.Enabled {
		t.Fatal("production mode rendered a configuration with mTLS disabled")
	}
	if !cc.AdminAuth.Enabled || cc.AdminAuth.TokenFile == "" {
		t.Fatalf("production mode rendered no admin token source: %+v", cc.AdminAuth)
	}

	edgePath := filepath.Join(dir, "edge.yaml")
	if err := os.WriteFile(edgePath, []byte(substitute(cms["edge.yaml.tmpl"],
		"edgemesh-edge-0",
		"edgemesh-edge-0.edgemesh-edge-headless.default.svc.cluster.local")), 0o600); err != nil {
		t.Fatal(err)
	}
	ec, err := config.LoadEdge(edgePath)
	if err != nil {
		t.Fatalf("the production edge configuration is invalid: %v", err)
	}
	if !ec.Security.Enabled {
		t.Fatal("production mode rendered an edge with mTLS disabled")
	}
	if ec.Telemetry.EnablePprof {
		t.Fatal("production mode rendered an edge exposing pprof")
	}
}

// The chart must refuse configurations that cannot work, at install time
// rather than at pod start.
func TestChartRejectsUnworkableValues(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	cases := []struct {
		name string
		args []string
	}{
		{"control count below three", []string{"--set", "control.replicaCount=1"}},
		{"even control count", []string{"--set", "control.replicaCount=4"}},
		{"non power-of-two shards", []string{"--set", "edge.cache.shards=100"}},
		{"replication factor above edge count", []string{"--set", "edge.cache.replicationFactor=9"}},
		{"object larger than the tier", []string{"--set", "edge.cache.maxObjectBytes=999999999999"}},
		{"production without mTLS", []string{"--set", "mode=production"}},
		{"admin auth without a secret", []string{"--set", "adminAuth.enabled=true"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			args := append([]string{"template", "edgemesh", chartPath}, c.args...)
			if out, err := exec.Command("helm", args...).CombinedOutput(); err == nil {
				t.Fatalf("the chart accepted an unworkable configuration\n%s", out)
			}
		})
	}
}

// The Compose configurations ship with the repository and must stay valid.
func TestComposeConfigsAreValid(t *testing.T) {
	for _, n := range []string{"1", "2", "3"} {
		if _, err := config.LoadControl(filepath.Join("..", "..", "deploy", "compose", "configs", "control-"+n+".yaml")); err != nil {
			t.Errorf("deploy/compose/configs/control-%s.yaml: %v", n, err)
		}
		if _, err := config.LoadEdge(filepath.Join("..", "..", "deploy", "compose", "configs", "edge-"+n+".yaml")); err != nil {
			t.Errorf("deploy/compose/configs/edge-%s.yaml: %v", n, err)
		}
	}
}
