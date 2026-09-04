package fixtures

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/origin"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
	"github.com/sheehanlloyd/edgemesh/internal/routing"
)

const examplesDir = "../../configs/examples"

// loadExample parses a YAML example into a protobuf message exactly as the CLI
// does: YAML, then JSON, then protojson. Using the real path means a document
// that passes here is one the admin API would accept.
func loadExample(t *testing.T, name string, m proto.Message) {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(examplesDir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s as YAML: %v", name, err)
	}
	jsonBytes, err := yamlToJSON(doc)
	if err != nil {
		t.Fatalf("convert %s to JSON: %v", name, err)
	}
	// Unknown fields are rejected, exactly as the admin API rejects them: a
	// misspelled policy field in an example would teach the wrong name.
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(jsonBytes, m); err != nil {
		t.Fatalf("decode %s: %v\n\nThe admin API would reject this document.", name, err)
	}
}

func TestExampleRoutesAreValid(t *testing.T) {
	for _, name := range []string{"route-basic.yaml", "route-advanced.yaml"} {
		t.Run(name, func(t *testing.T) {
			var route edgemeshv1.Route
			loadExample(t, name, &route)

			if err := routing.Validate(&route); err != nil {
				t.Fatalf("%s fails route validation: %v", name, err)
			}
			if route.GetId() == "" || route.GetHostname() == "" {
				t.Fatalf("%s decoded to an empty route: %+v", name, &route)
			}
		})
	}
}

func TestExampleOriginPoolIsValid(t *testing.T) {
	var pool edgemeshv1.OriginPool
	loadExample(t, "origin-pool.yaml", &pool)

	if err := origin.ValidatePool(&pool); err != nil {
		t.Fatalf("origin-pool.yaml fails validation: %v", err)
	}
	if len(pool.GetOrigins()) < 2 {
		t.Fatalf("the example pool should show more than one origin, got %d", len(pool.GetOrigins()))
	}
}

func TestExampleSettingsAreValid(t *testing.T) {
	var settings edgemeshv1.GlobalSettings
	loadExample(t, "settings.yaml", &settings)

	// Settings are validated by the state machine, so applying them is the
	// honest check.
	sm := statemachine.New()
	if _, err := sm.Apply(1, &statemachine.Command{
		Type: statemachine.CommandSetGlobalSettings, TimestampUnixMs: 1, Settings: &settings,
	}); err != nil {
		t.Fatalf("settings.yaml is rejected by the state machine: %v", err)
	}
}

// The examples must work together: applying the pool and then the route must
// succeed, including the cross-object reference between them.
func TestExamplesApplyTogether(t *testing.T) {
	var pool edgemeshv1.OriginPool
	loadExample(t, "origin-pool.yaml", &pool)
	var route edgemeshv1.Route
	loadExample(t, "route-basic.yaml", &route)

	if route.GetOriginPoolId() != pool.GetId() {
		t.Fatalf("route-basic.yaml references pool %q but origin-pool.yaml defines %q; "+
			"the examples would not work as a pair",
			route.GetOriginPoolId(), pool.GetId())
	}

	sm := statemachine.New()
	if _, err := sm.Apply(1, &statemachine.Command{
		Type: statemachine.CommandCreateOriginPool, TimestampUnixMs: 1, OriginPool: &pool,
	}); err != nil {
		t.Fatalf("applying the example pool failed: %v", err)
	}
	if _, err := sm.Apply(2, &statemachine.Command{
		Type: statemachine.CommandCreateRoute, TimestampUnixMs: 1, Route: &route,
	}); err != nil {
		t.Fatalf("applying the example route failed: %v", err)
	}

	// The pool cannot be deleted while the route references it, which is what
	// keeps a route from pointing at nothing.
	if _, err := sm.Apply(3, &statemachine.Command{
		Type: statemachine.CommandDeleteOriginPool, TimestampUnixMs: 1, ID: pool.GetId(),
	}); err == nil {
		t.Fatal("a referenced pool was deletable")
	}
}

// The advanced example must not collide with the basic one, so an operator can
// apply both.
func TestExampleRoutesDoNotCollide(t *testing.T) {
	var basic, advanced edgemeshv1.Route
	loadExample(t, "route-basic.yaml", &basic)
	loadExample(t, "route-advanced.yaml", &advanced)

	if err := routing.ValidateSet([]*edgemeshv1.Route{&basic, &advanced}); err != nil {
		t.Fatalf("the two example routes cannot coexist: %v", err)
	}
}
