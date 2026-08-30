package routing

import (
	"testing"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

func route(id, host, prefix string) *edgemeshv1.Route {
	return &edgemeshv1.Route{
		Id:           id,
		Hostname:     host,
		PathPrefix:   prefix,
		OriginPoolId: "pool-1",
		Enabled:      true,
	}
}

func TestNormalizeHostname(t *testing.T) {
	cases := []struct{ in, want string }{
		{"demo.edgemesh.local", "demo.edgemesh.local"},
		{"DEMO.EdgeMesh.Local", "demo.edgemesh.local"},
		{"demo.edgemesh.local:8080", "demo.edgemesh.local"},
		{"demo.edgemesh.local.", "demo.edgemesh.local"},
		{"  demo.edgemesh.local  ", "demo.edgemesh.local"},
		{"[::1]:8080", "::1"},
		{"", ""},
	}
	for _, c := range cases {
		if got := NormalizeHostname(c.in); got != c.want {
			t.Errorf("NormalizeHostname(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestNormalizePath(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", "/"},
		{"/", "/"},
		{"api", "/api"},
		{"/api", "/api"},
		{"/api/", "/api"},
		{"/api///", "/api"},
		{"/api/v1/users", "/api/v1/users"},
	}
	for _, c := range cases {
		if got := NormalizePath(c.in); got != c.want {
			t.Errorf("NormalizePath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// The slash-boundary rule is the correctness-sensitive part of prefix matching.
func TestPrefixMatchesRespectsSlashBoundaries(t *testing.T) {
	cases := []struct {
		prefix, path string
		want         bool
	}{
		{"/api", "/api", true},
		{"/api", "/api/", true},
		{"/api", "/api/v1", true},
		{"/api", "/apix", false},
		{"/api", "/apix/v1", false},
		{"/api", "/ap", false},
		{"/api/", "/api/v1", true},
		{"/", "/anything", true},
		{"/", "/", true},
		{"/a/b", "/a/bc", false},
		{"/a/b", "/a/b/c", true},
	}
	for _, c := range cases {
		if got := PrefixMatches(c.prefix, c.path); got != c.want {
			t.Errorf("PrefixMatches(%q, %q) = %v, want %v", c.prefix, c.path, got, c.want)
		}
	}
}

func TestMatchLongestPrefixWins(t *testing.T) {
	tbl := NewTable([]*edgemeshv1.Route{
		route("root", "demo.local", "/"),
		route("api", "demo.local", "/api"),
		route("apiv1", "demo.local", "/api/v1"),
	}, 7)

	cases := []struct{ path, want string }{
		{"/", "root"},
		{"/other", "root"},
		{"/api", "api"},
		{"/api/v2", "api"},
		{"/api/v1", "apiv1"},
		{"/api/v1/users", "apiv1"},
		{"/apix", "root"},
	}
	for _, c := range cases {
		r, ok := tbl.Match("demo.local", c.path)
		if !ok {
			t.Fatalf("no match for %q", c.path)
		}
		if r.GetId() != c.want {
			t.Errorf("Match(%q) = %q, want %q", c.path, r.GetId(), c.want)
		}
	}
	if tbl.Version() != 7 {
		t.Errorf("version = %d, want 7", tbl.Version())
	}
}

func TestMatchIsHostScoped(t *testing.T) {
	tbl := NewTable([]*edgemeshv1.Route{
		route("a", "a.local", "/"),
		route("b", "b.local", "/"),
	}, 1)

	r, _ := tbl.Match("a.local", "/x")
	if r.GetId() != "a" {
		t.Errorf("got %q, want a", r.GetId())
	}
	r, _ = tbl.Match("B.LOCAL:8080", "/x")
	if r.GetId() != "b" {
		t.Errorf("host normalization failed, got %q", r.GetId())
	}
	if _, ok := tbl.Match("c.local", "/x"); ok {
		t.Error("unknown host must not match")
	}
}

func TestDisabledRoutesNeverMatch(t *testing.T) {
	disabled := route("api", "demo.local", "/api")
	disabled.Enabled = false
	tbl := NewTable([]*edgemeshv1.Route{disabled, route("root", "demo.local", "/")}, 1)

	r, ok := tbl.Match("demo.local", "/api/thing")
	if !ok || r.GetId() != "root" {
		t.Fatalf("disabled route matched or fell through incorrectly: %v ok=%v", r.GetId(), ok)
	}
	// It must still be addressable by ID for admin reads.
	if _, ok := tbl.ByID("api"); !ok {
		t.Error("disabled route must remain retrievable by id")
	}
	if tbl.Len() != 1 {
		t.Errorf("Len() = %d, want 1 enabled route", tbl.Len())
	}
}

// The wildcard host is a deliberate fallback, never a competitor to an exact
// hostname match.
func TestWildcardHostIsCheckedLast(t *testing.T) {
	tbl := NewTable([]*edgemeshv1.Route{
		route("exact", "demo.local", "/"),
		route("catchall", Wildcard, "/"),
		route("catchall-api", Wildcard, "/api"),
	}, 1)

	r, _ := tbl.Match("demo.local", "/api/x")
	if r.GetId() != "exact" {
		t.Errorf("exact host must win over wildcard, got %q", r.GetId())
	}
	r, _ = tbl.Match("other.local", "/api/x")
	if r.GetId() != "catchall-api" {
		t.Errorf("wildcard longest prefix expected, got %q", r.GetId())
	}
	r, _ = tbl.Match("other.local", "/z")
	if r.GetId() != "catchall" {
		t.Errorf("wildcard root expected, got %q", r.GetId())
	}
}

func TestMatchOnEmptyAndNilTable(t *testing.T) {
	if _, ok := NewTable(nil, 0).Match("h", "/"); ok {
		t.Error("empty table must not match")
	}
	var nilTable *Table
	if _, ok := nilTable.Match("h", "/"); ok {
		t.Error("nil table must not match")
	}
	if nilTable.Len() != 0 || nilTable.Version() != 0 {
		t.Error("nil table accessors must be zero-valued")
	}
}

// Route ordering must not depend on the order routes arrive from the control
// plane, otherwise two edges could disagree about which route serves a request.
func TestTableOrderIndependence(t *testing.T) {
	rs := []*edgemeshv1.Route{
		route("a", "demo.local", "/api"),
		route("b", "demo.local", "/api/v1"),
		route("c", "demo.local", "/"),
	}
	forward := NewTable(rs, 1)
	reversed := NewTable([]*edgemeshv1.Route{rs[2], rs[1], rs[0]}, 1)

	for _, p := range []string{"/", "/api", "/api/v1", "/api/v1/x", "/zzz"} {
		f, _ := forward.Match("demo.local", p)
		r, _ := reversed.Match("demo.local", p)
		if f.GetId() != r.GetId() {
			t.Fatalf("path %q resolved to %q vs %q depending on input order", p, f.GetId(), r.GetId())
		}
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*edgemeshv1.Route)
		wantErr bool
	}{
		{"valid", func(*edgemeshv1.Route) {}, false},
		{"missing id", func(r *edgemeshv1.Route) { r.Id = "" }, true},
		{"missing hostname", func(r *edgemeshv1.Route) { r.Hostname = "" }, true},
		{"hostname with slash", func(r *edgemeshv1.Route) { r.Hostname = "a/b" }, true},
		{"wildcard hostname", func(r *edgemeshv1.Route) { r.Hostname = Wildcard }, false},
		{"missing prefix", func(r *edgemeshv1.Route) { r.PathPrefix = "" }, true},
		{"relative prefix", func(r *edgemeshv1.Route) { r.PathPrefix = "api" }, true},
		{"prefix with query", func(r *edgemeshv1.Route) { r.PathPrefix = "/api?x=1" }, true},
		{"missing pool", func(r *edgemeshv1.Route) { r.OriginPoolId = "" }, true},
		{"ttl exceeds max", func(r *edgemeshv1.Route) {
			r.CachePolicy = &edgemeshv1.CachePolicy{DefaultTtlSeconds: 100, MaxTtlSeconds: 50}
		}, true},
		{"ttl within max", func(r *edgemeshv1.Route) {
			r.CachePolicy = &edgemeshv1.CachePolicy{DefaultTtlSeconds: 30, MaxTtlSeconds: 50}
		}, false},
		{"bad cacheable status", func(r *edgemeshv1.Route) {
			r.CachePolicy = &edgemeshv1.CachePolicy{CacheableStatuses: []int32{99}}
		}, true},
		{"rate limit without rate", func(r *edgemeshv1.Route) {
			r.RateLimitPolicy = &edgemeshv1.RateLimitPolicy{Enabled: true, Burst: 1}
		}, true},
		{"header strategy without header", func(r *edgemeshv1.Route) {
			r.RateLimitPolicy = &edgemeshv1.RateLimitPolicy{
				Enabled: true, RatePerSecond: 1, Burst: 1,
				KeyStrategy: edgemeshv1.KeyStrategy_KEY_STRATEGY_HEADER,
			}
		}, true},
		{"excessive retries", func(r *edgemeshv1.Route) {
			r.RetryPolicy = &edgemeshv1.RetryPolicy{Enabled: true, MaxRetries: 50}
		}, true},
		{"backoff base above max", func(r *edgemeshv1.Route) {
			r.RetryPolicy = &edgemeshv1.RetryPolicy{Enabled: true, MaxRetries: 2, BackoffBaseMs: 900, BackoffMaxMs: 100}
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := route("id", "demo.local", "/api")
			c.mutate(r)
			err := Validate(r)
			if c.wantErr != (err != nil) {
				t.Fatalf("Validate error = %v, wantErr = %v", err, c.wantErr)
			}
			if err != nil && !errs.IsClass(err, errs.ClassValidation) {
				t.Fatalf("expected a validation-classified error, got class %q", errs.ClassOf(err))
			}
		})
	}
	if err := Validate(nil); err == nil {
		t.Fatal("nil route must be rejected")
	}
}

func TestValidateSetRejectsDuplicates(t *testing.T) {
	if err := ValidateSet([]*edgemeshv1.Route{
		route("a", "demo.local", "/api"),
		route("b", "demo.local", "/api/"), // same after normalization
	}); err == nil {
		t.Fatal("duplicate (hostname, path_prefix) must be rejected")
	}
	if err := ValidateSet([]*edgemeshv1.Route{
		route("a", "demo.local", "/api"),
		route("a", "demo.local", "/other"),
	}); err == nil {
		t.Fatal("duplicate route id must be rejected")
	}
	if err := ValidateSet([]*edgemeshv1.Route{
		route("a", "demo.local", "/api"),
		route("b", "demo.local", "/other"),
		route("c", "other.local", "/api"),
	}); err != nil {
		t.Fatalf("valid set rejected: %v", err)
	}
}

func TestHolder(t *testing.T) {
	h := NewHolder()
	if h.Load() == nil || h.Load().Len() != 0 {
		t.Fatal("holder must start with a usable empty table")
	}
	h.Store(NewTable([]*edgemeshv1.Route{route("a", "demo.local", "/")}, 5))
	if h.Load().Version() != 5 {
		t.Fatal("store did not publish")
	}
	h.Store(nil)
	if h.Load().Version() != 5 {
		t.Fatal("nil store must not clear the table")
	}
}

func BenchmarkMatch(b *testing.B) {
	rs := make([]*edgemeshv1.Route, 0, 64)
	rs = append(rs, route("root", "demo.local", "/"))
	for i := 0; i < 63; i++ {
		rs = append(rs, route(string(rune('a'+i%26))+"-"+string(rune('0'+i%10)), "demo.local", "/api/v"+string(rune('0'+i%10))+"/x"))
	}
	tbl := NewTable(rs, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := tbl.Match("demo.local:8080", "/api/v3/x/resource"); !ok {
			b.Fatal("no match")
		}
	}
}
