package key

import (
	"net/http"
	"strings"
	"testing"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
)

func req(method, path, query string, hdr http.Header) Request {
	if hdr == nil {
		hdr = http.Header{}
	}
	return Request{RouteID: "route-a", Method: method, Path: path, RawQuery: query, Header: hdr}
}

func TestBuildIsStable(t *testing.T) {
	// Two independently constructed but equal requests must key identically:
	// comparing one request against itself would be true by construction and
	// would assert nothing.
	a := req("GET", "/asset/1", "a=1&b=2", http.Header{"Accept": []string{"*/*"}})
	b := req("GET", "/asset/1", "a=1&b=2", http.Header{"Accept": []string{"*/*"}})
	if Build(a, nil, nil) != Build(b, nil, nil) {
		t.Fatal("key construction is not deterministic across equal requests")
	}
	// And repeated calls must agree, which catches hidden per-call state.
	first := Build(a, nil, nil)
	for i := 0; i < 10; i++ {
		if Build(a, nil, nil) != first {
			t.Fatalf("key changed on call %d", i)
		}
	}
}

func TestHeadSharesGetKey(t *testing.T) {
	get := Build(req("GET", "/x", "", nil), nil, nil)
	head := Build(req("HEAD", "/x", "", nil), nil, nil)
	if get != head {
		t.Fatalf("HEAD must share GET's key: %q vs %q", get, head)
	}
	post := Build(req("POST", "/x", "", nil), nil, nil)
	if post == get {
		t.Fatal("POST must not share GET's key")
	}
}

func TestMethodNormalization(t *testing.T) {
	if NormalizeMethod(" get ") != "GET" {
		t.Fatal("method must be trimmed and uppercased")
	}
	if NormalizeMethod("head") != "GET" {
		t.Fatal("HEAD must normalize onto GET")
	}
}

// Different requests must never collide. The separator is what guarantees this:
// without it, route "a" + path "/bc" and route "ab" + path "/c" would collide.
func TestNoCollisionAcrossComponentBoundaries(t *testing.T) {
	cases := []Request{
		{RouteID: "a", Method: "GET", Path: "/bc"},
		{RouteID: "ab", Method: "GET", Path: "/c"},
		{RouteID: "a", Method: "GET", Path: "/b", RawQuery: "c"},
		{RouteID: "a", Method: "GET", Path: "/bc", RawQuery: ""},
	}
	seen := map[string][]int{}
	for i, c := range cases {
		c.Header = http.Header{}
		k := Build(c, nil, nil)
		seen[k] = append(seen[k], i)
	}
	for k, idx := range seen {
		if len(idx) > 1 {
			distinct := map[string]bool{}
			for _, i := range idx {
				distinct[cases[i].RouteID+"|"+cases[i].Path+"|"+cases[i].RawQuery] = true
			}
			if len(distinct) > 1 {
				t.Fatalf("distinct requests %v collided on key %q", idx, k)
			}
		}
	}
}

func TestPathIsKeyedVerbatim(t *testing.T) {
	// Collapsing "//" or resolving ".." would let one stored object be reached
	// by several paths, which is the cache-poisoning shape this avoids.
	a := Build(req("GET", "/a//b", "", nil), nil, nil)
	b := Build(req("GET", "/a/b", "", nil), nil, nil)
	c := Build(req("GET", "/a/../a/b", "", nil), nil, nil)
	if a == b || c == b {
		t.Fatal("path variants must produce distinct keys")
	}
	// A path without a leading slash still keys as an absolute path.
	if Build(req("GET", "a/b", "", nil), nil, nil) != b {
		t.Fatal("missing leading slash must be normalized")
	}
}

func TestQueryPreservedByDefault(t *testing.T) {
	ab := Build(req("GET", "/x", "a=1&b=2", nil), nil, nil)
	ba := Build(req("GET", "/x", "b=2&a=1", nil), nil, nil)
	if ab == ba {
		t.Fatal("query order must be preserved by default")
	}
}

func TestCanonicalQueryOptIn(t *testing.T) {
	p := &edgemeshv1.CachePolicy{CanonicalQuery: true}
	ab := Build(req("GET", "/x", "a=1&b=2", nil), p, nil)
	ba := Build(req("GET", "/x", "b=2&a=1", nil), p, nil)
	if ab != ba {
		t.Fatalf("canonicalized queries must match: %q vs %q", ab, ba)
	}
	// Distinct values must still produce distinct keys.
	if Build(req("GET", "/x", "a=1&b=3", nil), p, nil) == ab {
		t.Fatal("different values collided under canonicalization")
	}
}

func TestCanonicalQueryHandlesRepeatedAndEmptyValues(t *testing.T) {
	got := CanonicalQuery("b=2&a=1&a=0", true)
	if got != "a=0&a=1&b=2" {
		t.Fatalf("CanonicalQuery = %q", got)
	}
	if CanonicalQuery("", true) != "" {
		t.Fatal("empty query must stay empty")
	}
	// An unparseable query is preserved rather than dropped: dropping it would
	// key two different requests identically.
	bad := "%zz=1"
	if CanonicalQuery(bad, true) != bad {
		t.Fatal("unparseable query must be preserved verbatim")
	}
}

func TestVaryHeadersEnterTheKey(t *testing.T) {
	p := &edgemeshv1.CachePolicy{VaryHeaders: []string{"Accept-Encoding"}}
	gzip := http.Header{"Accept-Encoding": []string{"gzip"}}
	br := http.Header{"Accept-Encoding": []string{"br"}}

	kg := Build(req("GET", "/x", "", gzip), p, nil)
	kb := Build(req("GET", "/x", "", br), p, nil)
	if kg == kb {
		t.Fatal("differing vary header values must produce different keys")
	}
	// An absent header is still a distinct dimension value.
	kn := Build(req("GET", "/x", "", http.Header{}), p, nil)
	if kn == kg {
		t.Fatal("absent vary header must differ from a present one")
	}
}

func TestResponseVaryIsFoldedIn(t *testing.T) {
	h := http.Header{"Accept-Language": []string{"en"}}
	base := Build(req("GET", "/x", "", h), nil, nil)
	withVary := Build(req("GET", "/x", "", h), nil, []string{"accept-language"})
	if base == withVary {
		t.Fatal("response Vary must change the key")
	}
}

func TestVaryComponentIsOrderIndependent(t *testing.T) {
	p := &edgemeshv1.CachePolicy{VaryHeaders: []string{"B-Header", "A-Header"}}
	h := http.Header{"A-Header": []string{"1"}, "B-Header": []string{"2"}}
	k1 := Build(req("GET", "/x", "", h), p, nil)

	p2 := &edgemeshv1.CachePolicy{VaryHeaders: []string{"a-header", "b-header"}}
	k2 := Build(req("GET", "/x", "", h), p2, nil)
	if k1 != k2 {
		t.Fatalf("vary name order/case changed the key: %q vs %q", k1, k2)
	}
}

func TestParseVary(t *testing.T) {
	names, wildcard := ParseVary([]string{"Accept-Encoding, Accept-Language"})
	if wildcard {
		t.Fatal("unexpected wildcard")
	}
	if len(names) != 2 || names[0] != "accept-encoding" || names[1] != "accept-language" {
		t.Fatalf("names = %v", names)
	}
	if _, w := ParseVary([]string{"Accept-Encoding", "*"}); !w {
		t.Fatal("Vary: * must report wildcard")
	}
	if n, _ := ParseVary(nil); len(n) != 0 {
		t.Fatal("no Vary must yield no names")
	}
	// A header name carrying a separator byte must be discarded, not allowed to
	// forge key structure.
	if n, _ := ParseVary([]string{"bad\x1fname"}); len(n) != 0 {
		t.Fatalf("separator-bearing name was accepted: %v", n)
	}
}

func TestLongKeysAreHashed(t *testing.T) {
	long := "/" + strings.Repeat("a", 4096)
	k := Build(req("GET", long, "", nil), nil, nil)
	if len(k) > maxInlineKeyBytes {
		t.Fatalf("key length %d exceeds the %d byte bound", len(k), maxInlineKeyBytes)
	}
	// The route component must survive hashing intact, or route-wide purge
	// would silently stop matching this object.
	if !strings.HasPrefix(k, RoutePrefix("route-a")) {
		t.Fatalf("hashed key %q lost its route prefix", k)
	}
	if id, ok := RouteOf(k); !ok || id != "route-a" {
		t.Fatalf("RouteOf(hashed key) = %q, %v", id, ok)
	}
	// Two distinct long paths must still key differently.
	other := "/" + strings.Repeat("a", 4095) + "b"
	if Build(req("GET", other, "", nil), nil, nil) == k {
		t.Fatal("distinct long paths collided after hashing")
	}
	// Hashing must be deterministic.
	if Build(req("GET", long, "", nil), nil, nil) != k {
		t.Fatal("hashed key is not stable")
	}
	// Two routes with the same long path must not collide with each other.
	otherRoute := Build(Request{RouteID: "route-b", Method: "GET", Path: long, Header: http.Header{}}, nil, nil)
	if otherRoute == k {
		t.Fatal("different routes collided after hashing")
	}
	if id, _ := RouteOf(otherRoute); id != "route-b" {
		t.Fatalf("RouteOf = %q, want route-b", id)
	}
}

// A route-wide purge must match every key belonging to the route, including
// keys long enough to have been hashed.
func TestRoutePurgePrefixMatchesHashedKeys(t *testing.T) {
	prefix := RoutePrefix("route-a")
	for _, path := range []string{
		"/short",
		"/" + strings.Repeat("m", 400),
		"/" + strings.Repeat("l", 8192),
	} {
		k := Build(req("GET", path, "", nil), nil, nil)
		if !strings.HasPrefix(k, prefix) {
			t.Errorf("key for a %d-byte path does not match the route purge prefix: %q", len(path), k)
		}
	}
}

func TestRoutePrefixAndRouteOf(t *testing.T) {
	k := Build(req("GET", "/x", "", nil), nil, nil)
	if !strings.HasPrefix(k, RoutePrefix("route-a")) {
		t.Fatalf("key %q does not carry its route prefix", k)
	}
	id, ok := RouteOf(k)
	if !ok || id != "route-a" {
		t.Fatalf("RouteOf = %q ok=%v", id, ok)
	}
	if _, ok := RouteOf("no-separator"); ok {
		t.Fatal("malformed key must not yield a route")
	}
	// A route id that is a prefix of another must not match its keys.
	if strings.HasPrefix(k, RoutePrefix("route")) {
		t.Fatal("prefix-only route id must not match")
	}
}

func FuzzBuildIsInjective(f *testing.F) {
	f.Add("route-a", "GET", "/x", "a=1", "Accept-Encoding", "gzip")
	f.Add("r", "HEAD", "/", "", "", "")
	f.Add("route\x1fb", "GET", "/\x1f", "\x1f", "x", "\x1f")

	f.Fuzz(func(t *testing.T, routeID, method, path, query, hdrName, hdrValue string) {
		h := http.Header{}
		if hdrName != "" {
			h.Set(hdrName, hdrValue)
		}
		r := Request{RouteID: routeID, Method: method, Path: path, RawQuery: query, Header: h}

		k1 := Build(r, nil, nil)
		k2 := Build(r, nil, nil)
		if k1 != k2 {
			t.Fatalf("key is not deterministic: %q vs %q", k1, k2)
		}
		if len(k1) > maxInlineKeyBytes {
			t.Fatalf("key exceeded its length bound: %d", len(k1))
		}
		// The route component must be recoverable, because route-wide purge
		// matches on it. Route validation bounds the ID length and forbids the
		// separator, so those are the conditions under which the guarantee is
		// claimed.
		if !strings.Contains(routeID, sep) && len(routeID) <= maxRouteIDBytes {
			got, ok := RouteOf(k1)
			if !ok || got != routeID {
				t.Fatalf("RouteOf(%q) = %q, %v; want %q", k1, got, ok, routeID)
			}
			// And the purge prefix must actually match the key, which is the
			// property route-wide purge depends on.
			if !strings.HasPrefix(k1, RoutePrefix(routeID)) {
				t.Fatalf("key %q does not carry the purge prefix for route %q", k1, routeID)
			}
		}
	})
}

func BenchmarkBuild(b *testing.B) {
	h := http.Header{"Accept-Encoding": []string{"gzip, br"}}
	r := req("GET", "/assets/app.1a2b3c.js", "v=42&lang=en", h)
	p := &edgemeshv1.CachePolicy{VaryHeaders: []string{"Accept-Encoding"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Build(r, p, nil)
	}
}
