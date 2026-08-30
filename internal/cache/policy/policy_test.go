package policy

import (
	"net/http"
	"testing"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
)

func enabled() *edgemeshv1.CachePolicy {
	return &edgemeshv1.CachePolicy{Enabled: true, DefaultTtlSeconds: 60, MaxTtlSeconds: 3600}
}

func hdr(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Add(kv[i], kv[i+1])
	}
	return h
}

func TestEvaluateRequest(t *testing.T) {
	cases := []struct {
		name   string
		method string
		h      http.Header
		p      *edgemeshv1.CachePolicy
		want   bool
		reason Reason
	}{
		{"get is cacheable", "GET", hdr(), enabled(), true, ReasonCacheable},
		{"head is cacheable", "HEAD", hdr(), enabled(), true, ReasonCacheable},
		{"lowercase method", "get", hdr(), enabled(), true, ReasonCacheable},
		{"post is not", "POST", hdr(), enabled(), false, ReasonMethodNotAllowed},
		{"put is not", "PUT", hdr(), enabled(), false, ReasonMethodNotAllowed},
		{"policy disabled", "GET", hdr(), &edgemeshv1.CachePolicy{}, false, ReasonPolicyDisabled},
		{"nil policy", "GET", hdr(), nil, false, ReasonPolicyDisabled},
		{"authorization bypasses", "GET", hdr("Authorization", "Bearer x"), enabled(), false, ReasonAuthorization},
		{"cookie bypasses", "GET", hdr("Cookie", "sid=1"), enabled(), false, ReasonCookie},
		{"range bypasses", "GET", hdr("Range", "bytes=0-99"), enabled(), false, ReasonRangeRequest},
		{"request no-store", "GET", hdr("Cache-Control", "no-store"), enabled(), false, ReasonRequestNoStore},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := EvaluateRequest(c.method, c.h, c.p)
			if d.Cacheable != c.want || d.Reason != c.reason {
				t.Fatalf("got cacheable=%v reason=%q, want %v/%q", d.Cacheable, d.Reason, c.want, c.reason)
			}
		})
	}
}

func TestAllowedCookiesOptIn(t *testing.T) {
	p := enabled()
	p.AllowedCookies = []string{"lang", "theme"}

	if d := EvaluateRequest("GET", hdr("Cookie", "lang=en; theme=dark"), p); !d.Cacheable {
		t.Fatalf("allowlisted cookies must be tolerated, got %q", d.Reason)
	}
	if d := EvaluateRequest("GET", hdr("Cookie", "lang=en; sid=secret"), p); d.Cacheable {
		t.Fatal("an unlisted cookie must bypass the cache")
	}
	// Cookies split across multiple header lines must all be checked.
	h := hdr("Cookie", "lang=en")
	h.Add("Cookie", "sid=secret")
	if d := EvaluateRequest("GET", h, p); d.Cacheable {
		t.Fatal("an unlisted cookie on a second header line must bypass")
	}
}

func TestRequestNoCacheStillAdmits(t *testing.T) {
	d := EvaluateRequest("GET", hdr("Cache-Control", "no-cache"), enabled())
	if !d.Cacheable {
		t.Fatal("no-cache must not make the request uncacheable")
	}
	if !d.NoCache {
		t.Fatal("no-cache must be reported so the caller revalidates")
	}
}

func TestEvaluateResponseDefaults(t *testing.T) {
	now := time.Now()
	const maxObj = 1 << 20

	d := EvaluateResponse(200, hdr(), 100, maxObj, enabled(), now)
	if !d.Admit || d.TTL != 60*time.Second {
		t.Fatalf("route default TTL not applied: admit=%v ttl=%s", d.Admit, d.TTL)
	}
	// Default policy: 200 only.
	for _, s := range []int{201, 204, 301, 404, 500} {
		if EvaluateResponse(s, hdr(), 100, maxObj, enabled(), now).Admit {
			t.Fatalf("status %d must not be admitted by default", s)
		}
	}
}

func TestEvaluateResponseRejections(t *testing.T) {
	now := time.Now()
	const maxObj = 1024

	cases := []struct {
		name   string
		status int
		h      http.Header
		size   int64
		reason Reason
	}{
		{"no-store", 200, hdr("Cache-Control", "no-store"), 10, ReasonResponseNoStore},
		{"private", 200, hdr("Cache-Control", "private"), 10, ReasonResponsePrivate},
		{"set-cookie", 200, hdr("Set-Cookie", "sid=1"), 10, ReasonSetCookie},
		{"vary wildcard", 200, hdr("Vary", "*"), 10, ReasonVaryWildcard},
		{"too large", 200, hdr(), maxObj + 1, ReasonTooLarge},
		{"status", 503, hdr(), 10, ReasonStatusNotAllowed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := EvaluateResponse(c.status, c.h, c.size, maxObj, enabled(), now)
			if d.Admit {
				t.Fatal("expected rejection")
			}
			if d.Reason != c.reason {
				t.Fatalf("reason = %q, want %q", d.Reason, c.reason)
			}
		})
	}
}

func TestSetCookieOptIn(t *testing.T) {
	now := time.Now()
	p := enabled()
	p.AllowSetCookie = true
	if !EvaluateResponse(200, hdr("Set-Cookie", "a=1"), 10, 1<<20, p, now).Admit {
		t.Fatal("explicit opt-in must permit Set-Cookie admission")
	}
}

func TestTTLPrecedence(t *testing.T) {
	now := time.Now()
	const maxObj = 1 << 20
	p := enabled()

	// s-maxage beats max-age because EdgeMesh is a shared cache.
	d := EvaluateResponse(200, hdr("Cache-Control", "max-age=100, s-maxage=30"), 10, maxObj, p, now)
	if d.TTL != 30*time.Second {
		t.Fatalf("s-maxage must win, got %s", d.TTL)
	}
	// max-age beats the route default.
	d = EvaluateResponse(200, hdr("Cache-Control", "max-age=120"), 10, maxObj, p, now)
	if d.TTL != 120*time.Second {
		t.Fatalf("max-age not applied, got %s", d.TTL)
	}
	// Expires beats the route default when no directive is present.
	h := hdr("Expires", now.Add(45*time.Second).UTC().Format(http.TimeFormat),
		"Date", now.UTC().Format(http.TimeFormat))
	d = EvaluateResponse(200, h, 10, maxObj, p, now)
	if d.TTL < 44*time.Second || d.TTL > 46*time.Second {
		t.Fatalf("Expires not applied, got %s", d.TTL)
	}
	// The route max caps everything.
	p.MaxTtlSeconds = 10
	d = EvaluateResponse(200, hdr("Cache-Control", "max-age=9999"), 10, maxObj, p, now)
	if d.TTL != 10*time.Second {
		t.Fatalf("max TTL cap not applied, got %s", d.TTL)
	}
}

func TestZeroTTLIsNotAdmitted(t *testing.T) {
	now := time.Now()
	p := &edgemeshv1.CachePolicy{Enabled: true} // no default TTL
	d := EvaluateResponse(200, hdr(), 10, 1<<20, p, now)
	if d.Admit || d.Reason != ReasonNoTTL {
		t.Fatalf("expected no-TTL rejection, got admit=%v reason=%q", d.Admit, d.Reason)
	}
	// An explicit max-age=0 is also not admissible.
	d = EvaluateResponse(200, hdr("Cache-Control", "max-age=0"), 10, 1<<20, enabled(), now)
	if d.Admit {
		t.Fatal("max-age=0 must not be admitted")
	}
}

func TestUnknownSizeIsAdmitted(t *testing.T) {
	// A chunked response has no declared length. The caller enforces the byte
	// bound while streaming; policy must not reject it up front.
	d := EvaluateResponse(200, hdr(), -1, 1024, enabled(), time.Now())
	if !d.Admit {
		t.Fatalf("unknown-length response rejected: %q", d.Reason)
	}
}

func TestNegativeCaching(t *testing.T) {
	now := time.Now()
	p := enabled()
	p.NegativeCaching = true
	p.NegativeTtlSeconds = 5

	d := EvaluateResponse(404, hdr(), 10, 1<<20, p, now)
	if !d.Admit || !d.Negative || d.TTL != 5*time.Second {
		t.Fatalf("negative caching: admit=%v negative=%v ttl=%s", d.Admit, d.Negative, d.TTL)
	}
	// A long origin max-age must not extend a negative cache entry.
	d = EvaluateResponse(500, hdr("Cache-Control", "max-age=86400"), 10, 1<<20, p, now)
	if !d.Admit || d.TTL != 5*time.Second {
		t.Fatalf("origin freshness leaked into a negative entry: ttl=%s", d.TTL)
	}
	// Statuses outside the conservative list stay uncached.
	if EvaluateResponse(418, hdr(), 10, 1<<20, p, now).Admit {
		t.Fatal("418 must not be negatively cached")
	}
	// Without the opt-in nothing negative is cached.
	p.NegativeCaching = false
	if EvaluateResponse(404, hdr(), 10, 1<<20, p, now).Admit {
		t.Fatal("negative caching must be opt-in")
	}
}

func TestStaleWhileRevalidate(t *testing.T) {
	now := time.Now()
	p := enabled()
	p.StaleWhileRevalidateSeconds = 30

	d := EvaluateResponse(200, hdr(), 10, 1<<20, p, now)
	if d.StaleWhileRevalidate != 30*time.Second {
		t.Fatalf("route swr not applied: %s", d.StaleWhileRevalidate)
	}
	// The origin's directive overrides the route default.
	d = EvaluateResponse(200, hdr("Cache-Control", "max-age=60, stale-while-revalidate=90"), 10, 1<<20, p, now)
	if d.StaleWhileRevalidate != 90*time.Second {
		t.Fatalf("origin swr not applied: %s", d.StaleWhileRevalidate)
	}
}

func TestParseCacheControl(t *testing.T) {
	cc := ParseCacheControl([]string{"public, max-age=300, s-maxage=60, stale-while-revalidate=30, must-revalidate"})
	if !cc.Public || !cc.MustRevalidate {
		t.Fatal("flags not parsed")
	}
	if cc.MaxAge != 300*time.Second || cc.SMaxAge != 60*time.Second || cc.StaleWhileRevalidate != 30*time.Second {
		t.Fatalf("durations not parsed: %+v", cc)
	}
	// A malformed delta-seconds must be ignored, not treated as zero: treating
	// it as zero would silently disable caching.
	cc = ParseCacheControl([]string{"max-age=abc"})
	if cc.HasMaxAge {
		t.Fatal("malformed max-age must be ignored")
	}
	cc = ParseCacheControl([]string{"max-age=-5"})
	if cc.HasMaxAge {
		t.Fatal("negative max-age must be ignored")
	}
	cc = ParseCacheControl([]string{"max-age"})
	if cc.HasMaxAge {
		t.Fatal("valueless max-age must be ignored")
	}
	// Quoted and spaced forms are accepted.
	cc = ParseCacheControl([]string{` max-age = "42" `})
	if !cc.HasMaxAge || cc.MaxAge != 42*time.Second {
		t.Fatalf("quoted max-age not parsed: %+v", cc)
	}
	// Absurd values are clamped rather than overflowing downstream arithmetic.
	cc = ParseCacheControl([]string{"max-age=999999999999"})
	if cc.MaxAge != DefaultMaxTTL {
		t.Fatalf("clamp not applied: %s", cc.MaxAge)
	}
}

func TestWithExpires(t *testing.T) {
	now := time.Now()
	// An explicit directive wins; Expires is not consulted.
	cc := ParseCacheControl([]string{"max-age=10"}).WithExpires(
		hdr("Expires", now.Add(time.Hour).UTC().Format(http.TimeFormat)), now)
	if cc.HasExpires {
		t.Fatal("Expires must not override an explicit max-age")
	}
	// An invalid Expires means already-expired.
	cc = ParseCacheControl(nil).WithExpires(hdr("Expires", "0"), now)
	if cc.HasExpires {
		t.Fatal("invalid Expires must not yield a TTL")
	}
	// A past Expires yields no TTL.
	cc = ParseCacheControl(nil).WithExpires(
		hdr("Expires", now.Add(-time.Hour).UTC().Format(http.TimeFormat)), now)
	if cc.HasExpires {
		t.Fatal("past Expires must not yield a TTL")
	}
}

func FuzzParseCacheControl(f *testing.F) {
	f.Add("max-age=300, public")
	f.Add("no-store")
	f.Add("s-maxage=\"60\"")
	f.Add(",,,=,=,")
	f.Fuzz(func(t *testing.T, v string) {
		cc := ParseCacheControl([]string{v})
		// Parsing must never produce a negative or absurd duration.
		for _, d := range []time.Duration{cc.MaxAge, cc.SMaxAge, cc.StaleWhileRevalidate} {
			if d < 0 || d > DefaultMaxTTL {
				t.Fatalf("out-of-range duration %s from %q", d, v)
			}
		}
	})
}

func FuzzEvaluateResponse(f *testing.F) {
	f.Add(200, "max-age=60", "", int64(100))
	f.Add(404, "", "*", int64(0))
	f.Fuzz(func(t *testing.T, status int, cc, vary string, size int64) {
		h := http.Header{}
		if cc != "" {
			h.Set("Cache-Control", cc)
		}
		if vary != "" {
			h.Set("Vary", vary)
		}
		d := EvaluateResponse(status, h, size, 1<<20, enabled(), time.Now())
		if d.Admit {
			if d.TTL <= 0 {
				t.Fatalf("admitted with a non-positive TTL %s", d.TTL)
			}
			if d.TTL > DefaultMaxTTL {
				t.Fatalf("admitted with an unbounded TTL %s", d.TTL)
			}
			if size >= 0 && size > 1<<20 {
				t.Fatalf("admitted an oversized object of %d bytes", size)
			}
		}
	})
}
