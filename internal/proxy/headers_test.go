package proxy

import (
	"net/http"
	"testing"
)

func TestStripHopByHop(t *testing.T) {
	h := http.Header{
		"Connection":        []string{"keep-alive, X-Custom-Hop"},
		"Keep-Alive":        []string{"timeout=5"},
		"Transfer-Encoding": []string{"chunked"},
		"Upgrade":           []string{"websocket"},
		"X-Custom-Hop":      []string{"secret"},
		"Content-Type":      []string{"text/plain"},
	}
	stripHopByHop(h)

	for _, name := range []string{"Connection", "Keep-Alive", "Transfer-Encoding", "Upgrade"} {
		if h.Get(name) != "" {
			t.Errorf("hop-by-hop header %q survived", name)
		}
	}
	// The Connection-nominated header is the one a client controls, and is the
	// request-smuggling primitive if it is forwarded.
	if h.Get("X-Custom-Hop") != "" {
		t.Error("a Connection-nominated header was forwarded")
	}
	if h.Get("Content-Type") != "text/plain" {
		t.Error("an end-to-end header was removed")
	}
}

func TestTrustedProxiesParsing(t *testing.T) {
	if _, err := NewTrustedProxies([]string{"not-a-cidr"}); err == nil {
		t.Fatal("an invalid CIDR must be rejected")
	}
	tp, err := NewTrustedProxies([]string{"10.0.0.0/8", "192.168.0.0/16"})
	if err != nil {
		t.Fatal(err)
	}
	if !tp.Trusts("10.1.2.3:1234") || !tp.Trusts("192.168.1.1:80") {
		t.Error("a configured proxy was not trusted")
	}
	if tp.Trusts("8.8.8.8:53") {
		t.Error("an unconfigured address was trusted")
	}
	// An empty configuration must trust nobody.
	empty, _ := NewTrustedProxies(nil)
	if empty.Trusts("10.0.0.1:1") {
		t.Error("the default configuration trusted a peer")
	}
}

// A client must not be able to choose its own identity for rate limiting or
// logging by sending X-Forwarded-For.
func TestClientIPIgnoresUntrustedForwardedFor(t *testing.T) {
	tp, _ := NewTrustedProxies(nil)
	r := &http.Request{
		RemoteAddr: "203.0.113.5:4444",
		Header:     http.Header{"X-Forwarded-For": []string{"1.2.3.4"}},
	}
	if got := tp.ClientIP(r); got != "203.0.113.5" {
		t.Fatalf("ClientIP = %q; a forged X-Forwarded-For was trusted", got)
	}
}

func TestClientIPUsesLastHopFromTrustedProxy(t *testing.T) {
	tp, _ := NewTrustedProxies([]string{"10.0.0.0/8"})
	r := &http.Request{
		RemoteAddr: "10.0.0.9:4444",
		// The first entries are client-supplied and forgeable; the last was
		// appended by the trusted proxy itself.
		Header: http.Header{"X-Forwarded-For": []string{"1.2.3.4, 198.51.100.7"}},
	}
	if got := tp.ClientIP(r); got != "198.51.100.7" {
		t.Fatalf("ClientIP = %q, want the last forwarded hop", got)
	}
}

func TestSetForwardingHeadersReplacesForUntrustedClients(t *testing.T) {
	tp, _ := NewTrustedProxies(nil)
	in := &http.Request{
		RemoteAddr: "203.0.113.5:4444",
		Host:       "demo.local",
		Header: http.Header{
			"X-Forwarded-For":   []string{"1.2.3.4"},
			"X-Forwarded-Proto": []string{"https"},
			"Forwarded":         []string{"for=1.2.3.4"},
		},
	}
	out := &http.Request{Header: in.Header.Clone()}
	tp.setForwardingHeaders(out, in, "http")

	if got := out.Header.Get("X-Forwarded-For"); got != "203.0.113.5" {
		t.Fatalf("X-Forwarded-For = %q, want the observed peer only", got)
	}
	if got := out.Header.Get("X-Forwarded-Proto"); got != "http" {
		t.Fatalf("X-Forwarded-Proto = %q, want the observed scheme", got)
	}
	if out.Header.Get("Forwarded") != "" {
		t.Error("a client-supplied Forwarded header was preserved")
	}
	if got := out.Header.Get("X-Forwarded-Host"); got != "demo.local" {
		t.Fatalf("X-Forwarded-Host = %q", got)
	}
}

func TestSetForwardingHeadersExtendsForTrustedProxy(t *testing.T) {
	tp, _ := NewTrustedProxies([]string{"10.0.0.0/8"})
	in := &http.Request{
		RemoteAddr: "10.0.0.9:4444",
		Host:       "demo.local",
		Header:     http.Header{"X-Forwarded-For": []string{"1.2.3.4"}},
	}
	out := &http.Request{Header: in.Header.Clone()}
	tp.setForwardingHeaders(out, in, "https")

	if got := out.Header.Get("X-Forwarded-For"); got != "1.2.3.4, 10.0.0.9" {
		t.Fatalf("X-Forwarded-For = %q, want the chain extended", got)
	}
}

func TestSanitizeResponseHeaders(t *testing.T) {
	h := http.Header{
		"Connection":        []string{"close"},
		"Transfer-Encoding": []string{"chunked"},
		"Content-Type":      []string{"application/json"},
		"Cache-Control":     []string{"max-age=60"},
	}
	sanitizeResponseHeaders(h)
	if h.Get("Connection") != "" || h.Get("Transfer-Encoding") != "" {
		t.Error("connection-scoped response headers survived")
	}
	if h.Get("Content-Type") == "" || h.Get("Cache-Control") == "" {
		t.Error("an end-to-end response header was removed")
	}
}

func TestRequestIDSanitization(t *testing.T) {
	// A generated ID when none is supplied.
	r := &http.Request{Header: http.Header{}}
	if id := requestIDFrom(r); len(id) < 8 {
		t.Fatalf("generated request id is too short: %q", id)
	}
	// A usable supplied ID is reused for trace correlation.
	r.Header.Set(HeaderRequestID, "abc12345")
	if got := requestIDFrom(r); got != "abc12345" {
		t.Fatalf("supplied id = %q", got)
	}
	// Control characters must be stripped: they would let a client forge log
	// lines by injecting newlines.
	r.Header.Set(HeaderRequestID, "abc\n\rdef INJECTED")
	got := requestIDFrom(r)
	for _, c := range got {
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_'
		if !ok {
			t.Fatalf("sanitized id %q still contains %q", got, c)
		}
	}
	// An unbounded value must be truncated.
	long := make([]byte, 500)
	for i := range long {
		long[i] = 'a'
	}
	r.Header.Set(HeaderRequestID, string(long))
	if len(requestIDFrom(r)) > 64 {
		t.Fatal("a long request id was not bounded")
	}
	// A too-short value falls back to a generated ID rather than being used.
	r.Header.Set(HeaderRequestID, "ab")
	if requestIDFrom(r) == "ab" {
		t.Fatal("an implausibly short id was accepted")
	}
}

func FuzzRequestID(f *testing.F) {
	f.Add("abc12345")
	f.Add("")
	f.Add("\n\r\x00 injected")
	f.Fuzz(func(t *testing.T, v string) {
		r := &http.Request{Header: http.Header{}}
		r.Header.Set(HeaderRequestID, v)
		got := requestIDFrom(r)
		if len(got) > 64 {
			t.Fatalf("id exceeded its bound: %d bytes", len(got))
		}
		if got == "" {
			t.Fatal("a request id must always be produced")
		}
		for _, c := range got {
			ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
				(c >= '0' && c <= '9') || c == '-' || c == '_'
			if !ok {
				t.Fatalf("unsanitized character %q in %q", c, got)
			}
		}
	})
}

func FuzzStripHopByHop(f *testing.F) {
	f.Add("keep-alive, X-Custom")
	f.Add("")
	f.Add(",,,")
	f.Fuzz(func(t *testing.T, connection string) {
		h := http.Header{
			"Connection":   []string{connection},
			"Content-Type": []string{"text/plain"},
		}
		stripHopByHop(h)
		if h.Get("Connection") != "" {
			t.Fatal("Connection survived stripping")
		}
	})
}
