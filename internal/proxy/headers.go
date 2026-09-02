// Package proxy implements EdgeMesh's HTTP reverse proxy.
package proxy

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// hopByHopHeaders are connection-scoped and must not be forwarded (RFC 9110
// section 7.6.1). Forwarding them corrupts the next hop's connection handling.
var hopByHopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// stripHopByHop removes hop-by-hop headers, including any the peer nominated
// through Connection.
//
// The Connection-nominated part matters: a client can name arbitrary headers
// there, and a proxy that forwards them lets a client control what reaches the
// origin's connection layer. This is a request-smuggling primitive if ignored.
func stripHopByHop(h http.Header) {
	for _, v := range h.Values("Connection") {
		for _, name := range strings.Split(v, ",") {
			if name = strings.TrimSpace(name); name != "" {
				h.Del(name)
			}
		}
	}
	for _, name := range hopByHopHeaders {
		h.Del(name)
	}
}

// forwardingHeaders are the ones EdgeMesh itself owns. A client-supplied value
// is never trusted: EdgeMesh sets them from what it actually observed.
var forwardingHeaders = []string{
	"X-Forwarded-For",
	"X-Forwarded-Proto",
	"X-Forwarded-Host",
	"X-Forwarded-Port",
	"Forwarded",
}

// TrustedProxies decides whether an inbound X-Forwarded-For may be extended
// rather than replaced.
type TrustedProxies struct {
	prefixes []netip.Prefix
}

// NewTrustedProxies parses trusted CIDRs. An empty list trusts nobody, which is
// the correct default: appending to a forwarded chain from an untrusted peer
// lets a client forge its own source address for rate limiting and logs.
func NewTrustedProxies(cidrs []string) (*TrustedProxies, error) {
	t := &TrustedProxies{}
	for _, c := range cidrs {
		p, err := netip.ParsePrefix(c)
		if err != nil {
			return nil, err
		}
		t.prefixes = append(t.prefixes, p)
	}
	return t, nil
}

// Trusts reports whether remoteAddr is a trusted proxy.
func (t *TrustedProxies) Trusts(remoteAddr string) bool {
	if t == nil || len(t.prefixes) == 0 {
		return false
	}
	addr, err := addrFromRemote(remoteAddr)
	if err != nil {
		return false
	}
	for _, p := range t.prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

func addrFromRemote(remoteAddr string) (netip.Addr, error) {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, err
	}
	return a.Unmap(), nil
}

// ClientIP returns the address EdgeMesh attributes the request to.
//
// It uses the socket peer address unless that peer is a configured trusted
// proxy, in which case the last entry of X-Forwarded-For is used. Taking the
// *last* entry rather than the first is deliberate: earlier entries were
// supplied by whoever was upstream and can be forged, while the last was
// appended by the trusted proxy itself.
func (t *TrustedProxies) ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if !t.Trusts(r.RemoteAddr) {
		return host
	}
	xff := r.Header.Values("X-Forwarded-For")
	if len(xff) == 0 {
		return host
	}
	parts := strings.Split(strings.Join(xff, ","), ",")
	for i := len(parts) - 1; i >= 0; i-- {
		if v := strings.TrimSpace(parts[i]); v != "" {
			return v
		}
	}
	return host
}

// setForwardingHeaders rewrites the forwarding chain EdgeMesh emits.
//
// From an untrusted client the chain is replaced outright; from a trusted proxy
// it is extended. Either way the value the origin sees is one EdgeMesh vouches
// for.
func (t *TrustedProxies) setForwardingHeaders(out *http.Request, in *http.Request, scheme string) {
	clientHost, _, err := net.SplitHostPort(in.RemoteAddr)
	if err != nil {
		clientHost = in.RemoteAddr
	}

	if t.Trusts(in.RemoteAddr) {
		existing := strings.Join(in.Header.Values("X-Forwarded-For"), ", ")
		if existing != "" {
			out.Header.Set("X-Forwarded-For", existing+", "+clientHost)
		} else {
			out.Header.Set("X-Forwarded-For", clientHost)
		}
	} else {
		for _, h := range forwardingHeaders {
			out.Header.Del(h)
		}
		out.Header.Set("X-Forwarded-For", clientHost)
	}

	out.Header.Set("X-Forwarded-Proto", scheme)
	if in.Host != "" {
		out.Header.Set("X-Forwarded-Host", in.Host)
	}
}

// responseHeadersToStrip are removed from an origin response before it is
// stored or returned. They describe the origin's connection, not the object.
var responseHeadersToStrip = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Transfer-Encoding",
	"Upgrade",
}

// sanitizeResponseHeaders prepares an origin response for caching and return.
func sanitizeResponseHeaders(h http.Header) {
	stripHopByHop(h)
	for _, name := range responseHeadersToStrip {
		h.Del(name)
	}
}
