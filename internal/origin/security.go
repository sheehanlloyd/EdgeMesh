// Package origin manages origin pools, health checking, and origin selection.
package origin

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// dnsResolveTimeout bounds a configuration-time origin lookup so an
// unresponsive resolver cannot stall a configuration apply.
const dnsResolveTimeout = 5 * time.Second

// Guard decides whether an origin address may be dialed.
//
// Administrators configure origins, so this is not a defence against a hostile
// user. It is a defence against a *mistake*: a route pointing at
// 169.254.169.254 would turn the proxy into an instance-metadata exfiltration
// gadget, and a route pointing at 127.0.0.1 would let it reach services the
// operator never intended to expose. Production therefore fails closed and an
// operator must opt in explicitly.
type Guard struct {
	allowLoopback bool
	allowPrivate  bool
	allowed       []netip.Prefix
	denied        []netip.Prefix
}

// metadataRanges are always denied unless explicitly allowlisted. These are the
// cloud instance-metadata endpoints whose exposure through a proxy is a
// well-known credential-theft path.
var metadataRanges = []string{
	"169.254.169.254/32", // AWS/GCP/Azure IMDS
	"169.254.170.2/32",   // ECS task metadata
	"fd00:ec2::254/128",  // AWS IMDSv6
}

// NewGuard builds a guard from configuration.
func NewGuard(c config.OriginSecurity) (*Guard, error) {
	g := &Guard{allowLoopback: c.AllowLoopback, allowPrivate: c.AllowPrivateNetworks}

	for _, s := range c.AllowedCIDRs {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return nil, errs.Wrap(errs.ClassValidation, err, "origin guard: invalid allowed CIDR %q", s)
		}
		g.allowed = append(g.allowed, p)
	}
	for _, s := range append(append([]string(nil), c.DeniedCIDRs...), metadataRanges...) {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return nil, errs.Wrap(errs.ClassValidation, err, "origin guard: invalid denied CIDR %q", s)
		}
		g.denied = append(g.denied, p)
	}
	return g, nil
}

// CheckScheme rejects anything that is not plain HTTP or HTTPS. No file://,
// unix sockets, or arbitrary schemes exist in V1.
func CheckScheme(scheme string) error {
	switch strings.ToLower(scheme) {
	case "http", "https":
		return nil
	default:
		return errs.New(errs.ClassValidation,
			"origin scheme %q is not permitted; only http and https are supported", scheme)
	}
}

// CheckAddr validates a resolved address.
//
// It is called both at configuration time and again from the transport's dial
// hook. Re-checking at dial time is what closes the DNS-rebinding window: a
// hostname that resolved to a public address during validation may resolve to
// a loopback address when the connection is actually made.
func (g *Guard) CheckAddr(addr netip.Addr) error {
	addr = addr.Unmap()

	for _, p := range g.denied {
		if p.Contains(addr) {
			return errs.New(errs.ClassValidation,
				"origin address %s is inside the denied range %s", addr, p)
		}
	}
	// An explicit allowlist entry overrides the category rules below.
	for _, p := range g.allowed {
		if p.Contains(addr) {
			return nil
		}
	}
	switch {
	case addr.IsLoopback():
		if !g.allowLoopback {
			return errs.New(errs.ClassValidation,
				"origin address %s is loopback; enable origin_security.allow_loopback to permit it", addr)
		}
	case addr.IsLinkLocalUnicast(), addr.IsLinkLocalMulticast():
		return errs.New(errs.ClassValidation,
			"origin address %s is link-local and is never permitted", addr)
	case addr.IsMulticast():
		return errs.New(errs.ClassValidation, "origin address %s is multicast", addr)
	case addr.IsUnspecified():
		return errs.New(errs.ClassValidation, "origin address %s is unspecified", addr)
	case addr.IsInterfaceLocalMulticast():
		return errs.New(errs.ClassValidation, "origin address %s is interface-local", addr)
	case isPrivate(addr):
		if !g.allowPrivate {
			return errs.New(errs.ClassValidation,
				"origin address %s is a private network address; enable origin_security.allow_private_networks to permit it", addr)
		}
	}
	return nil
}

// CheckHostPort resolves host and validates every address it resolves to.
//
// Every resolved address must pass. Accepting a hostname because *one* of its
// addresses is safe would let a rebinding origin present a safe address at
// validation time and a dangerous one at dial time.
//
// This is the configuration-time check. The dial-time check in the proxy's
// transport is what actually closes the rebinding window, since a name can
// resolve differently between the two.
func (g *Guard) CheckHostPort(ctx context.Context, host string) error {
	if addr, err := netip.ParseAddr(host); err == nil {
		return g.CheckAddr(addr)
	}
	// A bounded resolver: an unresponsive DNS server must not stall
	// configuration validation indefinitely.
	ctx, cancel := context.WithTimeout(ctx, dnsResolveTimeout)
	defer cancel()

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return errs.Wrap(errs.ClassValidation, err, "origin host %q does not resolve", host)
	}
	if len(ips) == 0 {
		return errs.New(errs.ClassValidation, "origin host %q resolved to no addresses", host)
	}
	for _, ip := range ips {
		addr, ok := netip.AddrFromSlice(ip.IP)
		if !ok {
			return errs.New(errs.ClassValidation, "origin host %q resolved to an unusable address", host)
		}
		if err := g.CheckAddr(addr); err != nil {
			return err
		}
	}
	return nil
}

// isPrivate reports whether addr is in a private/unique-local range.
func isPrivate(addr netip.Addr) bool {
	if addr.Is4() {
		return addr.IsPrivate()
	}
	// Unique local addresses fc00::/7.
	return addr.IsPrivate() || addr.Is6() && addr.As16()[0]&0xfe == 0xfc
}
