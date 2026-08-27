// Package safefetch builds HTTP clients for outbound requests whose
// destination comes from operator configuration or from log record content.
//
// The destination is checked after DNS resolution, at the moment the socket is
// dialled, rather than by inspecting the URL string. A hostname that resolves
// to a blocked address is therefore refused however it was spelled, the check
// cannot be invalidated by a DNS answer that changes between validation and
// connect, and a redirect chain is covered because every hop that reaches a new
// address has to dial it.
package safefetch

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"syscall"
	"time"
)

const (
	dialTimeout           = 10 * time.Second
	dialKeepAlive         = 30 * time.Second
	tlsHandshakeTimeout   = 10 * time.Second
	responseHeaderTimeout = 30 * time.Second
	idleConnTimeout       = 90 * time.Second
	maxIdleConnsPerHost   = 4
	maxRedirects          = 10
)

// ErrBlockedDestination is returned when a resolved address falls in a range
// the policy denies.
var ErrBlockedDestination = errors.New("destination address is not permitted")

// ErrUnsupportedScheme is returned for a URL the policy will not fetch at all.
var ErrUnsupportedScheme = errors.New("only http and https URLs can be fetched")

// Policy decides which resolved destinations an outbound request may reach.
// The zero Policy denies every address that is not routable on the public
// internet.
type Policy struct {
	// AllowPrivate permits the site-local ranges: loopback, 10/8, 172.16/12,
	// 192.168/16, carrier-grade NAT 100.64/10 and IPv6 unique-local. An
	// operator sets it for a service hosted on the cluster's own network. It
	// does not reach the never-permitted ranges below.
	AllowPrivate bool
}

// neverAllowed are ranges no policy permits. They address the local link, or
// translate to some other address family where a private destination would
// reappear as a public-looking one, or are not unicast destinations at all.
// Every cloud instance metadata endpoint reachable by address lives here or in
// siteLocal, so no combination of flags turns a fetch into a metadata read.
var neverAllowed = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),       // "this network"
	netip.MustParsePrefix("169.254.0.0/16"),  // link-local, incl. 169.254.169.254
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments, incl. 192.0.0.170 NAT64 discovery
	netip.MustParsePrefix("192.0.2.0/24"),    // TEST-NET-1
	netip.MustParsePrefix("192.88.99.0/24"),  // 6to4 relay anycast
	netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking
	netip.MustParsePrefix("198.51.100.0/24"), // TEST-NET-2
	netip.MustParsePrefix("203.0.113.0/24"),  // TEST-NET-3
	netip.MustParsePrefix("240.0.0.0/4"),     // reserved, incl. 255.255.255.255
	netip.MustParsePrefix("::/96"),           // IPv4-compatible IPv6
	netip.MustParsePrefix("64:ff9b::/96"),    // NAT64: embeds any IPv4 destination
	netip.MustParsePrefix("64:ff9b:1::/48"),  // local-use NAT64
	netip.MustParsePrefix("100::/64"),        // discard-only
	netip.MustParsePrefix("2001::/32"),       // Teredo: embeds an IPv4 destination
	netip.MustParsePrefix("2001:10::/28"),    // ORCHID
	netip.MustParsePrefix("2001:db8::/32"),   // documentation
	netip.MustParsePrefix("2002::/16"),       // 6to4: embeds an IPv4 destination
	netip.MustParsePrefix("fe80::/10"),       // link-local, incl. fe80::a9fe:a9fe
	netip.MustParsePrefix("fec0::/10"),       // deprecated site-local
}

// siteLocal are the ranges AllowPrivate re-opens: reachable only from the
// network around the node, which is what an operator hosting a lookup service
// there opts into.
var siteLocal = []netip.Prefix{
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // carrier-grade NAT, incl. some pod networks
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("fc00::/7"), // unique-local
}

// CheckURL rejects a URL whose scheme is not http or https, or that carries no
// host. It is a pre-flight convenience: the address check at dial time is what
// actually enforces the destination policy.
func CheckURL(u *url.URL) error {
	if u == nil {
		return ErrUnsupportedScheme
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: %q", ErrUnsupportedScheme, u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("%w: no host", ErrUnsupportedScheme)
	}
	return nil
}

// CheckAddr reports whether a resolved "host:port" address may be dialled.
// The host part must already be an IP literal — this runs after resolution.
func (p Policy) CheckAddr(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: unparsable address %q", ErrBlockedDestination, address)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("%w: unresolved address %q", ErrBlockedDestination, host)
	}
	if !p.Allows(ip.Unmap()) {
		return fmt.Errorf("%w: %s", ErrBlockedDestination, ip)
	}
	return nil
}

// Allows reports whether p permits a request to reach ip. Ranges are named
// explicitly rather than derived from IsGlobalUnicast, which is true for
// carrier-grade NAT and for the prefixes that embed an IPv4 destination.
func (p Policy) Allows(ip netip.Addr) bool {
	ip = ip.Unmap()
	switch {
	case !ip.IsValid(), ip.IsUnspecified(), ip.IsMulticast():
		return false
	case inAny(siteLocal, ip):
		// Checked first because ::1 sits inside the IPv4-compatible ::/96
		// prefix below, and loopback is the operator's to open.
		return p.AllowPrivate
	case inAny(neverAllowed, ip):
		return false
	default:
		// Backstop for anything the tables do not name: still has to be a
		// routable unicast destination.
		return ip.IsGlobalUnicast()
	}
}

func inAny(prefixes []netip.Prefix, ip netip.Addr) bool {
	for _, p := range prefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// Transport returns an HTTP transport that validates every dialled address
// against p.
func Transport(p Policy) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: dialKeepAlive,
		Control: func(_, address string, _ syscall.RawConn) error {
			return p.CheckAddr(address)
		},
	}
	return &http.Transport{
		// No proxy: a proxied request is dialled to the proxy, so the address
		// the policy inspects would no longer be the request's destination.
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ResponseHeaderTimeout: responseHeaderTimeout,
	}
}

// Client returns an HTTP client that refuses to connect to destinations p
// denies, on the initial request and on every redirect hop alike. A timeout of
// zero leaves the client without an overall deadline.
func Client(p Policy, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: Transport(p),
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return fmt.Errorf("stopped after %d redirects", maxRedirects)
			}
			return CheckURL(req.URL)
		},
	}
}
