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
	// AllowPrivate permits loopback, private (10/8, 172.16/12, 192.168/16) and
	// IPv6 unique-local destinations. An operator sets it for a service hosted
	// on the cluster's own network. It does not reach link-local space, which
	// stays denied whatever the policy says.
	AllowPrivate bool
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

// Allows reports whether p permits a request to reach ip.
func (p Policy) Allows(ip netip.Addr) bool {
	switch {
	case !ip.IsValid():
		return false
	case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(), ip.IsInterfaceLocalMulticast():
		// 169.254.0.0/16 and fe80::/10 carry the cloud instance metadata
		// endpoints and host nothing an operator would point a fetch at, so
		// the escape hatch deliberately stops short of them.
		return false
	case ip.IsMulticast(), ip.IsUnspecified():
		return false
	case ip.IsLoopback(), ip.IsPrivate():
		// Private is 10/8, 172.16/12, 192.168/16 and IPv6 unique-local
		// fc00::/7 — reachable only from the surrounding network, which is
		// exactly what an operator opts into.
		return p.AllowPrivate
	default:
		// Excludes the IPv4 broadcast address and anything else that is not a
		// routable unicast destination.
		return ip.IsGlobalUnicast()
	}
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
