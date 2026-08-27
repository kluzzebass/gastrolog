package safefetch

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPolicyAllows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		addr         string
		wantDefault  bool
		wantAllowing bool // with Policy{AllowPrivate: true}
	}{
		{"loopback v4", "127.0.0.1", false, true},
		{"loopback v6", "::1", false, true},
		{"private 10/8", "10.1.2.3", false, true},
		{"private 172.16/12", "172.20.0.5", false, true},
		{"private 192.168/16", "192.168.1.1", false, true},
		{"unique local v6", "fd00::1", false, true},
		{"ipv4-mapped loopback", "::ffff:127.0.0.1", false, true},
		{"ipv4-mapped private", "::ffff:10.0.0.1", false, true},
		{"link-local v4 metadata", "169.254.169.254", false, false},
		{"link-local v4", "169.254.1.1", false, false},
		{"link-local v6", "fe80::1", false, false},
		{"multicast v4", "224.0.0.1", false, false},
		{"multicast v6", "ff02::1", false, false},
		{"unspecified v4", "0.0.0.0", false, false},
		{"unspecified v6", "::", false, false},
		{"broadcast v4", "255.255.255.255", false, false},
		{"public v4", "93.184.216.34", true, true},
		{"public v6", "2606:2800:220:1:248:1893:25c8:1946", true, true},
		// Carrier-grade NAT holds one vendor's metadata service and some pod
		// networks, so it is site-local, not public.
		{"carrier-grade nat", "100.64.0.1", false, true},
		{"carrier-grade nat metadata", "100.100.100.200", false, true},
		// Ranges that embed another destination or address the local
		// infrastructure stay denied whatever the operator asks for.
		{"nat64", "64:ff9b::a9fe:a9fe", false, false},
		{"nat64 local use", "64:ff9b:1::a9fe:a9fe", false, false},
		{"6to4", "2002:0a00:0001::1", false, false},
		{"teredo", "2001:0:53aa:64c:0:0:0:1", false, false},
		{"ipv4-compatible v6", "::a9fe:a9fe", false, false},
		{"protocol assignments", "192.0.0.170", false, false},
		{"benchmarking", "198.18.0.1", false, false},
		{"6to4 relay anycast", "192.88.99.1", false, false},
		{"site-local v6", "fec0::1", false, false},
		{"reserved 240/4", "240.0.0.1", false, false},
		{"documentation v6", "2001:db8::1", false, false},
		{"orchidv2", "2001:20::1", false, false},
		{"documentation 3fff/20", "3fff::1", false, false},
		{"siit ipv4-translated", "::ffff:0:7f00:1", false, false},
		// A zone identifier makes an address local to one interface, and makes
		// prefix tests match nothing — so every family needs a zoned row.
		{"zoned loopback v6", "::1%lo0", false, false},
		{"zoned unique local", "fc00::1%en0", false, false},
		{"zoned unique local fd", "fd00::1%en0", false, false},
		{"zoned link-local v6", "fe80::1%en0", false, false},
		{"zoned nat64", "64:ff9b::a9fe:a9fe%en0", false, false},
		{"zoned ipv4-compatible v6", "::a9fe:a9fe%en0", false, false},
		{"zoned teredo", "2001::1%en0", false, false},
		{"zoned site-local v6", "fec0::1%en0", false, false},
		{"zoned multicast v6", "ff02::1%en0", false, false},
		{"zoned public v6", "2606:2800:220:1:248:1893:25c8:1946%en0", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ip, err := netip.ParseAddr(tt.addr)
			if err != nil {
				t.Fatalf("parse %q: %v", tt.addr, err)
			}
			ip = ip.Unmap()

			if got := (Policy{}).Allows(ip); got != tt.wantDefault {
				t.Errorf("Policy{}.Allows(%s) = %v, want %v", tt.addr, got, tt.wantDefault)
			}
			if got := (Policy{AllowPrivate: true}).Allows(ip); got != tt.wantAllowing {
				t.Errorf("Policy{AllowPrivate:true}.Allows(%s) = %v, want %v", tt.addr, got, tt.wantAllowing)
			}
		})
	}
}

func TestCheckAddrRejectsUnresolved(t *testing.T) {
	t.Parallel()

	// The dial-time check only ever sees literals; a name here means resolution
	// was bypassed, which must fail closed.
	for _, addr := range []string{"metadata.internal:80", "not-an-address", "127.0.0.1"} {
		if err := (Policy{AllowPrivate: true}).CheckAddr(addr); !errors.Is(err, ErrBlockedDestination) {
			t.Errorf("CheckAddr(%q) = %v, want ErrBlockedDestination", addr, err)
		}
	}
}

// TestCheckAddrRejectsZonedDestination covers the address form the dialer
// actually hands the Control hook for a URL like http://[fc00::1%25en0]/ —
// the zone survives resolution, and no policy may let it through.
func TestCheckAddrRejectsZonedDestination(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{
		"[fc00::1%en0]:80",
		"[fd00::1%en0]:80",
		"[64:ff9b::a9fe:a9fe%en0]:80",
		"[::a9fe:a9fe%en0]:80",
		"[2001::1%en0]:80",
		"[::1%lo0]:80",
	} {
		for _, p := range []Policy{{}, {AllowPrivate: true}} {
			if err := p.CheckAddr(addr); !errors.Is(err, ErrBlockedDestination) {
				t.Errorf("Policy{AllowPrivate:%v}.CheckAddr(%q) = %v, want ErrBlockedDestination", p.AllowPrivate, addr, err)
			}
		}
	}
}

func TestCheckURLScheme(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"file:///etc/passwd", "ftp://host/x", "gopher://host:70/", "http://"} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		if err := CheckURL(u); !errors.Is(err, ErrUnsupportedScheme) {
			t.Errorf("CheckURL(%q) = %v, want ErrUnsupportedScheme", raw, err)
		}
	}
	for _, raw := range []string{"http://example.com/x", "https://example.com/x"} {
		u, _ := url.Parse(raw)
		if err := CheckURL(u); err != nil {
			t.Errorf("CheckURL(%q) = %v, want nil", raw, err)
		}
	}

	// A zoned literal is refused before the request is ever built.
	for _, raw := range []string{"http://[fc00::1%25en0]/x", "https://[fe80::1%25lo0]:8080/x"} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		if err := CheckURL(u); !errors.Is(err, ErrBlockedDestination) {
			t.Errorf("CheckURL(%q) = %v, want ErrBlockedDestination", raw, err)
		}
	}
}

// TestHostnameResolvingToBlockedAddressIsRefused is the rebinding-shaped case:
// the URL carries a hostname no string check would flag, and the block has to
// come from the address it resolves to at connect time.
func TestHostnameResolvingToBlockedAddressIsRefused(t *testing.T) {
	t.Parallel()

	addrs, err := net.DefaultResolver.LookupHost(context.Background(), "localhost")
	if err != nil || len(addrs) == 0 {
		t.Skipf("localhost does not resolve here: %v", err)
	}
	for _, a := range addrs {
		ip, perr := netip.ParseAddr(a)
		if perr != nil || !ip.IsLoopback() {
			t.Skipf("localhost resolves to %v, not loopback", addrs)
		}
	}

	var reached bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatalf("split test server address: %v", err)
	}
	target := "http://localhost:" + port + "/"

	resp, err := Client(Policy{}, 5*time.Second).Get(target) //nolint:noctx // client carries the timeout
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("request to a hostname resolving to loopback succeeded")
	}
	if !strings.Contains(err.Error(), ErrBlockedDestination.Error()) {
		t.Fatalf("error = %v, want a blocked-destination error", err)
	}
	if reached {
		t.Fatal("the blocked request still reached the server")
	}

	// Premise: the same URL is reachable once the operator opts in, so the
	// refusal above is the policy and not a broken request.
	resp, err = Client(Policy{AllowPrivate: true}, 5*time.Second).Get(target) //nolint:noctx // client carries the timeout
	if err != nil {
		t.Fatalf("permitted request failed: %v", err)
	}
	_ = resp.Body.Close()
	if !reached {
		t.Fatal("permitted request did not reach the server")
	}
}

// TestRedirectToBlockedDestination proves the policy follows the request rather
// than the URL it started from: the first hop is permitted, the redirect target
// is not.
func TestRedirectToBlockedDestination(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data/", http.StatusFound)
	}))
	defer srv.Close()

	// AllowPrivate reaches the loopback test server but never link-local space.
	resp, err := Client(Policy{AllowPrivate: true}, 5*time.Second).Get(srv.URL) //nolint:noctx // client carries the timeout
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("redirect to the metadata endpoint was followed")
	}
	if !strings.Contains(err.Error(), ErrBlockedDestination.Error()) {
		t.Fatalf("error = %v, want a blocked-destination error", err)
	}
}

// TestRedirectToUnsupportedScheme covers the hop the address check cannot see.
func TestRedirectToUnsupportedScheme(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "file:///etc/passwd", http.StatusFound)
	}))
	defer srv.Close()

	resp, err := Client(Policy{AllowPrivate: true}, 5*time.Second).Get(srv.URL) //nolint:noctx // client carries the timeout
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("redirect to a file:// URL was followed")
	}
	if !strings.Contains(err.Error(), ErrUnsupportedScheme.Error()) {
		t.Fatalf("error = %v, want an unsupported-scheme error", err)
	}
}

// TestClientIgnoresProxyEnvironment pins the invariant that makes the dial-time
// check meaningful: a proxy would hide the real destination behind its own
// address.
func TestClientIgnoresProxyEnvironment(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:9/")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:9/")

	tr := Transport(Policy{})
	if tr.Proxy != nil {
		t.Fatal("transport consults a proxy, which bypasses the destination check")
	}
}
