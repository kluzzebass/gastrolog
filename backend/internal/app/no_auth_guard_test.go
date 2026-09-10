package app

// --no-auth hands administrator rights to every caller that can open the
// port, so the flag is only a development convenience while the set of such
// callers is this machine. Startup refuses it on any other listen address,
// and raises a standing alarm when it is allowed, because otherwise nothing
// tells an operator why every caller is an admin.

import (
	"strings"
	"testing"

	"gastrolog/internal/alert"
)

func TestNoAuthRefusedOffLoopback(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		addr       string
		wantRefuse bool
	}{
		{"wildcard", ":4564", true},
		{"all interfaces v4", "0.0.0.0:4564", true},
		{"all interfaces v6", "[::]:4564", true},
		{"routable address", "10.1.2.3:4564", true},
		{"hostname", "gastrolog.internal:4564", true},
		{"loopback name", "localhost:4564", false},
		{"loopback v4", "127.0.0.1:4564", false},
		{"loopback v4 alias", "127.0.0.53:4564", false},
		{"loopback v6", "[::1]:4564", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkNoAuthIsLocal(true, tc.addr)
			if tc.wantRefuse && err == nil {
				t.Fatalf("--no-auth allowed on %s", tc.addr)
			}
			if !tc.wantRefuse && err != nil {
				t.Fatalf("--no-auth refused on %s: %v", tc.addr, err)
			}
			if tc.wantRefuse && !strings.Contains(err.Error(), tc.addr) {
				t.Fatalf("refusal does not name the address: %v", err)
			}
		})
	}
}

// The guard must not touch a node that did not ask for --no-auth, whatever
// it binds.
func TestAuthenticatedServerBindsAnywhere(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{":4564", "0.0.0.0:4564", "10.1.2.3:4564", ""} {
		if err := checkNoAuthIsLocal(false, addr); err != nil {
			t.Fatalf("authenticated server refused on %s: %v", addr, err)
		}
	}
}

func TestNoAuthAlarmIsCataloged(t *testing.T) {
	t.Parallel()
	c := alert.New()
	c.Raise(noAuthAlarmType, "", "listening on localhost:4564")

	alarms := c.Standing()
	if len(alarms) != 1 {
		t.Fatalf("raised one alarm, collector holds %d", len(alarms))
	}
	a := alarms[0]
	if a.Priority == 0 || a.Response == "" {
		// An uncataloged type is stamped as a software fault with no
		// guidance, which is not what an operator needs to read here.
		t.Fatalf("alarm %q is not cataloged: priority=%v response=%q", a.ID, a.Priority, a.Response)
	}
}
