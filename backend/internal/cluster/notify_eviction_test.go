package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster/tlsutil"
)

const evictionLeader = "node-leader"

// evictionServer is a node that follows evictionLeader — here it is that
// leader itself, the one configuration a single in-process Raft can hold
// with a leader — with node-member as a further, non-voting member.
func evictionServer(t *testing.T, bootstrap bool) *Server {
	t.Helper()
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.stopCtx, s.stopCancel = context.WithCancel(context.Background())
	t.Cleanup(s.stopCancel)
	s.raft = createVaultRaft(t, evictionLeader, &hraft.MockFSM{}, bootstrap, nil)
	if bootstrap {
		waitVaultLeader(t, s.raft, 5*time.Second)
		if err := s.raft.AddNonvoter("node-member", "node-member", 0, time.Second).Error(); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func callerCtx(leaf *x509.Certificate) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{
		State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}},
	}})
}

// A removal is taken in whether or not the server is stopping: a node removed
// during a rolling restart must come back removed, not as a member the
// cluster refuses. The handler has run by the time the RPC answers, which is
// what lets a graceful stop, which waits for in-flight calls, hand it on.
func TestNotifyEviction_RunsTheHandlerBeforeAnswering(t *testing.T) {
	t.Parallel()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	for _, stopping := range []bool{false, true} {
		var got []string
		s := evictionServer(t, true)
		s.SetEvictionHandler(func(reason string) { got = append(got, reason) })
		if stopping {
			s.stopCancel()
		}
		if _, err := s.notifyEviction(callerCtx(leafNamed(t, ca, evictionLeader)), &gastrologv1.NotifyEvictionRequest{Reason: "removed"}); err != nil {
			t.Fatalf("notifyEviction (stopping=%v): %v", stopping, err)
		}
		if len(got) != 1 || got[0] != "removed" {
			t.Fatalf("stopping=%v: handler saw %v by the time the RPC answered, want [removed]", stopping, got)
		}
	}
}

// Telling a node it was removed is the leader's alone. Any other member, a
// node outside the configuration (a removed one among them), or anyone at
// all while this node does not know its membership is refused, and nothing
// is taken in.
func TestNotifyEviction_RefusesAnyoneButTheLeader(t *testing.T) {
	t.Parallel()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		caller    string
		bootstrap bool
	}{
		{"a member that does not lead", "node-member", true},
		{"a node outside the configuration", "node-removed", true},
		{"the leader's name while membership is unknown", evictionLeader, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got []string
			s := evictionServer(t, tc.bootstrap)
			s.SetEvictionHandler(func(reason string) { got = append(got, reason) })
			_, err := s.notifyEviction(callerCtx(leafNamed(t, ca, tc.caller)), &gastrologv1.NotifyEvictionRequest{Reason: "forged"})
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("eviction notice from %s: %v, want PermissionDenied", tc.caller, err)
			}
			if len(got) != 0 {
				t.Fatalf("a refused eviction notice was taken in: %v", got)
			}
		})
	}
}

func TestEvictionAuthorized(t *testing.T) {
	t.Parallel()
	configured := []string{"node-l", "node-m"}
	for _, tc := range []struct {
		caller, leader string
		configured     []string
		want           codes.Code
	}{
		{"node-l", "node-l", configured, codes.OK},
		{"node-m", "node-l", configured, codes.PermissionDenied},
		{"node-x", "node-l", configured, codes.PermissionDenied},
		{"node-l", "", configured, codes.PermissionDenied},
		{"node-l", "node-l", nil, codes.PermissionDenied},
		{"node-l", "node-l", []string{"node-m"}, codes.PermissionDenied},
		{"", "node-l", configured, codes.Unauthenticated},
	} {
		if got := status.Code(evictionAuthorized(tc.caller, tc.leader, tc.configured)); got != tc.want {
			t.Errorf("caller %q, leader %q, configuration %v: %v, want %v", tc.caller, tc.leader, tc.configured, got, tc.want)
		}
	}
}
