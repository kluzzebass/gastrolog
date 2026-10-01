package cluster

// A certificate signed by the cluster CA used to be the whole answer, so a
// node removed from the cluster kept working until every certificate in the
// cluster was reissued. Authority now also asks whether the node the
// certificate names is one this cluster currently has, which is what makes
// removal take effect.

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net"
	"testing"

	"gastrolog/internal/cluster/tlsutil"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// certNaming issues a certificate for nodeID under a throwaway CA. Only the
// subject matters here; the chain has already been verified by the TLS stack
// before authority is consulted.
func certNaming(t *testing.T, nodeID string) *x509.Certificate {
	t.Helper()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tlsutil.GenerateNodeCert(ca.CertPEM, ca.KeyPEM, nodeID, nil)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(pemBlock(t, pair.CertPEM))
	if err != nil {
		t.Fatal(err)
	}
	return leaf
}

// pemBlock returns the DER inside a single-block PEM.
func pemBlock(t *testing.T, pemBytes []byte) []byte {
	t.Helper()
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		t.Fatal("decode PEM")
	}
	return block.Bytes
}

// ctxWithPeerCert builds the context gRPC hands an interceptor when a client
// has presented a certificate the TLS stack verified.
func ctxWithPeerCert(leaf *x509.Certificate) context.Context {
	var state credentials.TLSInfo
	state.State.VerifiedChains = [][]*x509.Certificate{{leaf}}
	return peer.NewContext(context.Background(), &peer.Peer{
		Addr:     &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4566},
		AuthInfo: state,
	})
}

// Enrol is reachable with no certificate at all: it is the call that hands
// one out, so requiring one would close the only door in.
func TestEnrollNeedsNoCertificate(t *testing.T) {
	t.Parallel()
	s := &Server{}
	if err := s.requirePeerAuthority(context.Background(), "/gastrolog.v1.ClusterService/Enroll"); err != nil {
		t.Fatalf("enrolment demanded a certificate it exists to issue: %v", err)
	}
}

// Everything else does need one, including the membership request the joiner
// makes immediately after enrolling.
func TestOtherCallsNeedACertificate(t *testing.T) {
	t.Parallel()
	s := &Server{}
	for _, method := range []string{
		"/gastrolog.v1.ClusterService/RequestMembership",
		"/gastrolog.v1.ClusterService/ForwardApply",
	} {
		if got := status.Code(s.requirePeerAuthority(context.Background(), method)); got != codes.Unauthenticated {
			t.Fatalf("%s answered %v to a caller with no certificate, want Unauthenticated", method, got)
		}
	}
}

// RequestMembership is the one call a node makes holding a certificate while
// still outside the configuration — it is asking to be put in it. Requiring
// membership here would make joining impossible.
func TestRequestMembershipDoesNotRequireMembership(t *testing.T) {
	t.Parallel()
	s := &Server{}
	ctx := ctxWithPeerCert(certNaming(t, "node-9"))
	if err := s.requirePeerAuthority(ctx, "/gastrolog.v1.ClusterService/RequestMembership"); err != nil {
		t.Fatalf("a joiner was refused the call that admits it: %v", err)
	}
}

func TestRequireCurrentMember(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		configured []string
		nodeID     string
		wantCode   codes.Code
		why        string
	}{
		{"a node the cluster has", []string{"node-1", "node-2"}, "node-2", codes.OK,
			"the certificate names a current member"},
		{"a node the cluster removed", []string{"node-1"}, "node-2", codes.PermissionDenied,
			"removal is what revokes a certificate, and this is where that takes effect"},
		{"a name the cluster never had", []string{"node-1"}, "attacker", codes.PermissionDenied,
			"a valid signature is not a membership"},
		{"a configuration this node has not learned yet", nil, "node-2", codes.OK,
			"an empty configuration means unknown, not empty — refusing would strand a joiner whose peers cannot reach it"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := checkMembership(tc.configured, certNaming(t, tc.nodeID))
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("got %v (%v), want %v — %s", got, err, tc.wantCode, tc.why)
			}
		})
	}
}

// A certificate with no subject names no node, so there is nothing to check
// against the configuration. Admitting it would reopen the hole.
func TestACertificateNamingNobodyIsRefused(t *testing.T) {
	t.Parallel()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	// The CA names itself, not a node — the shape of any certificate that
	// was not issued through enrolment.
	leaf, err := x509.ParseCertificate(pemBlock(t, ca.CertPEM))
	if err != nil {
		t.Fatal(err)
	}
	leaf.Subject.CommonName = ""

	if got := status.Code(checkMembership([]string{"node-1"}, leaf)); got != codes.Unauthenticated {
		t.Fatalf("got %v, want Unauthenticated", got)
	}
}
