package cluster

// A node takes a refusal as word of its own removal only when it carries the
// removal detail naming this node, and only from the peer it dialled as a
// member, proven by that peer's certificate. Anything less would let a
// stranger, or a refusal meant for another node, take a member out.

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"gastrolog/internal/cluster/tlsutil"
)

func peerNamed(t *testing.T, ca tlsutil.CAKeyPair, nodeID string) *peer.Peer {
	t.Helper()
	cert, err := tlsutil.GenerateNodeCert(ca.CertPEM, ca.KeyPEM, nodeID, LaneSANs)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(cert.CertPEM)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}}}
}

func TestOnlyTheDialledMembersRefusalOfThisNodeEndsMembership(t *testing.T) {
	t.Parallel()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	const self, member = "node-self", "node-member"
	for _, tc := range []struct {
		name string
		err  error
		peer *peer.Peer
		want bool
	}{
		{"the member refuses this node as removed", removedRefusal(self, "removed"), peerNamed(t, ca, member), true},
		{"wrapped on the way up", fmt.Errorf("raft rpc: %w", removedRefusal(self, "removed")), peerNamed(t, ca, member), true},
		{"the refusal names another node", removedRefusal("node-other", "removed"), peerNamed(t, ca, member), false},
		{"the answering certificate names another node", removedRefusal(self, "removed"), peerNamed(t, ca, "node-impostor"), false},
		{"no certificate", removedRefusal(self, "removed"), &peer.Peer{}, false},
		{"a refusal without the detail", status.Error(codes.PermissionDenied, "node node-self was removed from this cluster"), peerNamed(t, ca, member), false},
		{"another failure", errors.New("connection refused"), peerNamed(t, ca, member), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var heard []string
			m := NewPeerConnManager(PeerConnManagerConfig{
				NodeID:             self,
				OnRefusedAsRemoved: func(by string) { heard = append(heard, by) },
			})
			m.observeRefusal(tc.err, member, tc.peer)
			if got := len(heard) == 1 && heard[0] == member; got != tc.want {
				t.Fatalf("membership ended: %v (heard %v), want %v", got, heard, tc.want)
			}
		})
	}
}
