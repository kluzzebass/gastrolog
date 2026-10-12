package cluster

// A node takes a refusal as word of its own removal only when it carries the
// removal detail naming this node, and only from the peer it dialled as a
// member, proven by that peer's certificate. Anything less would let a
// stranger, or a refusal meant for another node, take a member out.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"gastrolog/internal/cluster/tlsutil"
)

func leafNamed(t *testing.T, ca tlsutil.CAKeyPair, nodeID string) *x509.Certificate {
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
	return leaf
}

func peerNamed(t *testing.T, ca tlsutil.CAKeyPair, nodeID string) *peer.Peer {
	t.Helper()
	return &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{leafNamed(t, ca, nodeID)}}}}
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

// The refusal of an eviction notice is never word of the sender's own
// removal: the node it goes to is the one being removed, and a removed node
// answering the leader's notice must not take the leader out with it.
func TestARefusedEvictionNoticeNeverEndsTheSendersMembership(t *testing.T) {
	t.Parallel()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	const self, removed = "node-leader", "node-removed"
	var heard []string
	m := NewPeerConnManager(PeerConnManagerConfig{
		NodeID:             self,
		OnRefusedAsRemoved: func(by string) { heard = append(heard, by) },
	})
	answer := peerNamed(t, ca, removed)
	invoke := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, opts ...grpc.CallOption) error {
		for _, o := range opts {
			if po, ok := o.(grpc.PeerCallOption); ok {
				*po.PeerAddr = *answer
			}
		}
		return removedRefusal(self, "you were removed")
	}
	intercept := m.observeRemovalUnary(removed)
	_ = intercept(context.Background(), notifyEvictionMethod, nil, nil, nil, invoke)
	if len(heard) != 0 {
		t.Fatalf("a refused eviction notice ended the sender's membership: %v", heard)
	}
	_ = intercept(context.Background(), "/gastrolog.v1.ClusterService/Broadcast", nil, nil, nil, invoke)
	if len(heard) != 1 {
		t.Fatalf("a refusal to another call reached the membership check %d times, want 1", len(heard))
	}
}

// Only a current member this node does not know to be removed may tell it
// it was removed. A removed node still answers dials made to it — the
// eviction notice, vault-ctl Raft traffic until its groups drop it, pooled
// service calls — and must not be able to take a member out by answering.
func TestOnlyACurrentNonRemovedMemberMayEndMembership(t *testing.T) {
	t.Parallel()
	configured := []string{"node-a", "node-b"}
	removed := func(id string) bool { return id == "node-b" || id == "node-gone" }
	for _, tc := range []struct {
		byPeer string
		want   bool
	}{
		{"node-a", true},
		{"node-b", false},    // configured, but in the removal record
		{"node-gone", false}, // removed and no longer configured
		{"node-x", false},    // never a member
		{"", false},
	} {
		if got := refusalEndsMembership(configured, tc.byPeer, removed); got != tc.want {
			t.Errorf("refusal from %q ends membership: %v, want %v", tc.byPeer, got, tc.want)
		}
	}
	if refusalEndsMembership(nil, "node-a", removed) {
		t.Error("a node that cannot read its configuration took a removal claim")
	}
}
