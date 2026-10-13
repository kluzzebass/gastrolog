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
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

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

// One member's word never ends a membership: a compromised member, or a
// just-removed one this node has not yet seen go, would otherwise take out
// every node that dials it. A majority of the other voters must refuse it,
// each counted once, within one configuration.
func TestRemovalClaimsNeedAMajorityOfTheOtherVoters(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		others    []string
		claimants []string
		want      bool
	}{
		{"two-node cluster: the one other voter", []string{"a"}, []string{"a"}, true},
		{"three-node cluster: one of two", []string{"a", "b"}, []string{"a"}, false},
		{"three-node cluster: both", []string{"a", "b"}, []string{"a", "b"}, true},
		{"four-node cluster: one of three", []string{"a", "b", "c"}, []string{"a"}, false},
		{"four-node cluster: two of three", []string{"a", "b", "c"}, []string{"a", "c"}, true},
		{"one claimant, counted once however often it claims", []string{"a", "b", "c"}, []string{"a", "a", "a"}, false},
		{"claims from non-voters do not count", []string{"a", "b"}, []string{"a", "learner"}, false},
		{"no other voter", nil, []string{"a"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var c removalClaims
			reached := false
			for i, by := range tc.claimants {
				_, reached, _ = c.add("config", uint64(i+1), by, tc.others, noneRemoved)
			}
			if reached != tc.want {
				t.Fatalf("claims from %v against other voters %v reach a majority: %v, want %v", tc.claimants, tc.others, reached, tc.want)
			}
		})
	}

	t.Run("a configuration change starts the tally over", func(t *testing.T) {
		t.Parallel()
		var c removalClaims
		others := []string{"a", "b"}
		c.add("config-1", 1, "a", others, noneRemoved)
		if _, reached, _ := c.add("config-2", 2, "b", others, noneRemoved); reached {
			t.Fatal("a claim made under an earlier configuration still counted")
		}
	})

	// The cluster marks a node removed in its FSM before it changes the
	// configuration; in between, a claim that node made earlier must stop
	// counting, or it outlives the standing it was counted on.
	t.Run("a claimant later marked removed stops counting", func(t *testing.T) {
		t.Parallel()
		var c removalClaims
		others := []string{"a", "b", "c"}
		marked := map[string]bool{}
		removed := func(id string) bool { return marked[id] }
		c.add("config", 1, "a", others, removed)
		marked["a"] = true
		if n, reached, _ := c.add("config", 2, "b", others, removed); reached || n != 1 {
			t.Fatalf("after a was marked removed: %d claims counted, majority %v; want b's alone, no majority", n, reached)
		}
	})
}

func noneRemoved(string) bool { return false }

// Through the server's own configuration: in a three-voter configuration
// the first member's refusal is not acted on, a repeat of it changes
// nothing, and the second member's refusal ends membership.
func TestOneMembersRefusalDoesNotEndMembership(t *testing.T) {
	t.Parallel()
	const self = "node-self"
	members := []hraft.Server{
		{ID: self, Address: self},
		{ID: "node-a", Address: "node-a"},
		{ID: "node-b", Address: "node-b"},
	}
	s := &Server{cfg: Config{NodeID: self}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.raft = createVaultRaft(t, self, &hraft.MockFSM{}, true, members)
	var ended []string
	s.SetRefusedAsRemovedObserver(func(by string) { ended = append(ended, by) })

	s.observeRefusedAsRemoved("node-a")
	s.observeRefusedAsRemoved("node-a")
	if len(ended) != 0 {
		t.Fatalf("one member's refusal ended membership: %v", ended)
	}
	s.observeRefusedAsRemoved("node-b")
	if len(ended) != 1 {
		t.Fatalf("refusals from both other voters ended membership %d times, want once", len(ended))
	}
}

// claimWeigher is a server whose configuration and log a test controls, with
// its log captured.
func claimWeigher(t *testing.T, self string, members []hraft.Server) (*Server, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	s := &Server{cfg: Config{NodeID: self}, logger: slog.New(slog.NewTextHandler(logs, nil))}
	s.raft = createVaultRaft(t, self, &hraft.MockFSM{}, true, members)
	return s, logs
}

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) count(substr string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Count(b.b.String(), substr)
}

const waitingForMajority = "waiting for a majority of the other voters"

// One claimant answering every call this node makes — Raft traffic among
// them — costs one configuration read and one log line, not one per call,
// while nothing it is weighed against has changed.
func TestRepeatedRefusalsFromOneClaimantAreWeighedOnce(t *testing.T) {
	t.Parallel()
	const self = "node-self"
	s, logs := claimWeigher(t, self, []hraft.Server{
		{ID: self, Address: self},
		{ID: "node-a", Address: "node-a"},
		{ID: "node-b", Address: "node-b"},
		{ID: "node-c", Address: "node-c"},
	})
	for range 50 {
		s.observeRefusedAsRemoved("node-a")
	}
	if got := s.configReads.Load(); got != 1 {
		t.Fatalf("50 refusals from one claimant read the configuration %d times, want once", got)
	}
	if got := logs.count(waitingForMajority); got != 1 {
		t.Fatalf("50 refusals from one claimant logged %d times, want once", got)
	}
}

// When the log does move — a healthy node's log always does — the claimant
// is weighed again, and still logged once.
func TestRepeatedRefusalsLogOnceWhileTheLogMoves(t *testing.T) {
	t.Parallel()
	const self = "node-self"
	s, logs := claimWeigher(t, self, nil)
	waitVaultLeader(t, s.raft, 5*time.Second)
	if err := s.raft.AddNonvoter("node-a", "node-a", 0, time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if err := s.raft.Apply([]byte("entry"), time.Second).Error(); err != nil {
			t.Fatal(err)
		}
		s.observeRefusedAsRemoved("node-a")
	}
	if got := s.configReads.Load(); got != 10 {
		t.Fatalf("configuration read %d times across 10 log moves, want 10", got)
	}
	if got := logs.count(waitingForMajority); got != 1 {
		t.Fatalf("one claimant logged %d times while the log moved, want once", got)
	}
}

// The tally starts over when the configuration changes, which it detects by
// who is in the configuration and with what suffrage.
func TestConfigurationFingerprintTracksMembershipAndSuffrage(t *testing.T) {
	t.Parallel()
	base := localConfiguration{servers: []hraft.Server{
		{ID: "a", Suffrage: hraft.Voter}, {ID: "b", Suffrage: hraft.Voter},
	}}
	reordered := localConfiguration{servers: []hraft.Server{
		{ID: "b", Suffrage: hraft.Voter}, {ID: "a", Suffrage: hraft.Voter},
	}}
	demoted := localConfiguration{servers: []hraft.Server{
		{ID: "a", Suffrage: hraft.Voter}, {ID: "b", Suffrage: hraft.Nonvoter},
	}}
	shrunk := localConfiguration{servers: []hraft.Server{{ID: "a", Suffrage: hraft.Voter}}}
	if base.fingerprint() != reordered.fingerprint() {
		t.Error("the same configuration listed in another order reads as a change")
	}
	if base.fingerprint() == demoted.fingerprint() {
		t.Error("a suffrage change does not read as a configuration change")
	}
	if base.fingerprint() == shrunk.fingerprint() {
		t.Error("a membership change does not read as a configuration change")
	}
}
