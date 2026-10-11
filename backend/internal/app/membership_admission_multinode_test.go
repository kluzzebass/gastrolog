package app

// Removal is the cluster's only revocation: a removed node's certificate
// still chains to the CA every member trusts, and the node keeps its key. The
// tests here drive the membership request — the one call such a certificate
// can still make — over real mTLS, against a real Raft configuration, from
// every position a caller can take: a fresh joiner, a learner asking again, a
// removed node asking through a follower, through the leader, and through a
// leader elected after the removal.

import (
	"context"
	"errors"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/glid"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/system/raftstore"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

// admissionNode is one cluster node with the production membership and
// removal wiring over a real cluster-ctl Raft and an mTLS cluster port.
type admissionNode struct {
	id          string
	srv         *cluster.Server
	raft        *hraft.Raft
	store       *raftstore.Store
	tls         *cluster.ClusterTLS
	setSuffrage func(ctx context.Context, targetNodeID string, voter bool) error
	remove      cluster.RemoveNodeFunc
	gate        *membershipGate
}

func newAdmissionNode(t *testing.T, ca tlsutil.CAKeyPair, bootstrap bool) *admissionNode {
	t.Helper()
	id := glid.New().String()

	cert, err := tlsutil.GenerateNodeCert(ca.CertPEM, ca.KeyPEM, id, cluster.LaneSANs)
	if err != nil {
		t.Fatalf("GenerateNodeCert: %v", err)
	}
	ctls := cluster.NewClusterTLS()
	if err := ctls.Load(cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		t.Fatalf("Load cluster TLS: %v", err)
	}

	srv, err := cluster.New(cluster.Config{
		ClusterAddr: "127.0.0.1:0",
		NodeID:      id,
		TLS:         ctls,
		Logger:      discardLogger(),
	})
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}
	transport := srv.Transport()

	fsm := raftfsm.New()
	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(id)
	conf.LogOutput = discardWriter{}
	conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.ElectionTimeout = 500 * time.Millisecond
	conf.LeaderLeaseTimeout = 250 * time.Millisecond

	r, err := hraft.NewRaft(conf, fsm, hraft.NewInmemStore(), hraft.NewInmemStore(), hraft.NewInmemSnapshotStore(), transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	if bootstrap {
		boot := hraft.Configuration{Servers: []hraft.Server{
			{ID: hraft.ServerID(id), Address: transport.LocalAddr()},
		}}
		if err := r.BootstrapCluster(boot).Error(); err != nil {
			t.Fatalf("BootstrapCluster: %v", err)
		}
	}

	store := raftstore.New(r, fsm, 10*time.Second)
	srv.SetRaft(r)
	srv.SetApplyFn(func(_ context.Context, data []byte) (uint64, error) {
		return store.ApplyRaw(data)
	})
	fwd := cluster.NewForwarder(r, srv.PeerConns())
	store.SetForwarder(fwd)

	var gate membershipGate
	n := &admissionNode{id: id, srv: srv, raft: r, store: store, tls: ctls, gate: &gate}
	n.remove = makeRemoveNodeFunc(srv, store, &gate, id, discardLogger())
	n.setSuffrage = makeSetNodeSuffrageFunc(srv, store, &gate, id, nil, discardLogger())

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = fwd.Close()
		_ = r.Shutdown().Error()
		srv.Stop()
	})
	return n
}

// discardWriter swallows Raft's own log output.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// join asks via to put joiner in the configuration, the way a node does at
// startup: holding the certificate enrolment issued it.
func join(t *testing.T, joiner, via *admissionNode) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return cluster.JoinCluster(ctx, nil, via.srv.Addr(), joiner.id, joiner.srv.Addr(), joiner.tls)
}

// requestMembershipClaimingVote sends a membership request carrying a voter
// claim in the field the request once used for one. A hostile client can
// send any bytes it likes; the claim must buy nothing.
func requestMembershipClaimingVote(t *testing.T, joiner, via *admissionNode) error {
	t.Helper()
	conn, err := grpc.NewClient(via.srv.Addr(), grpc.WithTransportCredentials(joiner.tls.TransportCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	req := &gastrologv1.RequestMembershipRequest{NodeId: []byte(joiner.id), NodeAddr: joiner.srv.Addr()}
	req.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 3, protowire.VarintType), 1))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return conn.Invoke(ctx, "/gastrolog.v1.ClusterService/RequestMembership", req, &gastrologv1.RequestMembershipResponse{})
}

// suffrageIn returns id's suffrage in the configuration n holds, or "" when
// it is absent.
func suffrageIn(t *testing.T, n *admissionNode, id string) string {
	t.Helper()
	s, err := configuredSuffrage(n.srv, id)
	if err != nil {
		t.Fatalf("read configuration: %v", err)
	}
	return s
}

// configurationOf returns the configuration n holds, as ID → suffrage.
func configurationOf(t *testing.T, n *admissionNode) map[string]string {
	t.Helper()
	servers, err := n.srv.Servers()
	if err != nil {
		t.Fatalf("read configuration: %v", err)
	}
	out := make(map[string]string, len(servers))
	for _, s := range servers {
		out[s.ID] = s.Suffrage
	}
	return out
}

func sameConfiguration(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for id, s := range a {
		if b[id] != s {
			return false
		}
	}
	return true
}

// requireRefusedAsRemoved asserts a membership request was refused as a
// removed node's, and that the configuration did not move.
func requireRefusedAsRemoved(t *testing.T, err error, leader *admissionNode, before map[string]string, how string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: the removed node was let back into the cluster", how)
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("%s: refused with %v (%v), want PermissionDenied", how, status.Code(err), err)
	}
	if after := configurationOf(t, leader); !sameConfiguration(before, after) {
		t.Fatalf("%s: the refused request changed the configuration:\n before %v\n  after %v", how, before, after)
	}
}

func TestRemovedNodeStaysOutOfTheCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node real-raft membership test")
	}
	t.Parallel()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	n1 := newAdmissionNode(t, ca, true)
	select {
	case <-n1.raft.LeaderCh():
	case <-time.After(10 * time.Second):
		t.Fatal("bootstrap node never became leader")
	}
	n2 := newAdmissionNode(t, ca, false)
	n3 := newAdmissionNode(t, ca, false)
	n4 := newAdmissionNode(t, ca, false)

	// Fresh joiners with a certificate from enrolment enter as learners and
	// are promoted the normal way.
	for _, n := range []*admissionNode{n2, n3} {
		if err := join(t, n, n1); err != nil {
			t.Fatalf("fresh joiner refused: %v", err)
		}
		if got := suffrageIn(t, n1, n.id); got != "Nonvoter" {
			t.Fatalf("fresh joiner admitted as %q, want Nonvoter", got)
		}
		if err := n1.setSuffrage(ctx, n.id, true); err != nil {
			t.Fatalf("promote: %v", err)
		}
		if got := suffrageIn(t, n1, n.id); got != suffrageVoter {
			t.Fatalf("promoted learner is %q, want Voter", got)
		}
	}

	// A joiner reaching a follower is admitted the same way.
	if err := join(t, n4, n2); err != nil {
		t.Fatalf("joiner through a follower refused: %v", err)
	}
	if got := suffrageIn(t, n1, n4.id); got != "Nonvoter" {
		t.Fatalf("joiner through a follower admitted as %q, want Nonvoter", got)
	}

	if err := n1.remove(ctx, n4.id, cluster.RemoveNodeOptions{Policy: cluster.RemovalPolicyOperator}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	before := configurationOf(t, n1)
	if _, ok := before[n4.id]; ok {
		t.Fatal("the removal did not take the node out of the configuration")
	}

	// The removed node's certificate still chains to the CA. Asking back in,
	// with or without a claim to a vote, through a follower or the leader,
	// buys nothing.
	requireRefusedAsRemoved(t, join(t, n4, n2), n1, before, "through a follower")
	requireRefusedAsRemoved(t, join(t, n4, n1), n1, before, "through the leader")
	requireRefusedAsRemoved(t, requestMembershipClaimingVote(t, n4, n2), n1, before, "claiming a vote through a follower")
	requireRefusedAsRemoved(t, requestMembershipClaimingVote(t, n4, n1), n1, before, "claiming a vote through the leader")

	// The record is cluster state, held by every member.
	for _, n := range []*admissionNode{n1, n2, n3} {
		if err := n.store.Barrier(ctx); err != nil {
			t.Fatalf("barrier: %v", err)
		}
		if removed, err := n.store.IsNodeRemoved(ctx, n4.id); err != nil || !removed {
			t.Fatalf("member %s holds removed=%v (%v) for the removed node", n.id, removed, err)
		}
	}

	// A node the cluster never removed and never admitted claims a vote:
	// it gets a learner's seat, and asking again does not promote it.
	n5 := newAdmissionNode(t, ca, false)
	if err := requestMembershipClaimingVote(t, n5, n1); err != nil {
		t.Fatalf("fresh joiner claiming a vote was refused outright: %v", err)
	}
	if got := suffrageIn(t, n1, n5.id); got != "Nonvoter" {
		t.Fatalf("a joiner that claimed a vote was admitted as %q", got)
	}
	if err := requestMembershipClaimingVote(t, n5, n2); err != nil {
		t.Fatalf("a learner's repeated request was refused: %v", err)
	}
	if got := suffrageIn(t, n1, n5.id); got != "Nonvoter" {
		t.Fatalf("a learner asking again was raised to %q", got)
	}

	// Leadership moves. The new leader has the record only because it was
	// replicated to it, and refuses on it all the same.
	observed := make(chan hraft.Observation, 64)
	n2.srv.RegisterLeaderObserver(observed)
	n3.srv.RegisterLeaderObserver(observed)
	if err := n1.srv.LeadershipTransfer(); err != nil {
		t.Fatalf("leadership transfer: %v", err)
	}
	newLeader := awaitLeaderOtherThan(t, observed, n1.id, n2, n3)
	before = configurationOf(t, newLeader)
	requireRefusedAsRemoved(t, join(t, n4, newLeader), newLeader, before, "through a leader elected after the removal")
}

// awaitLeaderOtherThan waits for a leader observation naming a node other
// than old, and returns that node.
func awaitLeaderOtherThan(t *testing.T, observed chan hraft.Observation, old string, candidates ...*admissionNode) *admissionNode {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case o := <-observed:
			lo, ok := o.Data.(hraft.LeaderObservation)
			if !ok || string(lo.LeaderID) == "" || string(lo.LeaderID) == old {
				continue
			}
			for _, c := range candidates {
				if c.id == string(lo.LeaderID) {
					return c
				}
			}
		case <-deadline:
			t.Fatal("no new leader was observed after the transfer")
		}
	}
}

// TestPromotionRefusesANodeTheClusterRemoved: a removal writes its record
// before the node leaves the configuration, and the configuration change can
// fail after the record lands. While a removed node is still configured,
// neither an operator's promotion nor the learner promoter's may make it a
// voter — hashicorp/raft's AddVoter would otherwise grant the removed node a
// vote, or put it back after its RemoveServer.
func TestPromotionRefusesANodeTheClusterRemoved(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node real-raft membership test")
	}
	t.Parallel()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	n1 := newAdmissionNode(t, ca, true)
	select {
	case <-n1.raft.LeaderCh():
	case <-time.After(10 * time.Second):
		t.Fatal("bootstrap node never became leader")
	}
	n2 := newAdmissionNode(t, ca, false)
	if err := join(t, n2, n1); err != nil {
		t.Fatalf("fresh joiner refused: %v", err)
	}
	if got := suffrageIn(t, n1, n2.id); got != "Nonvoter" {
		t.Fatalf("fresh joiner admitted as %q, want Nonvoter", got)
	}

	if err := n1.store.MarkNodeRemoved(ctx, n2.id); err != nil {
		t.Fatalf("record removal: %v", err)
	}

	err = n1.setSuffrage(ctx, n2.id, true)
	if !errors.Is(err, cluster.ErrNodeRemoved) {
		t.Fatalf("operator promotion of a removed node: err = %v, want ErrNodeRemoved", err)
	}
	if got := suffrageIn(t, n1, n2.id); got != "Nonvoter" {
		t.Fatalf("operator promotion made the removed node %q", got)
	}

	promote := func(nodeID string) error {
		return changeMemberSuffrageOnLeader(ctx, n1.srv, n1.store, n1.gate, discardLogger(), nodeID, true)
	}
	if err := promote(n2.id); !errors.Is(err, cluster.ErrNodeRemoved) {
		t.Fatalf("learner promotion of a removed node: err = %v, want ErrNodeRemoved", err)
	}
	if got := suffrageIn(t, n1, n2.id); got != "Nonvoter" {
		t.Fatalf("learner promotion made the removed node %q", got)
	}
}
