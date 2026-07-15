package cluster_test

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/system/raftstore"

	"github.com/Jille/raftadmin/proto"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

func sharedTestClusterTLS(t *testing.T) *cluster.ClusterTLS {
	t.Helper()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	cert, err := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, cluster.LaneSANs)
	if err != nil {
		t.Fatalf("GenerateClusterCert: %v", err)
	}
	ctls := cluster.NewClusterTLS()
	if err := ctls.Load(cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		t.Fatalf("Load cluster TLS: %v", err)
	}
	return ctls
}

func newTLSClusterNode(t *testing.T, nodeID string, ctls *cluster.ClusterTLS, bootstrap bool) *testNode {
	t.Helper()

	srv, err := cluster.New(cluster.Config{
		ClusterAddr: "127.0.0.1:0",
		NodeID:      nodeID,
		TLS:         ctls,
		Logger:      slog.Default(),
	})
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}

	transport := srv.Transport()

	fsm := raftfsm.New()
	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(nodeID)
	conf.LogOutput = io.Discard
	conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.ElectionTimeout = 500 * time.Millisecond
	conf.LeaderLeaseTimeout = 250 * time.Millisecond

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()

	r, err := hraft.NewRaft(conf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}

	if bootstrap {
		boot := hraft.Configuration{
			Servers: []hraft.Server{
				{ID: hraft.ServerID(nodeID), Address: transport.LocalAddr()},
			},
		}
		if err := r.BootstrapCluster(boot).Error(); err != nil {
			t.Fatalf("BootstrapCluster: %v", err)
		}
	}

	store := raftstore.New(r, fsm, 10*time.Second)
	srv.SetRaft(r)
	srv.SetApplyFn(func(ctx context.Context, data []byte) (uint64, error) {
		return store.ApplyRaw(data)
	})
	fwd := cluster.NewForwarder(r, srv.PeerConns())
	store.SetForwarder(fwd)
	t.Cleanup(func() { _ = fwd.Close() })

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	return &testNode{srv: srv, raft: r, store: store, fsm: fsm}
}

func addVoterTLS(t *testing.T, ctls *cluster.ClusterTLS, leaderAddr, voterID, voterAddr string) {
	t.Helper()
	conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(ctls.TransportCredentials()))
	if err != nil {
		t.Fatalf("dial leader for AddVoter: %v", err)
	}
	defer conn.Close()

	client := proto.NewRaftAdminClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.AddVoter(ctx, &proto.AddVoterRequest{
		Id:      voterID,
		Address: voterAddr,
	})
	if err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	if _, err := client.Await(ctx, resp); err != nil {
		t.Fatalf("Await AddVoter: %v", err)
	}
}

func waitAnyLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.After(timeout)
	for {
		for _, n := range nodes {
			if n.raft.State() == hraft.Leader {
				return n
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for a leader with TLS lane isolation")
		default:
			runtime.Gosched()
		}
	}
}

// TestFourNodeClusterElectionWithTLSLaneIsolation verifies cluster-ctl can
// elect a leader when outbound raft dials use per-group SNI (gastrolog-raft.config).
// Regression for gastrolog-1dg8z: broken TLS verification on per-group SNIs
// caused silent pre-vote RPC failures (refused=3) after switching from the
// legacy gastrolog-raft lane.
func TestFourNodeClusterElectionWithTLSLaneIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node TLS cluster test in short mode")
	}

	ctls := sharedTestClusterTLS(t)

	node1 := newTLSClusterNode(t, "node-1", ctls, true)
	t.Cleanup(node1.close)
	waitLeader(t, node1.raft, 10*time.Second)

	node2 := newTLSClusterNode(t, "node-2", ctls, false)
	t.Cleanup(node2.close)
	node3 := newTLSClusterNode(t, "node-3", ctls, false)
	t.Cleanup(node3.close)
	node4 := newTLSClusterNode(t, "node-4", ctls, false)
	t.Cleanup(node4.close)

	addVoterTLS(t, ctls, node1.srv.Addr(), "node-2", node2.srv.Addr())
	addVoterTLS(t, ctls, node1.srv.Addr(), "node-3", node3.srv.Addr())
	addVoterTLS(t, ctls, node1.srv.Addr(), "node-4", node4.srv.Addr())

	nodes := []*testNode{node1, node2, node3, node4}
	waitAnyLeader(t, nodes, 15*time.Second)

	mgr := node1.srv.PeerConns()
	handle, err := mgr.AcquireRaftPeer("node-2", cluster.ConfigGroupID, "test/election")
	if err != nil {
		t.Fatalf("AcquireRaftPeer config lane: %v", err)
	}
	defer handle.Release()
	st := handle.GRPC().GetState()
	if st != connectivity.Ready && st != connectivity.Idle {
		t.Fatalf("raft lane conn state = %v, want Ready or Idle", st)
	}
}
