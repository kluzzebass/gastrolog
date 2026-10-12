package cluster

// A node asking to rejoin reaches whatever member it was told about, and the
// leader's refusal of a removed node has to come back to it as a refusal —
// identical to what the leader would have answered directly — or a removed
// node reaching a follower would read an opaque internal error and an
// operator could not tell a retired identity from a broken cluster.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
)

// startForwardSuffrageLeader stands up a real gRPC ClusterService whose
// suffrage callback is fn, and returns a PeerConnManager that resolves
// "leader" to it.
func startForwardSuffrageLeader(t *testing.T, fn func(ctx context.Context, nodeID, nodeAddr string, voter bool) error) *PeerConnManager {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	leader := &Server{setNodeSuffrageFn: fn}
	gsrv := grpc.NewServer()
	gsrv.RegisterService(&clusterServiceDesc, leader)
	go func() { _ = gsrv.Serve(lis) }()

	mgr := NewStaticPeerConns("follower", func(id string) (string, bool) {
		if id == "leader" {
			return lis.Addr().String(), true
		}
		return "", false
	})
	t.Cleanup(func() {
		_ = mgr.Close()
		gsrv.Stop()
		_ = lis.Close()
	})
	return mgr
}

func TestForwardSetNodeSuffrage_RemovedNodeRefusalSurvivesTheHop(t *testing.T) {
	t.Parallel()
	mgr := startForwardSuffrageLeader(t, func(_ context.Context, nodeID, _ string, _ bool) error {
		return fmt.Errorf("%s: %w", nodeID, ErrNodeRemoved)
	})

	err := ForwardSetNodeSuffrage(context.Background(), mgr, "leader", "node-gone", "node-gone:4566", false)
	if !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("the leader's refusal of a removed node lost its identity crossing the hop: %v", err)
	}
	if want := "node-gone: " + ErrNodeRemoved.Error(); err.Error() != want {
		t.Fatalf("follower reads differently from the leader:\n follower: %s\n   leader: %s", err, want)
	}
}

// The translation must be specific: a leader failure that is not a removal
// refusal must not be presented as one.
func TestForwardSetNodeSuffrage_OtherFailuresStayFailures(t *testing.T) {
	t.Parallel()
	mgr := startForwardSuffrageLeader(t, func(context.Context, string, string, bool) error {
		return errors.New("raft: configuration change timed out")
	})

	err := ForwardSetNodeSuffrage(context.Background(), mgr, "leader", "node-2", "node-2:4566", false)
	if err == nil {
		t.Fatal("a failed suffrage change was reported as success")
	}
	if errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("an unrelated failure was presented as a removed-node refusal: %v", err)
	}
}

// The request arrives at the leader as it left the follower.
func TestForwardSetNodeSuffrage_CarriesTheRequest(t *testing.T) {
	t.Parallel()
	type got struct {
		id, addr string
		voter    bool
	}
	seen := make(chan got, 1)
	mgr := startForwardSuffrageLeader(t, func(_ context.Context, nodeID, nodeAddr string, voter bool) error {
		seen <- got{nodeID, nodeAddr, voter}
		return nil
	})

	if err := ForwardSetNodeSuffrage(context.Background(), mgr, "leader", "node-2", "node-2:4566", true); err != nil {
		t.Fatalf("ForwardSetNodeSuffrage: %v", err)
	}
	if g := <-seen; g != (got{"node-2", "node-2:4566", true}) {
		t.Fatalf("request altered across the hop: %+v", g)
	}
}
