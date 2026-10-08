package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
)

// A RemoveNode request can land on any node — the gates live on the
// leader, so a follower forwards. The removal POLICY has to survive that
// hop: a preStop self-removal forwarded from a follower must still be
// evaluated optimistically on the leader, and an operator removal
// pessimistically, or the gate's stance would depend on which node the
// caller happened to reach.

// startForwardRemoveNodeLeader stands up a real gRPC ClusterService whose
// removeNodeFn is fn, and returns a PeerConnManager that resolves
// "leader" to it plus a teardown.
func startForwardRemoveNodeLeader(t *testing.T, fn RemoveNodeFunc) (*PeerConnManager, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	leader := &Server{removeNodeFn: fn}
	gsrv := grpc.NewServer()
	gsrv.RegisterService(&clusterServiceDesc, leader)
	go func() { _ = gsrv.Serve(lis) }()

	mgr := NewStaticPeerConns("follower", func(id string) (string, bool) {
		if id == "leader" {
			return lis.Addr().String(), true
		}
		return "", false
	})
	return mgr, func() {
		_ = mgr.Close()
		gsrv.Stop()
		_ = lis.Close()
	}
}

// forwardFromFollower performs the follower-side half of the hop through
// the same function production calls, so the tests cover the translation the
// operator actually sees.
func forwardFromFollower(t *testing.T, mgr *PeerConnManager, target string, opts RemoveNodeOptions) error {
	t.Helper()
	return ForwardRemoveNode(context.Background(), mgr, "leader", target, opts)
}

// TestForwardRemoveNode_CarriesPolicyAndForce: every combination of
// policy and force reaches the leader's gates intact.
func TestForwardRemoveNode_CarriesPolicyAndForce(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]RemoveNodeOptions{
		"operator":       {Policy: RemovalPolicyOperator},
		"operator+force": {Policy: RemovalPolicyOperator, Force: true},
		"self":           {Policy: RemovalPolicySelf},
		"self+force":     {Policy: RemovalPolicySelf, Force: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var gotNode string
			var gotOpts RemoveNodeOptions
			mgr, cleanup := startForwardRemoveNodeLeader(t, func(_ context.Context, nodeID string, opts RemoveNodeOptions) error {
				mu.Lock()
				defer mu.Unlock()
				gotNode, gotOpts = nodeID, opts
				return nil
			})
			defer cleanup()

			if err := forwardFromFollower(t, mgr, "node-target", want); err != nil {
				t.Fatalf("ForwardRemoveNode: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if gotNode != "node-target" {
				t.Fatalf("target: got %q, want node-target", gotNode)
			}
			if gotOpts != want {
				t.Fatalf("options across the hop: got %+v, want %+v", gotOpts, want)
			}
		})
	}
}

// A gate refusal on the leader comes back to the forwarding node with
// its message intact — that string is what the RPC layer classifies as
// operator-correctable and what the operator reads.
func TestForwardRemoveNode_RefusalReachesCaller(t *testing.T) {
	t.Parallel()
	refusal := `refusing to remove node node-target: removal would drop a vault below its replication factor — 1 vault(s) affected: "logs"`
	mgr, cleanup := startForwardRemoveNodeLeader(t, func(context.Context, string, RemoveNodeOptions) error {
		return errors.New(refusal)
	})
	defer cleanup()

	err := forwardFromFollower(t, mgr, "node-target", RemoveNodeOptions{Policy: RemovalPolicyOperator})
	if err == nil {
		t.Fatal("expected the leader's refusal to reach the follower")
	}
	for _, want := range []string{"refusing to remove node", "replication factor", "logs"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("forwarded refusal missing %q in: %v", want, err)
		}
	}
}

// A not-in-cluster refusal must keep its identity across the hop: the
// sentinel cannot cross the wire as a Go error, so the leader encodes it as
// the NotFound status code and the follower translates it back. Without the
// encoding, a follower-received removal of an unknown node sanitizes into an
// opaque internal error instead of an operator-readable refusal.
func TestForwardRemoveNode_NotInClusterSurvivesTheHop(t *testing.T) {
	t.Parallel()
	mgr, cleanup := startForwardRemoveNodeLeader(t, func(_ context.Context, target string, _ RemoveNodeOptions) error {
		return fmt.Errorf("remove server: %s: %w", target, ErrNodeNotInCluster)
	})
	defer cleanup()

	err := forwardFromFollower(t, mgr, "node-ghost", RemoveNodeOptions{Policy: RemovalPolicyOperator})
	if err == nil {
		t.Fatal("expected the leader's not-in-cluster refusal to reach the follower")
	}
	if !errors.Is(err, ErrNodeNotInCluster) {
		t.Fatalf("refusal lost its sentinel crossing the hop: %v", err)
	}
	if want := "remove server: node-ghost: node not in cluster configuration"; err.Error() != want {
		t.Fatalf("follower reads differently from the leader:\n follower: %s\n   leader: %s", err, want)
	}
}

// A removal-gate refusal must survive the hop as a refusal: the leader
// encodes it as FailedPrecondition, and the follower translates it into an
// error matching ErrRemovalRefused that carries the leader's message — which
// gate, which vaults — verbatim. Without the translation the RemoveNode
// handler on a follower cannot recognize the refusal and sanitizes the
// operator-actionable detail into an opaque internal error.
func TestForwardRemoveNode_GateRefusalSurvivesTheHop(t *testing.T) {
	t.Parallel()
	for _, gate := range []error{ErrWouldDropBelowRF, ErrWouldOrphanVaults} {
		mgr, cleanup := startForwardRemoveNodeLeader(t, func(_ context.Context, target string, _ RemoveNodeOptions) error {
			return fmt.Errorf("refusing to remove node %s: %w — 1 vault(s) affected: \"logs\"", target, gate)
		})

		err := forwardFromFollower(t, mgr, "node-target", RemoveNodeOptions{Policy: RemovalPolicyOperator})
		cleanup()
		if !errors.Is(err, ErrRemovalRefused) {
			t.Fatalf("gate %q: refusal lost its identity crossing the hop: %v", gate, err)
		}
		want := fmt.Sprintf("refusing to remove node node-target: %v — 1 vault(s) affected: \"logs\"", gate)
		if err.Error() != want {
			t.Fatalf("gate %q: follower reads differently from the leader:\n follower: %s\n   leader: %s", gate, err, want)
		}
	}
}
