package cluster

import (
	"context"
	"errors"
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
// caller happened to reach (gastrolog-3vyex).

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

// forwardFromFollower performs the follower-side half of the hop: build
// the request the way app.makeRemoveNodeFunc does and invoke it over the
// service lane.
func forwardFromFollower(t *testing.T, mgr *PeerConnManager, target string, opts RemoveNodeOptions) error {
	t.Helper()
	return NewForwardRemoveNodeClient(&peerConnInvoker{mgr: mgr}).ForwardRemoveNode(context.Background(), target, opts)
}

// peerConnInvoker adapts the PeerConnManager's service lane to the
// grpc.ClientConnInterface the generated forward client expects.
type peerConnInvoker struct {
	mgr *PeerConnManager
}

func (p *peerConnInvoker) Invoke(ctx context.Context, method string, req, reply any, _ ...grpc.CallOption) error {
	return p.mgr.InvokeService(ctx, "leader", PurposeRemoveNode, method, req, reply)
}

func (p *peerConnInvoker) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("streaming not supported")
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
