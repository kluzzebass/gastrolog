package cluster

import (
	"context"
	"errors"
	"fmt"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Forwarder sends pre-marshaled ConfigCommand bytes to the current Raft leader's
// cluster port via the ForwardApply RPC. Used by raftstore.Store on follower
// nodes to transparently proxy config writes.
type Forwarder struct {
	raft  *hraft.Raft
	peers *PeerConnManager
}

// NewForwarder creates a Forwarder that resolves the leader from r and dials
// through the peer connection manager.
func NewForwarder(r *hraft.Raft, peers *PeerConnManager) *Forwarder {
	return &Forwarder{raft: r, peers: peers}
}

// Forward sends a pre-marshaled ConfigCommand to the leader for raft.Apply().
func (f *Forwarder) Forward(ctx context.Context, data []byte) (uint64, error) {
	_, leaderID := f.raft.LeaderWithID()
	if leaderID == "" {
		return 0, errors.New("no known leader")
	}

	h, err := f.peers.AcquireService(string(leaderID), PurposeForward)
	if err != nil {
		return 0, err
	}
	defer h.Release()

	ctx, cancel := context.WithTimeout(ctx, ReplicationTimeout)
	defer cancel()
	client := NewForwardApplyClient(h.GRPC())
	resp, err := client.ForwardApply(ctx, &gastrologv1.ForwardApplyRequest{Command: data})
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.FailedPrecondition {
			// The leader applied the command and rejected it; the
			// connection is fine.
			return 0, fmt.Errorf("%w: %s", system.ErrCommandRejected, st.Message())
		}
		h.Invalidate(err)
		return 0, err
	}
	return resp.GetAppliedIndex(), nil
}

// Close is a no-op — connection lifecycle is managed by PeerConnManager.
func (f *Forwarder) Close() error { return nil }
