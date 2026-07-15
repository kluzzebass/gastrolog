package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	hraft "github.com/hashicorp/raft"
)

// ErrNoVaultRaftLeader is returned when the vault control-plane Raft group
// has no elected leader.
var ErrNoVaultRaftLeader = errors.New("no vault raft leader")

// VaultApplyForwarder applies pre-marshaled vault control-plane FSM commands.
// If this node is the Raft leader, it applies locally; otherwise it forwards
// via ForwardVaultApply (same pattern as VaultCtlChunkApplyForwarder).
type VaultApplyForwarder struct {
	raft    *hraft.Raft
	groupID string
	peers   *PeerConnManager
	timeout time.Duration
}

// NewVaultApplyForwarder creates a forwarder for a vault control-plane Raft group.
func NewVaultApplyForwarder(r *hraft.Raft, groupID string, peers *PeerConnManager, timeout time.Duration) *VaultApplyForwarder {
	return &VaultApplyForwarder{
		raft:    r,
		groupID: groupID,
		peers:   peers,
		timeout: timeout,
	}
}

// Apply applies a vault control-plane command. Tries locally first; forwards on
// ErrNotLeader.
func (f *VaultApplyForwarder) Apply(data []byte) error {
	future := f.raft.Apply(data, f.timeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, hraft.ErrNotLeader) {
			return f.forwardToLeader(data)
		}
		return err
	}
	return nil
}

func (f *VaultApplyForwarder) forwardToLeader(data []byte) error {
	_, leaderID := f.raft.LeaderWithID()
	if leaderID == "" {
		return ErrNoVaultRaftLeader
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
	defer cancel()

	req := &gastrologv1.ForwardVaultApplyRequest{
		GroupId: []byte(f.groupID),
		Command: data,
	}
	resp := &gastrologv1.ForwardVaultApplyResponse{}
	if err := f.peers.InvokeService(ctx, string(leaderID), PurposeVaultApply,
		"/gastrolog.v1.ClusterService/ForwardVaultApply", req, resp); err != nil {
		return fmt.Errorf("forward vault apply to %s: %w", leaderID, err)
	}
	return nil
}
