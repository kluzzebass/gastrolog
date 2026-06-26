package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft"

	hraft "github.com/hashicorp/raft"
)

// ErrNoRaftLeader is returned when the target Raft group has no elected leader.
var ErrNoRaftLeader = errors.New("no raft leader")

// VaultCtlChunkApplyForwarder applies chunk-FSM commands to the vault
// control-plane Raft group. Every payload is wrapped as a vaultraft
// OpVaultChunkFSM entry keyed by vault-instance ID. If this node is the
// vault-ctl Raft leader, Apply runs locally; otherwise it forwards via
// ForwardVaultApply RPC to the current leader. Constructed via
// NewVaultCtlChunkApplyForwarder.
type VaultCtlChunkApplyForwarder struct {
	raft            *hraft.Raft
	vaultCtlGroupID string
	vaultID          glid.GLID
	peers           *PeerConnManager
	timeout         time.Duration
}

// NewVaultCtlChunkApplyForwarder creates a forwarder that applies vaultctlfsm
// commands to the vault control-plane Raft group, wrapping each payload
// with OpVaultChunkFSM + instance ID. ForwardVaultApply uses the vault-ctl
// group_id.
func NewVaultCtlChunkApplyForwarder(r *hraft.Raft, vaultCtlGroupID string, vaultID glid.GLID, peers *PeerConnManager, timeout time.Duration) *VaultCtlChunkApplyForwarder {
	return &VaultCtlChunkApplyForwarder{
		raft:            r,
		vaultCtlGroupID: vaultCtlGroupID,
		vaultID:          vaultID,
		peers:           peers,
		timeout:         timeout,
	}
}

// Apply applies a chunk-FSM command. Tries locally first; forwards to the
// vault-ctl Raft leader on ErrNotLeader.
func (f *VaultCtlChunkApplyForwarder) Apply(data []byte) error {
	payload := vaultraft.MarshalVaultChunkCommand(f.vaultID, data)
	future := f.raft.Apply(payload, f.timeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, hraft.ErrNotLeader) {
			return f.forwardToLeader(payload)
		}
		return err
	}
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok && err != nil {
			return err
		}
	}
	return nil
}

func (f *VaultCtlChunkApplyForwarder) forwardToLeader(data []byte) error {
	_, leaderID := f.raft.LeaderWithID()
	if leaderID == "" {
		return ErrNoRaftLeader
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
	defer cancel()

	req := &gastrologv1.ForwardVaultApplyRequest{
		GroupId: []byte(f.vaultCtlGroupID),
		Command: data,
	}
	resp := &gastrologv1.ForwardVaultApplyResponse{}
	if err := f.peers.InvokeService(ctx, string(leaderID), "vault-ctl-chunk-apply-forward",
		"/gastrolog.v1.ClusterService/ForwardVaultApply", req, resp); err != nil {
		return fmt.Errorf("forward vault-ctl chunk apply RPC to %s: %w", leaderID, err)
	}
	return nil
}
