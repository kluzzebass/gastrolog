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

// ErrNoTierRaftLeader is returned when the tier Raft group has no elected leader.
var ErrNoTierRaftLeader = errors.New("no tier raft leader")

// TierApplyForwarder applies pre-marshaled tier FSM commands to a tier Raft
// group. If this node is the tier Raft leader, it applies locally. Otherwise,
// it forwards to the current leader via the ForwardTierApply RPC.
//
// This decouples the config placement leader (which runs retention and chunk
// lifecycle) from the tier Raft leader (which may be on a different node after
// a cluster restart).
type TierApplyForwarder struct {
	raft    *hraft.Raft
	groupID string
	peers   *PeerConns
	timeout time.Duration
	// xform, when non-nil, maps a tierfsm wire payload to the bytes passed to
	// Raft.Apply / ForwardTierApply (e.g. vault OpTierFSM wrapper).
	xform func([]byte) []byte
}

// NewTierApplyForwarder creates a forwarder for a specific tier Raft group.
func NewTierApplyForwarder(r *hraft.Raft, groupID string, peers *PeerConns, timeout time.Duration) *TierApplyForwarder {
	return &TierApplyForwarder{
		raft:    r,
		groupID: groupID,
		peers:   peers,
		timeout: timeout,
	}
}

// NewVaultCtlTierApplyForwarder creates a forwarder that applies tierfsm commands
// to the vault control-plane Raft group, wrapping each payload with
// OpTierFSM + tier ID. ForwardTierApply uses the vault ctl group_id.
func NewVaultCtlTierApplyForwarder(r *hraft.Raft, vaultCtlGroupID string, tierID glid.GLID, peers *PeerConns, timeout time.Duration) *TierApplyForwarder {
	return &TierApplyForwarder{
		raft:    r,
		groupID: vaultCtlGroupID,
		peers:   peers,
		timeout: timeout,
		xform: func(p []byte) []byte {
			return vaultraft.MarshalTierCommand(tierID, p)
		},
	}
}

func (f *TierApplyForwarder) wirePayload(data []byte) []byte {
	if f.xform != nil {
		return f.xform(data)
	}
	return data
}

// Apply applies a tier FSM command. Tries locally first; forwards to the
// tier Raft leader on ErrNotLeader.
func (f *TierApplyForwarder) Apply(data []byte) error {
	payload := f.wirePayload(data)
	future := f.raft.Apply(payload, f.timeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, hraft.ErrNotLeader) {
			return f.forwardToLeader(payload)
		}
		return err
	}
	return nil
}

func (f *TierApplyForwarder) forwardToLeader(data []byte) error {
	_, leaderID := f.raft.LeaderWithID()
	if leaderID == "" {
		return ErrNoTierRaftLeader
	}

	conn, err := f.peers.Conn(string(leaderID))
	if err != nil {
		return fmt.Errorf("dial tier raft leader %s: %w", leaderID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
	defer cancel()

	req := &gastrologv1.ForwardTierApplyRequest{
		GroupId: []byte(f.groupID),
		Command: data,
	}
	resp := &gastrologv1.ForwardTierApplyResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardTierApply", req, resp); err != nil {
		f.peers.Invalidate(string(leaderID), err)
		return fmt.Errorf("forward tier apply to %s: %w", leaderID, err)
	}
	return nil
}
