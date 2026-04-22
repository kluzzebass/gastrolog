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

// TierApplyForwarder applies commands to a Raft group (historically tier-only).
// If this node is the leader, it applies locally; otherwise it forwards via
// ForwardTierApply. Use NewVaultCtlTierApplyForwarder for vault ctl + OpTierFSM.
type TierApplyForwarder struct {
	raft    *hraft.Raft
	groupID string
	peers   *PeerConns
	timeout time.Duration
	// xform, when non-nil, maps a tierfsm wire payload to the bytes passed to
	// Raft.Apply / ForwardTierApply (e.g. vault OpTierFSM wrapper).
	xform func([]byte) []byte
}

// NewTierApplyForwarder creates a forwarder for a specific multiraft group (tests and generic forwarding).
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
// Raft leader on ErrNotLeader.
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
		return ErrNoRaftLeader
	}

	conn, err := f.peers.Conn(string(leaderID))
	if err != nil {
		return fmt.Errorf("dial raft leader %s: %w", leaderID, err)
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
		return fmt.Errorf("forward tier apply RPC to %s: %w", leaderID, err)
	}
	return nil
}
