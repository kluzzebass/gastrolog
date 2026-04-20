package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

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

// Apply applies a tier FSM command. Tries locally first; forwards to the
// tier Raft leader on ErrNotLeader.
func (f *TierApplyForwarder) Apply(data []byte) error {
	future := f.raft.Apply(data, f.timeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, hraft.ErrNotLeader) {
			return f.forwardToLeader(data)
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
