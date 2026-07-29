package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/applywait"

	hraft "github.com/hashicorp/raft"
)

// ErrNoVaultRaftLeader is returned when the vault control-plane Raft group
// has no elected leader.
var ErrNoVaultRaftLeader = errors.New("no vault raft leader")

// VaultApplyForwarder applies pre-marshaled vault control-plane FSM commands.
// If this node is the Raft leader, it applies locally; otherwise it forwards
// via ForwardVaultApply (same pattern as VaultCtlChunkApplyForwarder) and
// blocks until the local group FSM has applied the leader's index — the
// read-after-write barrier (gastrolog-4l24u).
type VaultApplyForwarder struct {
	raft      *hraft.Raft
	groupID   string
	applyWait *applywait.Tracker
	peers     *PeerConnManager
	timeout   time.Duration
}

// NewVaultApplyForwarder creates a forwarder for a vault control-plane Raft
// group. applyWait is the group FSM's apply tracker (vaultraft.FSM.ApplyWait);
// it drives the post-forward read-after-write barrier. A nil tracker skips
// the barrier — only for groups whose FSM does not expose one.
func NewVaultApplyForwarder(r *hraft.Raft, groupID string, applyWait *applywait.Tracker, peers *PeerConnManager, timeout time.Duration) *VaultApplyForwarder {
	return &VaultApplyForwarder{
		raft:      r,
		groupID:   groupID,
		applyWait: applyWait,
		peers:     peers,
		timeout:   timeout,
	}
}

// Apply applies a vault control-plane command. Tries locally first; forwards on
// ErrNotLeader, and retries while a leadership transfer is in progress
// (gastrolog-4jh4mb — see applyRetryingLeadershipTransfer for why those two
// errors get different treatment). When forwarded, Apply returns only after
// this node's own group FSM has caught up to the leader's applied index, so an
// immediate local read sees post-mutation state.
func (f *VaultApplyForwarder) Apply(data []byte) error {
	err := applyRetryingLeadershipTransfer(func() error {
		return f.raft.Apply(data, f.timeout).Error()
	}, nil)
	if err != nil {
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
	return waitForGroupApply(f.applyWait, f.groupID, resp.GetAppliedIndex(), f.timeout)
}

// waitForGroupApply blocks until the local vault-ctl group FSM has applied
// at least target, bounded by timeout. Shared by both vault-ctl forward
// paths (VaultApplyForwarder, VaultCtlChunkApplyForwarder).
//
// Event-driven: the group FSM advances its applywait.Tracker as it applies
// each committed entry (and on snapshot restore), waking this wait the
// moment the mutation is locally visible — never a poll (gastrolog-3klg1
// mechanism, mirrored onto vault-ctl by gastrolog-4l24u). A zero target
// (nothing meaningful to wait for) and a nil tracker return immediately.
// Times out if the follower never catches up (partitioned, log truncated,
// etc.) so a stuck group surfaces as a caller-visible error rather than a
// hang.
func waitForGroupApply(tracker *applywait.Tracker, groupID string, target uint64, timeout time.Duration) error {
	if tracker == nil || target == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := tracker.Wait(ctx, target); err != nil {
		return fmt.Errorf("wait for local group %s FSM apply at index %d: timeout (last applied %d)",
			groupID, target, tracker.Applied())
	}
	return nil
}
