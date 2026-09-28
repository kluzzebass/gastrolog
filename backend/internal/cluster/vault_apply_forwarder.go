package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/applywait"
	"gastrolog/internal/raftutil"

	hraft "github.com/hashicorp/raft"

	"google.golang.org/grpc/status"

	"google.golang.org/grpc/codes"
)

// ErrNoVaultRaftLeader is returned when the vault control-plane Raft group
// has no elected leader.
var ErrNoVaultRaftLeader = errors.New("no vault raft leader")

// VaultApplyForwarder applies pre-marshaled vault control-plane FSM commands.
// If this node is the Raft leader, it applies locally; otherwise it forwards
// via ForwardVaultApply (same pattern as VaultCtlChunkApplyForwarder) and
// blocks until the local group FSM has applied the leader's index — the
// read-after-write barrier.
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
// ErrNotLeader, and retries while a leadership transfer is in progress (see
// applyRetryingLeadershipTransfer for why those two errors get different
// treatment). When forwarded, Apply returns only after this node's own group
// FSM has caught up to the leader's applied index, so an immediate local read
// sees post-mutation state.
func (f *VaultApplyForwarder) Apply(data []byte) error {
	err := raftutil.ApplyRetryingLeadershipTransfer(func() error {
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
	appliedIndex, err := forwardVaultApplyResolving(f.raft, f.peers, PurposeVaultApply,
		f.groupID, data, f.timeout, ErrNoVaultRaftLeader)
	if err != nil {
		return err
	}
	return waitForGroupApply(f.applyWait, f.groupID, appliedIndex, f.timeout)
}

// forwardLeaderRetryPause is the wait between forward attempts while a
// leadership transfer settles. A follower learns the new leader on the next
// heartbeat, so a couple of these cover the window at the test heartbeat
// (300ms) and comfortably at the production one (2s budget / 25ms = many
// rounds).
const forwardLeaderRetryPause = 25 * time.Millisecond

// forwardVaultApplyResolving forwards a group command to the leader,
// re-resolving the leader between attempts inside one timeout budget.
//
// The forwarding follower's view of the leader trails a transfer by a
// heartbeat or two, so the first resolution can name a node that already
// stepped down. That node refuses before appending anything and says so as
// FailedPrecondition; the next resolution lands on its successor. Any other
// failure is returned as-is — only the refusal that is safe to retry is
// retried. An empty view waits out the same budget, since mid-transfer the
// group has a leader seconds away; a genuinely leaderless group ends as
// noLeaderErr.
func forwardVaultApplyResolving(r *hraft.Raft, peers *PeerConnManager, purpose string, groupID string, data []byte, timeout time.Duration, noLeaderErr error) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for {
		_, leaderID := r.LeaderWithID()
		if leaderID == "" {
			if time.Now().After(deadline) {
				return 0, noLeaderErr
			}
			time.Sleep(forwardLeaderRetryPause)
			continue
		}

		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		req := &gastrologv1.ForwardVaultApplyRequest{
			GroupId: []byte(groupID),
			Command: data,
		}
		resp := &gastrologv1.ForwardVaultApplyResponse{}
		err := peers.InvokeService(ctx, string(leaderID), purpose,
			"/gastrolog.v1.ClusterService/ForwardVaultApply", req, resp)
		cancel()
		if err == nil {
			return resp.GetAppliedIndex(), nil
		}
		if status.Code(err) != codes.FailedPrecondition || time.Now().After(deadline) {
			return 0, fmt.Errorf("forward vault apply to %s: %w", leaderID, err)
		}
		time.Sleep(forwardLeaderRetryPause)
	}
}

// waitForGroupApply blocks until the local vault-ctl group FSM has applied
// at least target, bounded by timeout. Shared by both vault-ctl forward
// paths (VaultApplyForwarder, VaultCtlChunkApplyForwarder).
//
// Event-driven: the group FSM advances its applywait.Tracker as it applies
// each committed entry (and on snapshot restore), waking this wait the
// moment the mutation is locally visible — never a poll. Deliberately the
// same applywait mechanism the system-config forward barrier in
// system/raftstore uses; keep the two in step. A zero target (nothing
// meaningful to wait for) and a nil tracker return immediately.
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
