package server_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// stubCloudIndexReconciler answers for peers. A node in dead fails; every other
// node reports having dropped one stale entry.
//
// The fan-out calls this from one goroutine per peer, so the tally is
// guarded — an unsynchronized counter here is enough to keep the whole
// package off the race detector.
type stubCloudIndexReconciler struct {
	dead map[string]bool

	mu    sync.Mutex
	calls map[string]int
}

func (s *stubCloudIndexReconciler) ReconcileCloudIndex(_ context.Context, nodeID string, _ *gastrologv1.ForwardReconcileCloudIndexRequest) (*gastrologv1.ForwardReconcileCloudIndexResponse, error) {
	s.mu.Lock()
	if s.calls == nil {
		s.calls = map[string]int{}
	}
	s.calls[nodeID]++
	s.mu.Unlock()

	if s.dead[nodeID] {
		return nil, errors.New("connection refused")
	}
	return &gastrologv1.ForwardReconcileCloudIndexResponse{
		Repair: &gastrologv1.CloudIndexRepair{RemovedEntries: 1},
	}, nil
}

// callCount reports how many times a peer was asked.
func (s *stubCloudIndexReconciler) callCount(nodeID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[nodeID]
}

// Every homing node must be asked: the cloud index is per-node, so repairing
// only where the request landed leaves the other caches wrong.
func TestReconcileCloudIndex_RepairsEveryHomingNode(t *testing.T) {
	t.Parallel()

	const localID, peerA, peerB = "node-local", "node-a", "node-b"
	reconciler := &stubCloudIndexReconciler{}
	vs, vaultID := newContributionVaultServer(t, localID, []string{peerA, peerB},
		contributionDeps{reconciler: reconciler})

	resp, err := vs.ReconcileCloudIndex(context.Background(),
		connect.NewRequest(&gastrologv1.ReconcileCloudIndexRequest{Vault: vaultID}))
	if err != nil {
		t.Fatalf("ReconcileCloudIndex: %v", err)
	}
	if reconciler.callCount(peerA) != 1 || reconciler.callCount(peerB) != 1 {
		t.Errorf("peer calls = %d/%d, want one each for %s and %s",
			reconciler.callCount(peerA), reconciler.callCount(peerB), peerA, peerB)
	}
	if len(resp.Msg.GetRepairs()) != 2 {
		t.Fatalf("got %d repairs, want 2 (one per peer)", len(resp.Msg.GetRepairs()))
	}
	for _, r := range resp.Msg.GetRepairs() {
		if r.GetNodeId() == "" {
			t.Error("a repair carries no node ID; the result cannot say which cache was fixed")
		}
	}
}

// A peer that could not be reached still has a broken cache. Reporting success
// for the rest without naming it would read as a completed repair.
func TestReconcileCloudIndex_PartialFanOutNamesDeadPeer(t *testing.T) {
	t.Parallel()

	const localID, aliveID, deadID = "node-local", "node-alive", "node-dead"
	reconciler := &stubCloudIndexReconciler{dead: map[string]bool{deadID: true}}
	vs, vaultID := newContributionVaultServer(t, localID, []string{aliveID, deadID},
		contributionDeps{reconciler: reconciler})

	resp, err := vs.ReconcileCloudIndex(context.Background(),
		connect.NewRequest(&gastrologv1.ReconcileCloudIndexRequest{Vault: vaultID}))
	if err != nil {
		t.Fatalf("ReconcileCloudIndex: %v", err)
	}
	report := resp.Msg.GetContributionReport()
	if report == nil {
		t.Fatalf("ContributionReport = nil, want the dead peer named")
	}
	if len(report.GetDegraded()) != 1 || report.GetDegraded()[0].GetNodeId() != deadID {
		t.Errorf("degraded = %+v, want just %s", report.GetDegraded(), deadID)
	}
	if len(resp.Msg.GetRepairs()) != 1 {
		t.Errorf("got %d repairs, want 1 (the reachable peer)", len(resp.Msg.GetRepairs()))
	}
}

// A vault with no cloud store must succeed with an empty result rather than
// error: "nothing to rebuild" is a legitimate answer an operator can read.
func TestReconcileCloudIndex_LocalOnlyVaultIsNotAnError(t *testing.T) {
	t.Parallel()

	vs, vaultID := newContributionVaultServer(t, "node-local", nil, contributionDeps{})

	resp, err := vs.ReconcileCloudIndex(context.Background(),
		connect.NewRequest(&gastrologv1.ReconcileCloudIndexRequest{Vault: vaultID}))
	if err != nil {
		t.Fatalf("ReconcileCloudIndex on a local-only vault: %v", err)
	}
	if len(resp.Msg.GetRepairs()) != 0 {
		t.Errorf("got %d repairs for a vault with no cloud store", len(resp.Msg.GetRepairs()))
	}
}

func TestReconcileCloudIndex_RequiresVault(t *testing.T) {
	t.Parallel()

	vs, _ := newContributionVaultServer(t, "node-local", nil, contributionDeps{})

	if _, err := vs.ReconcileCloudIndex(context.Background(),
		connect.NewRequest(&gastrologv1.ReconcileCloudIndexRequest{})); err == nil {
		t.Fatal("an empty vault name returned no error")
	}
}
