package orchestrator

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func TestFenceCoordinatorCountPolicyPublish(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 100)

	now := time.Unix(0, 1).UTC()
	orch.SubmitFenceHint(vaultID, FenceHint{NodeID: "replica-a", H: 100, ObservedAt: now})

	policy := &system.RotationPolicyConfig{MaxRecords: new(int64(100))}
	if err := orch.fenceCoordinator(vaultID).evaluateAndPublish(now, policy, false); err != nil {
		t.Fatal(err)
	}
	if got := orch.vaultFenceHighWatermark(vaultID); got != 100 {
		t.Fatalf("fence HWM = %d, want 100", got)
	}
	st := orch.FenceState(vaultID)
	if len(st.Records) != 1 || st.Records[0].UpperBoundSeq != 100 {
		t.Fatalf("fence state = %+v", st)
	}
}

func TestFenceCoordinatorLeaderNotHolderUsesHints(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)

	now := time.Now().UTC()
	orch.SubmitFenceHint(vaultID, FenceHint{NodeID: "replica-1", H: 50, ObservedAt: now})
	orch.SubmitFenceHint(vaultID, FenceHint{NodeID: "replica-2", H: 75, ObservedAt: now})

	policy := &system.RotationPolicyConfig{MaxRecords: new(int64(75))}
	if err := orch.fenceCoordinator(vaultID).evaluateAndPublish(now, policy, false); err != nil {
		t.Fatal(err)
	}
	if got := orch.vaultFenceHighWatermark(vaultID); got != 75 {
		t.Fatalf("fence HWM = %d, want 75", got)
	}
}

func TestFenceCoordinatorIgnoresStaleHintsForPublish(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)

	now := time.Unix(0, 3).UTC()
	orch.SubmitFenceHint(vaultID, FenceHint{NodeID: "fast", H: 200, ObservedAt: now})
	if orch.SubmitFenceHint(vaultID, FenceHint{NodeID: "slow", H: 150, ObservedAt: now.Add(time.Second)}) {
		t.Fatal("stale hint should be rejected")
	}

	policy := &system.RotationPolicyConfig{MaxRecords: new(int64(200))}
	if err := orch.fenceCoordinator(vaultID).evaluateAndPublish(now, policy, false); err != nil {
		t.Fatal(err)
	}
	if got := orch.vaultFenceHighWatermark(vaultID); got != 200 {
		t.Fatalf("fence HWM = %d, want 200", got)
	}
}

func TestFenceCoordinatorTimeTriggerUsesHNow(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)

	now := time.Unix(0, 9).UTC()
	orch.SubmitFenceHint(vaultID, FenceHint{NodeID: "replica-a", H: 42, ObservedAt: now})

	policy := &system.RotationPolicyConfig{MaxAge: new("1h")}
	if err := orch.fenceCoordinator(vaultID).evaluateAndPublish(now, policy, true); err != nil {
		t.Fatal(err)
	}
	if got := orch.vaultFenceHighWatermark(vaultID); got != 42 {
		t.Fatalf("fence HWM = %d, want 42", got)
	}
}

func newSequencedFenceTestOrch(t *testing.T, localH uint64) (*Orchestrator, glid.GLID) {
	t.Helper()
	vaultID := glid.New()
	orch, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	registerSequencedTestVault(t, orch, vaultID, nil)
	wireTestSeqAllocator(orch, vaultID)
	if localH > 0 {
		store := orch.vaultSpoolStore(vaultID)
		ingesterID := glid.New()
		for seq := uint64(1); seq <= localH; seq++ {
			rec := sequencedTestRecord("x", ingesterID, uint32(seq))
			rec.VaultSeq = seq
			if err := store.AppendTentative(rec); err != nil {
				t.Fatal(err)
			}
			if err := store.CommitAcceptance(rec); err != nil {
				t.Fatal(err)
			}
		}
	}
	return orch, vaultID
}
