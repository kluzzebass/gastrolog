package orchestrator

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
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

func TestFencePublishesAcrossUnassignedGaps(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)
	store := orch.vaultSpoolStore(vaultID)
	if err := store.EnsureSwathWindow(1, 512); err != nil {
		t.Fatal(err)
	}
	ingesterID := glid.New()
	commit := func(seq uint64) {
		t.Helper()
		rec := sequencedTestRecord("gap", ingesterID, uint32(seq))
		rec.VaultSeq = seq
		if err := store.AppendTentative(rec); err != nil {
			t.Fatal(err)
		}
		if err := store.CommitAcceptance(rec); err != nil {
			t.Fatal(err)
		}
	}
	for seq := uint64(1); seq <= 100; seq++ {
		commit(seq)
	}
	for seq := uint64(251); seq <= 300; seq++ {
		commit(seq)
	}
	if got := store.IngestHighWatermark(); got != 300 {
		t.Fatalf("H = %d, want 300 with gap 101-250", got)
	}

	now := time.Unix(0, 5).UTC()
	policy := &system.RotationPolicyConfig{MaxRecords: new(int64(200))}
	if err := orch.fenceCoordinator(vaultID).evaluateAndPublish(now, policy, false); err != nil {
		t.Fatal(err)
	}
	st := orch.FenceState(vaultID)
	if len(st.Records) != 1 || st.Records[0].UpperBoundSeq != 200 {
		t.Fatalf("fence should cut at seq label 200 despite missing slots 101-200; state=%+v", st)
	}
	if !vaultctlfsm.FenceContainsSeq(0, 200, 150) {
		t.Fatal("fence membership includes unassigned seq inside range")
	}
	if vaultctlfsm.FenceContainsSeq(0, 200, 201) {
		t.Fatal("seq above upper bound must be excluded")
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
		if err := store.EnsureSwathWindow(1, localH+256); err != nil {
			t.Fatal(err)
		}
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
