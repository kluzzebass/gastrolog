package orchestrator

import (
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func TestReconcileFenceConvergenceAfterMaterialize(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 20)
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 20, PrevBoundSeq: 0}
	if _, err := orch.materializeFence(vaultID, fence); err != nil {
		t.Fatal(err)
	}
	if got := orch.convergenceWatermark(vaultID); got != 20 {
		t.Fatalf("C_r = %d, want 20", got)
	}
	if !orch.IsFenceConvergeSealed(vaultID, fence) {
		t.Fatal("expected converge-sealed after clean materialize")
	}
}

func TestReconcileBlocksOnAssignedMissing(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)
	store := orch.vaultSpoolStore(vaultID)
	if err := store.EnsureSwathWindow(1, 512); err != nil {
		t.Fatal(err)
	}
	ingesterID := glid.New()
	rec1 := sequencedTestRecord("a", ingesterID, 1)
	rec1.VaultSeq = 1
	rec3 := sequencedTestRecord("c", ingesterID, 3)
	rec3.VaultSeq = 3
	for _, rec := range []chunk.Record{rec1, rec3} {
		if err := store.AppendTentative(rec); err != nil {
			t.Fatal(err)
		}
		if err := store.CommitAcceptance(rec); err != nil {
			t.Fatal(err)
		}
	}
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 3, PrevBoundSeq: 0}
	if _, err := orch.materializeFence(vaultID, fence); err == nil {
		t.Fatal("expected materialize failure for assigned-missing hole at seq 2")
	}
	if err := orch.reconcileFenceConvergence(vaultID, fence); err == nil {
		t.Fatal("expected reconcile error when fence not materialized locally")
	}
}

func TestReconcileConvergenceRequiresMaterialization(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 5)
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 5, PrevBoundSeq: 0}
	if err := orch.reconcileFenceConvergence(vaultID, fence); err == nil {
		t.Fatal("expected error when M_r < fence upper bound")
	}
}

func TestLocalSeqPresentFallsBackToMaterializationWatermark(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)
	store := orch.vaultSpoolStore(vaultID)
	if err := store.EnsureSwathWindow(1, 512); err != nil {
		t.Fatal(err)
	}
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 3, PrevBoundSeq: 0}

	if orch.localSeqPresentForReconcile(vaultID, fence, 2) {
		t.Fatal("seq 2 should be absent without spool or M_r witness")
	}

	ingesterID := glid.New()
	rec := sequencedTestRecord("x", ingesterID, 2)
	rec.VaultSeq = 2
	if err := store.AppendTentative(rec); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitAcceptance(rec); err != nil {
		t.Fatal(err)
	}
	if !orch.localSeqPresentForReconcile(vaultID, fence, 2) {
		t.Fatal("bySeq should satisfy probe while accepted metadata remains")
	}

	store.setMaterializationWatermark(3)
	orch.recordMaterializationCoverage(vaultID, &FenceMaterializationCoverage{Fence: fence, RecordCount: 3})
	if !orch.localSeqPresentForReconcile(vaultID, fence, 1) {
		t.Fatal("materialization coverage + M_r should cover fence range after spool-independent proof")
	}
}
