package orchestrator

import (
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	spoolfile "gastrolog/internal/spool/file"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func TestLocalSeqPresentChunkFallbackAfterSpoolEvicted(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 3)
	store := orch.vaultSpoolStore(vaultID)
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 3, PrevBoundSeq: 0}
	if _, err := orch.materializeFence(vaultID, fence); err != nil {
		t.Fatal(err)
	}

	store.mu.Lock()
	store.bySeq = make(map[uint64]chunk.Record)
	store.byEventID = make(map[chunk.EventID]uint64)
	store.mu.Unlock()

	if !orch.localSeqPresentForReconcile(vaultID, fence, 2) {
		t.Fatal("expected chunk fallback for materialized seq after spool metadata cleared")
	}
}

func TestLocalSeqPresentRejectsTrueAssignedMissing(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)
	store := orch.vaultSpoolStore(vaultID)
	if err := store.EnsureSwathWindow(1, 512); err != nil {
		t.Fatal(err)
	}
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 3, PrevBoundSeq: 0}
	store.setMaterializationWatermark(1)
	if orch.localSeqPresentForReconcile(vaultID, fence, 2) {
		t.Fatal("seq 2 must stay absent when M_r does not cover fence upper bound")
	}
}

func TestReconcileAdvancesAfterCheckpointReloadWithoutSpoolMetadata(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	spoolDir := filepath.Join(dir, "spool")
	vaultID := glid.New()
	orch, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	registerFileSpoolSequencedVault(t, orch, vaultID, spoolDir)
	store := orch.vaultSpoolStore(vaultID)
	if err := store.EnsureSwathWindow(1, 512); err != nil {
		t.Fatal(err)
	}
	ingesterID := glid.New()
	for seq := uint64(1); seq <= 3; seq++ {
		rec := sequencedTestRecord("x", ingesterID, uint32(seq))
		rec.VaultSeq = seq
		if err := store.AppendTentative(rec); err != nil {
			t.Fatal(err)
		}
		if err := store.CommitAcceptance(rec); err != nil {
			t.Fatal(err)
		}
	}
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 3, PrevBoundSeq: 0}
	if _, err := orch.materializeFence(vaultID, fence); err != nil {
		t.Fatal(err)
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}

	m2, err := spoolfile.NewManager(spoolfile.Config{Dir: spoolDir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m2.Close() })
	reloaded := newVaultSpoolStore(vaultID, m2)
	reloaded.mu.Lock()
	reloaded.bySeq = make(map[uint64]chunk.Record)
	reloaded.byEventID = make(map[chunk.EventID]uint64)
	reloaded.mu.Unlock()
	orch.mu.Lock()
	orch.vaults[vaultID].spool = reloaded
	orch.mu.Unlock()

	if err := orch.reconcileFenceConvergence(vaultID, fence); err != nil {
		t.Fatalf("reconcile after durable reload: %v", err)
	}
}
