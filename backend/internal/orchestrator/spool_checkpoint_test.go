package orchestrator

import (
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"
	spoolfile "gastrolog/internal/spool/file"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func registerFileSpoolSequencedVault(t *testing.T, orch *Orchestrator, vaultID glid.GLID, spoolDir string) {
	t.Helper()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})
	im := indexmem.NewManager(nil, nil, nil, nil, nil)
	qe := query.New(cm, im, nil)
	v := NewVault(vaultID, &VaultInstance{
		VaultID:   vaultID,
		Type:      "memory",
		Chunks:    cm,
		Indexes:   im,
		Query:     qe,
		SpoolDir:  spoolDir,
	})
	v.WriteModel = system.VaultWriteModelSequenced
	v.ReplicationFactor = 1
	inst := v.Instance
	inst.ListManifest = func() []chunk.ChunkID {
		metas, err := cm.List()
		if err != nil {
			return nil
		}
		ids := make([]chunk.ChunkID, len(metas))
		for i, meta := range metas {
			ids[i] = meta.ID
		}
		return ids
	}
	orch.RegisterVault(v)
	wireTestSeqAllocator(orch, vaultID)
}

func TestDurableWatermarksSurviveSpoolStoreRestart(t *testing.T) {
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
	for seq := uint64(1); seq <= 5; seq++ {
		rec := sequencedTestRecord("x", ingesterID, uint32(seq))
		rec.VaultSeq = seq
		if err := store.AppendTentative(rec); err != nil {
			t.Fatal(err)
		}
		if err := store.CommitAcceptance(rec); err != nil {
			t.Fatal(err)
		}
	}

	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 5, PrevBoundSeq: 0}
	if _, err := orch.materializeFence(vaultID, fence); err != nil {
		t.Fatal(err)
	}
	if got := orch.convergenceWatermark(vaultID); got != 5 {
		t.Fatalf("C_r before restart = %d, want 5", got)
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
	if got := reloaded.MaterializationWatermark(); got != 5 {
		t.Fatalf("M_r after restart = %d, want 5", got)
	}
	if got := reloaded.ConvergenceWatermark(); got != 5 {
		t.Fatalf("C_r after restart = %d, want 5", got)
	}
	if got := m2.ReclaimThroughSeq(); got != 5 {
		t.Fatalf("reclaim watermark after restart = %d, want 5", got)
	}

	// Idempotent reconcile without spool bySeq metadata.
	reloaded.bySeq = make(map[uint64]chunk.Record)
	reloaded.byEventID = make(map[chunk.EventID]uint64)
	orch.materializationCoverage.Store(vaultID, &FenceMaterializationCoverage{Fence: fence, RecordCount: 5})
	orch.mu.Lock()
	orch.vaults[vaultID].spool = reloaded
	orch.mu.Unlock()
	if err := orch.reconcileFenceConvergence(vaultID, fence); err != nil {
		t.Fatalf("reconcile after restart: %v", err)
	}
}

func TestMaterializeFenceIdempotentAfterCheckpointReload(t *testing.T) {
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
	cm := orch.vaults[vaultID].Instance.Chunks
	sealedBefore, err := cm.List()
	if err != nil {
		t.Fatal(err)
	}

	m2, err := spoolfile.NewManager(spoolfile.Config{Dir: spoolDir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m2.Close() })
	reloaded := newVaultSpoolStore(vaultID, m2)
	orch.mu.Lock()
	orch.vaults[vaultID].spool = reloaded
	orch.mu.Unlock()

	if _, err := orch.materializeFence(vaultID, fence); err != nil {
		t.Fatalf("idempotent materialize: %v", err)
	}
	sealedAfter, err := cm.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(sealedAfter) != len(sealedBefore) {
		t.Fatalf("sealed chunks = %d, want %d (no duplicate materialize)", len(sealedAfter), len(sealedBefore))
	}
}
