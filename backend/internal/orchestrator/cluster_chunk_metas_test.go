package orchestrator_test

import (
	"errors"
	"testing"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
)

// A node that hosts an instance reports the instance's chunk set, open chunk
// included — the same set ListAllChunkMetas reports.
func TestListClusterChunkMetasIncludingOpen_LocalInstance(t *testing.T) {
	t.Parallel()
	s := memtest.MustNewVault(t, chunkmem.Config{})
	vaultID := glid.New()
	orch := mustNewTestOrch(t, orchestrator.Config{})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, s.CM, s.IM, s.QE))

	for _, raw := range []string{"one", "two"} {
		if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, chunk.Record{SourceTS: t1, IngestTS: t1, Raw: []byte(raw)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if _, err := orch.SealActive(vaultID); err != nil {
		t.Fatalf("SealActive: %v", err)
	}
	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, chunk.Record{SourceTS: t2, IngestTS: t2, Raw: []byte("open")}); err != nil {
		t.Fatalf("append post-seal: %v", err)
	}

	metas, err := orch.ListClusterChunkMetasIncludingOpen(vaultID)
	if err != nil {
		t.Fatalf("ListClusterChunkMetasIncludingOpen: %v", err)
	}
	var records, sealed int64
	for _, m := range metas {
		records += m.RecordCount
		if m.Sealed {
			sealed++
		}
	}
	if len(metas) != 2 || sealed != 1 || records != 3 {
		t.Errorf("got %d chunks (%d sealed) holding %d records, want 2 chunks (1 sealed) holding 3", len(metas), sealed, records)
	}

	all, err := orch.ListAllChunkMetas(vaultID)
	if err != nil {
		t.Fatalf("ListAllChunkMetas: %v", err)
	}
	if len(all) != len(metas) {
		t.Errorf("ListAllChunkMetas = %d chunks, ListClusterChunkMetasIncludingOpen = %d", len(all), len(metas))
	}
}

// A vault this node knows but can read no manifest for is reported as
// unknown, never as an empty chunk set that would read as zero records.
func TestListClusterChunkMetasIncludingOpen_NoManifest(t *testing.T) {
	t.Parallel()
	orch := mustNewTestOrch(t, orchestrator.Config{})
	shell := glid.New()
	orch.RegisterVault(orchestrator.NewVault(shell, nil))

	metas, err := orch.ListClusterChunkMetasIncludingOpen(shell)
	if !errors.Is(err, orchestrator.ErrNoVaultManifest) {
		t.Errorf("registered vault without instance or FSM: err = %v, want ErrNoVaultManifest", err)
	}
	if metas != nil {
		t.Errorf("registered vault without instance or FSM: got %d chunks, want none", len(metas))
	}

	if _, err := orch.ListClusterChunkMetasIncludingOpen(glid.New()); !errors.Is(err, orchestrator.ErrVaultNotFound) {
		t.Errorf("unknown vault: err = %v, want ErrVaultNotFound", err)
	}
}

// An empty vault is a known zero, distinct from an unknown one.
func TestListClusterChunkMetasIncludingOpen_EmptyVault(t *testing.T) {
	t.Parallel()
	s := memtest.MustNewVault(t, chunkmem.Config{})
	vaultID := glid.New()
	orch := mustNewTestOrch(t, orchestrator.Config{})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, s.CM, s.IM, s.QE))

	metas, err := orch.ListClusterChunkMetasIncludingOpen(vaultID)
	if err != nil {
		t.Fatalf("empty vault: %v", err)
	}
	if len(metas) != 0 {
		t.Errorf("empty vault: got %d chunks, want none", len(metas))
	}
}
