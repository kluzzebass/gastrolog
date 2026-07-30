package orchestrator_test

import (
	"testing"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
)

// TestManifestReaderMemoryModeProjection covers the local-projection tier of
// the unified manifest read core: with no GroupManager and
// no vault-ctl FSM, ManifestReader projects from the memory-mode chunk
// manager, honoring the sealed-only contract of manifest.Reader.
func TestManifestReaderMemoryModeProjection(t *testing.T) {
	t.Parallel()
	s := memtest.MustNewVault(t, chunkmem.Config{})
	vaultID := glid.New()
	orch := mustNewTestOrch(t, orchestrator.Config{})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, s.CM, s.IM, s.QE))

	// Two records into the active chunk, then seal; one more record opens a
	// fresh active chunk that must stay invisible to the sealed-only Reader.
	for _, ts := range []struct{ raw string }{{"one"}, {"two"}} {
		if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, chunk.Record{
			SourceTS: t1, IngestTS: t1, Raw: []byte(ts.raw),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if _, err := orch.SealActive(vaultID); err != nil {
		t.Fatalf("SealActive: %v", err)
	}
	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, chunk.Record{
		SourceTS: t2, IngestTS: t2, Raw: []byte("active"),
	}); err != nil {
		t.Fatalf("append post-seal: %v", err)
	}

	reader := orch.ManifestReader()

	entries := reader.EntriesForVault(vaultID)
	if len(entries) != 1 {
		t.Fatalf("EntriesForVault: got %d entries, want 1 sealed", len(entries))
	}
	if !entries[0].IsSealed() {
		t.Errorf("EntriesForVault returned unsealed entry state=%v", entries[0].State)
	}
	if entries[0].RecordCount != 2 {
		t.Errorf("sealed entry RecordCount = %d, want 2", entries[0].RecordCount)
	}

	// Entry resolves the sealed chunk and refuses the active one.
	if _, ok := reader.Entry(entries[0].ID); !ok {
		t.Error("Entry(sealed chunk) not found")
	}
	if active := s.CM.Active(); active != nil {
		if _, ok := reader.Entry(active.ID); ok {
			t.Error("Entry(active chunk) resolved; active chunks are not part of the manifest read surface")
		}
	} else {
		t.Fatal("no active chunk after post-seal append")
	}

	// Unhappy paths: unknown chunk, unknown vault.
	if _, ok := reader.Entry(chunk.NewChunkID()); ok {
		t.Error("Entry(unknown chunk) resolved")
	}
	if got := reader.EntriesForVault(glid.New()); len(got) != 0 {
		t.Errorf("EntriesForVault(unknown vault) = %d entries, want none", len(got))
	}

	// Memory mode has no vault-ctl FSM: the open-inclusive FSM surface
	// reports nothing and callers use their local fallback.
	if got := orch.VaultManifestEntriesIncludingOpen(vaultID); got != nil {
		t.Errorf("VaultManifestEntriesIncludingOpen without FSM = %d entries, want nil", len(got))
	}
}
