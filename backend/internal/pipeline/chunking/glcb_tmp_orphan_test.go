package chunking_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
)

// TestRecoverOnceLeavesGLCBTempOrphan pins CURRENT restart behavior for
// gastrolog-5do8sh gap 7: BuildGLCBFile stages the blob via
// os.CreateTemp(workDir, ".glcb.tmp.*") with only an in-process defer
// cleanup, so a crash between CreateTemp and the rename strands the temp
// file in the chunk workdir forever. There is NO startup sweep — recovery
// neither removes the orphan nor is blocked by it. Removing orphans on
// restart is a documented follow-up candidate; this test documents what the
// system does today, not what it should do.
func TestRecoverOnceLeavesGLCBTempOrphan(t *testing.T) {
	t.Parallel()
	fx := setupSealingChunkWithBuiltGLCB(t)

	// Plant a crash-shaped orphan next to the valid built GLCB, exactly where
	// BuildGLCBFile would have left it (workDir = the chunk's directory).
	orphanPath := filepath.Join(filepath.Dir(fx.glcbPath), ".glcb.tmp.12345")
	if err := os.WriteFile(orphanPath, []byte("stranded partial GLCB build"), 0o600); err != nil {
		t.Fatalf("plant orphan temp file: %v", err)
	}

	mgr := registerFixtureVault(t, fx)

	// Successful recovery path (same shape as TestRecoverOnceSealsFromExistingGLCB):
	// the orphan must not block it.
	if err := mgr.RecoverOnce(context.Background(), fx.vaultID); err != nil {
		t.Fatalf("RecoverOnce with orphan temp file present: %v", err)
	}
	entry := fx.fsm.Get(fx.chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk entry = %+v, want Sealed", entry)
	}
	if entry.RecordCount != 2 {
		t.Fatalf("RecordCount = %d, want 2", entry.RecordCount)
	}

	// Pin: the orphan survives recovery. No startup sweep exists today.
	if _, err := os.Stat(orphanPath); err != nil {
		t.Fatalf("orphan temp file should still exist after recovery (no sweep is current behavior): %v", err)
	}

	// The built GLCB itself is untouched by the orphan's presence.
	blob, err := chunkcloud.OpenMappedBlob(fx.glcbPath)
	if err != nil {
		t.Fatalf("open GLCB alongside orphan: %v", err)
	}
	if blob.Meta().RecordCount != 2 {
		_ = blob.Close()
		t.Fatalf("GLCB records = %d, want 2", blob.Meta().RecordCount)
	}
	if err := blob.Close(); err != nil {
		t.Fatalf("close GLCB: %v", err)
	}

	// A subsequent recovery pass is also not blocked (idempotent), and the
	// orphan still lingers afterwards.
	if err := mgr.RecoverOnce(context.Background(), fx.vaultID); err != nil {
		t.Fatalf("second RecoverOnce with orphan temp file present: %v", err)
	}
	if _, err := os.Stat(orphanPath); err != nil {
		t.Fatalf("orphan temp file should still exist after second recovery: %v", err)
	}
}
