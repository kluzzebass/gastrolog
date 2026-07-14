package chunking_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/pipeline/chunking"
)

// TestRecoverOnceSweepsGLCBTempOrphan verifies the gastrolog-66hmx3 fix for
// gastrolog-5do8sh gap 7: BuildGLCBFile stages the blob via
// os.CreateTemp(workDir, glcbBuildTmpPrefix+"*") with only an in-process
// defer cleanup, so a crash between CreateTemp and the rename used to
// strand the temp file in the chunk workdir forever (no startup sweep).
// RecoverOnce now sweeps glcbBuildTmpPrefix orphans (sweepOrphanGLCBBuildTmp)
// before doing anything else, so the leak no longer survives a restart.
func TestRecoverOnceSweepsGLCBTempOrphan(t *testing.T) {
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

	// The orphan must be swept by the first RecoverOnce pass.
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan temp file should be swept by RecoverOnce, stat err = %v", err)
	}

	// The built GLCB itself is untouched by the sweep.
	blob, err := glcb.OpenMappedBlob(fx.glcbPath)
	if err != nil {
		t.Fatalf("open GLCB after sweep: %v", err)
	}
	if blob.Meta().RecordCount != 2 {
		_ = blob.Close()
		t.Fatalf("GLCB records = %d, want 2", blob.Meta().RecordCount)
	}
	if err := blob.Close(); err != nil {
		t.Fatalf("close GLCB: %v", err)
	}

	// A subsequent recovery pass is idempotent with nothing left to sweep.
	if err := mgr.RecoverOnce(context.Background(), fx.vaultID); err != nil {
		t.Fatalf("second RecoverOnce with no orphan present: %v", err)
	}
}

// TestIsGLCBBuildTmpName_MatchesRealBuildGLCBFileOutput drives the exact
// os.CreateTemp pattern BuildGLCBFile uses for its staging file and
// asserts the sweep predicate matches the real produced name, not a
// hand-typed guess — the writer/sweeper drift is what let this orphan
// survive every restart before gastrolog-66hmx3.
func TestIsGLCBBuildTmpName_MatchesRealBuildGLCBFileOutput(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	tmp, err := os.CreateTemp(dir, chunking.GLCBBuildTmpPrefix+"*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := filepath.Base(tmp.Name())
	_ = tmp.Close()
	_ = os.Remove(tmp.Name())

	if !chunking.IsGLCBBuildTmpName(name) {
		t.Fatalf("IsGLCBBuildTmpName(%q) = false, want true (this is the real name BuildGLCBFile produces)", name)
	}
	if chunking.IsGLCBBuildTmpName(glcb.BlobFilename) {
		t.Fatalf("IsGLCBBuildTmpName(%q) = true, want false (this is the final sealed artifact, not a temp file)", glcb.BlobFilename)
	}
}
