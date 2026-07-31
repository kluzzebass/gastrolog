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

// TestRecoverOnceSweepsGLCBTempOrphan pins the crash-orphan sweep:
// BuildGLCBFile stages the blob via os.CreateTemp(workDir,
// glcbBuildTmpPrefix+"*") with only an in-process defer cleanup, so a crash
// between CreateTemp and the rename strands the temp file in the chunk
// workdir. RecoverOnce sweeps glcbBuildTmpPrefix orphans
// (sweepOrphanGLCBBuildTmp) before doing anything else, so the leak does not
// survive a restart.
func TestRecoverOnceSweepsGLCBTempOrphan(t *testing.T) {
	t.Parallel()
	fx := setupSealingChunkWithBuiltGLCB(t)

	// Plant a crash-shaped orphan next to the valid built GLCB, exactly where
	// BuildGLCBFile would have left it (workDir = the chunk's directory).
	orphanPath := filepath.Join(filepath.Dir(fx.glcbPath), ".glcb.tmp.12345")
	if err := os.WriteFile(orphanPath, []byte("stranded partial GLCB build"), 0o600); err != nil {
		t.Fatalf("plant orphan temp file: %v", err)
	}

	mgr := registerFixtureVault(t, fx, nil)

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
// hand-typed guess — writer/sweeper drift is what lets an orphan survive
// every restart.
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

// TestRecoverOnceSweepSerializesWithInFlightBuild pins the sweep's
// serialization: RecoverOnce runs on the vault-registration catch-up
// goroutine concurrently with the wake-driven worker's build pass, so an
// unserialized sweepOrphanGLCBBuildTmp deletes the ".glcb.tmp.*" a live
// BuildGLCBFile is about to rename ("BuildOnce: rename ... no such file or
// directory"). The sweep runs under buildMu.
//
// The mid-flight assertion is deterministic: while this test holds buildMu,
// the sweep cannot have run, so the staged file must still exist no matter
// how the goroutines interleave. Only a regression (sweep outside buildMu)
// can make it fail.
func TestRecoverOnceSweepSerializesWithInFlightBuild(t *testing.T) {
	t.Parallel()
	fx := setupSealingChunkWithBuiltGLCB(t)

	// A "live" staging file, as if BuildGLCBFile is between CreateTemp and
	// its rename right now.
	stagedPath := filepath.Join(filepath.Dir(fx.glcbPath), chunking.GLCBBuildTmpPrefix+"999999")
	if err := os.WriteFile(stagedPath, []byte("in-flight GLCB build staging"), 0o600); err != nil {
		t.Fatalf("plant staged temp file: %v", err)
	}

	mgr := registerFixtureVault(t, fx, nil)

	// Stand in for the in-flight build pass.
	unlock := mgr.LockBuildForTest(fx.vaultID)

	done := make(chan error, 1)
	go func() { done <- mgr.RecoverOnce(context.Background(), fx.vaultID) }()

	// While the "build" holds buildMu the sweep must not have removed the
	// staged file.
	if _, err := os.Stat(stagedPath); err != nil {
		unlock()
		t.Fatalf("staged file removed while build in flight: %v", err)
	}

	unlock()
	if err := <-done; err != nil {
		t.Fatalf("RecoverOnce after build released: %v", err)
	}

	// With no build in flight the same file IS an orphan and gets swept.
	if _, err := os.Stat(stagedPath); !os.IsNotExist(err) {
		t.Fatalf("orphan should be swept once the build released, stat err = %v", err)
	}
}
