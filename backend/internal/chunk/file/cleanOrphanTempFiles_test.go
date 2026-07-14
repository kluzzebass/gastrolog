package file

import (
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
)

// This file pins the writer <-> sweeper contract for cleanOrphanTempFiles
// (gastrolog-66hmx3): every temp file this package's writers can leave
// behind in a chunk directory must be matched by isOrphanTempFileName,
// using the writer's *actual* produced name rather than a hand-typed
// pattern guess. A mismatch here is exactly the class of bug that let
// data.glcb.tmp survive the startup sweep forever (gastrolog-5do8sh gap
// 7d) — cleanOrphanTempFiles's patterns silently drifted from what
// sealToGLCB actually wrote.

// TestIsOrphanTempFileName_MatchesSealToGLCBTmpConstant pins the exact
// literal sealToGLCB uses for its fixed-name output tmp file.
func TestIsOrphanTempFileName_MatchesSealToGLCBTmpConstant(t *testing.T) {
	t.Parallel()
	if !isOrphanTempFileName(dataGLCBTmpFileName) {
		t.Fatalf("isOrphanTempFileName(%q) = false, want true (sealToGLCB writes this exact name)", dataGLCBTmpFileName)
	}
}

// TestIsOrphanTempFileName_MatchesGLCBWriterStagingFile drives the real
// glcb.Writer staging path (ensureStaging, used whenever a Writer is not
// bound to an output file via BindOutput — sealToGLCB is the only such
// caller in this codebase) and asserts the sweep matches whatever name it
// actually produces, not a guessed pattern.
func TestIsOrphanTempFileName_MatchesGLCBWriterStagingFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	w, err := glcb.NewWriter(chunk.NewChunkID(), glid.New(), dir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Add without BindOutput forces the staging path (ensureStaging),
	// which is exactly what sealToGLCB triggers when a chunk crashes
	// mid-Add before the tmp output file is even opened.
	if err := w.Add(chunk.Record{Raw: []byte("x")}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	defer func() { _ = w.Close() }()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	var staged string
	for _, e := range entries {
		if !e.IsDir() {
			staged = e.Name()
			break
		}
	}
	if staged == "" {
		t.Fatal("expected glcb.Writer to have created a staging file, found none")
	}
	if !isOrphanTempFileName(staged) {
		t.Fatalf("isOrphanTempFileName(%q) = false, want true (this is the real name glcb.Writer.ensureStaging produced)", staged)
	}
}

// TestIsOrphanTempFileName_MatchesCloudCacheDownloadTmp drives the exact
// os.CreateTemp pattern downloadCloudBlobToChunkDir uses for its cloud-blob
// cache tmp file and asserts the sweep matches the real produced name.
func TestIsOrphanTempFileName_MatchesCloudCacheDownloadTmp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	tmp, err := os.CreateTemp(dir, dataGLCBFileName+".tmp.*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := filepath.Base(tmp.Name())
	_ = tmp.Close()
	_ = os.Remove(tmp.Name())

	if !isOrphanTempFileName(name) {
		t.Fatalf("isOrphanTempFileName(%q) = false, want true (this is the real name downloadCloudBlobToChunkDir produces)", name)
	}
}

// TestIsOrphanTempFileName_RejectsFinalArtifacts guards against the sweep
// becoming so broad it eats real sealed artifacts.
func TestIsOrphanTempFileName_RejectsFinalArtifacts(t *testing.T) {
	t.Parallel()
	for _, name := range []string{dataGLCBFileName, "raw.log", "idx.log", "attr.log", "attr_dict.log", "meta.json"} {
		if isOrphanTempFileName(name) {
			t.Errorf("isOrphanTempFileName(%q) = true, want false (this is a real artifact, not a temp file)", name)
		}
	}
}

// TestCleanOrphanTempFiles_SweepsAllKnownWriterTmpNames is an integration
// version of the above: it plants one file per known writer contract
// directly (not via the predicate) and asserts a real cleanOrphanTempFiles
// pass removes every one of them and none of the real artifacts.
func TestCleanOrphanTempFiles_SweepsAllKnownWriterTmpNames(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: filepath.Dir(dir)})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	orphans := []string{
		dataGLCBTmpFileName,
		glcb.RecordsStagingPrefix + "123456.tmp",
		dataGLCBFileName + ".tmp.789",
		".compress-abc",
	}
	keep := []string{dataGLCBFileName, "raw.log", "idx.log"}

	for _, name := range orphans {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatalf("plant %s: %v", name, err)
		}
	}
	for _, name := range keep {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatalf("plant %s: %v", name, err)
		}
	}

	m.cleanOrphanTempFiles(dir)

	for _, name := range orphans {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("orphan %s not swept, stat err = %v", name, err)
		}
	}
	for _, name := range keep {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("real artifact %s was swept away: %v", name, err)
		}
	}
}
