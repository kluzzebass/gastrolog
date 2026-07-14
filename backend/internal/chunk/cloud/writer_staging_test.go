package cloud_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk/cloud"
)

func TestNewWriterRejectsEmptyWorkDir(t *testing.T) {
	t.Parallel()
	chunkID, vaultID, _ := testRecords()
	if _, err := cloud.NewWriter(chunkID, vaultID, ""); err == nil {
		t.Fatal("expected error for empty workDir")
	}
}

func TestWriterWorkFilesLiveInWorkDirNotTMPDIR(t *testing.T) {
	tmpRoot := t.TempDir()
	workDir := t.TempDir()
	t.Setenv("TMPDIR", tmpRoot)

	chunkID, vaultID, records := testRecords()
	w, err := cloud.NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, rec := range records {
		if err := w.Add(rec); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	out, err := os.CreateTemp(workDir, "data.glcb.*")
	if err != nil {
		t.Fatalf("create output: %v", err)
	}
	if _, err := w.WriteTo(out); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	_ = out.Close()

	if matches, _ := filepath.Glob(filepath.Join(tmpRoot, "glcb-records-*.tmp")); len(matches) != 0 {
		t.Fatalf("unexpected temp files under TMPDIR: %v", matches)
	}
	if matches, _ := filepath.Glob(filepath.Join(workDir, "glcb-records-*.tmp")); len(matches) != 0 {
		t.Fatalf("work file not cleaned up under %s: %v", workDir, matches)
	}
}

func TestWriterRejectsOutputInDifferentDirectory(t *testing.T) {
	t.Parallel()
	workDir := t.TempDir()
	otherDir := t.TempDir()

	chunkID, vaultID, records := testRecords()
	w, err := cloud.NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Add(records[0]); err != nil {
		t.Fatalf("Add: %v", err)
	}

	out, err := os.CreateTemp(otherDir, "data.glcb.*")
	if err != nil {
		t.Fatalf("create output: %v", err)
	}
	if _, err := w.WriteTo(out); err == nil {
		t.Fatal("expected error when output directory differs from work directory")
	}
	_ = out.Close()
}

func TestWriterCloseWithoutWriteToRemovesWorkFile(t *testing.T) {
	t.Parallel()
	workDir := t.TempDir()

	chunkID, vaultID, records := testRecords()
	w, err := cloud.NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Add(records[0]); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(workDir, "glcb-records-*.tmp"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("work file left behind: %v", matches)
	}
}

// TestWriterAbandonedStagingFilePersists pins CURRENT behavior for a
// writer that dies mid-stream (gastrolog-5do8sh gap 7): glcb-records-*.tmp
// staging files are removed only by the in-process closeStaging (Close /
// Finish / WriteTo). There is no startup or periodic sweep, so a staging
// file orphaned by a crash persists in the chunk dir indefinitely — even
// after a later writer builds a blob in the same directory. Adding a
// sweep is a documented follow-up candidate; this test only documents the
// orphan, it does not assert cleanup.
func TestWriterAbandonedStagingFilePersists(t *testing.T) {
	t.Parallel()
	workDir := t.TempDir()

	chunkID, vaultID, records := testRecords()

	// First writer: Add creates the staging file, then the writer is
	// abandoned without Close/Finish/WriteTo — the in-process shape of a
	// crash mid-stream.
	abandoned, err := cloud.NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		t.Fatalf("NewWriter (abandoned): %v", err)
	}
	if err := abandoned.Add(records[0]); err != nil {
		t.Fatalf("Add (abandoned): %v", err)
	}
	orphans, err := filepath.Glob(filepath.Join(workDir, "glcb-records-*.tmp"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(orphans) != 1 {
		t.Fatalf("staging files after Add = %v, want exactly 1", orphans)
	}
	orphan := orphans[0]

	// Second writer in the same workDir completes normally; its own
	// staging file is cleaned up by WriteTo.
	w, err := cloud.NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Add(records[0]); err != nil {
		t.Fatalf("Add: %v", err)
	}
	out, err := os.CreateTemp(workDir, "data.glcb.*")
	if err != nil {
		t.Fatalf("create output: %v", err)
	}
	if _, err := w.WriteTo(out); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	_ = out.Close()

	// The first writer's orphan still exists: nothing sweeps it.
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("orphaned staging file %s: %v (current behavior is that it persists)", orphan, err)
	}
	matches, err := filepath.Glob(filepath.Join(workDir, "glcb-records-*.tmp"))
	if err != nil {
		t.Fatalf("glob after build: %v", err)
	}
	if len(matches) != 1 || matches[0] != orphan {
		t.Fatalf("staging files after second build = %v, want only the orphan %s", matches, orphan)
	}
}

func TestWriterAllowsWriteToMemoryAfterDiskBuild(t *testing.T) {
	t.Parallel()
	workDir := t.TempDir()

	chunkID, vaultID, records := testRecords()
	w, err := cloud.NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Add(records[0]); err != nil {
		t.Fatalf("Add: %v", err)
	}

	out, err := os.CreateTemp(workDir, "data.glcb.*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteTo(out); err != nil {
		t.Fatalf("WriteTo file: %v", err)
	}
	_ = out.Close()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Unnamed writers (tests) may still emit to memory after the work file is gone.
	if _, err := w.WriteTo(&bytes.Buffer{}); err != nil {
		t.Fatalf("WriteTo memory: %v", err)
	}
}
