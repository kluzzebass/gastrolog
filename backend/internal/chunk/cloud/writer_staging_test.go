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
