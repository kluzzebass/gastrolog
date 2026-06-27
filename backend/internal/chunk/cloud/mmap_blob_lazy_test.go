package cloud

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

func writeTestGLCB(t *testing.T) string {
	t.Helper()
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	rec := chunk.Record{
		IngestTS: now,
		WriteTS:  now,
		Raw:      []byte("log line"),
	}

	path := filepath.Join(t.TempDir(), "data.glcb")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWriter(f, chunkID, vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(rec); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// Section-only GLCB access (histogram ITSI lookups) must not heap-decode dict
// or record index — gastrolog-2o9e9 histogram amplifier.
func TestMappedBlobSectionSkipsRecordTables(t *testing.T) {
	t.Parallel()
	path := writeTestGLCB(t)

	blob, err := OpenMappedBlob(path)
	if err != nil {
		t.Fatalf("OpenMappedBlob: %v", err)
	}
	defer blob.Close()

	section, ok := blob.Section(SectionIngestTSIndex)
	if !ok || len(section) == 0 {
		t.Fatal("expected non-empty ingest TS section")
	}
	if blob.dict != nil || blob.indexBytes != nil {
		t.Fatal("dict/index should not be loaded before Reader()")
	}

	rd, err := blob.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	if _, err := rd.ReadRecord(0); err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if blob.dict == nil || blob.indexBytes == nil {
		t.Fatal("dict/index should be loaded after Reader()")
	}
}

func TestMappedBlobTryReleaseRecordTables(t *testing.T) {
	t.Parallel()
	path := writeTestGLCB(t)

	blob, err := OpenMappedBlob(path)
	if err != nil {
		t.Fatalf("OpenMappedBlob: %v", err)
	}
	defer blob.Close()

	blob.Retain()
	if _, err := blob.Reader(); err != nil {
		t.Fatalf("Reader: %v", err)
	}
	if !blob.RecordTablesLoaded() {
		t.Fatal("expected loaded tables after Reader")
	}
	if blob.TryReleaseRecordTables() {
		t.Fatal("TryReleaseRecordTables should fail while pinned")
	}
	blob.Release()
	if !blob.TryReleaseRecordTables() {
		t.Fatal("TryReleaseRecordTables should succeed with no pins")
	}
	if blob.RecordTablesLoaded() {
		t.Fatal("tables should be released")
	}

	blob.Retain()
	if _, err := blob.Reader(); err != nil {
		t.Fatalf("re-Reader after release: %v", err)
	}
	blob.Release()
}

func TestReadRecordSurvivesBlobClose(t *testing.T) {
	t.Parallel()
	path := writeTestGLCB(t)

	blob, err := OpenMappedBlob(path)
	if err != nil {
		t.Fatalf("OpenMappedBlob: %v", err)
	}

	rd, err := blob.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	rec, err := rd.ReadRecord(0)
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if err := rd.Close(); err != nil {
		t.Fatalf("reader Close: %v", err)
	}
	if err := blob.Close(); err != nil {
		t.Fatalf("blob Close: %v", err)
	}

	if string(rec.Raw) != "log line" {
		t.Fatalf("Raw after munmap = %q, want log line", rec.Raw)
	}
}
