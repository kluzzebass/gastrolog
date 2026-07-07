package cloud_test

import (
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/cloud"
)

func TestMappedBlobRoundTrip(t *testing.T) {
	t.Parallel()
	chunkID, vaultID, records := testRecords()

	path := filepath.Join(t.TempDir(), "data.glcb")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := cloud.OpenWriter(f, chunkID, vaultID)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		if err := w.Add(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	blob, err := cloud.OpenMappedBlob(path)
	if err != nil {
		t.Fatalf("OpenMappedBlob: %v", err)
	}
	defer blob.Close()

	rd, err := blob.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	for i := range records {
		got, err := rd.ReadRecord(uint32(i))
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		assertRecord(t, i, got, records[i])
	}

	ingest, ok := blob.Section(cloud.SectionIngestTSIndex)
	if !ok || len(ingest) == 0 {
		t.Fatal("expected non-empty ingest TS section")
	}
}

// TestMappedBlobRecordsSurviveUnmap pins the detach contract behind
// gastrolog-11y2iv's clone reduction: records returned by ReadRecord must
// remain fully readable (attrs AND raw) after the blob is closed and its
// mapping released. Raw is detached by cloneMmapRecord; attrs strings are
// interned heap copies from MmapStringDict. If either ever aliases the
// mapping again, this test reads freed memory and crashes under -race /
// segfaults outright.
func TestMappedBlobRecordsSurviveUnmap(t *testing.T) {
	t.Parallel()
	chunkID, vaultID, records := testRecords()

	path := filepath.Join(t.TempDir(), "data.glcb")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := cloud.OpenWriter(f, chunkID, vaultID)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		if err := w.Add(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	blob, err := cloud.OpenMappedBlob(path)
	if err != nil {
		t.Fatalf("OpenMappedBlob: %v", err)
	}
	rd, err := blob.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	got := make([]chunk.Record, len(records))
	for i := range records {
		if got[i], err = rd.ReadRecord(uint32(i)); err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
	}
	if err := rd.Close(); err != nil {
		t.Fatalf("reader close: %v", err)
	}
	if err := blob.Close(); err != nil {
		t.Fatalf("blob close: %v", err)
	}

	// The mapping is gone; every retained record must still verify.
	for i := range records {
		assertRecord(t, i, got[i], records[i])
	}
}

// BenchmarkMappedBlobReadRecord measures the per-record read cost on the
// mmap path — the retention-drain and search hot loop (gastrolog-11y2iv).
func BenchmarkMappedBlobReadRecord(b *testing.B) {
	chunkID, vaultID, records := testRecords()

	path := filepath.Join(b.TempDir(), "data.glcb")
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	w, err := cloud.OpenWriter(f, chunkID, vaultID)
	if err != nil {
		b.Fatal(err)
	}
	for _, rec := range records {
		if err := w.Add(rec); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		b.Fatal(err)
	}
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}

	blob, err := cloud.OpenMappedBlob(path)
	if err != nil {
		b.Fatal(err)
	}
	defer blob.Close()
	rd, err := blob.Reader()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		rec, err := rd.ReadRecord(uint32(i % len(records))) //nolint:gosec // G115: bounded by len
		if err != nil {
			b.Fatal(err)
		}
		if len(rec.Raw) == 0 {
			b.Fatal("empty record")
		}
	}
}

func TestMappedBlobRetainDefersUnmap(t *testing.T) {
	t.Parallel()
	chunkID, vaultID, records := testRecords()

	path := filepath.Join(t.TempDir(), "data.glcb")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := cloud.OpenWriter(f, chunkID, vaultID)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		if err := w.Add(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	blob, err := cloud.OpenMappedBlob(path)
	if err != nil {
		t.Fatal(err)
	}
	blob.Retain()
	if err := blob.Close(); err != nil {
		t.Fatal(err)
	}
	section, ok := blob.Section(cloud.SectionIngestTSIndex)
	if !ok || len(section) == 0 {
		t.Fatal("expected section to remain readable while retained")
	}
	blob.Release()
	section, ok = blob.Section(cloud.SectionIngestTSIndex)
	if ok && len(section) > 0 {
		t.Fatal("expected section unavailable after release following eviction")
	}
}
