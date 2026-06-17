package cloud_test

import (
	"os"
	"path/filepath"
	"testing"

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

	rd := blob.Reader()
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
