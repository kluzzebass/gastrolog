package glcb_test

import (
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk/glcb"
)

func TestOpenWriterRoundTrip(t *testing.T) {
	t.Parallel()
	chunkID, vaultID, records := testRecords()

	path := filepath.Join(t.TempDir(), "data.glcb")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	w, err := glcb.OpenWriter(f, chunkID, vaultID)
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

	blob, err := glcb.OpenMappedBlob(path)
	if err != nil {
		t.Fatal(err)
	}
	defer blob.Close()

	rd, err := blob.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()

	for i := range records {
		got, err := rd.ReadRecord(uint32(i))
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		assertRecord(t, i, got, records[i])
	}
}
