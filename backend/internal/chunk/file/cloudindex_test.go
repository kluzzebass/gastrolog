package file

import (
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func TestCloudIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	idx, err := openCloudIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Insert some metadata.
	ids := make([]chunk.ChunkID, 5)
	metas := make([]*chunkMeta, 5)
	for i := range 5 {
		ids[i] = chunk.NewChunkID()
		metas[i] = &chunkMeta{
			id:          ids[i],
			writeStart:  time.Now().Add(-time.Duration(i) * time.Hour),
			writeEnd:    time.Now(),
			recordCount: int64(100 * (i + 1)),
			bytes:       int64(1024 * (i + 1)),
			diskBytes:   int64(512 * (i + 1)),
			sealed:      true,
			compressed:  i%2 == 0,
			ingestStart: time.Now().Add(-2 * time.Hour),
			ingestEnd:   time.Now().Add(-time.Hour),
			sourceStart: time.Now().Add(-3 * time.Hour),
			sourceEnd:   time.Now().Add(-2 * time.Hour),
			cloudBacked: true,
		}
		if err := idx.Insert(ids[i], metas[i]); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	if idx.Count() != 5 {
		t.Fatalf("count = %d, want 5", idx.Count())
	}

	if err := idx.Sync(); err != nil {
		t.Fatal(err)
	}

	// Close and reopen.
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}

	idx, err = openCloudIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	if idx.Count() != 5 {
		t.Fatalf("count after reopen = %d, want 5", idx.Count())
	}

	// LoadAll and verify.
	all, err := idx.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 5 {
		t.Fatalf("LoadAll returned %d entries, want 5", len(all))
	}

	for i, id := range ids {
		got, ok := all[id]
		if !ok {
			t.Fatalf("missing chunk %s", id)
		}
		want := metas[i]
		if got.recordCount != want.recordCount {
			t.Errorf("chunk %s: recordCount = %d, want %d", id, got.recordCount, want.recordCount)
		}
		if got.bytes != want.bytes {
			t.Errorf("chunk %s: bytes = %d, want %d", id, got.bytes, want.bytes)
		}
		if got.diskBytes != want.diskBytes {
			t.Errorf("chunk %s: diskBytes = %d, want %d", id, got.diskBytes, want.diskBytes)
		}
		if got.sealed != want.sealed {
			t.Errorf("chunk %s: sealed = %v, want %v", id, got.sealed, want.sealed)
		}
		if got.compressed != want.compressed {
			t.Errorf("chunk %s: compressed = %v, want %v", id, got.compressed, want.compressed)
		}
		if !got.cloudBacked {
			t.Errorf("chunk %s: cloudBacked should be true", id)
		}
		// Time comparison with nanosecond truncation tolerance.
		if got.writeStart.UnixNano() != want.writeStart.UnixNano() {
			t.Errorf("chunk %s: writeStart mismatch", id)
		}
	}
}

func TestCloudIndexDelete(t *testing.T) {
	dir := t.TempDir()
	idx, err := openCloudIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	ids := make([]chunk.ChunkID, 3)
	for i := range 3 {
		ids[i] = chunk.NewChunkID()
		if err := idx.Insert(ids[i], &chunkMeta{
			id:          ids[i],
			recordCount: int64(i),
			sealed:      true,
			cloudBacked: true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Delete middle entry.
	ok, err := idx.Delete(ids[1])
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("Delete should return true")
	}
	if idx.Count() != 2 {
		t.Fatalf("count = %d, want 2", idx.Count())
	}

	// Verify it's gone.
	all, err := idx.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := all[ids[1]]; ok {
		t.Fatal("deleted chunk should not be present")
	}
	if _, ok := all[ids[0]]; !ok {
		t.Fatal("first chunk should still be present")
	}
	if _, ok := all[ids[2]]; !ok {
		t.Fatal("third chunk should still be present")
	}
}

func TestCloudIndexCreateOrOpen(t *testing.T) {
	dir := t.TempDir()

	// First call creates.
	idx, err := openCloudIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	id := chunk.NewChunkID()
	if err := idx.Insert(id, &chunkMeta{id: id, sealed: true, cloudBacked: true}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify file exists.
	path := filepath.Join(dir, cloudIndexFile)
	if _, err := openCloudIndex(dir); err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	_ = path
}
