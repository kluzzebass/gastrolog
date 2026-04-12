package file

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// TestListDeduplicatesCloudAndLocal verifies that List() returns each chunk
// exactly once, even when the same chunk exists in both m.metas (local) and
// the cloud index. The cloud version (CloudBacked=true) takes precedence.
//
// This can happen briefly during upload (after cloud index insert, before
// metas delete) or when stale local files survive after adoptCloudBlob.
// See gastrolog-68fqk.
func TestListDeduplicatesCloudAndLocal(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vaultID := uuid.Must(uuid.NewV7())
	store := blobstore.NewMemory()

	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	// Ingest a few records, seal, and upload to cloud.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 5 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: []byte("dedup-test"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}

	// Find the sealed chunk before upload.
	metas, _ := cm.List()
	var sealedID chunk.ChunkID
	for _, m := range metas {
		if m.Sealed {
			sealedID = m.ID
			break
		}
	}
	if sealedID == (chunk.ChunkID{}) {
		t.Fatal("no sealed chunk found")
	}

	// Upload to cloud — this moves the chunk from metas to cloud index.
	if err := cm.PostSealProcess(context.Background(), sealedID); err != nil {
		t.Fatal(err)
	}

	// After upload, chunk should appear exactly once with CloudBacked=true.
	afterUpload, _ := cm.List()
	count := countChunkID(afterUpload, sealedID)
	if count != 1 {
		t.Fatalf("after upload: chunk %s appears %d times, want 1", sealedID, count)
	}
	for _, m := range afterUpload {
		if m.ID == sealedID && !m.CloudBacked {
			t.Error("after upload: chunk should be CloudBacked=true")
		}
	}

	// Simulate the stale-local-files scenario: re-insert the chunk into
	// metas (as if local idx.log was somehow still present and loaded on
	// restart). This creates a duplicate: metas has CloudBacked=false,
	// cloud index has CloudBacked=true.
	cm.mu.Lock()
	cm.metas[sealedID] = &chunkMeta{
		id:         sealedID,
		writeStart: t0,
		writeEnd:   t0.Add(4 * time.Microsecond),
		sealed:     true,
		// CloudBacked intentionally false — simulating stale local state.
	}
	cm.mu.Unlock()

	// List() should still return the chunk exactly once, with CloudBacked=true.
	withDup, _ := cm.List()
	count = countChunkID(withDup, sealedID)
	if count != 1 {
		t.Fatalf("with stale local meta: chunk %s appears %d times, want 1", sealedID, count)
	}
	for _, m := range withDup {
		if m.ID == sealedID {
			if !m.CloudBacked {
				t.Error("dedup should prefer cloud version (CloudBacked=true)")
			}
			break
		}
	}
}

// TestListNoDuplicatesWithoutCloudIndex verifies that List() still works
// normally when there is no cloud index (non-cloud tier).
func TestListNoDuplicatesWithoutCloudIndex(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 3 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: []byte("no-cloud"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, err := cm.List()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[chunk.ChunkID]int)
	for _, m := range metas {
		seen[m.ID]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("chunk %s appears %d times without cloud index", id, n)
		}
	}
}

func countChunkID(metas []chunk.ChunkMeta, target chunk.ChunkID) int {
	n := 0
	for _, m := range metas {
		if m.ID == target {
			n++
		}
	}
	return n
}
