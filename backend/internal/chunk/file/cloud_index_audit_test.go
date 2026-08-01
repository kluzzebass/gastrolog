package file

import (
	"context"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// The two views a cloud-index audit needs from a manager: what the blob store
// actually holds, and what the node-local cloud index believes it holds. They
// are deliberately separate reads — the index is a cache, so the whole point of
// the audit is that the two can disagree.

func TestListCloudBlobsReportsStoreTruth(t *testing.T) {
	t.Parallel()
	store := blobstore.NewMemory()
	cm := newRestartableCloudManager(t, t.TempDir(), glid.New(), store)
	defer cm.Close()

	id := appendSealAndUpload(t, cm, 20)

	blobs, err := cm.ListCloudBlobs(context.Background())
	if err != nil {
		t.Fatalf("ListCloudBlobs: %v", err)
	}
	if len(blobs) != 1 {
		t.Fatalf("got %d blobs, want 1: %+v", len(blobs), blobs)
	}
	if blobs[0].ID != id {
		t.Errorf("blob ID = %s, want %s", blobs[0].ID, id)
	}
	if blobs[0].Size <= 0 {
		t.Errorf("blob size = %d, want positive", blobs[0].Size)
	}
	if blobs[0].Archived {
		t.Error("freshly uploaded blob reports archived")
	}
}

func TestCloudIndexEntriesReportsCacheContents(t *testing.T) {
	t.Parallel()
	store := blobstore.NewMemory()
	cm := newRestartableCloudManager(t, t.TempDir(), glid.New(), store)
	defer cm.Close()

	id := appendSealAndUpload(t, cm, 20)

	entries := cm.CloudIndexEntries()
	if len(entries) != 1 {
		t.Fatalf("got %d index entries, want 1: %+v", len(entries), entries)
	}
	if entries[0].ID != id {
		t.Errorf("entry ID = %s, want %s", entries[0].ID, id)
	}
	if entries[0].CloudBytes <= 0 {
		t.Errorf("entry CloudBytes = %d, want positive", entries[0].CloudBytes)
	}
}

// The divergence the audit exists to find. A blob deleted out of band — a
// provider lifecycle rule, an operator with a console — emits no event, so the
// index keeps claiming the chunk is durable while the bytes are gone.
func TestOutOfBandBlobDeletionDivergesFromIndex(t *testing.T) {
	t.Parallel()
	store := blobstore.NewMemory()
	cm := newRestartableCloudManager(t, t.TempDir(), glid.New(), store)
	defer cm.Close()

	id := appendSealAndUpload(t, cm, 20)
	if err := store.Delete(context.Background(), cm.blobKey(id)); err != nil {
		t.Fatalf("out-of-band delete: %v", err)
	}

	blobs, err := cm.ListCloudBlobs(context.Background())
	if err != nil {
		t.Fatalf("ListCloudBlobs: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("store still lists %d blobs after the delete", len(blobs))
	}
	if entries := cm.CloudIndexEntries(); len(entries) != 1 {
		t.Fatalf("index should still hold the stale entry, got %d", len(entries))
	}
}

// A blob written under the vault prefix that no chunk ever announced: leaked
// bytes an operator is paying for. It must surface as store truth even though
// nothing indexed it.
func TestListCloudBlobsIncludesUnindexedObjects(t *testing.T) {
	t.Parallel()
	store := blobstore.NewMemory()
	vaultID := glid.New()
	cm := newRestartableCloudManager(t, t.TempDir(), vaultID, store)
	defer cm.Close()

	appendSealAndUpload(t, cm, 20)

	stray := chunk.NewChunkID()
	if err := store.Upload(context.Background(), cm.blobKey(stray),
		strings.NewReader("bytes nothing announced"), nil); err != nil {
		t.Fatalf("upload stray blob: %v", err)
	}

	blobs, err := cm.ListCloudBlobs(context.Background())
	if err != nil {
		t.Fatalf("ListCloudBlobs: %v", err)
	}
	if len(blobs) != 2 {
		t.Fatalf("got %d blobs, want 2 (one uploaded, one stray)", len(blobs))
	}
	var sawStray bool
	for _, b := range blobs {
		if b.ID == stray {
			sawStray = true
		}
	}
	if !sawStray {
		t.Error("the stray blob is not reported; unindexed objects would go unnoticed")
	}
	if len(cm.CloudIndexEntries()) != 1 {
		t.Error("the stray blob was indexed; listing must not mutate the cache")
	}
}

// A manager with no cloud store must say so rather than report an empty store,
// which a caller would read as "the vault has no cloud objects".
func TestListCloudBlobsWithoutCloudStore(t *testing.T) {
	t.Parallel()
	cm, err := NewManager(Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		VaultID:        glid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	if _, err := cm.ListCloudBlobs(context.Background()); err == nil {
		t.Fatal("ListCloudBlobs on a local-only vault returned no error")
	}
	if entries := cm.CloudIndexEntries(); len(entries) != 0 {
		t.Errorf("local-only manager reports %d cloud index entries", len(entries))
	}
}
