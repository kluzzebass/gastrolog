package file

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// RepairCloudIndex rebuilds the node-local cloud index from the blob store. It
// repairs a CACHE: it never deletes an object and never touches cluster state,
// so it is safe to run on a live vault.

func TestRepairCloudIndexDropsStaleEntry(t *testing.T) {
	t.Parallel()
	store := blobstore.NewMemory()
	cm := newRestartableCloudManager(t, t.TempDir(), glid.New(), store)
	defer cm.Close()

	id := appendSealAndUpload(t, cm, 20)
	if err := store.Delete(context.Background(), cm.blobKey(id)); err != nil {
		t.Fatalf("out-of-band delete: %v", err)
	}
	if len(cm.CloudIndexEntries()) != 1 {
		t.Fatal("premise: the stale entry is not in the index")
	}

	repair, err := cm.RepairCloudIndex(context.Background())
	if err != nil {
		t.Fatalf("RepairCloudIndex: %v", err)
	}
	if repair.RemovedEntries != 1 {
		t.Errorf("RemovedEntries = %d, want 1 (%+v)", repair.RemovedEntries, repair)
	}
	if entries := cm.CloudIndexEntries(); len(entries) != 0 {
		t.Errorf("index still holds %d entries after repair", len(entries))
	}
}

func TestRepairCloudIndexReindexesLostEntry(t *testing.T) {
	t.Parallel()
	store := blobstore.NewMemory()
	cm := newRestartableCloudManager(t, t.TempDir(), glid.New(), store)
	defer cm.Close()

	id := appendSealAndUpload(t, cm, 20)
	// The cache lost the entry while the object is intact — what a wiped or
	// codec-mismatched cloud.idx leaves behind.
	dropCloudIndexEntry(t, cm, id)

	repair, err := cm.RepairCloudIndex(context.Background())
	if err != nil {
		t.Fatalf("RepairCloudIndex: %v", err)
	}
	if repair.IndexedBlobs != 1 {
		t.Errorf("IndexedBlobs = %d, want 1 (%+v)", repair.IndexedBlobs, repair)
	}
	entries := cm.CloudIndexEntries()
	if len(entries) != 1 || entries[0].ID != id {
		t.Fatalf("index = %+v, want just %s", entries, id)
	}
	if entries[0].RecordCount != 20 {
		t.Errorf("reindexed RecordCount = %d, want 20 — the rebuild must recover real metadata, not zeros",
			entries[0].RecordCount)
	}
}

func TestRepairCloudIndexCorrectsSize(t *testing.T) {
	t.Parallel()
	store := blobstore.NewMemory()
	cm := newRestartableCloudManager(t, t.TempDir(), glid.New(), store)
	defer cm.Close()

	id := appendSealAndUpload(t, cm, 20)
	before := cm.CloudIndexEntries()
	if len(before) != 1 || before[0].CloudBytes <= 0 {
		t.Fatalf("premise: no cached size to corrupt (%+v)", before)
	}
	realSize := before[0].CloudBytes
	setCloudIndexBytes(t, cm, id, realSize+4096)

	repair, err := cm.RepairCloudIndex(context.Background())
	if err != nil {
		t.Fatalf("RepairCloudIndex: %v", err)
	}
	if repair.CorrectedSizes != 1 {
		t.Errorf("CorrectedSizes = %d, want 1 (%+v)", repair.CorrectedSizes, repair)
	}
	after := cm.CloudIndexEntries()
	if len(after) != 1 || after[0].CloudBytes != realSize {
		t.Errorf("CloudBytes = %d, want the store's %d", after[0].CloudBytes, realSize)
	}
}

// A repair on a healthy vault must be a no-op, and a second repair after a real
// one must report nothing left to do. Without this a repair that "fixed"
// something every run would look identical to one that works.
func TestRepairCloudIndexIsIdempotent(t *testing.T) {
	t.Parallel()
	store := blobstore.NewMemory()
	cm := newRestartableCloudManager(t, t.TempDir(), glid.New(), store)
	defer cm.Close()

	id := appendSealAndUpload(t, cm, 20)
	dropCloudIndexEntry(t, cm, id)

	first, err := cm.RepairCloudIndex(context.Background())
	if err != nil {
		t.Fatalf("first repair: %v", err)
	}
	if first.Clean() {
		t.Fatal("premise: the first repair changed nothing")
	}

	second, err := cm.RepairCloudIndex(context.Background())
	if err != nil {
		t.Fatalf("second repair: %v", err)
	}
	if !second.Clean() {
		t.Errorf("second repair still reports work: %+v", second)
	}
}

// The line this must never cross. Repair fixes a cache; deleting an object
// would destroy the only copy of ingested records, and an untracked object is
// exactly the case where the local view is the one that is wrong.
func TestRepairCloudIndexNeverDeletesObjects(t *testing.T) {
	t.Parallel()
	store := blobstore.NewMemory()
	cm := newRestartableCloudManager(t, t.TempDir(), glid.New(), store)
	defer cm.Close()

	appendSealAndUpload(t, cm, 20)
	before := countStoreObjects(t, store)
	if before == 0 {
		t.Fatal("premise: nothing in the store to protect")
	}

	if _, err := cm.RepairCloudIndex(context.Background()); err != nil {
		t.Fatalf("RepairCloudIndex: %v", err)
	}
	if after := countStoreObjects(t, store); after != before {
		t.Fatalf("store went from %d objects to %d; repair must never delete bytes", before, after)
	}
}

func TestRepairCloudIndexWithoutCloudStore(t *testing.T) {
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

	if _, err := cm.RepairCloudIndex(context.Background()); err == nil {
		t.Fatal("RepairCloudIndex on a local-only vault returned no error")
	}
}

// dropCloudIndexEntry removes one entry from the cache without touching the
// store, modelling a lost or wiped cloud.idx.
func dropCloudIndexEntry(t *testing.T, cm *Manager, id chunk.ChunkID) {
	t.Helper()
	cm.cloudIdxMu.Lock()
	defer cm.cloudIdxMu.Unlock()
	if _, err := cm.cloudIdx.Delete(id); err != nil {
		t.Fatalf("drop cloud index entry: %v", err)
	}
	if err := cm.cloudIdx.Sync(); err != nil {
		t.Fatalf("sync after drop: %v", err)
	}
}

// setCloudIndexBytes rewrites a cached entry's recorded transport size,
// modelling a size that drifted from the object it describes.
func setCloudIndexBytes(t *testing.T, cm *Manager, id chunk.ChunkID, n int64) {
	t.Helper()
	cm.cloudIdxMu.Lock()
	defer cm.cloudIdxMu.Unlock()
	meta, ok := cm.cloudIdx.Lookup(id)
	if !ok {
		t.Fatalf("setCloudIndexBytes: %s not in cloud index", id)
	}
	meta.cloudBytes = n
	if _, err := cm.cloudIdx.Delete(id); err != nil {
		t.Fatalf("setCloudIndexBytes: delete: %v", err)
	}
	if err := cm.cloudIdx.Insert(id, meta); err != nil {
		t.Fatalf("setCloudIndexBytes: insert: %v", err)
	}
	if err := cm.cloudIdx.Sync(); err != nil {
		t.Fatalf("setCloudIndexBytes: sync: %v", err)
	}
}

func countStoreObjects(t *testing.T, store *blobstore.Memory) int {
	t.Helper()
	n := 0
	err := store.List(context.Background(), "", func(blobstore.BlobInfo) error {
		n++
		return nil
	})
	if err != nil {
		t.Fatalf("list store: %v", err)
	}
	return n
}
