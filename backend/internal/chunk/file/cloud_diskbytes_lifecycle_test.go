package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// TestCloudDiskBytesLifecycle drives the real Manager through the full
// cloud-backed chunk lifecycle against a fake blob store, pinning the
// gastrolog-33ul6h measurement fix: DiskBytes always means the chunk's
// LOCAL on-disk footprint (the warm-cache state), never the compressed
// cloud object size — and CloudBytes always means the cloud object size,
// never the local footprint.
//
//   - sealed -> uploaded: the local data.glcb stays on disk as the warm
//     cache (upload only deletes the redundant multi-file artifacts), so
//     DiskBytes must still reflect that local file's real size, not the
//     compressed blob's transport size. CloudBytes must record the blob
//     size.
//   - evicted: DiskBytes drops to 0 (nothing local to reclaim) and the
//     disk claim (chunk.DiskClaim) follows it to 0. CloudBytes is
//     unchanged — the object is still in the cloud store.
//   - re-warmed: reading the chunk downloads it back into the local
//     cache; DiskBytes reports the newly-cached file's size again.
func TestCloudDiskBytesLifecycle(t *testing.T) {
	t.Parallel()
	now := time.Now()
	clock := &mutableClock{t: now}
	cm := newEvictionTestManager(t, "lru", 0, 0, clock.now)

	ids := uploadN(t, cm, 1, 200)
	if len(ids) != 1 {
		t.Fatalf("expected 1 cloud-backed chunk, got %d", len(ids))
	}
	id := ids[0]

	glcbPath := filepath.Join(cm.chunkDir(id), dataGLCBFileName)
	localInfo, err := os.Stat(glcbPath)
	if err != nil {
		t.Fatalf("stat warm cache after upload: %v", err)
	}
	localSize := localInfo.Size()

	// --- sealed -> uploaded: warm cache still on disk ---
	meta, err := cm.Meta(id)
	if err != nil {
		t.Fatalf("Meta after upload: %v", err)
	}
	if !meta.CloudBacked {
		t.Fatalf("expected chunk to be cloud-backed after upload")
	}
	if meta.DiskBytes != localSize {
		t.Errorf("post-upload DiskBytes = %d, want local warm-cache file size %d (must not be the compressed blob size)", meta.DiskBytes, localSize)
	}
	if meta.CloudBytes <= 0 {
		t.Errorf("post-upload CloudBytes = %d, want > 0 (the uploaded blob's transport size)", meta.CloudBytes)
	}
	if meta.CloudBytes == meta.DiskBytes && meta.CloudBytes == localSize {
		// Not a hard failure by itself (tiny fixtures can coincide), but
		// flag it — the two are different currencies and asserting the
		// right *source* matters more than the coincidental value.
		t.Logf("note: CloudBytes and DiskBytes coincide at %d for this tiny fixture", meta.CloudBytes)
	}
	cloudBytesAfterUpload := meta.CloudBytes

	if got := chunk.DiskClaim(meta, nil); got != localSize {
		t.Errorf("DiskClaim after upload = %d, want %d (local cache size)", got, localSize)
	}

	// --- evicted: nothing local to reclaim ---
	evicted, _ := cm.EvictCacheLRU(1)
	if evicted != 1 {
		t.Fatalf("EvictCacheLRU: evicted = %d, want 1", evicted)
	}
	if _, err := os.Stat(glcbPath); !os.IsNotExist(err) {
		t.Fatalf("expected warm cache file removed after eviction, stat err=%v", err)
	}

	meta, err = cm.Meta(id)
	if err != nil {
		t.Fatalf("Meta after eviction: %v", err)
	}
	if meta.DiskBytes != 0 {
		t.Errorf("post-eviction DiskBytes = %d, want 0", meta.DiskBytes)
	}
	if meta.CloudBytes != cloudBytesAfterUpload {
		t.Errorf("post-eviction CloudBytes = %d, want unchanged %d (object still in cloud store)", meta.CloudBytes, cloudBytesAfterUpload)
	}
	if got := chunk.DiskClaim(meta, nil); got != 0 {
		t.Errorf("DiskClaim after eviction = %d, want 0 (evicted cloud chunk reclaims nothing locally)", got)
	}

	// --- re-warmed: reading downloads it back into the local cache ---
	openAndDrain(t, cm, id)

	rewarmedInfo, err := os.Stat(glcbPath)
	if err != nil {
		t.Fatalf("stat warm cache after re-warm: %v", err)
	}

	meta, err = cm.Meta(id)
	if err != nil {
		t.Fatalf("Meta after re-warm: %v", err)
	}
	if meta.DiskBytes != rewarmedInfo.Size() {
		t.Errorf("post-re-warm DiskBytes = %d, want re-cached file size %d", meta.DiskBytes, rewarmedInfo.Size())
	}
	if meta.DiskBytes == 0 {
		t.Errorf("post-re-warm DiskBytes must be > 0")
	}
	if meta.CloudBytes != cloudBytesAfterUpload {
		t.Errorf("post-re-warm CloudBytes = %d, want unchanged %d", meta.CloudBytes, cloudBytesAfterUpload)
	}
	if got := chunk.DiskClaim(meta, nil); got != meta.DiskBytes {
		t.Errorf("DiskClaim after re-warm = %d, want %d", got, meta.DiskBytes)
	}
}

// TestCloudDiskBytesLifecycle_List pins that List() reports the same
// live DiskBytes as Meta() for a cloud-backed chunk across the
// evict/re-warm cycle — List() is served from the cached cloudListCache,
// which must be invalidated whenever the live local footprint changes.
func TestCloudDiskBytesLifecycle_List(t *testing.T) {
	t.Parallel()
	now := time.Now()
	clock := &mutableClock{t: now}
	cm := newEvictionTestManager(t, "lru", 0, 0, clock.now)

	ids := uploadN(t, cm, 1, 50)
	if len(ids) != 1 {
		t.Fatalf("expected 1 cloud-backed chunk, got %d", len(ids))
	}
	id := ids[0]

	findMeta := func(t *testing.T) chunk.ChunkMeta {
		t.Helper()
		metas, err := cm.List()
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		for _, m := range metas {
			if m.ID == id {
				return m
			}
		}
		t.Fatalf("chunk %s not found in List()", id)
		return chunk.ChunkMeta{}
	}

	if m := findMeta(t); m.DiskBytes <= 0 {
		t.Errorf("List() post-upload DiskBytes = %d, want > 0", m.DiskBytes)
	}

	if evicted, _ := cm.EvictCacheLRU(1); evicted != 1 {
		t.Fatalf("EvictCacheLRU: evicted = %d, want 1", evicted)
	}
	if m := findMeta(t); m.DiskBytes != 0 {
		t.Errorf("List() post-eviction DiskBytes = %d, want 0 (cloudListCache must be invalidated on eviction)", m.DiskBytes)
	}

	openAndDrain(t, cm, id)
	if m := findMeta(t); m.DiskBytes <= 0 {
		t.Errorf("List() post-re-warm DiskBytes = %d, want > 0 (cloudListCache must be invalidated on re-warm)", m.DiskBytes)
	}
}
