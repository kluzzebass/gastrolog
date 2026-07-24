package file

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// This file pins the lazy FSM-grounded cloud-backed resolver
// (gastrolog-5bnxc): the cloud index is a cache of the replicated vault-ctl
// manifest's CloudBacked entries with exactly ONE fill path — resolution on a
// lookup/enumeration miss — replacing the two retired eager mirrors (the
// reconciler's snapshot projection pass and the per-apply onUpload
// registration). The founding failure shape: a follower that caught up via
// snapshot install had the cloud entry in its FSM but nothing in its cloud
// index (Restore fires no per-apply effects), so OpenCursor returned
// ErrChunkNotFound and search streams aborted.

// wipeCloudEntry reproduces the snapshot-install follower state for a chunk
// this manager uploaded itself: blob in the cloud store, no cloud-index
// entry, no local meta. Returns the CloudBackedChunkInfo the FSM manifest
// would carry for the chunk (captured from the upload-time index entry).
func wipeCloudEntry(t *testing.T, cm *Manager, id chunk.ChunkID) chunk.CloudBackedChunkInfo {
	t.Helper()
	cm.cloudIdxMu.Lock()
	cmeta, ok := cm.cloudIdx.Lookup(id)
	cm.cloudIdxMu.Unlock()
	if !ok || cmeta == nil {
		t.Fatalf("fixture: chunk %s not in cloud index after upload", id)
	}
	info := chunk.CloudBackedChunkInfo{
		WriteStart:        cmeta.writeStart,
		WriteEnd:          cmeta.writeEnd,
		IngestStart:       cmeta.ingestStart,
		IngestEnd:         cmeta.ingestEnd,
		SourceStart:       cmeta.sourceStart,
		SourceEnd:         cmeta.sourceEnd,
		RecordCount:       cmeta.recordCount,
		Bytes:             cmeta.bytes,
		CloudBytes:        cmeta.cloudBytes,
		IngestIdxOffset:   cmeta.ingestIdxOffset,
		IngestIdxSize:     cmeta.ingestIdxSize,
		SourceIdxOffset:   cmeta.sourceIdxOffset,
		SourceIdxSize:     cmeta.sourceIdxSize,
		IngestTSMonotonic: cmeta.ingestTSMonotonic,
	}

	cm.cloudIdxMu.Lock()
	if _, err := cm.cloudIdx.Delete(id); err != nil {
		t.Fatalf("cloudIdx.Delete: %v", err)
	}
	if err := cm.cloudIdx.Sync(); err != nil {
		t.Fatalf("cloudIdx.Sync: %v", err)
	}
	cm.cloudIdxMu.Unlock()
	cm.mu.Lock()
	delete(cm.metas, id)
	cm.cloudListCache = nil
	cm.mu.Unlock()
	return info
}

// installCountingResolver wires a resolver that serves `info` for `id` and
// counts invocations, mirroring what wireLazyCloudBackedResolver installs
// from the vault-ctl FSM in production.
func installCountingResolver(cm *Manager, id chunk.ChunkID, info chunk.CloudBackedChunkInfo) *atomic.Int64 {
	var calls atomic.Int64
	cm.SetCloudBackedChunkResolver(func(q chunk.ChunkID) (chunk.CloudBackedChunkInfo, bool) {
		calls.Add(1)
		if q == id {
			return info, true
		}
		return chunk.CloudBackedChunkInfo{}, false
	})
	return &calls
}

// readAllCloudRecords opens a cursor on the chunk and reads it to completion and returns the record count.
func readAllCloudRecords(t *testing.T, cm *Manager, id chunk.ChunkID) int {
	t.Helper()
	cursor, err := cm.OpenCursor(id)
	if err != nil {
		t.Fatalf("OpenCursor(%s): %v", id, err)
	}
	got := 0
	for {
		_, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			_ = cursor.Close()
			t.Fatalf("cursor.Next: %v", err)
		}
		got++
	}
	_ = cursor.Close()
	return got
}

// TestOpenCursorCloudBackedResolvesLazilyFromFSM is the repurposed
// gastrolog-5bnxc pin (formerly TestOpenCursorCloudBackedRequiresCloudIndex,
// which pinned the eager mirrors): in the exact snapshot-install follower
// state — blob only in the cloud store, empty cloud index, no local meta —
// OpenCursor serves the chunk's records through the lazy resolver, with no
// eager registration pass anywhere. Resolution is metadata-only (the blob
// key is derived by blobKey()); the byte fetch happens when the cursor
// reads. The FSM hit is memoized by the cloud-index insert, so repeated
// reads cost one resolver call total.
func TestOpenCursorCloudBackedResolvesLazilyFromFSM(t *testing.T) {
	t.Parallel()

	cm := newEvictionTestManager(t, "lru", 0, 0, nil)

	const records = 50
	ids := uploadN(t, cm, 1, records)
	if len(ids) != 1 {
		t.Fatalf("expected 1 cloud-backed chunk, got %d", len(ids))
	}
	id := ids[0]

	// Evict the warm cache (removes the local data.glcb) ...
	if evicted, _ := cm.EvictCacheLRU(1); evicted != 1 {
		t.Fatalf("EvictCacheLRU: evicted = %d, want 1", evicted)
	}
	// ... and drop the cloud-index entry + local meta: the
	// snapshot-install follower state.
	info := wipeCloudEntry(t, cm, id)

	// Baseline: with no resolver, the miss stays a miss.
	if _, err := cm.OpenCursor(id); !errors.Is(err, chunk.ErrChunkNotFound) {
		t.Fatalf("OpenCursor without resolver: err = %v, want ErrChunkNotFound", err)
	}

	calls := installCountingResolver(cm, id, info)

	if got := readAllCloudRecords(t, cm, id); got != records {
		t.Errorf("served %d records via lazy resolution, want %d", got, records)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("resolver calls = %d, want 1 (memoized by the cloud-index insert)", got)
	}

	// Meta() and a second cursor hit the memoized entry — no new FSM reads.
	meta, err := cm.Meta(id)
	if err != nil {
		t.Fatalf("Meta after lazy resolution: %v", err)
	}
	if !meta.CloudBacked {
		t.Error("resolved meta: CloudBacked = false, want true")
	}
	if meta.RecordCount != records {
		t.Errorf("resolved meta: RecordCount = %d, want %d", meta.RecordCount, records)
	}
	if got := readAllCloudRecords(t, cm, id); got != records {
		t.Errorf("second read served %d records, want %d", got, records)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("resolver calls after re-read = %d, want 1", got)
	}
}

// TestLazyCloudBackedResolverMissStaysCleanNotFound pins the unhappy path: a
// chunk the FSM does not know as CloudBacked is a clean ErrChunkNotFound —
// and the miss is NOT memoized, so an entry that appears in the FSM later
// (e.g. the CmdUploadChunk lands after the first query) resolves then.
func TestLazyCloudBackedResolverMissStaysCleanNotFound(t *testing.T) {
	t.Parallel()

	cm := newEvictionTestManager(t, "lru", 0, 0, nil)
	const records = 20
	id := uploadN(t, cm, 1, records)[0]
	info := wipeCloudEntry(t, cm, id)

	// FSM has no CloudBacked entry yet.
	var uploaded atomic.Bool
	var calls atomic.Int64
	cm.SetCloudBackedChunkResolver(func(q chunk.ChunkID) (chunk.CloudBackedChunkInfo, bool) {
		calls.Add(1)
		if uploaded.Load() && q == id {
			return info, true
		}
		return chunk.CloudBackedChunkInfo{}, false
	})

	if _, err := cm.OpenCursor(id); !errors.Is(err, chunk.ErrChunkNotFound) {
		t.Fatalf("OpenCursor before FSM upload: err = %v, want ErrChunkNotFound", err)
	}
	if _, err := cm.Meta(id); !errors.Is(err, chunk.ErrChunkNotFound) {
		t.Fatalf("Meta before FSM upload: err = %v, want ErrChunkNotFound", err)
	}
	if calls.Load() < 2 {
		t.Fatalf("resolver calls = %d, want one per miss (no negative memoization)", calls.Load())
	}

	// The upload lands in the FSM — the next lookup resolves.
	uploaded.Store(true)
	if got := readAllCloudRecords(t, cm, id); got != records {
		t.Errorf("served %d records after FSM upload, want %d", got, records)
	}
}

// TestLazyCloudBackedResolverConcurrentFirstAccess pins that a stampede of
// first readers on an unresolved chunk is safe: m.mu serializes resolution,
// exactly one cloud-index entry is created, and every reader serves the full
// record set. Meaningful under -race.
func TestLazyCloudBackedResolverConcurrentFirstAccess(t *testing.T) {
	t.Parallel()

	cm := newEvictionTestManager(t, "lru", 0, 0, nil)
	const records = 30
	id := uploadN(t, cm, 1, records)[0]
	if evicted, _ := cm.EvictCacheLRU(1); evicted != 1 {
		t.Fatalf("EvictCacheLRU: evicted = %d, want 1", evicted)
	}
	info := wipeCloudEntry(t, cm, id)
	calls := installCountingResolver(cm, id, info)

	const readers = 16
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cursor, err := cm.OpenCursor(id)
			if err != nil {
				errs <- err
				return
			}
			got := 0
			for {
				_, _, err := cursor.Next()
				if errors.Is(err, chunk.ErrNoMoreRecords) {
					break
				}
				if err != nil {
					_ = cursor.Close()
					errs <- err
					return
				}
				got++
			}
			_ = cursor.Close()
			if got != records {
				errs <- errors.New("short read under concurrent first access")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent reader: %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Errorf("resolver calls = %d, want 1 (single-flight under m.mu)", got)
	}
	cm.cloudIdxMu.Lock()
	_, found := cm.cloudIdx.Lookup(id)
	count := cm.cloudIdx.Count()
	cm.cloudIdxMu.Unlock()
	if !found || count != 1 {
		t.Errorf("cloudIdx: found=%v count=%d, want the single resolved entry", found, count)
	}
}

// TestLazyCloudBackedResolverEvictThenReread pins the eviction interplay: a
// lazily-resolved chunk's warm cache participates in eviction like any other
// cloud-backed chunk, and a post-eviction read re-downloads without
// re-resolving (the cloud-index entry survives eviction).
func TestLazyCloudBackedResolverEvictThenReread(t *testing.T) {
	t.Parallel()

	cm := newEvictionTestManager(t, "lru", 0, 0, nil)
	const records = 40
	id := uploadN(t, cm, 1, records)[0]
	if evicted, _ := cm.EvictCacheLRU(1); evicted != 1 {
		t.Fatalf("EvictCacheLRU: evicted = %d, want 1", evicted)
	}
	info := wipeCloudEntry(t, cm, id)
	calls := installCountingResolver(cm, id, info)

	// First read resolves lazily and re-warms the cache.
	if got := readAllCloudRecords(t, cm, id); got != records {
		t.Fatalf("first read served %d records, want %d", got, records)
	}
	if !cm.hasLocalGLCB(id) {
		t.Fatal("warm cache not populated by the first cloud read")
	}

	// Evict the warm copy again — the resolved entry must survive.
	if evicted, _ := cm.EvictCacheLRU(1); evicted != 1 {
		t.Fatalf("EvictCacheLRU after lazy resolve: evicted = %d, want 1", evicted)
	}
	if got := readAllCloudRecords(t, cm, id); got != records {
		t.Errorf("post-eviction read served %d records, want %d", got, records)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("resolver calls = %d, want 1 (eviction drops bytes, not the index entry)", got)
	}
}

// unreliableStore wraps a blobstore.Store and fails all reads while `down`
// is set — the "cloud store unreachable at resolve time" shape.
type unreliableStore struct {
	blobstore.Store
	down atomic.Bool
}

var errStoreDown = errors.New("cloud store unreachable")

func (s *unreliableStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	if s.down.Load() {
		return nil, errStoreDown
	}
	return s.Store.Download(ctx, key)
}

func (s *unreliableStore) DownloadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if s.down.Load() {
		return nil, errStoreDown
	}
	return s.Store.DownloadRange(ctx, key, offset, length)
}

// TestLazyCloudBackedResolverStoreUnreachable pins the unreachable-store
// unhappy path: resolution itself is metadata-only and succeeds while the
// store is down (no byte fetch — the histogram no-fetch policy), the byte
// read propagates the store error, and the memoized entry is not poisoned —
// once the store heals, the same entry serves without re-resolving.
func TestLazyCloudBackedResolverStoreUnreachable(t *testing.T) {
	t.Parallel()

	store := &unreliableStore{Store: blobstore.NewMemory()}
	cm, err := NewManager(Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        glid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })

	const records = 25
	id := uploadN(t, cm, 1, records)[0]
	if evicted, _ := cm.EvictCacheLRU(1); evicted != 1 {
		t.Fatalf("EvictCacheLRU: evicted = %d, want 1", evicted)
	}
	info := wipeCloudEntry(t, cm, id)
	calls := installCountingResolver(cm, id, info)

	store.down.Store(true)

	// Metadata resolves without touching the store.
	meta, err := cm.Meta(id)
	if err != nil {
		t.Fatalf("Meta while store down: %v (resolution must not fetch bytes)", err)
	}
	if meta.RecordCount != records {
		t.Errorf("Meta while store down: RecordCount = %d, want %d", meta.RecordCount, records)
	}

	// The byte read fails loudly.
	if _, err := cm.OpenCursor(id); err == nil {
		t.Fatal("OpenCursor while store down: expected error, got nil")
	}

	// Store heals: the memoized entry serves; no re-resolution needed.
	store.down.Store(false)
	if got := readAllCloudRecords(t, cm, id); got != records {
		t.Errorf("served %d records after store recovery, want %d", got, records)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("resolver calls = %d, want 1 (store outage must not poison or evict the entry)", got)
	}
}

// TestLazyCloudBackedListerSurfacesInList pins the enumeration half: List()
// consults the cloud lister so a chunk the FSM knows as CloudBacked appears
// in enumeration (match-all search shape) without anything having named it
// by ID first — the gastrolog-3s26vr failure shape, cloud edition.
func TestLazyCloudBackedListerSurfacesInList(t *testing.T) {
	t.Parallel()

	cm := newEvictionTestManager(t, "lru", 0, 0, nil)
	const records = 15
	id := uploadN(t, cm, 1, records)[0]
	if evicted, _ := cm.EvictCacheLRU(1); evicted != 1 {
		t.Fatalf("EvictCacheLRU: evicted = %d, want 1", evicted)
	}
	info := wipeCloudEntry(t, cm, id)
	installCountingResolver(cm, id, info)
	cm.SetCloudBackedChunkLister(func() []chunk.ChunkID { return []chunk.ChunkID{id} })

	metas, err := cm.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var found bool
	for _, m := range metas {
		if m.ID == id {
			found = true
			if !m.CloudBacked {
				t.Error("listed lazy-resolved chunk: CloudBacked = false, want true")
			}
			if m.RecordCount != records {
				t.Errorf("listed lazy-resolved chunk: RecordCount = %d, want %d", m.RecordCount, records)
			}
		}
	}
	if !found {
		t.Fatal("FSM-known cloud-backed chunk missing from List() — lister not consulted")
	}
	// Enumeration memoized the entry; the follow-up read needs no lister.
	if got := readAllCloudRecords(t, cm, id); got != records {
		t.Errorf("read after List served %d records, want %d", got, records)
	}
}
