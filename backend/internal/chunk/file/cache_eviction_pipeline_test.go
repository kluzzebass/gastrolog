package file

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
)

// Pipeline-built chunks keep their data.glcb under the vault's chunk root,
// outside the manager's Dir. Once such a chunk is cloud-backed that file is
// its cached copy on every home, and the cache policy must reach it.

// pipelineHome is one node's view of a cloud-backed pipeline vault: its chunk
// manager and the chunk root the pipeline builds GLCBs into.
type pipelineHome struct {
	cm        *Manager
	chunkRoot string
	store     *countingStore
}

func newPipelineHome(t *testing.T, store blobstore.Store, vaultID glid.GLID, policy string, budget uint64, ttl time.Duration, now func() time.Time) *pipelineHome {
	t.Helper()
	if now == nil {
		now = time.Now
	}
	counting := &countingStore{Store: store}
	cm, err := NewManager(Config{
		Dir:              t.TempDir(),
		Now:              now,
		RotationPolicy:   chunk.NewRecordCountPolicy(10000),
		CloudStore:       counting,
		VaultID:          vaultID,
		CacheEviction:    policy,
		CacheBudgetBytes: budget,
		CacheTTL:         ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	h := &pipelineHome{cm: cm, chunkRoot: t.TempDir(), store: counting}
	h.installLocator()
	return h
}

// installLocator wires the pipeline-path locator the orchestrator installs.
func (h *pipelineHome) installLocator() {
	root := h.chunkRoot
	h.cm.SetExternalGLCBLocator(func(id chunk.ChunkID) (string, bool) {
		return filepath.Join(root, id.String(), dataGLCBFileName), true
	})
}

// buildPipelineChunk writes a sealed GLCB into the chunk root the way the
// pipeline build does and registers it with the manager, returning the chunk
// ID, the GLCB path and its registration info.
func (h *pipelineHome) buildPipelineChunk(t *testing.T, id chunk.ChunkID, records int) (string, chunk.ExternalGLCBInfo) {
	t.Helper()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	path, info := glcbtest.WriteSealedBlob(t, h.chunkRoot, id, h.cm.cfg.VaultID,
		glcbtest.Records(records, base, glid.New(), "pipeline-evict"))
	if err := h.cm.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}
	return path, info
}

// copyPipelineChunk places another home's pipeline GLCB for id into this
// home's chunk root and registers it — what every home's own build produces.
func (h *pipelineHome) copyPipelineChunk(t *testing.T, id chunk.ChunkID, src string, info chunk.ExternalGLCBInfo) string {
	t.Helper()
	dst := filepath.Join(h.chunkRoot, id.String(), dataGLCBFileName)
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Clean(src))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := h.cm.RegisterExternalGLCB(id, dst, info); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}
	return dst
}

// fakeManifest stands in for the vault-ctl FSM's CloudBacked set: the
// resolver and lister a home's manager consults to confirm an upload.
type fakeManifest struct {
	mu       sync.Mutex
	uploaded map[chunk.ChunkID]chunk.CloudBackedChunkInfo
}

func newFakeManifest() *fakeManifest {
	return &fakeManifest{uploaded: map[chunk.ChunkID]chunk.CloudBackedChunkInfo{}}
}

func (f *fakeManifest) markUploaded(id chunk.ChunkID, info chunk.ExternalGLCBInfo, cloudBytes int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uploaded[id] = chunk.CloudBackedChunkInfo{
		WriteStart:        info.WriteStart,
		WriteEnd:          info.WriteEnd,
		IngestStart:       info.IngestStart,
		IngestEnd:         info.IngestEnd,
		SourceStart:       info.SourceStart,
		SourceEnd:         info.SourceEnd,
		RecordCount:       info.RecordCount,
		Bytes:             info.Bytes,
		CloudBytes:        cloudBytes,
		IngestIdxOffset:   info.IngestIdxOffset,
		IngestIdxSize:     info.IngestIdxSize,
		SourceIdxOffset:   info.SourceIdxOffset,
		SourceIdxSize:     info.SourceIdxSize,
		IngestTSMonotonic: info.IngestTSMonotonic,
	}
}

func (f *fakeManifest) install(cm *Manager) {
	cm.SetCloudBackedChunkResolver(func(id chunk.ChunkID) (chunk.CloudBackedChunkInfo, bool) {
		f.mu.Lock()
		defer f.mu.Unlock()
		info, ok := f.uploaded[id]
		return info, ok
	})
	cm.SetCloudBackedChunkLister(func() []chunk.ChunkID {
		f.mu.Lock()
		defer f.mu.Unlock()
		ids := make([]chunk.ChunkID, 0, len(f.uploaded))
		for id := range f.uploaded {
			ids = append(ids, id)
		}
		return ids
	})
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func countRecords(t *testing.T, cm *Manager, id chunk.ChunkID) int {
	t.Helper()
	return len(drainCursorRaw(t, cm, id))
}

// uploadPipelineChunks builds n pipeline chunks on h and uploads each — the
// uploading home's shape, where the uploaded chunk keeps its pipeline GLCB as
// the cached copy.
func uploadPipelineChunks(t *testing.T, h *pipelineHome, n, records int) ([]chunk.ChunkID, []string) {
	t.Helper()
	var ids []chunk.ChunkID
	var paths []string
	for range n {
		id := chunk.NewChunkID()
		path, _ := h.buildPipelineChunk(t, id, records)
		if err := h.cm.UploadToCloud(id); err != nil {
			t.Fatalf("UploadToCloud(%s): %v", id, err)
		}
		ids = append(ids, id)
		paths = append(paths, path)
	}
	return ids, paths
}

// TestEvictCacheLRU_PipelineGLCB: on the uploading home, LRU eviction under
// budget pressure removes the least recently read cloud-backed chunk's
// pipeline GLCB (and its now-empty pipeline chunk dir), keeping the more
// recently read ones.
func TestEvictCacheLRU_PipelineGLCB(t *testing.T) {
	t.Parallel()
	clock := &mutableClock{t: time.Now()}
	h := newPipelineHome(t, blobstore.NewMemory(), glid.New(), "lru", 0, 0, clock.now)
	ids, paths := uploadPipelineChunks(t, h, 3, 50)

	clock.t = clock.t.Add(time.Second)
	openAndDrain(t, h.cm, ids[1])
	clock.t = clock.t.Add(time.Second)
	openAndDrain(t, h.cm, ids[2])

	info, err := os.Stat(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		total += fi.Size()
	}
	budget := uint64(total - info.Size()) //nolint:gosec // test sizes are small and positive

	evicted, freed := h.cm.EvictCacheLRU(budget)
	if evicted != 1 || freed != info.Size() {
		t.Fatalf("EvictCacheLRU = (%d, %d), want (1, %d)", evicted, freed, info.Size())
	}
	if fileExists(paths[0]) {
		t.Errorf("least recently read pipeline GLCB %s survived eviction", paths[0])
	}
	if fileExists(filepath.Dir(paths[0])) {
		t.Errorf("evicted chunk's pipeline dir %s left behind", filepath.Dir(paths[0]))
	}
	for _, p := range paths[1:] {
		if !fileExists(p) {
			t.Errorf("recently read pipeline GLCB %s evicted under a budget that fits it", p)
		}
	}
}

// TestEvictCacheTTL_PipelineGLCB: TTL mode removes pipeline GLCBs whose last
// access is older than the TTL and keeps the ones read inside it.
func TestEvictCacheTTL_PipelineGLCB(t *testing.T) {
	t.Parallel()
	clock := &mutableClock{t: time.Now()}
	h := newPipelineHome(t, blobstore.NewMemory(), glid.New(), "ttl", 0, time.Hour, clock.now)
	ids, paths := uploadPipelineChunks(t, h, 2, 30)

	clock.t = clock.t.Add(50 * time.Minute)
	openAndDrain(t, h.cm, ids[1])
	clock.t = clock.t.Add(20 * time.Minute)

	evicted, _ := h.cm.EvictCache()
	if evicted != 1 {
		t.Fatalf("EvictCache (ttl) evicted %d, want 1", evicted)
	}
	if fileExists(paths[0]) {
		t.Error("pipeline GLCB past its TTL survived eviction")
	}
	if !fileExists(paths[1]) {
		t.Error("pipeline GLCB read inside the TTL was evicted")
	}
}

// TestEvictCache_PipelineGLCBRereadRefetches: after its pipeline GLCB is
// evicted, a read of the cloud-backed chunk re-fetches the object into the
// chunk dir and serves every record; the pipeline path stays gone.
func TestEvictCache_PipelineGLCBRereadRefetches(t *testing.T) {
	t.Parallel()
	h := newPipelineHome(t, blobstore.NewMemory(), glid.New(), "lru", 1, 0, nil)
	ids, paths := uploadPipelineChunks(t, h, 1, 40)

	if evicted, _ := h.cm.EvictCache(); evicted != 1 {
		t.Fatalf("EvictCache evicted %d, want 1", evicted)
	}
	if got := countRecords(t, h.cm, ids[0]); got != 40 {
		t.Fatalf("read after eviction served %d records, want 40", got)
	}
	if h.store.downloads.Load() != 1 {
		t.Errorf("cloud downloads = %d, want 1 (the re-fetch)", h.store.downloads.Load())
	}
	if !fileExists(filepath.Join(h.cm.chunkDir(ids[0]), dataGLCBFileName)) {
		t.Error("re-fetch did not populate the chunk-dir cached copy")
	}
	if fileExists(paths[0]) {
		t.Error("re-fetch resurrected the evicted pipeline GLCB")
	}
}

// TestEvictCache_FollowerPipelineGLCB is the non-uploading home: its pipeline
// GLCB was registered as a local chunk before the upload committed. Eviction
// must wait for the manifest to confirm the upload, then remove the copy and
// leave the chunk served from the cloud.
func TestEvictCache_FollowerPipelineGLCB(t *testing.T) {
	t.Parallel()
	shared := blobstore.NewMemory()
	vaultID := glid.New()
	leader := newPipelineHome(t, shared, vaultID, "", 0, 0, nil)
	follower := newPipelineHome(t, shared, vaultID, "lru", 1, 0, nil)
	manifest := newFakeManifest()
	manifest.install(follower.cm)

	id := chunk.NewChunkID()
	src, info := leader.buildPipelineChunk(t, id, 25)
	path := follower.copyPipelineChunk(t, id, src, info)
	if got := countRecords(t, follower.cm, id); got != 25 {
		t.Fatalf("follower local read served %d records, want 25", got)
	}

	// Not uploaded yet: the follower's copy is the only durable one it has.
	if evicted, _ := follower.cm.EvictCache(); evicted != 0 {
		t.Fatalf("evicted %d chunks before the upload was confirmed, want 0", evicted)
	}
	if !fileExists(path) {
		t.Fatal("pipeline GLCB removed before its upload was confirmed")
	}

	if err := leader.cm.UploadToCloud(id); err != nil {
		t.Fatalf("leader UploadToCloud: %v", err)
	}
	manifest.markUploaded(id, info, 1)

	if evicted, _ := follower.cm.EvictCache(); evicted != 1 {
		t.Fatalf("evicted %d chunks after the upload was confirmed, want 1", evicted)
	}
	if fileExists(path) {
		t.Fatal("follower pipeline GLCB survived eviction")
	}
	meta, err := follower.cm.Meta(id)
	if err != nil {
		t.Fatalf("Meta after eviction: %v", err)
	}
	if !meta.CloudBacked {
		t.Error("follower still reports the evicted chunk as a local chunk")
	}
	if got := countRecords(t, follower.cm, id); got != 25 {
		t.Fatalf("follower read after eviction served %d records, want 25", got)
	}
	if follower.store.downloads.Load() != 1 {
		t.Errorf("follower cloud downloads = %d, want 1 (the re-fetch)", follower.store.downloads.Load())
	}
}

// TestEvictCache_NeverEvictsUnconfirmedUpload: a chunk whose blob sits in the
// store and in this node's cloud index, but which the manifest does not mark
// cloud-backed, keeps its local GLCB — the manifest's upload commit is the
// only proof eviction accepts.
func TestEvictCache_NeverEvictsUnconfirmedUpload(t *testing.T) {
	t.Parallel()
	h := newPipelineHome(t, blobstore.NewMemory(), glid.New(), "lru", 1, 0, nil)
	manifest := newFakeManifest()
	manifest.install(h.cm)
	ids, paths := uploadPipelineChunks(t, h, 1, 20)

	if evicted, _ := h.cm.EvictCache(); evicted != 0 {
		t.Fatalf("evicted %d chunks the manifest never marked uploaded, want 0", evicted)
	}
	if !fileExists(paths[0]) {
		t.Fatal("pipeline GLCB of an unconfirmed upload was removed")
	}
	if got := countRecords(t, h.cm, ids[0]); got != 20 {
		t.Fatalf("read served %d records, want 20", got)
	}
}

// TestPipelineGLCB_ServesAndEvictsAfterRestart: a restart leaves a cloud-backed
// chunk known only through the cloud index, with no registration naming its
// pipeline GLCB. Reads must still use that copy instead of downloading a
// second one, and eviction must still find it.
func TestPipelineGLCB_ServesAndEvictsAfterRestart(t *testing.T) {
	t.Parallel()
	store := blobstore.NewMemory()
	vaultID := glid.New()
	dir := t.TempDir()
	chunkRoot := t.TempDir()
	open := func(budget uint64) *pipelineHome {
		counting := &countingStore{Store: store}
		cm, err := NewManager(Config{
			Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(10000),
			CloudStore: counting, VaultID: vaultID, CacheEviction: "lru", CacheBudgetBytes: budget,
		})
		if err != nil {
			t.Fatal(err)
		}
		h := &pipelineHome{cm: cm, chunkRoot: chunkRoot, store: counting}
		h.installLocator()
		return h
	}

	first := open(0)
	ids, paths := uploadPipelineChunks(t, first, 1, 30)
	if err := first.cm.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := open(1)
	t.Cleanup(func() { _ = restarted.cm.Close() })
	if got := countRecords(t, restarted.cm, ids[0]); got != 30 {
		t.Fatalf("read after restart served %d records, want 30", got)
	}
	if n := restarted.store.downloads.Load(); n != 0 {
		t.Fatalf("read after restart downloaded %d times, want 0 (the pipeline GLCB is the cached copy)", n)
	}

	if evicted, _ := restarted.cm.EvictCache(); evicted != 1 {
		t.Fatalf("EvictCache after restart evicted %d, want 1", evicted)
	}
	if fileExists(paths[0]) {
		t.Fatal("pipeline GLCB survived eviction after restart")
	}
	if got := countRecords(t, restarted.cm, ids[0]); got != 30 {
		t.Fatalf("read after eviction served %d records, want 30", got)
	}
}

// TestCloudIndexWalksDoNotRace: the cloud index's B+ tree is not safe for
// concurrent use — even a read walk writes its page cache — so List's cache
// rebuild and eviction's uploaded-chunk walk must share the index lock. Run
// with -race; it reports the unguarded walk.
func TestCloudIndexWalksDoNotRace(t *testing.T) {
	t.Parallel()
	h := newPipelineHome(t, blobstore.NewMemory(), glid.New(), "lru", 0, 0, time.Now)
	uploadPipelineChunks(t, h, 8, 10)

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 50 {
				h.cm.mu.Lock()
				h.cm.rebuildCloudListCache()
				h.cm.mu.Unlock()
			}
		})
		wg.Go(func() {
			for range 50 {
				_ = h.cm.uploadedChunkIDs()
			}
		})
	}
	wg.Wait()
}
