package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"
)

// countingStore wraps a blobstore.Store and counts Download/DownloadRange calls.
type countingStore struct {
	blobstore.Store
	downloads      atomic.Int64
	downloadRanges atomic.Int64
}

func (s *countingStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	s.downloads.Add(1)
	return s.Store.Download(ctx, key)
}

func (s *countingStore) DownloadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	s.downloadRanges.Add(1)
	return s.Store.DownloadRange(ctx, key, offset, length)
}

func newCacheTestManager(t *testing.T) (*Manager, *countingStore) {
	t.Helper()
	vaultID := glid.New()
	inner := blobstore.NewMemory()
	store := &countingStore{Store: inner}

	cm, err := NewManager(Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	return cm, store
}

func ingestAndUpload(t *testing.T, cm *Manager, n int) chunk.ChunkID {
	t.Helper()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range n {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "cache-test-%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}
	metas, _ := cm.List()
	for _, m := range metas {
		if m.Sealed {
			if err := cm.PostSealProcess(context.Background(), m.ID); err != nil {
				t.Fatalf("PostSealProcess: %v", err)
			}
			return m.ID
		}
	}
	t.Fatal("no sealed chunk")
	return chunk.ChunkID{}
}

// TestUploadKeepsLocalGLCB asserts the post-step-7k contract: after a chunk
// is sealed and uploaded to the cloud, its local data.glcb file remains in
// place inside <chunkDir> as the warm cache. Step 7j stopped removing it;
// step 7k removed the parallel CacheDir copy.
func TestUploadKeepsLocalGLCB(t *testing.T) {
	t.Parallel()
	cm, _ := newCacheTestManager(t)

	chunkID := ingestAndUpload(t, cm, 200)

	path := filepath.Join(cm.chunkDir(chunkID), dataGLCBFileName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("expected in-tree warm cache at %s: %v", path, err)
	}
	if info.Size() == 0 {
		t.Error("warm cache file is empty")
	}
}

// TestCacheHitAvoidsCloudDownload covers the steady-state read: the chunk
// has just been uploaded, the local data.glcb is the warm cache, and reads
// must not touch the cloud at all.
func TestCacheHitAvoidsCloudDownload(t *testing.T) {
	t.Parallel()
	cm, store := newCacheTestManager(t)

	chunkID := ingestAndUpload(t, cm, 200)

	store.downloads.Store(0)
	store.downloadRanges.Store(0)

	cursor, err := cm.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	var count int
	for {
		_, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			_ = cursor.Close()
			t.Fatalf("cursor.Next at %d: %v", count, err)
		}
		count++
	}
	_ = cursor.Close()

	if count != 200 {
		t.Errorf("read %d records, expected 200", count)
	}
	if total := store.downloads.Load() + store.downloadRanges.Load(); total != 0 {
		t.Errorf("warm-cache hit should not trigger cloud calls, got %d", total)
	}
}

// TestColdCacheDownloadsToChunkDir simulates an evicted / never-cached
// cloud chunk: deleting the in-tree data.glcb forces openCloudCursor to
// download the blob fresh, which post step 7k lands at <chunkDir>/data.glcb
// so the next read goes through the warm-cache fast path.
func TestColdCacheDownloadsToChunkDir(t *testing.T) {
	t.Parallel()
	cm, store := newCacheTestManager(t)

	chunkID := ingestAndUpload(t, cm, 200)
	glcbPath := filepath.Join(cm.chunkDir(chunkID), dataGLCBFileName)
	if err := os.Remove(glcbPath); err != nil {
		t.Fatal(err)
	}

	store.downloads.Store(0)

	cursor, err := cm.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	count := drainCursor(t, cursor)
	if count != 200 {
		t.Errorf("read %d records, expected 200", count)
	}
	if downloads := store.downloads.Load(); downloads != 1 {
		t.Errorf("expected 1 cloud download after cold cache, got %d", downloads)
	}
	if _, err := os.Stat(glcbPath); err != nil {
		t.Errorf("downloaded blob should land at %s: %v", glcbPath, err)
	}
}

// TestDeleteRemovesWarmCache covers the chunk-delete path: deleting a
// cloud-backed chunk should remove the in-tree warm cache copy too so
// retention/disk-pressure semantics keep working without the legacy
// CacheDir.
func TestDeleteRemovesWarmCache(t *testing.T) {
	t.Parallel()
	cm, _ := newCacheTestManager(t)

	chunkID := ingestAndUpload(t, cm, 200)
	chunkDir := cm.chunkDir(chunkID)
	if _, err := os.Stat(filepath.Join(chunkDir, dataGLCBFileName)); err != nil {
		t.Fatal("warm cache should exist before delete")
	}

	if err := cm.Delete(chunkID); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(chunkDir); !os.IsNotExist(err) {
		t.Errorf("chunk dir should be gone after delete: stat err=%v", err)
	}
}

// drainCursor reads until ErrNoMoreRecords and returns the count, failing
// the test on any other error.
func drainCursor(t *testing.T, cursor chunk.RecordCursor) int {
	t.Helper()
	defer func() { _ = cursor.Close() }()
	var count int
	for {
		_, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return count
		}
		if err != nil {
			t.Fatalf("cursor.Next at %d: %v", count, err)
		}
		count++
	}
}

// staticVerifier returns a fixed expected digest for any chunk. (zero, false)
// means "no expectation on file" → verification is skipped.
type staticVerifier struct {
	digest [32]byte
	have   bool
}

func (v *staticVerifier) ExpectedDigest(chunk.ChunkID) ([32]byte, bool) {
	return v.digest, v.have
}

// TestColdCacheVerifiesBlobDigest covers the integrity check landed in
// gastrolog-grnc3: a cold-cache cloud download is rejected when the GLCB
// whole-blob digest read from the TOC footer doesn't match what the FSM
// stamped at upload time. Without verification the warm cache would
// happily seed itself with corrupted bytes and re-serve them forever.
func TestColdCacheVerifiesBlobDigest(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	inner := blobstore.NewMemory()
	store := &countingStore{Store: inner}
	verifier := &staticVerifier{}

	cm, err := NewManager(Config{
		Dir:               t.TempDir(),
		Now:               time.Now,
		RotationPolicy:    chunk.NewRecordCountPolicy(10000),
		CloudStore:        store,
		VaultID:           vaultID,
		IntegrityVerifier: verifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })

	chunkID := ingestAndUpload(t, cm, 50)
	glcbPath := filepath.Join(cm.chunkDir(chunkID), dataGLCBFileName)

	// Pull the actual digest off the just-uploaded blob so we can drive the
	// verifier's "match" / "mismatch" cases against real data.
	actualDigest := readBlobDigest(t, glcbPath)

	// Cold-cache + matching digest: download should succeed and the chunk
	// should read normally.
	if err := os.Remove(glcbPath); err != nil {
		t.Fatal(err)
	}
	verifier.digest = actualDigest
	verifier.have = true
	cursor, err := cm.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("OpenCursor (matching digest): %v", err)
	}
	if got := drainCursor(t, cursor); got != 50 {
		t.Errorf("matching digest: read %d records, expected 50", got)
	}

	// Cold-cache + wrong expected digest: the download must be rejected and
	// the tmp file cleaned up. Phase 6 (gastrolog-69fd5) removed the
	// range-request fallback — a digest mismatch now surfaces an error
	// rather than silently switching to per-frame range reads.
	if err := os.Remove(glcbPath); err != nil {
		t.Fatal(err)
	}
	var bogus [32]byte
	for i := range bogus {
		bogus[i] = 0xAB
	}
	verifier.digest = bogus
	verifier.have = true

	if _, err = cm.OpenCursor(chunkID); err == nil {
		t.Fatal("OpenCursor (mismatched digest): expected error, got nil")
	}
	if _, err := os.Stat(glcbPath); !os.IsNotExist(err) {
		t.Errorf("rejected blob should not be promoted to data.glcb (stat err=%v)", err)
	}
}

// readBlobDigest pulls the GLCB whole-blob digest out of a local data.glcb
// file's TOC footer.
func readBlobDigest(t *testing.T, path string) [32]byte {
	t.Helper()
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		t.Fatalf("open data.glcb: %v", err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat data.glcb: %v", err)
	}
	toc, err := chunkcloud.ReadTOC(f, info.Size())
	if err != nil {
		t.Fatalf("read TOC: %v", err)
	}
	return toc.BlobDigest
}
