package file

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
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

func newCacheTestManager(t *testing.T) (*Manager, *countingStore, string) {
	t.Helper()
	vaultID := glid.New()
	inner := blobstore.NewMemory()
	store := &countingStore{Store: inner}

	dir := t.TempDir()
	cacheDir := t.TempDir()

	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        vaultID,
		CacheDir:       cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	return cm, store, cacheDir
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

func TestCacheWriteThroughAfterUpload(t *testing.T) {
	t.Parallel()
	cm, _, cacheDir := newCacheTestManager(t)

	chunkID := ingestAndUpload(t, cm, 200)

	// GLCB file should exist in cache dir.
	cachePath := filepath.Join(cacheDir, chunkID.String()+".glcb")
	info, err := os.Stat(cachePath)
	if err != nil {
		t.Fatalf("expected cache file at %s: %v", cachePath, err)
	}
	if info.Size() == 0 {
		t.Error("cache file is empty")
	}
	t.Logf("cache file: %s (%d bytes)", cachePath, info.Size())
}

func TestCacheHitAvoidsCloudDownload(t *testing.T) {
	t.Parallel()
	cm, store, _ := newCacheTestManager(t)

	chunkID := ingestAndUpload(t, cm, 200)

	// Reset counters after upload.
	store.downloads.Store(0)
	store.downloadRanges.Store(0)

	// Open cursor — should hit cache, no cloud calls.
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

	downloads := store.downloads.Load()
	ranges := store.downloadRanges.Load()
	if downloads > 0 || ranges > 0 {
		t.Errorf("cache hit should not trigger cloud calls: downloads=%d ranges=%d", downloads, ranges)
	}
}

// TestLocalGLCBServesAfterCacheDirEvicted verifies the warm cache that
// matters post step 7j: the chunk's own data.glcb in <chunkDir>. Wiping
// the legacy <CacheDir>/<id>.glcb does not cause a download because the
// in-tree data.glcb still serves the read locally.
func TestLocalGLCBServesAfterCacheDirEvicted(t *testing.T) {
	t.Parallel()
	cm, store, cacheDir := newCacheTestManager(t)

	chunkID := ingestAndUpload(t, cm, 200)

	// Wipe the CacheDir cache copy to prove the in-tree data.glcb is what
	// serves reads now.
	if err := os.Remove(filepath.Join(cacheDir, chunkID.String()+".glcb")); err != nil {
		t.Fatal(err)
	}

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
		t.Errorf("expected zero cloud calls (local data.glcb is the cache), got %d", total)
	}
}

func TestDeleteCleansCacheFile(t *testing.T) {
	t.Parallel()
	cm, _, cacheDir := newCacheTestManager(t)

	chunkID := ingestAndUpload(t, cm, 200)

	cachePath := filepath.Join(cacheDir, chunkID.String()+".glcb")
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatal("cache file should exist before delete")
	}

	if err := cm.Delete(chunkID); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Error("cache file should be removed after chunk delete")
	}
}

// TestRangeRequestFallsBackWhenLocalGLCBMissing covers the cold-cache
// case: a cloud-backed chunk with no local data.glcb (e.g. evicted under
// disk pressure, or a follower that adopted via FSM without ever sealing
// locally) must still serve reads via cloud range requests. Simulated
// here by deleting both local data.glcb and the legacy CacheDir copy.
func TestRangeRequestFallsBackWhenLocalGLCBMissing(t *testing.T) {
	t.Parallel()
	cm, store, cacheDir := newCacheTestManager(t)

	chunkID := ingestAndUpload(t, cm, 200)

	// Wipe both caches: <chunkDir>/data.glcb (the new warm cache) AND
	// <CacheDir>/<id>.glcb (legacy CacheDir copy). The chunk should still
	// be readable via cloud range requests against the authoritative blob.
	if err := os.Remove(filepath.Join(cm.chunkDir(chunkID), dataGLCBFileName)); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(filepath.Join(cacheDir, chunkID.String()+".glcb"))

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
	if total := store.downloads.Load() + store.downloadRanges.Load(); total == 0 {
		t.Error("expected cloud calls when both local data.glcb and CacheDir cache are missing")
	}
}

func TestNoCacheDirSkipsCaching(t *testing.T) {
	t.Parallel()
	// Manager without CacheDir — original behavior.
	vaultID := glid.New()
	store := blobstore.NewMemory()
	dir := t.TempDir()

	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        vaultID,
		// No CacheDir
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	chunkID := ingestAndUpload(t, cm, 50)

	// No cache files anywhere in the dir.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".glcb" {
			t.Errorf("unexpected .glcb file without CacheDir: %s", e.Name())
		}
	}

	// Cursor should still work (via range requests).
	cursor, err := cm.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	_ = cursor.Close()
}
