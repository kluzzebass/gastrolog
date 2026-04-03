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

	"github.com/google/uuid"
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
	vaultID := uuid.Must(uuid.NewV7())
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

func TestCacheMissDownloadsAndCaches(t *testing.T) {
	t.Parallel()
	cm, store, cacheDir := newCacheTestManager(t)

	chunkID := ingestAndUpload(t, cm, 200)

	// Remove cache file to force a miss.
	cachePath := filepath.Join(cacheDir, chunkID.String()+".glcb")
	if err := os.Remove(cachePath); err != nil {
		t.Fatal(err)
	}

	store.downloads.Store(0)
	store.downloadRanges.Store(0)

	// Open cursor — should download from cloud and re-cache.
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

	// Should have called Download exactly once.
	if downloads := store.downloads.Load(); downloads != 1 {
		t.Errorf("expected 1 Download call (cache miss), got %d", downloads)
	}

	// Cache file should be recreated.
	if _, err := os.Stat(cachePath); err != nil {
		t.Errorf("cache file should be recreated after miss: %v", err)
	}
}

func TestCorruptCacheFallsBackToDownload(t *testing.T) {
	t.Parallel()
	cm, store, cacheDir := newCacheTestManager(t)

	chunkID := ingestAndUpload(t, cm, 200)

	// Corrupt the cache file.
	cachePath := filepath.Join(cacheDir, chunkID.String()+".glcb")
	if err := os.WriteFile(cachePath, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	store.downloads.Store(0)

	// Open cursor — corrupt cache should be deleted, blob re-downloaded.
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
	if downloads := store.downloads.Load(); downloads != 1 {
		t.Errorf("expected 1 Download after corrupt cache, got %d", downloads)
	}

	// Cache should be replaced with valid file.
	info, _ := os.Stat(cachePath)
	if info == nil || info.Size() < 100 {
		t.Error("cache file should be replaced with valid GLCB")
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

func TestNoCacheDirSkipsCaching(t *testing.T) {
	t.Parallel()
	// Manager without CacheDir — original behavior.
	vaultID := uuid.Must(uuid.NewV7())
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
