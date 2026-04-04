package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func TestEvictCacheLRU(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "lru", CacheBudget: "300"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	// Create 3 cached blobs: 100, 200, 150 bytes. Total = 450 > budget 300.
	writeFile(t, filepath.Join(cacheDir, "a.glcb"), 100, time.Now().Add(-3*time.Minute))
	writeFile(t, filepath.Join(cacheDir, "b.glcb"), 200, time.Now().Add(-2*time.Minute))
	writeFile(t, filepath.Join(cacheDir, "c.glcb"), 150, time.Now().Add(-1*time.Minute))

	cm.EvictCache()

	// Oldest (a=100) evicted first, total = 350 > 300. Next oldest (b=200) evicted, total = 150 ≤ 300.
	if fileExists(filepath.Join(cacheDir, "a.glcb")) {
		t.Error("expected a.glcb to be evicted (oldest)")
	}
	if fileExists(filepath.Join(cacheDir, "b.glcb")) {
		t.Error("expected b.glcb to be evicted (second oldest)")
	}
	if !fileExists(filepath.Join(cacheDir, "c.glcb")) {
		t.Error("expected c.glcb to survive (newest, within budget)")
	}
}

func TestEvictCacheLRUUnderBudget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "lru", CacheBudget: "1000"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "a.glcb"), 100, time.Now())
	writeFile(t, filepath.Join(cacheDir, "b.glcb"), 200, time.Now())

	cm.EvictCache()

	if !fileExists(filepath.Join(cacheDir, "a.glcb")) || !fileExists(filepath.Join(cacheDir, "b.glcb")) {
		t.Error("under-budget files should not be evicted")
	}
}

func TestEvictCacheTTL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "ttl", CacheTTL: "1m", CacheBudget: "10000"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "old.glcb"), 100, time.Now().Add(-5*time.Minute))
	writeFile(t, filepath.Join(cacheDir, "fresh.glcb"), 100, time.Now())

	cm.EvictCache()

	if fileExists(filepath.Join(cacheDir, "old.glcb")) {
		t.Error("expected old.glcb to be evicted (older than TTL)")
	}
	if !fileExists(filepath.Join(cacheDir, "fresh.glcb")) {
		t.Error("expected fresh.glcb to survive (within TTL)")
	}
}

func TestEvictCacheTTLWithBudgetCap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	// TTL keeps everything, but budget forces eviction.
	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "ttl", CacheTTL: "1h", CacheBudget: "100"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "a.glcb"), 80, time.Now().Add(-30*time.Minute))
	writeFile(t, filepath.Join(cacheDir, "b.glcb"), 80, time.Now())

	cm.EvictCache()

	// Both within TTL, but total 160 > budget 100. Oldest evicted.
	if fileExists(filepath.Join(cacheDir, "a.glcb")) {
		t.Error("expected a.glcb evicted by budget cap")
	}
	if !fileExists(filepath.Join(cacheDir, "b.glcb")) {
		t.Error("expected b.glcb to survive")
	}
}

func TestEvictCacheIgnoresNonGLCB(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "lru", CacheBudget: "1"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "keep.txt"), 1000, time.Now().Add(-time.Hour))
	writeFile(t, filepath.Join(cacheDir, "evict.glcb"), 100, time.Now())

	cm.EvictCache()

	if !fileExists(filepath.Join(cacheDir, "keep.txt")) {
		t.Error("non-.glcb files should never be evicted")
	}
	// The .glcb file (100 bytes) exceeds budget (1 byte) and should be evicted.
	if fileExists(filepath.Join(cacheDir, "evict.glcb")) {
		t.Error("expected evict.glcb to be evicted (exceeds budget)")
	}
}

func TestEvictCacheEmptyDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "lru", CacheBudget: "100"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	// Should not panic on empty directory.
	cm.EvictCache()
}

func TestEvictCacheDefaultBudget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	// CacheBudget = 0 → uses defaultCacheBudget (1 GiB). Small file should survive.
	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "tiny.glcb"), 10, time.Now())

	cm.EvictCache()

	if !fileExists(filepath.Join(cacheDir, "tiny.glcb")) {
		t.Error("tiny file should survive under default 1 GiB budget")
	}
}

// --- unhappy paths ---

func TestEvictCacheReadOnlyDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "lru", CacheBudget: "1"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "locked.glcb"), 100, time.Now())

	// Make cache dir read-only — eviction should not panic, file should survive.
	_ = os.Chmod(cacheDir, 0o555)
	t.Cleanup(func() { _ = os.Chmod(cacheDir, 0o750) })

	cm.EvictCache() // should not panic

	// Restore permissions to check the file survived.
	_ = os.Chmod(cacheDir, 0o750)
	if !fileExists(filepath.Join(cacheDir, "locked.glcb")) {
		t.Error("file should survive when removal fails (read-only dir)")
	}
}

func TestEvictCacheMissingDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	cm, err := NewManager(Config{Dir: dir, CacheDir: filepath.Join(dir, "nonexistent-cache")})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	cm.EvictCache() // should not panic on missing dir
}

func TestEvictCacheNoCacheDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	cm, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	cm.EvictCache() // CacheDir="" → no-op
}

func TestEvictCacheTTLInvalidDuration(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "ttl", CacheTTL: "not-a-duration", CacheBudget: "10000"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "a.glcb"), 100, time.Now().Add(-time.Hour))

	// Invalid TTL → TTL eviction skipped, budget still applies. 100 < 10000 → no eviction.
	cm.EvictCache()

	if !fileExists(filepath.Join(cacheDir, "a.glcb")) {
		t.Error("invalid TTL should not evict files (TTL phase skipped, budget not exceeded)")
	}
}

func TestEvictCacheTTLZeroDuration(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "ttl", CacheTTL: "0s", CacheBudget: "10000"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "a.glcb"), 100, time.Now().Add(-time.Second))

	// TTL=0 → everything is expired.
	cm.EvictCache()

	if fileExists(filepath.Join(cacheDir, "a.glcb")) {
		t.Error("TTL=0 should evict all files")
	}
}

// --- edge cases ---

func TestEvictCacheExactBudgetBoundary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "lru", CacheBudget: "200"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "a.glcb"), 100, time.Now().Add(-time.Minute))
	writeFile(t, filepath.Join(cacheDir, "b.glcb"), 100, time.Now())

	// Total = 200 = budget. Should NOT evict.
	cm.EvictCache()

	if !fileExists(filepath.Join(cacheDir, "a.glcb")) || !fileExists(filepath.Join(cacheDir, "b.glcb")) {
		t.Error("exact budget boundary should not trigger eviction")
	}
}

func TestEvictCacheAllExpired(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "ttl", CacheTTL: "1m", CacheBudget: "10000"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "a.glcb"), 50, time.Now().Add(-10*time.Minute))
	writeFile(t, filepath.Join(cacheDir, "b.glcb"), 50, time.Now().Add(-5*time.Minute))

	cm.EvictCache()

	entries, _ := os.ReadDir(cacheDir)
	var glcbCount int
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".glcb" {
			glcbCount++
		}
	}
	if glcbCount != 0 {
		t.Errorf("expected all files evicted, got %d", glcbCount)
	}
}

func TestEvictCacheConcurrentReads(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "lru", CacheBudget: "1"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "target.glcb"), 100, time.Now())

	// Open the file for reading (simulates concurrent cursor access).
	f, err := os.Open(filepath.Join(cacheDir, "target.glcb"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Eviction should not panic even if the file is held open.
	cm.EvictCache()
}

func TestEvictCacheConcurrentWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "lru", CacheBudget: "1"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	// Pre-populate cache with files that exceed the budget.
	writeFile(t, filepath.Join(cacheDir, "old.glcb"), 100, time.Now().Add(-time.Minute))

	// Simulate writeBlobToCache writing a temp file concurrently.
	tmpPath := filepath.Join(cacheDir, ".glcb-concurrent.tmp")
	writeFile(t, tmpPath, 500, time.Now())

	// Eviction should not touch .tmp files (only .glcb extension matched).
	cm.EvictCache()

	if !fileExists(tmpPath) {
		t.Error("eviction should not touch .tmp files from concurrent writeBlobToCache")
	}
}

func TestEvictCacheUnknownPolicy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	_ = os.MkdirAll(cacheDir, 0o750)

	cm, err := NewManager(Config{Dir: dir, CacheDir: cacheDir, CacheEviction: "fifo", CacheBudget: "1"})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	writeFile(t, filepath.Join(cacheDir, "a.glcb"), 100, time.Now())

	// Unknown policy falls through to LRU default.
	cm.EvictCache()

	if fileExists(filepath.Join(cacheDir, "a.glcb")) {
		t.Error("unknown policy should fall through to LRU and evict over-budget files")
	}
}

// --- helpers ---

func writeFile(t *testing.T, path string, size int, modTime time.Time) {
	t.Helper()
	data := make([]byte, size)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatal(err)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// Verify interface compliance.
var _ chunk.ChunkCacheEvictor = (*Manager)(nil)
