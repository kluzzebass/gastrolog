package file

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// newEvictionTestManager builds a Manager with the given policy + budget +
// TTL configured, plus a controllable now() so tests can advance time
// without real-clock waits. Empty policy defaults to "lru" inside
// EvictCache, matching production semantics.
func newEvictionTestManager(t *testing.T, policy string, budget uint64, ttl time.Duration, nowFn func() time.Time) *Manager {
	t.Helper()
	vaultID := glid.New()
	store := blobstore.NewMemory()
	cm, err := NewManager(Config{
		Dir:              t.TempDir(),
		Now:              nowFn,
		RotationPolicy:   chunk.NewRecordCountPolicy(10000),
		CloudStore:       store,
		VaultID:          vaultID,
		CacheEviction:    policy,
		CacheBudgetBytes: budget,
		CacheTTL:         ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	return cm
}

// uploadN seals + uploads `count` chunks (each with `recordsPerChunk`
// records), returning their IDs in upload order. Each chunk's seal advances
// the manager clock by 1ms so chunkID timestamps are distinct.
func uploadN(t *testing.T, cm *Manager, count, recordsPerChunk int) []chunk.ChunkID {
	t.Helper()
	var ids []chunk.ChunkID
	for c := range count {
		t0 := time.Date(2025, 6, 15, 10, 0, c, 0, time.UTC)
		for i := range recordsPerChunk {
			ts := t0.Add(time.Duration(i) * time.Microsecond)
			if _, _, err := cm.Append(chunk.Record{
				IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "evict-%d-%d", c, i),
			}); err != nil {
				t.Fatal(err)
			}
		}
		_ = cm.Seal()
	}
	metas, _ := cm.List()
	for _, m := range metas {
		if !m.Sealed || m.CloudBacked {
			continue
		}
		if err := cm.PostSealProcess(context.Background(), m.ID); err != nil {
			t.Fatalf("PostSealProcess: %v", err)
		}
		ids = append(ids, m.ID)
	}
	return ids
}

// TestEvictCacheLRU verifies size-based eviction: with a budget below the
// total cached bytes, the coldest entries (oldest lastAccess) must be
// evicted first until the cache fits.
func TestEvictCacheLRU(t *testing.T) {
	t.Parallel()
	now := time.Now()
	clock := &mutableClock{t: now}
	cm := newEvictionTestManager(t, "lru", 1, 0, clock.now) // tiny budget forces every entry to evict

	ids := uploadN(t, cm, 3, 100)
	if len(ids) != 3 {
		t.Fatalf("expected 3 cloud-backed chunks, got %d", len(ids))
	}

	// Touch ids[1] and ids[2] in order so they're hotter than ids[0].
	clock.t = clock.t.Add(time.Second)
	openAndDrain(t, cm, ids[1])
	clock.t = clock.t.Add(time.Second)
	openAndDrain(t, cm, ids[2])

	// Run eviction with a 1-byte budget — everything should go.
	evicted, freed := cm.EvictCacheLRU(1)
	if evicted != 3 {
		t.Errorf("evicted = %d, want 3", evicted)
	}
	if freed == 0 {
		t.Errorf("freed = 0, want > 0")
	}
	for _, id := range ids {
		path := filepath.Join(cm.chunkDir(id), dataGLCBFileName)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("warm cache for %s should be evicted: stat err=%v", id, err)
		}
	}
}

// TestEvictCacheLRU_Order verifies that LRU eviction respects access
// recency: with a budget that fits exactly two of three chunks, the
// coldest one should be the one that gets dropped.
func TestEvictCacheLRU_Order(t *testing.T) {
	t.Parallel()
	now := time.Now()
	clock := &mutableClock{t: now}
	cm := newEvictionTestManager(t, "lru", 0, 0, clock.now) // configure budget per-call below

	ids := uploadN(t, cm, 3, 100)
	if len(ids) != 3 {
		t.Fatalf("expected 3 cloud-backed chunks, got %d", len(ids))
	}

	// Touch ids[1] and ids[2] but NOT ids[0] — leaving ids[0] coldest.
	clock.t = clock.t.Add(10 * time.Second)
	openAndDrain(t, cm, ids[1])
	clock.t = clock.t.Add(10 * time.Second)
	openAndDrain(t, cm, ids[2])

	// Compute total cache size + pick a budget that fits 2 of 3 entries.
	var sizes []int64
	for _, id := range ids {
		info, err := os.Stat(filepath.Join(cm.chunkDir(id), dataGLCBFileName))
		if err != nil {
			t.Fatalf("stat %s: %v", id, err)
		}
		sizes = append(sizes, info.Size())
	}
	// Budget = sum of the two HOTTEST sizes (ids[1] + ids[2]); the coldest
	// (ids[0]) must be evicted to fit.
	budget := uint64(sizes[1] + sizes[2]) //nolint:gosec // G115: test-only

	evicted, _ := cm.EvictCacheLRU(budget)
	if evicted != 1 {
		t.Errorf("evicted = %d, want 1", evicted)
	}

	// ids[0] gone, ids[1] and ids[2] still present.
	if _, err := os.Stat(filepath.Join(cm.chunkDir(ids[0]), dataGLCBFileName)); !os.IsNotExist(err) {
		t.Errorf("coldest chunk %s should be evicted", ids[0])
	}
	for _, id := range ids[1:] {
		if _, err := os.Stat(filepath.Join(cm.chunkDir(id), dataGLCBFileName)); err != nil {
			t.Errorf("hot chunk %s should still be cached: %v", id, err)
		}
	}
}

// TestEvictCacheTTL verifies age-based eviction: entries whose lastAccess
// is older than the configured TTL get dropped; fresher ones stay.
func TestEvictCacheTTL(t *testing.T) {
	t.Parallel()
	now := time.Now()
	clock := &mutableClock{t: now}
	cm := newEvictionTestManager(t, "ttl", 0, time.Hour, clock.now)

	ids := uploadN(t, cm, 2, 100)
	if len(ids) != 2 {
		t.Fatalf("expected 2 cloud-backed chunks, got %d", len(ids))
	}

	// Both chunks are cached. Advance the clock past the TTL window for
	// ids[0] but keep ids[1] fresh by re-touching it after the jump.
	clock.t = clock.t.Add(2 * time.Hour)
	openAndDrain(t, cm, ids[1])

	evicted, _ := cm.EvictCacheTTL(time.Hour)
	if evicted != 1 {
		t.Errorf("TTL evicted = %d, want 1", evicted)
	}
	if _, err := os.Stat(filepath.Join(cm.chunkDir(ids[0]), dataGLCBFileName)); !os.IsNotExist(err) {
		t.Errorf("stale chunk %s should be evicted", ids[0])
	}
	if _, err := os.Stat(filepath.Join(cm.chunkDir(ids[1]), dataGLCBFileName)); err != nil {
		t.Errorf("fresh chunk %s should still be cached: %v", ids[1], err)
	}
}

// TestEvictCache_LRUIgnoresTTL pins down the mutual-exclusion contract:
// in LRU mode, CacheTTL is ignored — even if every entry is past TTL, no
// eviction happens unless the budget would force it. Pairs with
// TestEvictCache_TTLIgnoresBudget below.
func TestEvictCache_LRUIgnoresTTL(t *testing.T) {
	t.Parallel()
	now := time.Now()
	clock := &mutableClock{t: now}
	// Policy=lru, no budget, very short TTL. Real world: operator picked
	// LRU; the TTL field is left over from a previous tier config or a
	// confused operator. EvictCache must NOT silently apply TTL.
	cm := newEvictionTestManager(t, "lru", 0, time.Millisecond, clock.now)
	ids := uploadN(t, cm, 2, 100)

	// Advance clock well past TTL; in TTL mode this would evict everything.
	clock.t = clock.t.Add(time.Hour)

	evicted, _ := cm.EvictCache()
	if evicted != 0 {
		t.Errorf("LRU mode with no budget must ignore TTL, evicted=%d", evicted)
	}
	for _, id := range ids {
		if _, err := os.Stat(filepath.Join(cm.chunkDir(id), dataGLCBFileName)); err != nil {
			t.Errorf("chunk %s should still be cached: %v", id, err)
		}
	}
}

// TestEvictCache_TTLIgnoresBudget covers the inverse: in TTL mode, the
// budget is irrelevant. A TTL-mode tier with a tiny budget but every
// entry fresh must NOT evict anything.
func TestEvictCache_TTLIgnoresBudget(t *testing.T) {
	t.Parallel()
	now := time.Now()
	clock := &mutableClock{t: now}
	// Policy=ttl, tiny budget that would force LRU to evict everything,
	// long TTL so nothing has aged out.
	cm := newEvictionTestManager(t, "ttl", 1, time.Hour, clock.now)
	ids := uploadN(t, cm, 2, 100)

	evicted, _ := cm.EvictCache()
	if evicted != 0 {
		t.Errorf("TTL mode must ignore budget, evicted=%d (would be 2 if budget applied)", evicted)
	}
	for _, id := range ids {
		if _, err := os.Stat(filepath.Join(cm.chunkDir(id), dataGLCBFileName)); err != nil {
			t.Errorf("chunk %s should still be cached: %v", id, err)
		}
	}
}

// TestEvictCacheLRU_NoBudget is a no-op when CacheBudgetBytes is zero.
func TestEvictCacheLRU_NoBudget(t *testing.T) {
	t.Parallel()
	now := time.Now()
	clock := &mutableClock{t: now}
	cm := newEvictionTestManager(t, "lru", 0, 0, clock.now)

	ids := uploadN(t, cm, 2, 100)

	evicted, _ := cm.EvictCache()
	if evicted != 0 {
		t.Errorf("EvictCache with no policy should be a no-op, got %d", evicted)
	}
	for _, id := range ids {
		if _, err := os.Stat(filepath.Join(cm.chunkDir(id), dataGLCBFileName)); err != nil {
			t.Errorf("chunk %s should still be cached when no policy is configured: %v", id, err)
		}
	}
}

// mutableClock is a manual time source for eviction tests. Tests advance
// it by mutating .t directly between operations.
type mutableClock struct {
	t time.Time
}

func (c *mutableClock) now() time.Time { return c.t }

// openAndDrain opens a cursor on the chunk and reads it to completion,
// touching lastAccess as a side-effect.
func openAndDrain(t *testing.T, cm *Manager, id chunk.ChunkID) {
	t.Helper()
	cursor, err := cm.OpenCursor(id)
	if err != nil {
		t.Fatalf("OpenCursor(%s): %v", id, err)
	}
	for {
		_, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			_ = cursor.Close()
			t.Fatalf("cursor.Next: %v", err)
		}
	}
	_ = cursor.Close()
}
