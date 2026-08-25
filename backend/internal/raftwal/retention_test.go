package raftwal

// Coverage for heap retention: the per-group log index
// stays in memory, but payloads live in the recent window only up to
// Config.LogCacheBudgetBytes — older entries are read back from WAL segment
// files. Heap is bounded by the budget, not by log length.

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// windowState snapshots a group's recent-window bookkeeping and verifies the
// accounting invariants: cacheBytes matches the cached payloads exactly,
// never exceeds the budget, and every cached index is also indexed.
type windowState struct {
	cacheBytes int64
	cached     int
	indexed    int
}

func inspectWindow(t *testing.T, w *WAL, group string) windowState {
	t.Helper()
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	gid, ok := w.groupIDs[group]
	if !ok {
		t.Fatalf("group %q not registered", group)
	}
	gs := w.groups[gid]
	if gs == nil {
		t.Fatalf("group %q has no state", group)
	}
	var sum int64
	for idx, enc := range gs.cache {
		sum += int64(len(enc))
		if _, indexed := gs.logs[idx]; !indexed {
			t.Fatalf("group %q: cached index %d missing from log index", group, idx)
		}
	}
	if sum != gs.cacheBytes {
		t.Fatalf("group %q: cacheBytes=%d but cached payloads sum to %d", group, gs.cacheBytes, sum)
	}
	if gs.cacheBytes > w.cfg.LogCacheBudgetBytes {
		t.Fatalf("group %q: cacheBytes=%d exceeds budget %d", group, gs.cacheBytes, w.cfg.LogCacheBudgetBytes)
	}
	return windowState{cacheBytes: gs.cacheBytes, cached: len(gs.cache), indexed: len(gs.logs)}
}

func TestRetentionEvictedEntriesServedFromSegments(t *testing.T) {
	t.Parallel()
	w, err := Open(t.TempDir(), Config{LogCacheBudgetBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("ret-basic")
	// 100 entries x ~125B encoded ≈ 12.5KB — several times the 2KB budget.
	for i := uint64(1); i <= 100; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte(fmt.Sprintf("entry-%03d-%s", i, string(make([]byte, 80))))}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}

	ws := inspectWindow(t, w, "ret-basic")
	if ws.indexed != 100 {
		t.Fatalf("indexed=%d want 100 (index must stay complete)", ws.indexed)
	}
	if ws.cached >= 100 {
		t.Fatalf("cached=%d, expected eviction below 100 under a 2KB budget", ws.cached)
	}

	// Every entry — evicted or cached — reads back correctly.
	var log hraft.Log
	for i := uint64(1); i <= 100; i++ {
		if err := gs.GetLog(i, &log); err != nil {
			t.Fatalf("GetLog(%d): %v", i, err)
		}
		if log.Index != i || log.Term != 1 {
			t.Fatalf("GetLog(%d): got index=%d term=%d", i, log.Index, log.Term)
		}
		want := fmt.Sprintf("entry-%03d-", i)
		if string(log.Data[:len(want)]) != want {
			t.Fatalf("GetLog(%d): data prefix %q want %q", i, log.Data[:len(want)], want)
		}
	}
}

// Sub-entries of a batched StoreLogs record are individually addressable on
// disk: evicted batch entries pread at their offset INSIDE the batch record.
func TestRetentionBatchSubEntriesServedFromSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := Config{LogCacheBudgetBytes: 1024}
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	gs := w.GroupStore("ret-batch")
	if err := gs.StoreLogs(makeBatch(1, 60, 5, 100)); err != nil {
		t.Fatal(err)
	}

	verify := func(gs *GroupStore, phase string) {
		t.Helper()
		var log hraft.Log
		for i := uint64(1); i <= 60; i++ {
			if err := gs.GetLog(i, &log); err != nil {
				t.Fatalf("%s: GetLog(%d): %v", phase, i, err)
			}
			want := makeBatch(i, i, 5, 100)[0]
			if log.Index != want.Index || log.Term != want.Term || string(log.Data) != string(want.Data) {
				t.Fatalf("%s: GetLog(%d) mismatch", phase, i)
			}
		}
	}

	ws := inspectWindow(t, w, "ret-batch")
	if ws.cached >= 60 {
		t.Fatalf("cached=%d, expected eviction under a 1KB budget", ws.cached)
	}
	verify(gs, "live")

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	verify(w2.GroupStore("ret-batch"), "replayed")
	inspectWindow(t, w2, "ret-batch")
}

// An entry larger than the whole budget is never admitted to the window (it
// would evict everything for one payload) but must still read back from disk.
func TestRetentionOversizedEntryServedFromDisk(t *testing.T) {
	t.Parallel()
	w, err := Open(t.TempDir(), Config{LogCacheBudgetBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("ret-big")
	small := []byte("small")
	big := make([]byte, 4096)
	for i := range big {
		big[i] = byte(i)
	}
	if err := gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: small}); err != nil {
		t.Fatal(err)
	}
	if err := gs.StoreLog(&hraft.Log{Index: 2, Term: 1, Data: big}); err != nil {
		t.Fatal(err)
	}
	if err := gs.StoreLog(&hraft.Log{Index: 3, Term: 1, Data: small}); err != nil {
		t.Fatal(err)
	}

	ws := inspectWindow(t, w, "ret-big")
	if ws.indexed != 3 {
		t.Fatalf("indexed=%d want 3", ws.indexed)
	}

	var log hraft.Log
	if err := gs.GetLog(2, &log); err != nil {
		t.Fatalf("GetLog(2): %v", err)
	}
	if len(log.Data) != len(big) || string(log.Data) != string(big) {
		t.Fatalf("oversized entry corrupted on disk read (len=%d want %d)", len(log.Data), len(big))
	}
	for _, i := range []uint64{1, 3} {
		if err := gs.GetLog(i, &log); err != nil {
			t.Fatalf("GetLog(%d): %v", i, err)
		}
		if string(log.Data) != "small" {
			t.Fatalf("GetLog(%d): data=%q", i, log.Data)
		}
	}
}

// DeleteRange spanning the recent-window boundary must remove both cached and
// disk-only entries and keep the window accounting exact.
func TestRetentionDeleteRangeSpansWindowBoundary(t *testing.T) {
	t.Parallel()
	w, err := Open(t.TempDir(), Config{LogCacheBudgetBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("ret-del")
	// 40 x ~125B ≈ 5KB: indices near 1 are evicted to disk, indices near 40
	// are cached — DeleteRange(10, 35) spans the boundary.
	for i := uint64(1); i <= 40; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 100)}); err != nil {
			t.Fatal(err)
		}
	}
	before := inspectWindow(t, w, "ret-del")
	if before.cached == 0 || before.cached >= 40 {
		t.Fatalf("cached=%d, want partial window for a boundary-spanning delete", before.cached)
	}

	if err := gs.DeleteRange(10, 35); err != nil {
		t.Fatal(err)
	}

	after := inspectWindow(t, w, "ret-del")
	if after.indexed != 40-26 {
		t.Fatalf("indexed=%d after delete, want %d", after.indexed, 40-26)
	}

	var log hraft.Log
	for i := uint64(1); i <= 9; i++ {
		if err := gs.GetLog(i, &log); err != nil {
			t.Fatalf("GetLog(%d) surviving prefix: %v", i, err)
		}
	}
	for i := uint64(10); i <= 35; i++ {
		if err := gs.GetLog(i, &log); err != hraft.ErrLogNotFound {
			t.Fatalf("GetLog(%d) deleted: want ErrLogNotFound, got %v", i, err)
		}
	}
	for i := uint64(36); i <= 40; i++ {
		if err := gs.GetLog(i, &log); err != nil {
			t.Fatalf("GetLog(%d) surviving suffix: %v", i, err)
		}
	}
}

func TestRetentionRestartSurvival(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := Config{LogCacheBudgetBytes: 1024}

	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("ret-restart")
	for i := uint64(1); i <= 80; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 2, Data: []byte(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := gs.DeleteRange(1, 20); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	gs2 := w2.GroupStore("ret-restart")
	first, _ := gs2.FirstIndex()
	last, _ := gs2.LastIndex()
	if first != 21 || last != 80 {
		t.Fatalf("first=%d last=%d, want 21..80", first, last)
	}
	ws := inspectWindow(t, w2, "ret-restart")
	if ws.indexed != 60 {
		t.Fatalf("indexed=%d want 60", ws.indexed)
	}
	var log hraft.Log
	for i := uint64(21); i <= 80; i++ {
		if err := gs2.GetLog(i, &log); err != nil {
			t.Fatalf("GetLog(%d): %v", i, err)
		}
		if string(log.Data) != fmt.Sprintf("v%d", i) {
			t.Fatalf("GetLog(%d): data=%q", i, log.Data)
		}
	}
}

// Reclamation removes segment files under live readers; evicted entries must
// remain readable through the removal and after a subsequent restart.
func TestRetentionServesEvictedEntriesAcrossReclamation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var reclaims atomic.Int64
	cfg := Config{
		SegmentTargetSize:    1024,
		ScavengeMaxLiveBytes: 512,
		LogCacheBudgetBytes:  512,
		OnReclaim:            func(ReclaimStats) { reclaims.Add(1) },
	}
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	gs := w.GroupStore("ret-reclaim")
	for i := uint64(1); i <= 40; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 100)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := gs.DeleteRange(1, 10); err != nil {
		t.Fatal(err)
	}
	syncBarrier(t, w)
	if reclaims.Load() == 0 {
		t.Fatal("expected reclamation to run")
	}
	assertLiveBytesInvariant(t, w, "after reclamation")

	verify := func(gs *GroupStore, phase string) {
		t.Helper()
		var log hraft.Log
		for i := uint64(11); i <= 40; i++ {
			if err := gs.GetLog(i, &log); err != nil {
				t.Fatalf("%s: GetLog(%d): %v", phase, i, err)
			}
			if log.Index != i || len(log.Data) != 100 {
				t.Fatalf("%s: GetLog(%d): index=%d len=%d", phase, i, log.Index, len(log.Data))
			}
		}
		for i := uint64(1); i <= 10; i++ {
			if err := gs.GetLog(i, &log); err != hraft.ErrLogNotFound {
				t.Fatalf("%s: GetLog(%d): want ErrLogNotFound, got %v", phase, i, err)
			}
		}
	}
	verify(gs, "post-reclamation")
	inspectWindow(t, w, "ret-reclaim")

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	verify(w2.GroupStore("ret-reclaim"), "replayed")
}

// Two groups with independent windows: churn on one must not disturb the
// other's entries or accounting.
func TestRetentionMultiGroupIsolation(t *testing.T) {
	t.Parallel()
	w, err := Open(t.TempDir(), Config{LogCacheBudgetBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	ga := w.GroupStore("ret-iso-a")
	gb := w.GroupStore("ret-iso-b")
	for i := uint64(1); i <= 50; i++ {
		if err := ga.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte(fmt.Sprintf("a%d-%s", i, string(make([]byte, 60))))}); err != nil {
			t.Fatal(err)
		}
		if err := gb.StoreLog(&hraft.Log{Index: i, Term: 2, Data: []byte(fmt.Sprintf("b%d-%s", i, string(make([]byte, 60))))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ga.DeleteRange(1, 40); err != nil {
		t.Fatal(err)
	}

	wsA := inspectWindow(t, w, "ret-iso-a")
	wsB := inspectWindow(t, w, "ret-iso-b")
	if wsA.indexed != 10 {
		t.Fatalf("group a indexed=%d want 10", wsA.indexed)
	}
	if wsB.indexed != 50 {
		t.Fatalf("group b indexed=%d want 50 (churn on a leaked into b)", wsB.indexed)
	}

	var log hraft.Log
	for i := uint64(1); i <= 50; i++ {
		if err := gb.GetLog(i, &log); err != nil {
			t.Fatalf("group b GetLog(%d): %v", i, err)
		}
		want := fmt.Sprintf("b%d-", i)
		if string(log.Data[:len(want)]) != want {
			t.Fatalf("group b GetLog(%d): prefix %q want %q", i, log.Data[:len(want)], want)
		}
	}
	for i := uint64(41); i <= 50; i++ {
		if err := ga.GetLog(i, &log); err != nil {
			t.Fatalf("group a GetLog(%d): %v", i, err)
		}
	}
}

// Concurrent append/read/delete churn with a tiny window and small segments
// forces constant eviction, disk reads, and reclamation under -race.
func TestRetentionConcurrentChurnTinyWindow(t *testing.T) {
	t.Parallel()
	cfg := Config{
		SegmentTargetSize:    2048,
		SyncBatchWindow:      2 * time.Millisecond,
		ScavengeMaxLiveBytes: 512,
		LogCacheBudgetBytes:  512,
	}
	w, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const groups = 4
	var stores [groups]*GroupStore
	for i := range groups {
		stores[i] = w.GroupStore(fmt.Sprintf("ret-churn-%d", i))
	}

	var nextIdx [groups]uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var errCount atomic.Int64

	for g := range groups {
		// Writer: alternates single appends and batches.
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := make([]byte, 80)
			for {
				select {
				case <-stop:
					return
				default:
				}
				lo := atomic.AddUint64(&nextIdx[g], 3) - 2
				if err := stores[g].StoreLogs([]*hraft.Log{
					{Index: lo, Term: 1, Data: payload},
					{Index: lo + 1, Term: 1, Data: payload},
					{Index: lo + 2, Term: 1, Data: payload},
				}); err != nil {
					errCount.Add(1)
					return
				}
			}
		}()

		// Reader: probes head, middle, and tail (head is usually evicted to
		// disk under the tiny window).
		wg.Add(1)
		go func() {
			defer wg.Done()
			var log hraft.Log
			for {
				select {
				case <-stop:
					return
				default:
				}
				hi := atomic.LoadUint64(&nextIdx[g])
				if hi == 0 {
					continue
				}
				for _, idx := range []uint64{1, hi / 2, hi} {
					if idx == 0 {
						continue
					}
					err := stores[g].GetLog(idx, &log)
					if err != nil && err != hraft.ErrLogNotFound {
						errCount.Add(1)
						return
					}
				}
			}
		}()

		// Deleter: prefix deletes trigger reclamation (which removes
		// segment files under readers).
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				hi := atomic.LoadUint64(&nextIdx[g])
				if hi < 12 {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				if err := stores[g].DeleteRange(1, hi/3); err != nil {
					errCount.Add(1)
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	if n := errCount.Load(); n != 0 {
		t.Fatalf("concurrent churn had %d unexpected errors", n)
	}
	for g := range groups {
		inspectWindow(t, w, fmt.Sprintf("ret-churn-%d", g))
	}
}
