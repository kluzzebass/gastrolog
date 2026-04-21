package raftwal

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// harnessWalConfig is a small, fast WAL layout used by verification tests so
// segment rotation and compaction exercise real code paths without huge I/O.
func harnessWalConfig() Config {
	return Config{
		SegmentTargetSize:     2048,
		SyncBatchWindow:       2 * time.Millisecond,
		CompactionMinSegments: 2,
	}
}

var errHarnessSyncFail = errors.New("harness: injected sync failure")

func newestSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var bestPath string
	bestSeq := -1
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, walFilePrefix) || !strings.HasSuffix(name, walFileSuffix) {
			continue
		}
		seq, ok := parseSegmentSeq(name)
		if !ok {
			continue
		}
		if seq > bestSeq {
			bestSeq = seq
			bestPath = filepath.Join(dir, name)
		}
	}
	if bestPath == "" {
		t.Fatal("no wal segment files in dir")
	}
	return bestPath
}

func truncateFileTail(t *testing.T, path string, newSize int64) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if newSize < 0 || newSize > fi.Size() {
		t.Fatalf("truncateFileTail: newSize=%d fileSize=%d", newSize, fi.Size())
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o600) //nolint:gosec // G304: test-controlled path
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(newSize); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// Verifies replay tolerates a torn tail on the newest segment (CRC / EOF stop).
func TestHarnessTruncatedTailReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir, harnessWalConfig())
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("harness-a")
	payload := make([]byte, 400)
	for i := range payload {
		payload[i] = byte('a' + (i % 26))
	}
	for i := uint64(1); i <= 4; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Type: hraft.LogCommand, Data: payload}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	seg := newestSegmentPath(t, dir)
	fi, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}
	truncateFileTail(t, seg, fi.Size()-7)

	w2, err := Open(dir, harnessWalConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	gs2 := w2.GroupStore("harness-a")
	last, _ := gs2.LastIndex()
	if last < 1 {
		t.Fatalf("expected at least one replayed log after torn tail, last=%d", last)
	}
	var got hraft.Log
	for i := uint64(1); i <= last; i++ {
		if err := gs2.GetLog(i, &got); err != nil {
			t.Fatalf("GetLog(%d): %v", i, err)
		}
		if got.Term != 1 {
			t.Fatalf("index %d term=%d", i, got.Term)
		}
	}
	if err := gs2.GetLog(last+1, &got); err != hraft.ErrLogNotFound {
		t.Fatalf("GetLog past last: want ErrLogNotFound, got %v", err)
	}
}

// Verifies replay stops cleanly on a partial header at end-of-file.
func TestHarnessPartialHeaderTailReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir, harnessWalConfig())
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("harness-b")
	if err := gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Type: hraft.LogCommand, Data: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	seg := newestSegmentPath(t, dir)
	fi, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}
	// Leave a few bytes so the reader stops mid-header without a full record.
	tail := int64(5)
	if fi.Size() <= tail {
		t.Fatalf("segment too small: %d", fi.Size())
	}
	truncateFileTail(t, seg, tail)

	w2, err := Open(dir, harnessWalConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	gs2 := w2.GroupStore("harness-b")
	last, _ := gs2.LastIndex()
	if last != 0 {
		t.Fatalf("want empty log after partial-header tail, last=%d", last)
	}
}

// Deterministic multi-group interleaving, DeleteRange, compaction, and reopen.
func TestHarnessMultiGroupChurnCompactionRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := harnessWalConfig()
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	g1 := w.GroupStore("harness-g1")
	g2 := w.GroupStore("harness-g2")
	g3 := w.GroupStore("harness-g3")

	for round := 0; round < 6; round++ {
		idx := uint64(round + 1)
		if err := g1.StoreLog(&hraft.Log{Index: idx, Term: 1, Data: []byte("g1")}); err != nil {
			t.Fatal(err)
		}
		if err := g2.StoreLog(&hraft.Log{Index: idx, Term: 2, Data: []byte("g2")}); err != nil {
			t.Fatal(err)
		}
		if err := g3.StoreLog(&hraft.Log{Index: idx, Term: 3, Data: []byte("g3")}); err != nil {
			t.Fatal(err)
		}
	}
	// Prefix delete on g1 only (suffix semantics vs InmemStore).
	if err := g1.DeleteRange(3, 10); err != nil {
		t.Fatal(err)
	}
	// Grow enough segments for compaction.
	big := make([]byte, 700)
	for i := uint64(10); i <= 28; i++ {
		if err := g2.StoreLog(&hraft.Log{Index: i, Term: 2, Data: big}); err != nil {
			t.Fatalf("g2 StoreLog %d: %v", i, err)
		}
	}
	if err := g2.DeleteRange(1, 8); err != nil {
		t.Fatal(err)
	}
	stats := w.LastCompactionStats()
	if stats.ReclaimedSegments == 0 {
		t.Fatalf("expected compaction to reclaim segments, stats=%+v", stats)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	g1b := w2.GroupStore("harness-g1")
	g2b := w2.GroupStore("harness-g2")
	g3b := w2.GroupStore("harness-g3")

	var log hraft.Log
	for _, idx := range []uint64{1, 2} {
		if err := g1b.GetLog(idx, &log); err != nil {
			t.Fatalf("g1 GetLog %d: %v", idx, err)
		}
	}
	for _, idx := range []uint64{3, 4, 5} {
		if err := g1b.GetLog(idx, &log); err != hraft.ErrLogNotFound {
			t.Fatalf("g1 GetLog %d: want ErrLogNotFound, got %v", idx, err)
		}
	}
	if err := g2b.GetLog(20, &log); err != nil {
		t.Fatalf("g2 GetLog 20: %v", err)
	}
	if err := g3b.GetLog(6, &log); err != nil {
		t.Fatalf("g3 GetLog 6: %v", err)
	}
}

// Injected fsync failure on the Nth batch sync must surface to the writer.
func TestHarnessSegmentSyncFailurePropagates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := harnessWalConfig()
	var syncCalls int
	cfg.SegmentSync = func(f *os.File) error {
		syncCalls++
		// First batch is often group registration alone; fail the third sync so
		// the first user StoreLog batch succeeds and the second hits the fault.
		if syncCalls == 3 {
			return errHarnessSyncFail
		}
		return f.Sync()
	}
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	gs := w.GroupStore("harness-sync")

	if err := gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: []byte("a")}); err != nil {
		t.Fatalf("first StoreLog: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := gs.StoreLog(&hraft.Log{Index: 2, Term: 1, Data: []byte("b")}); err == nil || !errors.Is(err, errHarnessSyncFail) {
		t.Fatalf("second StoreLog: want errHarnessSyncFail, got %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := gs.StoreLog(&hraft.Log{Index: 3, Term: 1, Data: []byte("c")}); err != nil {
		t.Fatalf("third StoreLog after hook passes: %v", err)
	}
	if syncCalls < 4 {
		t.Fatalf("expected at least 4 sync calls, got %d", syncCalls)
	}
}

// Concurrency smoke test: multiple groups concurrently append, read, and delete.
// Intended to be run with `-race` to catch data races, and to exercise batch
// writer / in-memory map updates under contention.
func TestHarnessConcurrentChurn(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := harnessWalConfig()
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const groups = 6
	var stores [groups]*GroupStore
	for i := 0; i < groups; i++ {
		stores[i] = w.GroupStore("harness-conc-g" + string(rune('a'+i)))
	}

	var nextIdx [groups]uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var errCount atomic.Int64

	// Writers.
	for g := 0; g < groups; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := []byte("p" + string(rune('a'+g)))
			term := uint64(g + 1)
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := atomic.AddUint64(&nextIdx[g], 1)
				if err := stores[g].StoreLog(&hraft.Log{Index: i, Term: term, Type: hraft.LogCommand, Data: payload}); err != nil {
					errCount.Add(1)
					return
				}
			}
		}()
	}

	// Readers: randomly probe near the head and tail.
	for g := 0; g < groups; g++ {
		g := g
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
					time.Sleep(1 * time.Millisecond)
					continue
				}
				// Probe both an early and late index; tolerate not found due to deletes.
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
	}

	// Deleters: issue prefix and suffix deletes to exercise bound updates.
	for g := 0; g < groups; g++ {
		g := g
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
				if hi < 10 {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				// Prefix compaction-ish: delete 1..k
				k := hi / 3
				if k > 0 {
					_ = stores[g].DeleteRange(1, k)
				}
				// Suffix truncate-ish: delete mid..hi
				lo := (hi / 2) + 1
				if lo <= hi {
					_ = stores[g].DeleteRange(lo, hi)
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
}
