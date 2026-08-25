package raftwal

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

func TestGroupStoreLogRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")

	// Store 100 logs.
	for i := uint64(1); i <= 100; i++ {
		if err := gs.StoreLog(&hraft.Log{
			Index: i,
			Term:  1,
			Type:  hraft.LogCommand,
			Data:  []byte(fmt.Sprintf("entry-%d", i)),
		}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}

	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first != 1 || last != 100 {
		t.Fatalf("first=%d last=%d, want 1..100", first, last)
	}

	// Read them back.
	for i := uint64(1); i <= 100; i++ {
		var log hraft.Log
		if err := gs.GetLog(i, &log); err != nil {
			t.Fatalf("GetLog %d: %v", i, err)
		}
		if log.Index != i || string(log.Data) != fmt.Sprintf("entry-%d", i) {
			t.Fatalf("log %d: got index=%d data=%q", i, log.Index, log.Data)
		}
	}

	// GetLog for non-existent index.
	var log hraft.Log
	if err := gs.GetLog(101, &log); err != hraft.ErrLogNotFound {
		t.Fatalf("expected ErrLogNotFound, got %v", err)
	}
}

func TestGroupStoreDeleteRange(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")

	for i := uint64(1); i <= 10; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("x")})
	}

	// Delete 1..5.
	if err := gs.DeleteRange(1, 5); err != nil {
		t.Fatal(err)
	}

	first, _ := gs.FirstIndex()
	if first != 6 {
		t.Fatalf("first=%d after delete, want 6", first)
	}

	// Deleted entries return ErrLogNotFound.
	var log hraft.Log
	if err := gs.GetLog(3, &log); err != hraft.ErrLogNotFound {
		t.Fatalf("expected ErrLogNotFound for deleted entry, got %v", err)
	}

	// Surviving entries still readable.
	if err := gs.GetLog(7, &log); err != nil {
		t.Fatalf("GetLog 7: %v", err)
	}
}

func TestGroupStoreStableRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")

	// Set/Get bytes.
	if err := gs.Set([]byte("CurrentTerm"), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	val, err := gs.Get([]byte("CurrentTerm"))
	if err != nil || string(val) != "hello" {
		t.Fatalf("Get: val=%q err=%v", val, err)
	}

	// SetUint64/GetUint64.
	if err := gs.SetUint64([]byte("LastVote"), 42); err != nil {
		t.Fatal(err)
	}
	n, err := gs.GetUint64([]byte("LastVote"))
	if err != nil || n != 42 {
		t.Fatalf("GetUint64: n=%d err=%v", n, err)
	}

	// Missing key returns empty.
	val, err = gs.Get([]byte("missing"))
	if err != nil || val != nil {
		t.Fatalf("missing key: val=%v err=%v", val, err)
	}
	n, err = gs.GetUint64([]byte("missing"))
	if err != nil || n != 0 {
		t.Fatalf("missing uint64: n=%d err=%v", n, err)
	}
}

func TestMultipleGroupsIsolated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	g1 := w.GroupStore("vault-1")
	g2 := w.GroupStore("vault-2")

	// Write to g1.
	_ = g1.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: []byte("g1")})
	_ = g1.SetUint64([]byte("term"), 5)

	// Write to g2.
	_ = g2.StoreLog(&hraft.Log{Index: 1, Term: 2, Data: []byte("g2")})
	_ = g2.SetUint64([]byte("term"), 10)

	// g1 reads its own data.
	var log hraft.Log
	_ = g1.GetLog(1, &log)
	if string(log.Data) != "g1" {
		t.Fatalf("g1 got %q", log.Data)
	}
	n, _ := g1.GetUint64([]byte("term"))
	if n != 5 {
		t.Fatalf("g1 term=%d want 5", n)
	}

	// g2 reads its own data.
	_ = g2.GetLog(1, &log)
	if string(log.Data) != "g2" {
		t.Fatalf("g2 got %q", log.Data)
	}
	n, _ = g2.GetUint64([]byte("term"))
	if n != 10 {
		t.Fatalf("g2 term=%d want 10", n)
	}

	// g1 doesn't see g2's log.
	if _ = g1.GetLog(1, &log); string(log.Data) == "g2" {
		t.Fatal("g1 returned g2's log entry")
	}
}

func TestConcurrentGroups(t *testing.T) {
	if testing.Short() {
		t.Skip("concurrency stress across 10 groups x 100 logs; -short skips")
	}
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const numGroups = 10
	const logsPerGroup = 100

	var wg sync.WaitGroup
	errs := make(chan error, numGroups)

	for g := range numGroups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gs := w.GroupStore(fmt.Sprintf("vault-%d", g))
			for i := uint64(1); i <= logsPerGroup; i++ {
				if err := gs.StoreLog(&hraft.Log{
					Index: i,
					Term:  1,
					Data:  []byte(fmt.Sprintf("g%d-e%d", g, i)),
				}); err != nil {
					errs <- fmt.Errorf("group %d log %d: %w", g, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}

	// Verify all groups.
	for g := range numGroups {
		gs := w.GroupStore(fmt.Sprintf("vault-%d", g))
		first, _ := gs.FirstIndex()
		last, _ := gs.LastIndex()
		if first != 1 || last != logsPerGroup {
			t.Errorf("group %d: first=%d last=%d", g, first, last)
		}
	}
}

func TestCrashRecoveryTruncatedEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Write some good entries.
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("vault-1")
	for i := uint64(1); i <= 10; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte(fmt.Sprintf("e%d", i))})
	}
	w.Close()

	// Corrupt the WAL: append a partial header (simulates crash mid-write).
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			f, _ := os.OpenFile(filepath.Join(dir, e.Name()), os.O_WRONLY|os.O_APPEND, 0)
			_, _ = f.Write([]byte{0x01, 0x02, 0x03}) // 3 bytes, less than headerSize
			_ = f.Close()
		}
	}

	// Reopen — should recover the 10 good entries, ignore the garbage.
	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	gs2 := w2.GroupStore("vault-1")
	last, _ := gs2.LastIndex()
	if last != 10 {
		t.Fatalf("last=%d after crash recovery, want 10", last)
	}
}

func TestCrashRecoveryBadCRC(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("vault-1")
	for i := uint64(1); i <= 5; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("ok")})
	}
	w.Close()

	// Corrupt a byte in the middle of the WAL.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			path := filepath.Join(dir, e.Name())
			data, _ := os.ReadFile(path)
			if len(data) > 50 {
				data[50] ^= 0xFF // flip a byte
				_ = os.WriteFile(path, data, 0o644)
			}
		}
	}

	// Reopen — replay stops at the corrupted entry.
	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	gs2 := w2.GroupStore("vault-1")
	last, _ := gs2.LastIndex()
	// Some entries should survive (those before the corruption).
	// Exact count depends on where byte 50 falls.
	if last > 5 {
		t.Fatalf("last=%d, should be <= 5 after CRC corruption", last)
	}
}

func TestConcurrentStoreLogsStress(t *testing.T) {
	if testing.Short() {
		t.Skip("concurrency stress across 20 groups x 500 logs; -short skips")
	}
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const numGroups = 20
	const logsPerGroup = 500

	var wg sync.WaitGroup
	errs := make(chan error, numGroups)

	for g := range numGroups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gs := w.GroupStore(fmt.Sprintf("vault-%d", g))
			for i := uint64(1); i <= logsPerGroup; i++ {
				if err := gs.StoreLog(&hraft.Log{
					Index: i,
					Term:  uint64(g),
					Data:  []byte(fmt.Sprintf("g%d-e%d", g, i)),
				}); err != nil {
					errs <- fmt.Errorf("group %d log %d: %w", g, i, err)
					return
				}
			}
			// Also stress stable store.
			if err := gs.SetUint64([]byte("term"), uint64(g*100)); err != nil {
				errs <- fmt.Errorf("group %d SetUint64: %w", g, err)
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}

	// Verify all groups.
	for g := range numGroups {
		gs := w.GroupStore(fmt.Sprintf("vault-%d", g))
		first, _ := gs.FirstIndex()
		last, _ := gs.LastIndex()
		if first != 1 || last != logsPerGroup {
			t.Errorf("group %d: first=%d last=%d, want 1..%d", g, first, last, logsPerGroup)
		}
		n, _ := gs.GetUint64([]byte("term"))
		if n != uint64(g*100) {
			t.Errorf("group %d: term=%d want %d", g, n, g*100)
		}
	}
}

func TestReplayAfterReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Write some data.
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("vault-1")
	for i := uint64(1); i <= 50; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte(fmt.Sprintf("e%d", i))})
	}
	_ = gs.SetUint64([]byte("term"), 7)
	_ = gs.DeleteRange(1, 10)
	w.Close()

	// Reopen and verify.
	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	gs2 := w2.GroupStore("vault-1")
	first, _ := gs2.FirstIndex()
	last, _ := gs2.LastIndex()
	if first != 11 || last != 50 {
		t.Fatalf("after reopen: first=%d last=%d, want 11..50", first, last)
	}

	var log hraft.Log
	if err := gs2.GetLog(25, &log); err != nil {
		t.Fatalf("GetLog 25: %v", err)
	}
	if string(log.Data) != "e25" {
		t.Fatalf("log 25: got %q", log.Data)
	}

	n, _ := gs2.GetUint64([]byte("term"))
	if n != 7 {
		t.Fatalf("term=%d want 7", n)
	}

	// Deleted entry still gone.
	if err := gs2.GetLog(5, &log); err != hraft.ErrLogNotFound {
		t.Fatalf("expected ErrLogNotFound for deleted entry after reopen, got %v", err)
	}
}

// --- Edge cases ---

func TestEmptyWALOpenClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen empty WAL.
	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w2.Close()
}

func TestGroupStoreEmptyGroup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("empty")
	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first != 0 || last != 0 {
		t.Fatalf("empty group: first=%d last=%d", first, last)
	}
	var log hraft.Log
	if err := gs.GetLog(1, &log); err != hraft.ErrLogNotFound {
		t.Fatalf("empty group GetLog: %v", err)
	}
	val, _ := gs.Get([]byte("anything"))
	if val != nil {
		t.Fatalf("empty group Get: %v", val)
	}
	n, _ := gs.GetUint64([]byte("anything"))
	if n != 0 {
		t.Fatalf("empty group GetUint64: %d", n)
	}
}

func TestStoreLogSingleEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("single")
	_ = gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: []byte("only")})

	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first != 1 || last != 1 {
		t.Fatalf("first=%d last=%d", first, last)
	}
}

func TestDeleteRangeEntireLog(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	for i := uint64(1); i <= 5; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("x")})
	}

	// Delete everything.
	_ = gs.DeleteRange(1, 5)

	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first != 0 || last != 0 {
		t.Fatalf("after full delete: first=%d last=%d, want 0/0", first, last)
	}
}

func TestDeleteRangeThenAppend(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	for i := uint64(1); i <= 10; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("old")})
	}
	_ = gs.DeleteRange(1, 8)

	// Append new entries starting after the gap (like Raft does after snapshot restore).
	for i := uint64(11); i <= 15; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 2, Data: []byte("new")})
	}

	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first != 9 || last != 15 {
		t.Fatalf("first=%d last=%d, want 9..15", first, last)
	}

	// Old surviving entries.
	var log hraft.Log
	_ = gs.GetLog(9, &log)
	if string(log.Data) != "old" {
		t.Fatalf("log 9: %q", log.Data)
	}

	// New entries.
	_ = gs.GetLog(12, &log)
	if string(log.Data) != "new" {
		t.Fatalf("log 12: %q", log.Data)
	}
}

func TestDeleteRangeIdempotent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	for i := uint64(1); i <= 5; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("x")})
	}

	// Double delete of same range.
	_ = gs.DeleteRange(1, 3)
	_ = gs.DeleteRange(1, 3)

	first, _ := gs.FirstIndex()
	if first != 4 {
		t.Fatalf("first=%d want 4", first)
	}
}

// Suffix-style DeleteRange must not poison reads of the surviving prefix
// (hashicorp/raft appendEntries conflict path): a too-wide "deleted" horizon
// makes GetLog return ErrLogNotFound, which panics the Raft node.
func TestDeleteRangeSuffixPreservesPrefix(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	for i := uint64(1); i <= 10; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("x")})
	}
	if err := gs.DeleteRange(5, 10); err != nil {
		t.Fatal(err)
	}
	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first != 1 || last != 4 {
		t.Fatalf("first=%d last=%d, want 1..4", first, last)
	}
	var log hraft.Log
	for _, idx := range []uint64{1, 2, 3, 4} {
		if err := gs.GetLog(idx, &log); err != nil {
			t.Fatalf("GetLog(%d): %v", idx, err)
		}
	}
	for _, idx := range []uint64{5, 6, 10} {
		if err := gs.GetLog(idx, &log); err != hraft.ErrLogNotFound {
			t.Fatalf("GetLog(%d): want ErrLogNotFound, got %v", idx, err)
		}
	}
}

func TestDeleteRangeBeyondLastIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	for i := uint64(1); i <= 5; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("x")})
	}

	// Delete range extends beyond the last entry: suffix is cleared; prefix
	// indices below lo remain (same semantics as hashicorp/raft InmemStore).
	if err := gs.DeleteRange(3, 100); err != nil {
		t.Fatal(err)
	}
	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first != 1 || last != 2 {
		t.Fatalf("first=%d last=%d, want 1..2 after delete past end", first, last)
	}
	var log hraft.Log
	for _, idx := range []uint64{1, 2} {
		if err := gs.GetLog(idx, &log); err != nil {
			t.Fatalf("GetLog(%d): %v", idx, err)
		}
	}
	for _, idx := range []uint64{3, 4, 5} {
		if err := gs.GetLog(idx, &log); err != hraft.ErrLogNotFound {
			t.Fatalf("GetLog(%d): want ErrLogNotFound, got %v", idx, err)
		}
	}
}

// A mask spanning the whole index space applies at the cost of the live
// entries. hashicorp/raft issues one DeleteRange over the entire log after a
// snapshot install, and walking every masked index instead would hold
// stateMu.Lock for as many lookups as the range is wide — every group's reads
// on the node stall behind it.
func TestDeleteRangeWiderThanTheLiveIndexApplies(t *testing.T) {
	t.Parallel()
	w, _ := openTestWAL(t, Config{})
	gs := w.GroupStore("grp")
	const last = 50
	for i := uint64(1); i <= last; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 64)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}

	// lo above firstIndex keeps index 1 live, so the mask cannot be served by
	// discarding the index wholesale: the surviving entry has to be found among
	// the live ones rather than by walking the range.
	if err := gs.DeleteRange(2, math.MaxUint64); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	first, err := gs.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	lastIdx, err := gs.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if first != 1 || lastIdx != 1 {
		t.Errorf("bounds first=%d last=%d, want 1..1", first, lastIdx)
	}
	var lg hraft.Log
	if err := gs.GetLog(1, &lg); err != nil {
		t.Errorf("GetLog(1): %v", err)
	}
	for _, idx := range []uint64{2, 25, last} {
		if err := gs.GetLog(idx, &lg); !errors.Is(err, hraft.ErrLogNotFound) {
			t.Errorf("GetLog(%d) = %v, want ErrLogNotFound", idx, err)
		}
	}
	assertLiveBytesInvariant(t, w, "after a mask wider than the index")
}

// The whole-log truncation raft.MonotonicLogStore requires: the mask covers
// every live index, so the group is left empty, its segments drain and
// reclamation unlinks them, and replay of the mask lands on the same empty
// bounds.
func TestDeleteRangeWipesTheWholeIndex(t *testing.T) {
	t.Parallel()
	w, dir := openTestWAL(t, Config{})
	gs := w.GroupStore("grp")
	const last = 60
	for i := uint64(1); i <= last; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 64)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}

	// Premise: the wipe has a populated index and a populated recent window to
	// clear, spread over several segments.
	indexed, cached, _ := groupIndexCensus(t, w, gs)
	if indexed != last {
		t.Fatalf("indexed = %d before the wipe, want %d", indexed, last)
	}
	if cached == 0 {
		t.Fatal("recent window empty before the wipe, so the wipe cannot be shown to clear it")
	}
	if rotated := segmentsRotated(t, w); rotated < 2 {
		t.Fatalf("only %d segment(s) opened, so the census below proves nothing", rotated)
	}

	if err := gs.DeleteRange(1, last); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	first, err := gs.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	lastIdx, err := gs.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if first != 0 || lastIdx != 0 {
		t.Errorf("bounds first=%d last=%d after the wipe, want 0..0", first, lastIdx)
	}
	indexed, cached, cacheBytes := groupIndexCensus(t, w, gs)
	if indexed != 0 || cached != 0 || cacheBytes != 0 {
		t.Errorf("after the wipe: indexed=%d cached=%d cacheBytes=%d, want 0/0/0", indexed, cached, cacheBytes)
	}
	var lg hraft.Log
	for _, idx := range []uint64{1, last / 2, last} {
		if err := gs.GetLog(idx, &lg); !errors.Is(err, hraft.ErrLogNotFound) {
			t.Errorf("GetLog(%d) = %v, want ErrLogNotFound", idx, err)
		}
	}
	assertLiveBytesInvariant(t, w, "after the whole-index wipe")

	// The drained segments carry nothing the index still references, so
	// reclamation unlinks them; segment 1 holds the group registration.
	triggerReclaim(t, w, gs)
	if n := segmentFileCount(t, dir); n > 2 {
		t.Errorf("%d data-bearing segments after the wipe, want <= 2", n)
	}
	assertLiveBytesInvariant(t, w, "after reclaiming the wiped segments")

	// Replay applies the same mask against the rebuilt index and must land on
	// the same empty bounds.
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	w2, err := Open(dir, Config{SegmentTargetSize: 2048, ScavengeMaxLiveBytes: 128})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	gs2 := w2.GroupStore("grp")
	first, err = gs2.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex after replay: %v", err)
	}
	lastIdx, err = gs2.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex after replay: %v", err)
	}
	if first != 0 || lastIdx != 0 {
		t.Errorf("replayed bounds first=%d last=%d, want 0..0", first, lastIdx)
	}
	if indexed, _, _ := groupIndexCensus(t, w2, gs2); indexed != 0 {
		t.Errorf("replayed index holds %d entries, want 0", indexed)
	}
	assertLiveBytesInvariant(t, w2, "after replaying the wipe")
}

// A wipe followed by appends at a far higher index leaves a hole. A mask
// spanning that hole must remove exactly the live entries inside it and leave
// the survivors readable.
func TestDeleteRangeSpanningAHole(t *testing.T) {
	t.Parallel()
	w, _ := openTestWAL(t, Config{})
	gs := w.GroupStore("grp")
	for i := uint64(1); i <= 50; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 64)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	if err := gs.DeleteRange(1, 50); err != nil {
		t.Fatalf("wipe: %v", err)
	}
	for i := uint64(1000); i <= 1010; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 2, Data: make([]byte, 64)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}

	if err := gs.DeleteRange(1, 1009); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	first, err := gs.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	last, err := gs.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if first != 1010 || last != 1010 {
		t.Errorf("bounds first=%d last=%d, want 1010..1010", first, last)
	}
	var lg hraft.Log
	if err := gs.GetLog(1010, &lg); err != nil {
		t.Errorf("GetLog(1010): %v", err)
	}
	for _, idx := range []uint64{1, 25, 1000, 1009} {
		if err := gs.GetLog(idx, &lg); !errors.Is(err, hraft.ErrLogNotFound) {
			t.Errorf("GetLog(%d) = %v, want ErrLogNotFound", idx, err)
		}
	}
	if indexed, _, _ := groupIndexCensus(t, w, gs); indexed != 1 {
		t.Errorf("index holds %d entries, want 1", indexed)
	}
	assertLiveBytesInvariant(t, w, "after a mask spanning a hole")
}

// groupIndexCensus reports the group's indexed-entry count, cached-payload
// count and cached bytes.
func groupIndexCensus(t *testing.T, w *WAL, gs *GroupStore) (indexed, cached int, cacheBytes int64) {
	t.Helper()
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	state := w.groups[gs.groupID]
	if state == nil {
		return 0, 0, 0
	}
	return len(state.logs), len(state.cache), state.cacheBytes
}

var deleteRangePathNames = map[deleteRangePath]string{
	deleteRangeByIndex: "byIndex",
	deleteRangeByEntry: "byEntry",
	deleteRangeWipe:    "wipe",
}

func pathNames(paths []deleteRangePath) string {
	names := make([]string, len(paths))
	for i, p := range paths {
		names[i] = deleteRangePathNames[p]
	}
	return strings.Join(names, ", ")
}

func TestChooseDeleteRangePath(t *testing.T) {
	t.Parallel()
	populated := func(first, last uint64, indices ...uint64) *groupState {
		gs := newGroupState()
		for _, idx := range indices {
			gs.logs[idx] = logLoc{seg: 1, off: 0, length: 8}
		}
		gs.firstIndex, gs.lastIndex = first, last
		return gs
	}
	ten := func() *groupState {
		return populated(1, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	}
	cases := []struct {
		name   string
		gs     *groupState
		lo, hi uint64
		want   deleteRangePath
	}{
		{"exact cover", ten(), 1, 10, deleteRangeWipe},
		{"cover with slack on both ends", ten(), 1, 99, deleteRangeWipe},
		{"prefix leaves a survivor", ten(), 1, 4, deleteRangeByIndex},
		{"interior hole", ten(), 4, 6, deleteRangeByIndex},
		{"width equal to the live count", ten(), 2, 11, deleteRangeByIndex},
		{"width one past the live count", ten(), 2, 12, deleteRangeByEntry},
		{"suffix wider than the index space", ten(), 2, math.MaxUint64, deleteRangeByEntry},
		{"empty group", newGroupState(), 1, 10, deleteRangeByEntry},
		// Width arithmetic on the whole index space must not wrap into a walk
		// of every index: an empty group has no bounds to make this a wipe.
		{"whole index space on an empty group", newGroupState(), 0, math.MaxUint64, deleteRangeByEntry},
		{"mask spanning a hole", populated(1, 1000, 1, 2, 1000), 1, 999, deleteRangeByEntry},
		{"mask covering across a hole", populated(1, 1000, 1, 2, 1000), 1, 1000, deleteRangeWipe},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := chooseDeleteRangePath(tc.gs, tc.lo, tc.hi); got != tc.want {
				t.Errorf("path for [%d, %d] = %s, want %s",
					tc.lo, tc.hi, deleteRangePathNames[got], deleteRangePathNames[tc.want])
			}
		})
	}
}

// groupSnapshot is the state a mask may touch: the index, its bounds, the
// recent window and the per-segment live-bytes counters.
type groupSnapshot struct {
	Logs       map[uint64]logLoc
	FirstIndex uint64
	LastIndex  uint64
	Cache      map[uint64]string
	CacheBytes int64
	SegLive    map[int]int64
}

func snapshotGroup(t *testing.T, w *WAL, gs *GroupStore) groupSnapshot {
	t.Helper()
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	state := w.groups[gs.groupID]
	if state == nil {
		t.Fatalf("group %d absent", gs.groupID)
	}
	snap := groupSnapshot{
		Logs:       make(map[uint64]logLoc, len(state.logs)),
		FirstIndex: state.firstIndex,
		LastIndex:  state.lastIndex,
		Cache:      make(map[uint64]string, len(state.cache)),
		CacheBytes: state.cacheBytes,
		SegLive:    make(map[int]int64, len(w.segLive)),
	}
	for idx, loc := range state.logs {
		snap.Logs[idx] = loc
	}
	for idx, enc := range state.cache {
		snap.Cache[idx] = string(enc)
	}
	for seq, bytes := range w.segLive {
		snap.SegLive[seq] = bytes
	}
	return snap
}

// deleteRangeTwin builds a group whose entries span several segments with a
// populated recent window. Scavenging is held off so the twins' record
// locations depend only on the appends, which are identical.
func deleteRangeTwin(t *testing.T, last uint64) (*WAL, *GroupStore) {
	t.Helper()
	w, _ := openTestWAL(t, Config{SegmentTargetSize: 2048, ScavengeMaxLiveBytes: 1})
	gs := w.GroupStore("grp")
	for i := uint64(1); i <= last; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 64)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	return w, gs
}

// selectedDeleteRangePath reports the path production picks for the mask.
func selectedDeleteRangePath(t *testing.T, w *WAL, gs *GroupStore, lo, hi uint64) deleteRangePath {
	t.Helper()
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	return chooseDeleteRangePath(w.groups[gs.groupID], lo, hi)
}

// applyMaskThroughPath applies [lo, hi] to the group by the named path,
// bypassing path selection, and reports the state before and after.
func applyMaskThroughPath(t *testing.T, w *WAL, gs *GroupStore, path deleteRangePath, lo, hi uint64) (pre, post groupSnapshot) {
	t.Helper()
	pre = snapshotGroup(t, w, gs)
	w.stateMu.Lock()
	state := w.groups[gs.groupID]
	switch path {
	case deleteRangeWipe:
		w.dropWholeIndex(state)
	case deleteRangeByEntry:
		w.dropRangeByEntry(state, lo, hi)
	case deleteRangeByIndex:
		w.dropRangeByIndex(state, lo, hi)
	}
	state.shrinkBounds(lo, hi)
	w.stateMu.Unlock()
	return pre, snapshotGroup(t, w, gs)
}

// The three application paths are one behavior expressed three ways: the same
// mask must leave the same index, bounds, recent window and live-bytes
// counters whichever one applies it. Only the cost differs.
func TestDeleteRangeApplicationPathsAgree(t *testing.T) {
	t.Parallel()
	const last = 60
	allPaths := []deleteRangePath{deleteRangeByIndex, deleteRangeByEntry, deleteRangeWipe}
	partialPaths := []deleteRangePath{deleteRangeByIndex, deleteRangeByEntry}

	cases := []struct {
		name   string
		lo, hi uint64
		paths  []deleteRangePath
	}{
		{"whole index", 1, last, allPaths},
		{"whole index with slack", 1, 4 * last, allPaths},
		{"prefix", 1, last / 2, partialPaths},
		{"suffix", last / 2, last, partialPaths},
		{"interior", 20, 40, partialPaths},
		{"single index", 33, 33, partialPaths},
		{"past the end", last - 5, 4 * last, partialPaths},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var want groupSnapshot
			var wantPre groupSnapshot
			for i, path := range tc.paths {
				w, gs := deleteRangeTwin(t, last)
				// Selection and application must not drift apart: whatever
				// production picks for this mask has to be one of the paths
				// this case proves equivalent.
				if selected := selectedDeleteRangePath(t, w, gs, tc.lo, tc.hi); !slices.Contains(tc.paths, selected) {
					t.Fatalf("production selects %s for [%d, %d]; this case only compares %s",
						deleteRangePathNames[selected], tc.lo, tc.hi, pathNames(tc.paths))
				}
				pre, post := applyMaskThroughPath(t, w, gs, path, tc.lo, tc.hi)
				assertLiveBytesInvariant(t, w, "after the mask applied "+deleteRangePathNames[path])
				if i == 0 {
					wantPre, want = pre, post
					continue
				}
				// Premise: the twins are built identically, so a post-state
				// difference can only come from how the mask was applied.
				if !reflect.DeepEqual(pre, wantPre) {
					t.Fatalf("the %s twin differs before the mask:\n got %+v\nwant %+v",
						deleteRangePathNames[path], pre, wantPre)
				}
				if !reflect.DeepEqual(post, want) {
					t.Errorf("%s left:\n got %+v\nwant %+v", deleteRangePathNames[path], post, want)
				}
			}
		})
	}
}

func TestStableStoreOverwrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	_ = gs.Set([]byte("key"), []byte("v1"))
	_ = gs.Set([]byte("key"), []byte("v2"))
	_ = gs.Set([]byte("key"), []byte("v3"))

	val, _ := gs.Get([]byte("key"))
	if string(val) != "v3" {
		t.Fatalf("expected v3, got %q", val)
	}
}

func TestStableStoreUint64Overwrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	_ = gs.SetUint64([]byte("term"), 1)
	_ = gs.SetUint64([]byte("term"), 2)
	_ = gs.SetUint64([]byte("term"), 3)

	n, _ := gs.GetUint64([]byte("term"))
	if n != 3 {
		t.Fatalf("expected 3, got %d", n)
	}
}

func TestStableStoreEmptyValue(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	_ = gs.Set([]byte("key"), []byte{})
	val, _ := gs.Get([]byte("key"))
	if val == nil || len(val) != 0 {
		t.Fatalf("expected empty slice, got %v", val)
	}
}

func TestStableStoreEmptyKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	_ = gs.Set([]byte(""), []byte("val"))
	val, _ := gs.Get([]byte(""))
	if string(val) != "val" {
		t.Fatalf("expected val, got %q", val)
	}
}

func TestLogWithExtensions(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	ext := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	_ = gs.StoreLog(&hraft.Log{
		Index:      1,
		Term:       1,
		Type:       hraft.LogCommand,
		Data:       []byte("data"),
		Extensions: ext,
	})

	var log hraft.Log
	_ = gs.GetLog(1, &log)
	if string(log.Extensions) != string(ext) {
		t.Fatalf("extensions mismatch: got %x want %x", log.Extensions, ext)
	}
}

func TestLogWithEmptyData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	_ = gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: nil})

	var log hraft.Log
	_ = gs.GetLog(1, &log)
	if log.Index != 1 {
		t.Fatal("failed to read log with nil data")
	}
}

func TestLogAllTypes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	types := []hraft.LogType{
		hraft.LogCommand,
		hraft.LogNoop,
		hraft.LogBarrier,
		hraft.LogConfiguration,
	}
	for i, lt := range types {
		_ = gs.StoreLog(&hraft.Log{Index: uint64(i + 1), Term: 1, Type: lt, Data: []byte("x")})
	}
	for i, lt := range types {
		var log hraft.Log
		_ = gs.GetLog(uint64(i+1), &log)
		if log.Type != lt {
			t.Errorf("log %d: type=%d want %d", i+1, log.Type, lt)
		}
	}
}

func TestLargeLogEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	bigData := make([]byte, 1<<20) // 1MB
	for i := range bigData {
		bigData[i] = byte(i % 256)
	}
	_ = gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: bigData})

	var log hraft.Log
	_ = gs.GetLog(1, &log)
	if len(log.Data) != len(bigData) {
		t.Fatalf("data length %d, want %d", len(log.Data), len(bigData))
	}
	for i := range bigData {
		if log.Data[i] != bigData[i] {
			t.Fatalf("data mismatch at byte %d", i)
			break
		}
	}
}

func TestStoreLogsMultiple(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	logs := make([]*hraft.Log, 100)
	for i := range logs {
		logs[i] = &hraft.Log{Index: uint64(i + 1), Term: 1, Data: []byte(fmt.Sprintf("batch-%d", i))}
	}
	if err := gs.StoreLogs(logs); err != nil {
		t.Fatal(err)
	}

	last, _ := gs.LastIndex()
	if last != 100 {
		t.Fatalf("last=%d want 100", last)
	}
}

// --- Isolation tests ---

func TestGroupStoreGetDoesNotReturnInternalReference(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	_ = gs.Set([]byte("key"), []byte("original"))

	val, _ := gs.Get([]byte("key"))
	// Mutate the returned slice — should not affect internal state.
	val[0] = 'X'

	val2, _ := gs.Get([]byte("key"))
	if string(val2) != "original" {
		t.Fatalf("internal state mutated: got %q", val2)
	}
}

func TestGroupStoreGetLogDoesNotReturnInternalReference(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	_ = gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: []byte("original")})

	var log1 hraft.Log
	_ = gs.GetLog(1, &log1)
	log1.Data[0] = 'X'

	var log2 hraft.Log
	_ = gs.GetLog(1, &log2)
	if string(log2.Data) != "original" {
		t.Fatalf("internal state mutated: got %q", log2.Data)
	}
}

// --- Segment rotation ---

func TestSegmentRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ~70MB (1100x64KB) to trigger segment rotation; -short skips")
	}
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")

	// Write enough data to trigger segment rotation (64MB target).
	// Use 64KB entries — need ~1024 to hit 64MB.
	bigData := make([]byte, 64*1024)
	for i := uint64(1); i <= 1100; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: bigData}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}

	// Should have multiple segment files.
	entries, _ := os.ReadDir(dir)
	segCount := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".log") {
			segCount++
		}
	}
	if segCount < 2 {
		t.Fatalf("expected multiple segments, got %d", segCount)
	}

	// All entries still readable.
	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first != 1 || last != 1100 {
		t.Fatalf("first=%d last=%d", first, last)
	}
}

// --- Multiple reopen cycles ---

func TestMultipleReopenCycles(t *testing.T) {
	if testing.Short() {
		t.Skip("5 open/close restart-survival cycles; -short skips")
	}
	t.Parallel()
	dir := t.TempDir()

	for cycle := range 5 {
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("cycle %d open: %v", cycle, err)
		}
		gs := w.GroupStore("persistent")
		base := uint64(cycle*10 + 1)
		for i := base; i < base+10; i++ {
			_ = gs.StoreLog(&hraft.Log{Index: i, Term: uint64(cycle + 1), Data: []byte(fmt.Sprintf("c%d", cycle))})
		}
		_ = gs.SetUint64([]byte("cycle"), uint64(cycle))
		w.Close()
	}

	// Final reopen — verify all data.
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("persistent")
	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first != 1 || last != 50 {
		t.Fatalf("first=%d last=%d, want 1..50", first, last)
	}

	n, _ := gs.GetUint64([]byte("cycle"))
	if n != 4 {
		t.Fatalf("cycle=%d want 4", n)
	}
}

// --- Concurrent read/write ---

// TestReadsNotBlockedDuringFsync verifies GetLog does not wait on segment fsync.
// Before the stateMu/fsync split, batchWriter held the exclusive lock through
// syncActiveSegment and blocked all readers for the full fsync duration.
func TestReadsNotBlockedDuringFsync(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const fsyncDelay = 200 * time.Millisecond
	cfg := Config{
		SyncBatchWindow: 1 * time.Millisecond,
		SegmentSync: func(*os.File) error {
			time.Sleep(fsyncDelay)
			return nil
		},
	}
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	if err := gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: []byte("seed")}); err != nil {
		t.Fatal(err)
	}

	writeStarted := make(chan struct{})
	go func() {
		close(writeStarted)
		_ = gs.StoreLog(&hraft.Log{Index: 2, Term: 1, Data: []byte("slow-fsync")})
	}()

	<-writeStarted
	time.Sleep(10 * time.Millisecond) // let batchWriter reach fsync

	const readBudget = 50 * time.Millisecond
	start := time.Now()
	var log hraft.Log
	if err := gs.GetLog(1, &log); err != nil {
		t.Fatalf("GetLog during fsync: %v", err)
	}
	if elapsed := time.Since(start); elapsed > readBudget {
		t.Fatalf("GetLog blocked %v during fsync, want <%v", elapsed, readBudget)
	}
	if string(log.Data) != "seed" {
		t.Fatalf("GetLog data=%q want seed", log.Data)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("sustained concurrent reader/writer/stable-writer stress window; -short skips")
	}
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")

	// Pre-populate.
	for i := uint64(1); i <= 100; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("init")})
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	var errMu sync.Mutex
	var goroutineErrs []error
	recordErr := func(err error) {
		if err != nil {
			errMu.Lock()
			goroutineErrs = append(goroutineErrs, err)
			errMu.Unlock()
		}
	}

	// Writer: keeps appending. Signals its first append so the timed window
	// cannot start before this goroutine has run (the LastIndex > 100
	// assertion below false-fails if scheduling starves it).
	logWritten := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(101); ; i++ {
			select {
			case <-done:
				return
			default:
			}
			if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("new")}); err != nil {
				recordErr(err)
				return
			}
			if i == 101 {
				close(logWritten)
			}
		}
	}()

	// Reader: keeps reading existing entries.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			var log hraft.Log
			if err := gs.GetLog(50, &log); err != nil {
				recordErr(err)
				return
			}
			if string(log.Data) != "init" {
				recordErr(fmt.Errorf("GetLog(50) data=%q want init", log.Data))
				return
			}
			_, _ = gs.FirstIndex()
			_, _ = gs.LastIndex()
			_, _ = gs.Get([]byte("missing"))
			_, _ = gs.GetUint64([]byte("missing"))
		}
	}()

	// Stable writer. Starts at 1 (a single write of 0 would be
	// indistinguishable from no write in the final assertion) and signals
	// its first write so the timed window below cannot start before the
	// goroutine has been scheduled — full-suite load starved it past the
	// whole window and false-failed the "written at least once" check.
	counterWritten := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); ; i++ {
			select {
			case <-done:
				return
			default:
			}
			if err := gs.SetUint64([]byte("counter"), i); err != nil {
				recordErr(err)
				return
			}
			if i == 1 {
				close(counterWritten)
			}
		}
	}()

	for name, ch := range map[string]chan struct{}{"stable writer": counterWritten, "log writer": logWritten} {
		select {
		case <-ch:
		case <-time.After(30 * time.Second):
			close(done)
			wg.Wait()
			errMu.Lock()
			defer errMu.Unlock()
			t.Fatalf("%s never completed a write; goroutine errors: %v", name, goroutineErrs)
		}
	}

	// Run for 200ms then stop.
	time.Sleep(200 * time.Millisecond)
	close(done)
	wg.Wait()

	errMu.Lock()
	errs := append([]error(nil), goroutineErrs...)
	errMu.Unlock()
	if len(errs) > 0 {
		t.Fatalf("goroutine errors: %v", errs)
	}

	last, err := gs.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last <= 100 {
		t.Fatalf("expected writer to advance LastIndex past 100, got %d", last)
	}
	var log hraft.Log
	if err := gs.GetLog(50, &log); err != nil {
		t.Fatalf("GetLog(50): %v", err)
	}
	if string(log.Data) != "init" {
		t.Fatalf("GetLog(50) data=%q want init", log.Data)
	}
	n, err := gs.GetUint64([]byte("counter"))
	if err != nil {
		t.Fatalf("GetUint64(counter): %v", err)
	}
	if n == 0 {
		t.Fatal("expected counter to be written at least once")
	}
}

// --- WAL after close ---

func TestWriteAfterClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("vault-1")
	w.Close()

	err = gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: []byte("after-close")})
	if err == nil {
		t.Fatal("expected error writing after close")
	}
}

// --- Group name edge cases ---

func TestGroupNameEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("")
	_ = gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: []byte("empty-name")})
	var log hraft.Log
	_ = gs.GetLog(1, &log)
	if string(log.Data) != "empty-name" {
		t.Fatalf("got %q", log.Data)
	}
}

func TestGroupNameSpecialChars(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	names := []string{
		"vault/with/slashes",
		"vault with spaces",
		"vault-with-dashes-and-019d87f1-3ec2-7144-a042-uuid",
		"日本語",
		strings.Repeat("a", 1000),
	}
	for _, name := range names {
		gs := w.GroupStore(name)
		_ = gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: []byte(name)})
		var log hraft.Log
		if err := gs.GetLog(1, &log); err != nil {
			t.Errorf("name %q: GetLog: %v", name[:min(len(name), 20)], err)
		}
	}
}

func TestSameGroupStoreReturnsSameView(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs1 := w.GroupStore("vault-1")
	gs2 := w.GroupStore("vault-1")

	_ = gs1.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: []byte("from-gs1")})

	var log hraft.Log
	if err := gs2.GetLog(1, &log); err != nil {
		t.Fatal(err)
	}
	if string(log.Data) != "from-gs1" {
		t.Fatalf("gs2 got %q, want from-gs1", log.Data)
	}
}

// --- Non-contiguous indices ---

func TestNonContiguousIndices(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	// hashicorp/raft may store non-contiguous indices after snapshot restore.
	_ = gs.StoreLog(&hraft.Log{Index: 100, Term: 5, Data: []byte("after-snapshot")})
	_ = gs.StoreLog(&hraft.Log{Index: 101, Term: 5, Data: []byte("next")})

	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first != 100 || last != 101 {
		t.Fatalf("first=%d last=%d, want 100..101", first, last)
	}

	var log hraft.Log
	if err := gs.GetLog(99, &log); err != hraft.ErrLogNotFound {
		t.Fatalf("expected ErrLogNotFound for gap index 99, got %v", err)
	}
}

// --- High term numbers ---

func TestHighTermNumbers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("vault-1")
	_ = gs.StoreLog(&hraft.Log{Index: 1, Term: 1<<63 - 1, Data: []byte("max-term")})
	_ = gs.SetUint64([]byte("term"), 1<<64-1)

	var log hraft.Log
	_ = gs.GetLog(1, &log)
	if log.Term != 1<<63-1 {
		t.Fatalf("term=%d", log.Term)
	}

	n, _ := gs.GetUint64([]byte("term"))
	if n != 1<<64-1 {
		t.Fatalf("stable uint64=%d", n)
	}
}

// --- Replay with multiple groups ---

func TestReplayMultipleGroups(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for g := range 5 {
		gs := w.GroupStore(fmt.Sprintf("group-%d", g))
		for i := uint64(1); i <= 10; i++ {
			_ = gs.StoreLog(&hraft.Log{Index: i, Term: uint64(g + 1), Data: []byte(fmt.Sprintf("g%d", g))})
		}
		_ = gs.SetUint64([]byte("id"), uint64(g))
	}
	w.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	for g := range 5 {
		gs := w2.GroupStore(fmt.Sprintf("group-%d", g))
		last, _ := gs.LastIndex()
		if last != 10 {
			t.Errorf("group-%d: last=%d want 10", g, last)
		}
		n, _ := gs.GetUint64([]byte("id"))
		if n != uint64(g) {
			t.Errorf("group-%d: id=%d want %d", g, n, g)
		}
		var log hraft.Log
		_ = gs.GetLog(5, &log)
		if log.Term != uint64(g+1) {
			t.Errorf("group-%d: term=%d want %d", g, log.Term, g+1)
		}
	}
}

// --- Replay with delete ranges ---

func TestReplayWithDeleteRanges(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("vault-1")
	for i := uint64(1); i <= 100; i++ {
		_ = gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("x")})
	}
	// Multiple overlapping deletes.
	_ = gs.DeleteRange(1, 30)
	_ = gs.DeleteRange(20, 50)
	_ = gs.DeleteRange(45, 60)
	w.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	gs2 := w2.GroupStore("vault-1")
	first, _ := gs2.FirstIndex()
	last, _ := gs2.LastIndex()
	if first != 61 || last != 100 {
		t.Fatalf("first=%d last=%d, want 61..100", first, last)
	}

	// Deleted entries are gone.
	var log hraft.Log
	if err := gs2.GetLog(50, &log); err != hraft.ErrLogNotFound {
		t.Fatalf("expected ErrLogNotFound for 50, got %v", err)
	}

	// Surviving entries are present.
	if err := gs2.GetLog(75, &log); err != nil {
		t.Fatalf("GetLog 75: %v", err)
	}
}

func TestSegmentReclamationRemovesOldFilesAndPreservesState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	var reclaimed atomic.Int64
	var reclaimedBytes atomic.Int64
	w, err := Open(dir, Config{
		SegmentTargetSize: 1024,
		// A threshold well under the segment size, as in production: the
		// drained bulk unlinks and only a nearly-empty head is scavenged.
		ScavengeMaxLiveBytes: 256,
		OnReclaim: func(s ReclaimStats) {
			reclaimed.Add(1)
			reclaimedBytes.Add(s.ReclaimedBytes)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	gsA := w.GroupStore("group-a")
	gsB := w.GroupStore("group-b")

	payload := make([]byte, 256)
	for i := uint64(1); i <= 24; i++ {
		if err := gsA.StoreLog(&hraft.Log{Index: i, Term: 1, Data: payload}); err != nil {
			t.Fatalf("group-a StoreLog %d: %v", i, err)
		}
		if err := gsB.StoreLog(&hraft.Log{Index: i, Term: 2, Data: payload}); err != nil {
			t.Fatalf("group-b StoreLog %d: %v", i, err)
		}
	}
	if err := gsA.SetUint64([]byte("term"), 7); err != nil {
		t.Fatal(err)
	}
	if err := gsB.Set([]byte("vote"), []byte("n2")); err != nil {
		t.Fatal(err)
	}

	segmentsBefore := countWalSegments(t, dir)
	if segmentsBefore < 2 {
		t.Fatalf("expected multiple segments before reclamation, got %d", segmentsBefore)
	}

	if err := gsA.DeleteRange(1, 20); err != nil {
		t.Fatal(err)
	}
	if err := gsB.DeleteRange(1, 20); err != nil {
		t.Fatal(err)
	}
	syncBarrier(t, w)

	if reclaimed.Load() == 0 {
		t.Fatal("expected reclaimed segments > 0")
	}
	if reclaimedBytes.Load() <= 0 {
		t.Fatalf("expected reclaimed bytes > 0, got %d", reclaimedBytes.Load())
	}
	assertLiveBytesInvariant(t, w, "after reclamation")

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	segmentsAfter := countWalSegments(t, dir)
	if segmentsAfter > segmentsBefore {
		t.Fatalf("expected no segment growth after reclamation, before=%d after=%d", segmentsBefore, segmentsAfter)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	ga := w2.GroupStore("group-a")
	gb := w2.GroupStore("group-b")

	firstA, _ := ga.FirstIndex()
	lastA, _ := ga.LastIndex()
	if firstA != 21 || lastA != 24 {
		t.Fatalf("group-a first=%d last=%d, want 21..24", firstA, lastA)
	}
	firstB, _ := gb.FirstIndex()
	lastB, _ := gb.LastIndex()
	if firstB != 21 || lastB != 24 {
		t.Fatalf("group-b first=%d last=%d, want 21..24", firstB, lastB)
	}

	var log hraft.Log
	if err := ga.GetLog(10, &log); err != hraft.ErrLogNotFound {
		t.Fatalf("expected truncated log miss for group-a index 10, got %v", err)
	}
	if err := ga.GetLog(22, &log); err != nil {
		t.Fatalf("group-a GetLog 22: %v", err)
	}
	if err := gb.GetLog(22, &log); err != nil {
		t.Fatalf("group-b GetLog 22: %v", err)
	}

	term, _ := ga.GetUint64([]byte("term"))
	if term != 7 {
		t.Fatalf("group-a term=%d want 7", term)
	}
	vote, _ := gb.Get([]byte("vote"))
	if string(vote) != "n2" {
		t.Fatalf("group-b vote=%q want n2", vote)
	}
}

func TestSegmentReclamationPreservesSparseIndexAfterRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	cfg := Config{
		SegmentTargetSize:    1024,
		ScavengeMaxLiveBytes: 128,
	}
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("sparse")

	for i := uint64(1); i <= 10; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("old")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := gs.DeleteRange(1, 10); err != nil {
		t.Fatal(err)
	}
	for i := uint64(100); i <= 104; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 2, Data: []byte("new")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := gs.DeleteRange(11, 99); err != nil {
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

	gs2 := w2.GroupStore("sparse")
	first, _ := gs2.FirstIndex()
	last, _ := gs2.LastIndex()
	if first != 100 || last != 104 {
		t.Fatalf("first=%d last=%d, want 100..104", first, last)
	}

	var log hraft.Log
	if err := gs2.GetLog(50, &log); err != hraft.ErrLogNotFound {
		t.Fatalf("expected ErrLogNotFound for truncated sparse gap index 50, got %v", err)
	}
	if err := gs2.GetLog(102, &log); err != nil {
		t.Fatalf("GetLog 102: %v", err)
	}
}

func countWalSegments(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), walFilePrefix) && strings.HasSuffix(e.Name(), walFileSuffix) {
			count++
		}
	}
	return count
}
