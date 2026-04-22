package raftwal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	gs := w.GroupStore("tier-1")

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

	gs := w.GroupStore("tier-1")

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

	gs := w.GroupStore("tier-1")

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

	g1 := w.GroupStore("tier-1")
	g2 := w.GroupStore("tier-2")

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
			gs := w.GroupStore(fmt.Sprintf("tier-%d", g))
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
		gs := w.GroupStore(fmt.Sprintf("tier-%d", g))
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
	gs := w.GroupStore("tier-1")
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

	gs2 := w2.GroupStore("tier-1")
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
	gs := w.GroupStore("tier-1")
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

	gs2 := w2.GroupStore("tier-1")
	last, _ := gs2.LastIndex()
	// Some entries should survive (those before the corruption).
	// Exact count depends on where byte 50 falls.
	if last > 5 {
		t.Fatalf("last=%d, should be <= 5 after CRC corruption", last)
	}
}

func TestConcurrentStoreLogsStress(t *testing.T) {
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
			gs := w.GroupStore(fmt.Sprintf("tier-%d", g))
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
		gs := w.GroupStore(fmt.Sprintf("tier-%d", g))
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
	gs := w.GroupStore("tier-1")
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

	gs2 := w2.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

// Regression: suffix-style DeleteRange must not poison reads of the surviving
// prefix (hashicorp/raft appendEntries conflict path). A too-wide "deleted"
// horizon previously made GetLog panic the Raft node via ErrLogNotFound.
func TestDeleteRangeSuffixPreservesPrefix(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

func TestStableStoreOverwrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("tier-1")

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

func TestConcurrentReadWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("tier-1")

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

	// Writer: keeps appending.
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

	// Stable writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(0); ; i++ {
			select {
			case <-done:
				return
			default:
			}
			if err := gs.SetUint64([]byte("counter"), i); err != nil {
				recordErr(err)
				return
			}
		}
	}()

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
	gs := w.GroupStore("tier-1")
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
		"tier/with/slashes",
		"tier with spaces",
		"tier-with-dashes-and-019d87f1-3ec2-7144-a042-uuid",
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

	gs1 := w.GroupStore("tier-1")
	gs2 := w.GroupStore("tier-1")

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

	gs := w.GroupStore("tier-1")
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

	gs := w.GroupStore("tier-1")
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
	gs := w.GroupStore("tier-1")
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

	gs2 := w2.GroupStore("tier-1")
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

func TestSegmentCompactionReclaimsOldFilesAndPreservesState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	w, err := Open(dir, Config{
		SegmentTargetSize:     1024,
		CompactionMinSegments: 2,
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
		t.Fatalf("expected multiple segments before compaction, got %d", segmentsBefore)
	}

	if err := gsA.DeleteRange(1, 20); err != nil {
		t.Fatal(err)
	}
	if err := gsB.DeleteRange(1, 20); err != nil {
		t.Fatal(err)
	}

	stats := w.LastCompactionStats()
	if stats.ReclaimedSegments == 0 {
		t.Fatalf("expected reclaimed segments > 0, got %+v", stats)
	}
	if stats.ReclaimedBytes <= 0 {
		t.Fatalf("expected reclaimed bytes > 0, got %+v", stats)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	segmentsAfter := countWalSegments(t, dir)
	if segmentsAfter > segmentsBefore {
		t.Fatalf("expected no segment growth after compaction, before=%d after=%d", segmentsBefore, segmentsAfter)
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
		t.Fatalf("expected compacted log miss for group-a index 10, got %v", err)
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

func TestSegmentCompactionPreservesSparseIndexAfterRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	w, err := Open(dir, Config{
		SegmentTargetSize:     1024,
		CompactionMinSegments: 2,
	})
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

	w2, err := Open(dir)
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
		t.Fatalf("expected ErrLogNotFound for compacted sparse gap index 50, got %v", err)
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
