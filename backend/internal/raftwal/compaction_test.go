package raftwal

// Compaction observability. Compaction rewrites every live entry on the shared
// WAL while the batch writer is otherwise idle to callers, so its cost lands in
// every group's append latency. Reporting the run is what separates "consensus
// stalled" from "consensus stalled because the WAL was rewriting 128 MB".

import (
	"testing"

	hraft "github.com/hashicorp/raft"
)

func TestCompactionReportsRunToObserver(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	var observed []CompactionStats
	w, err := Open(dir, Config{
		SegmentTargetSize:     1024,
		CompactionMinSegments: 2,
		OnCompaction: func(s CompactionStats) {
			observed = append(observed, s)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("group-a")
	payload := make([]byte, 256)
	for i := uint64(1); i <= 24; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: payload}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}
	if countWalSegments(t, dir) < 2 {
		t.Fatal("expected multiple segments before compaction")
	}

	if err := gs.DeleteRange(1, 20); err != nil {
		t.Fatal(err)
	}

	if len(observed) != 1 {
		t.Fatalf("observer calls = %d, want 1", len(observed))
	}
	got := observed[0]
	if got.Duration <= 0 {
		t.Errorf("Duration = %v, want > 0", got.Duration)
	}
	if got.ReclaimedSegments == 0 {
		t.Errorf("ReclaimedSegments = 0, want > 0")
	}
	if got.ReclaimedBytes <= 0 {
		t.Errorf("ReclaimedBytes = %d, want > 0", got.ReclaimedBytes)
	}
	if got != w.LastCompactionStats() {
		t.Errorf("observer stats %+v != LastCompactionStats %+v", got, w.LastCompactionStats())
	}
}

// dataBearingSegments counts segments holding bytes, excluding the always-present
// reserved spare — the quantity the compaction floor is expressed in.
func dataBearingSegments(t *testing.T, w *WAL) int {
	t.Helper()
	segments, err := w.listSegments()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, s := range segments {
		if s.size > 0 {
			n++
		}
	}
	return n
}

// Tightening the compaction floor risks the opposite failure of the bug it fixes:
// a gate that never opens leaves the WAL growing without bound as truncations
// pile up. Repeated append/truncate cycles must stay bounded, and the log must
// still read correctly afterwards.
func TestCompactionKeepsWALBoundedAcrossTruncationCycles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	w, err := Open(dir, Config{
		SegmentTargetSize:     1024,
		CompactionMinSegments: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("group-a")
	payload := make([]byte, 256)
	const cycles = 12
	const perCycle = 10

	idx := uint64(0)
	peak := 0
	for c := range cycles {
		for range perCycle {
			idx++
			if err := gs.StoreLog(&hraft.Log{Index: idx, Term: 1, Data: payload}); err != nil {
				t.Fatalf("StoreLog %d: %v", idx, err)
			}
		}
		if err := gs.DeleteRange(idx-perCycle+1, idx-1); err != nil {
			t.Fatalf("DeleteRange at cycle %d: %v", c, err)
		}
		if n := dataBearingSegments(t, w); n > peak {
			peak = n
		}
	}

	// Without compaction every cycle's segments accumulate: 12 cycles x 10
	// entries x 256B against a 1KiB target is far more than a handful of
	// segments. The exact steady state is an implementation detail; being
	// bounded well below the unreclaimed count is the invariant.
	if peak > 6 {
		t.Errorf("peak data-bearing segments = %d, want <= 6 (WAL is not being reclaimed)", peak)
	}

	// The surviving entry must still be readable after all that rewriting.
	var log hraft.Log
	if err := gs.GetLog(idx, &log); err != nil {
		t.Fatalf("GetLog %d after %d compaction cycles: %v", idx, cycles, err)
	}
	if len(log.Data) != len(payload) {
		t.Errorf("payload len = %d, want %d", len(log.Data), len(payload))
	}
	first, err := gs.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	last, err := gs.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if last != idx {
		t.Errorf("LastIndex = %d, want %d", last, idx)
	}
	if first > last {
		t.Errorf("FirstIndex %d > LastIndex %d", first, last)
	}
}

// The floor is expressed in data-bearing segments, so the boundary is where the
// second one appears. Pins both sides of the gate against drift.
func TestCompactionTriggersAtTheDataBearingFloor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	runs := 0
	w, err := Open(dir, Config{
		SegmentTargetSize:     1024,
		CompactionMinSegments: 2,
		OnCompaction:          func(CompactionStats) { runs++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("group-a")
	payload := make([]byte, 256)

	// Fill exactly one segment's worth, then truncate: below the floor.
	idx := uint64(0)
	for dataBearingSegments(t, w) < 1 || idx < 2 {
		idx++
		if err := gs.StoreLog(&hraft.Log{Index: idx, Term: 1, Data: payload}); err != nil {
			t.Fatal(err)
		}
		if dataBearingSegments(t, w) >= 2 {
			t.Fatalf("rolled to 2 data-bearing segments at index %d before the below-floor probe", idx)
		}
	}
	if err := gs.DeleteRange(1, 1); err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("compaction runs = %d at 1 data-bearing segment, want 0", runs)
	}

	// Roll into a second data-bearing segment, then truncate: at the floor.
	for dataBearingSegments(t, w) < 2 {
		idx++
		if err := gs.StoreLog(&hraft.Log{Index: idx, Term: 1, Data: payload}); err != nil {
			t.Fatal(err)
		}
	}
	if err := gs.DeleteRange(2, idx-1); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("compaction runs = %d at 2 data-bearing segments, want 1", runs)
	}
}

// The reserved spare is a preallocated, logically empty next segment that
// always exists. Counting it toward CompactionMinSegments satisfies the gate
// from the very first write, so every DeleteRange rewrites the entire live log
// no matter how little there is to reclaim. The gate must count segments that
// hold data.
func TestCompactionDeclinesWhenOnlyOneSegmentHoldsData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	calls := 0
	w, err := Open(dir, Config{
		SegmentTargetSize:     1 << 20, // everything fits in one segment
		CompactionMinSegments: 2,
		OnCompaction:          func(CompactionStats) { calls++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("group-a")
	for i := uint64(1); i <= 8; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("x")}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}

	segments, err := w.listSegments()
	if err != nil {
		t.Fatal(err)
	}
	withData := 0
	for _, s := range segments {
		if s.size > 0 {
			withData++
		}
	}
	if withData != 1 {
		t.Fatalf("segments holding data = %d, want 1 (test premise)", withData)
	}

	if err := gs.DeleteRange(1, 4); err != nil {
		t.Fatal(err)
	}

	if calls != 0 {
		t.Fatalf("observer calls = %d, want 0: %d segment(s) hold data, below CompactionMinSegments=2",
			calls, withData)
	}
}
