package raftwal

import (
	"testing"

	hraft "github.com/hashicorp/raft"
)

// openTestWAL opens a WAL in a temp dir with small segments so rotation and
// reclamation exercise real code paths without large I/O.
func openTestWAL(t *testing.T, cfg Config) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	if cfg.SegmentTargetSize == 0 {
		cfg.SegmentTargetSize = 2048
	}
	// Removed in the compaction-retirement task along with the field.
	if cfg.CompactionMinSegments == 0 {
		cfg.CompactionMinSegments = 1 << 30
	}
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, dir
}

func TestStableAndRegLocationsTracked(t *testing.T) {
	w, _ := openTestWAL(t, Config{})
	gs := w.GroupStore("grp")
	if err := gs.Set([]byte("CurrentTerm"), []byte{9}); err != nil {
		t.Fatalf("set: %v", err)
	}

	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	state := w.groups[gs.groupID]
	if state.regName != "grp" {
		t.Errorf("regName = %q, want %q", state.regName, "grp")
	}
	if state.regLoc.length == 0 {
		t.Errorf("regLoc not recorded: %+v", state.regLoc)
	}
	sv, ok := state.stable["CurrentTerm"]
	if !ok {
		t.Fatal("stable key missing")
	}
	if len(sv.value) != 1 || sv.value[0] != 9 {
		t.Errorf("stable value = %v", sv.value)
	}
	if sv.loc.length == 0 || sv.loc.seg == 0 {
		t.Errorf("stable loc not recorded: %+v", sv.loc)
	}
	var _ = hraft.Log{} // imports used heavily by later tests in this file
}

// assertLiveBytesInvariant recomputes live bytes by full index scan and diffs
// against the incremental counters. Every accounting bug shows up here.
func assertLiveBytesInvariant(t *testing.T, w *WAL, when string) {
	t.Helper()
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	want := w.recomputeSegLive()
	for seq, bytes := range w.segLive {
		if want[seq] != bytes {
			t.Errorf("%s: segLive[%d] = %d, recomputed %d", when, seq, bytes, want[seq])
		}
	}
	for seq, bytes := range want {
		if _, ok := w.segLive[seq]; !ok && bytes != 0 {
			t.Errorf("%s: segment %d has %d live bytes but no counter", when, seq, bytes)
		}
	}
}

func TestLiveBytesAccounting(t *testing.T) {
	w, _ := openTestWAL(t, Config{})
	gs := w.GroupStore("grp")
	assertLiveBytesInvariant(t, w, "after registration")

	// Single appends, spanning several rotations.
	for i := uint64(1); i <= 60; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 64)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	assertLiveBytesInvariant(t, w, "after appends")

	// Batched append (entryLogBatch sub-entry accounting).
	batch := []*hraft.Log{
		{Index: 61, Term: 1, Data: make([]byte, 32)},
		{Index: 62, Term: 1, Data: make([]byte, 32)},
	}
	if err := gs.StoreLogs(batch); err != nil {
		t.Fatalf("store batch: %v", err)
	}
	assertLiveBytesInvariant(t, w, "after batch")

	// Overwrite (same index, new term) moves the live bytes to the new record.
	if err := gs.StoreLog(&hraft.Log{Index: 62, Term: 2, Data: make([]byte, 128)}); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	assertLiveBytesInvariant(t, w, "after overwrite")

	// Stable set + overwrite.
	if err := gs.SetUint64([]byte("CurrentTerm"), 1); err != nil {
		t.Fatalf("set term: %v", err)
	}
	if err := gs.SetUint64([]byte("CurrentTerm"), 2); err != nil {
		t.Fatalf("bump term: %v", err)
	}
	assertLiveBytesInvariant(t, w, "after stable overwrite")

	// DeleteRange kills a prefix.
	if err := gs.DeleteRange(1, 40); err != nil {
		t.Fatalf("delete range: %v", err)
	}
	assertLiveBytesInvariant(t, w, "after delete range")

	// Replay rebuilds identical counters.
	dir := w.dir
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	w2, err := Open(dir, Config{SegmentTargetSize: 2048, CompactionMinSegments: 1 << 30})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	assertLiveBytesInvariant(t, w2, "after replay")
}
