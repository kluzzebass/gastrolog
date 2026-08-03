package raftwal

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	// Keep the scavenge threshold a small fraction of the segment, as it is
	// in production (4 MiB of a 64 MiB segment). The default is larger than
	// these tiny test segments, which would make every segment scavengeable
	// and turn reclamation into a full rewrite on every pass.
	if cfg.ScavengeMaxLiveBytes == 0 {
		cfg.ScavengeMaxLiveBytes = 128
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
	w2, err := Open(dir, Config{SegmentTargetSize: 2048})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	assertLiveBytesInvariant(t, w2, "after replay")
}

// syncBarrier orders the test after any reclamation the writer is running:
// the barrier write's round-trip completes only after the previous flush's
// reclamation pass finished, because the writer goroutine is serial.
func syncBarrier(t *testing.T, w *WAL) {
	t.Helper()
	if err := w.GroupStore("barrier").SetUint64([]byte("b"), 1); err != nil {
		t.Fatalf("barrier: %v", err)
	}
}

// triggerReclaim fires one wired reclamation pass on the writer (an empty
// DeleteRange is a trigger and masks nothing) and waits for it to finish.
func triggerReclaim(t *testing.T, w *WAL, gs *GroupStore) {
	t.Helper()
	if err := gs.DeleteRange(1, 0); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	syncBarrier(t, w)
}

// fillAndDrain appends entries [from..to] of size payloadLen and deletes
// them all, so the segments they landed in drain.
func fillAndDrain(t *testing.T, gs *GroupStore, from, to uint64, payloadLen int) {
	t.Helper()
	for i := from; i <= to; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, payloadLen)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	if err := gs.DeleteRange(from, to); err != nil {
		t.Fatalf("delete range: %v", err)
	}
}

func segmentFileCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, walFilePrefix) && strings.HasSuffix(name, walFileSuffix) {
			info, err := e.Info()
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() > 0 { // ignore the preallocated-but-empty spare
				n++
			}
		}
	}
	return n
}

func TestReclaimUnlinksDrainedSegments(t *testing.T) {
	var mu sync.Mutex
	var reclaimed []ReclaimStats
	w, dir := openTestWAL(t, Config{
		OnReclaim: func(s ReclaimStats) { mu.Lock(); reclaimed = append(reclaimed, s); mu.Unlock() },
	})
	gs := w.GroupStore("grp")

	// The registration record pins segment 1 until the pass scavenges those
	// few bytes forward; everything behind it drains and unlinks outright.
	// Seal the drained segments behind a live append, then let the wired
	// pass run.
	fillAndDrain(t, gs, 1, 60, 64)
	if err := gs.StoreLog(&hraft.Log{Index: 100, Term: 1, Data: []byte("live")}); err != nil {
		t.Fatalf("store live: %v", err)
	}
	triggerReclaim(t, w, gs)

	// Structural bound: segment 1 (registration-pinned), the active
	// segment, and at most one sealed predecessor of it survive; the
	// drained bulk between them is gone.
	if n := segmentFileCount(t, dir); n > 3 {
		t.Fatalf("segment count = %d after reclamation, want <= 3", n)
	}
	mu.Lock()
	got := len(reclaimed)
	pureUnlinks := 0
	for _, s := range reclaimed {
		if s.ReclaimedBytes <= 0 {
			t.Errorf("ReclaimedBytes = %d, want > 0", s.ReclaimedBytes)
		}
		if s.ScavengedBytes > 128 {
			t.Errorf("ScavengedBytes = %d exceeds threshold 128", s.ScavengedBytes)
		}
		if s.ScavengedBytes == 0 {
			pureUnlinks++
		}
	}
	mu.Unlock()
	if got == 0 {
		t.Fatal("OnReclaim never invoked")
	}
	if pureUnlinks == 0 {
		t.Error("no drained segment was unlinked without scavenging")
	}
	assertLiveBytesInvariant(t, w, "after reclaim")

	var lg hraft.Log
	if err := gs.GetLog(30, &lg); err != hraft.ErrLogNotFound {
		t.Errorf("GetLog(30) = %v, want ErrLogNotFound", err)
	}
	if err := gs.GetLog(100, &lg); err != nil {
		t.Errorf("GetLog(100): %v", err)
	}
}

func TestReclaimIsStrictlyOldestFirst(t *testing.T) {
	w, dir := openTestWAL(t, Config{})
	gs := w.GroupStore("grp")

	// Segment 1 holds the registration + live entries 1..20. Later segments
	// drain completely. Oldest-first: NOTHING may be unlinked while the
	// still-live segment 1 is the oldest.
	for i := uint64(1); i <= 20; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 64)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	fillAndDrain(t, gs, 21, 80, 64)
	if err := gs.StoreLog(&hraft.Log{Index: 100, Term: 1, Data: []byte("seal")}); err != nil {
		t.Fatalf("seal: %v", err)
	}
	syncBarrier(t, w) // any wired pass from the drain has finished
	before := segmentFileCount(t, dir)

	triggerReclaim(t, w, gs)

	if after := segmentFileCount(t, dir); after != before {
		t.Fatalf("reclaimed %d segment(s) despite a live older segment", before-after)
	}
}

func TestReclaimAnomalyQuarantinesSegment(t *testing.T) {
	var anomalies atomic.Int32
	w, dir := openTestWAL(t, Config{
		OnReclaimAnomaly: func(seq, liveRefs int) { anomalies.Add(1) },
	})
	gs := w.GroupStore("grp")
	// Live entries first so segment 1 stays live even without its
	// registration record counted.
	for i := uint64(1); i <= 5; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 32)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	fillAndDrain(t, gs, 6, 60, 64)
	if err := gs.StoreLog(&hraft.Log{Index: 100, Term: 1, Data: []byte("live")}); err != nil {
		t.Fatalf("store: %v", err)
	}
	syncBarrier(t, w)

	// Inject counter drift: zero the oldest sealed segment's counter while
	// it still holds live records.
	w.stateMu.Lock()
	victim := w.oldestSealedSegment()
	w.segLive[victim] = 0
	w.stateMu.Unlock()
	victimPath := w.segmentPath(victim)

	triggerReclaim(t, w, gs)
	triggerReclaim(t, w, gs) // quarantine must suppress a second anomaly

	if n := anomalies.Load(); n != 1 {
		t.Errorf("anomalies = %d, want exactly 1", n)
	}
	if _, err := os.Stat(victimPath); err != nil {
		t.Fatalf("segment with live references was unlinked: %v", err)
	}
	_ = dir
}

// The opposite drift to TestReclaimAnomalyQuarantinesSegment: the counter
// claims live bytes the index does not reference. Such a segment is never
// nominated for unlink and, oldest-first, pins every segment behind it — so
// the scavenge scan finding nothing to carry must report it rather than
// return quietly.
func TestReclaimAnomalyDetectsInverseCounterDrift(t *testing.T) {
	var mu sync.Mutex
	var reportedRefs []int
	w, dir := openTestWAL(t, Config{
		OnReclaimAnomaly: func(_, liveRefs int) {
			mu.Lock()
			reportedRefs = append(reportedRefs, liveRefs)
			mu.Unlock()
		},
	})
	gs := w.GroupStore("grp")

	// Fill segment 1 past the scavenge threshold so nothing reclaims it
	// while the entries are live.
	for i := uint64(1); i <= 60; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 64)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	syncBarrier(t, w) // writer idle: no pass can run under the injection

	// Inject a missed debit on segment 1 — bytes the counter carries that
	// no index entry accounts for.
	const phantomBytes = 40
	w.stateMu.Lock()
	victim := w.oldestSealedSegment()
	w.segLive[victim] += phantomBytes
	w.stateMu.Unlock()
	if victim == 0 {
		t.Fatal("test premise: no sealed segment to drift")
	}
	victimPath := w.segmentPath(victim)

	// Kill every real record in it. The pass scavenges the registration out
	// and the victim is left holding nothing but the phantom count, which
	// keeps it off the unlink path forever.
	if err := gs.DeleteRange(1, 60); err != nil {
		t.Fatalf("delete range: %v", err)
	}
	syncBarrier(t, w)

	w.stateMu.RLock()
	stillLive := w.segLive[victim]
	realRefs := w.liveRefsForSegment(victim)
	w.stateMu.RUnlock()
	if stillLive != phantomBytes || realRefs != 0 {
		t.Fatalf("test premise: segment %d live=%d refs=%d, want %d and 0",
			victim, stillLive, realRefs, phantomBytes)
	}

	before := segmentFileCount(t, dir)
	triggerReclaim(t, w, gs)
	triggerReclaim(t, w, gs) // quarantine must suppress a second anomaly

	mu.Lock()
	got := append([]int(nil), reportedRefs...)
	mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("anomalies = %v, want exactly one", got)
	}
	if got[0] != 0 {
		t.Errorf("reported liveRefs = %d, want 0 (counter overstates an unreferenced segment)", got[0])
	}
	if _, err := os.Stat(victimPath); err != nil {
		t.Errorf("drifted segment must be retained: %v", err)
	}
	if after := segmentFileCount(t, dir); after < before {
		t.Errorf("reclaimed %d segment(s) behind a quarantined head", before-after)
	}
}

// Reclamation is deliberately lazy: a segment whose live remainder exceeds
// ScavengeMaxLiveBytes is retained until truncation drains it, and
// oldest-first keeps every segment behind it. This pins that documented
// stall, which on a shared WAL is one group holding space for all of them.
func TestReclaimStallsBehindHeadSegmentAboveThreshold(t *testing.T) {
	const threshold = 512
	w, dir := openTestWAL(t, Config{
		SegmentTargetSize:    1024,
		ScavengeMaxLiveBytes: threshold,
	})
	gs := w.GroupStore("grp")

	// Each cycle truncates all but its last entry, so survivors accumulate
	// at the head instead of draining it.
	payload := make([]byte, 256)
	idx := uint64(0)
	for range 12 {
		for range 10 {
			idx++
			if err := gs.StoreLog(&hraft.Log{Index: idx, Term: 1, Data: payload}); err != nil {
				t.Fatalf("store %d: %v", idx, err)
			}
		}
		if err := gs.DeleteRange(idx-9, idx-1); err != nil {
			t.Fatalf("delete range ending at %d: %v", idx, err)
		}
		syncBarrier(t, w)
	}

	w.stateMu.RLock()
	head := w.oldestSealedSegment()
	headLive := w.segLive[head]
	w.stateMu.RUnlock()
	if head == 0 || headLive <= threshold {
		t.Fatalf("test premise: head segment %d live=%d, want a head above the %d threshold",
			head, headLive, threshold)
	}

	before := segmentFileCount(t, dir)
	triggerReclaim(t, w, gs)
	triggerReclaim(t, w, gs)

	if after := segmentFileCount(t, dir); after < before {
		t.Errorf("reclaimed %d segment(s) behind a head above the threshold", before-after)
	}
	w.stateMu.RLock()
	stillHead := w.oldestSealedSegment()
	w.stateMu.RUnlock()
	if stillHead != head {
		t.Errorf("head segment moved from %d to %d; reclamation passed a segment above the threshold", head, stillHead)
	}
	if _, err := os.Stat(w.segmentPath(head)); err != nil {
		t.Errorf("head segment above the threshold must be retained: %v", err)
	}
	assertLiveBytesInvariant(t, w, "stalled behind an over-threshold head")
}

func TestScavengeReclaimsNearlyDrainedSegment(t *testing.T) {
	var mu sync.Mutex
	var reclaimed []ReclaimStats
	w, dir := openTestWAL(t, Config{
		ScavengeMaxLiveBytes: 512,
		OnReclaim:            func(s ReclaimStats) { mu.Lock(); reclaimed = append(reclaimed, s); mu.Unlock() },
	})
	gs := w.GroupStore("grp")

	// Segment 1 ends up holding the registration, a stable key, and a few
	// live log entries — all below the scavenge threshold — plus dead bulk.
	if err := gs.SetUint64([]byte("CurrentTerm"), 7); err != nil {
		t.Fatalf("set term: %v", err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 7, Data: []byte{byte(i)}}); err != nil {
			t.Fatalf("store survivor %d: %v", i, err)
		}
	}
	fillAndDrain(t, gs, 4, 60, 64)
	if err := gs.StoreLog(&hraft.Log{Index: 100, Term: 7, Data: []byte("seal")}); err != nil {
		t.Fatalf("seal: %v", err)
	}
	triggerReclaim(t, w, gs)
	triggerReclaim(t, w, gs) // one scavenge per pass; give it a second pass

	if n := segmentFileCount(t, dir); n > 2 {
		t.Fatalf("segment count = %d after scavenging, want <= 2", n)
	}
	mu.Lock()
	var scavenged bool
	for _, s := range reclaimed {
		if s.ScavengedBytes > 0 {
			scavenged = true
			if s.ScavengedBytes > 512 {
				t.Errorf("ScavengedBytes = %d exceeds threshold 512", s.ScavengedBytes)
			}
		}
	}
	mu.Unlock()
	if !scavenged {
		t.Fatal("no scavenge reported; the registration-pinned oldest segment was never freed")
	}
	assertLiveBytesInvariant(t, w, "after scavenge")

	// Survivors intact, served from their new locations.
	for i := uint64(1); i <= 3; i++ {
		var lg hraft.Log
		if err := gs.GetLog(i, &lg); err != nil {
			t.Fatalf("GetLog(%d) after scavenge: %v", i, err)
		}
		if lg.Term != 7 || len(lg.Data) != 1 || lg.Data[0] != byte(i) {
			t.Errorf("GetLog(%d) = %+v", i, lg)
		}
	}
	if v, err := gs.GetUint64([]byte("CurrentTerm")); err != nil || v != 7 {
		t.Errorf("GetUint64 = %d, %v", v, err)
	}

	// The scavenged copies are the durable truth across replay.
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	w2, err := Open(dir, Config{SegmentTargetSize: 2048, ScavengeMaxLiveBytes: 512})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	gs2 := w2.GroupStore("grp")
	var lg hraft.Log
	if err := gs2.GetLog(2, &lg); err != nil || lg.Term != 7 {
		t.Errorf("after replay GetLog(2) = %+v, %v", lg, err)
	}
	if v, err := gs2.GetUint64([]byte("CurrentTerm")); err != nil || v != 7 {
		t.Errorf("after replay GetUint64 = %d, %v", v, err)
	}
}

func TestScavengeSkipsSegmentAboveThreshold(t *testing.T) {
	w, dir := openTestWAL(t, Config{ScavengeMaxLiveBytes: 64})
	gs := w.GroupStore("grp")
	for i := uint64(1); i <= 20; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 64)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	if err := gs.DeleteRange(1, 2); err != nil { // mostly live
		t.Fatalf("delete: %v", err)
	}
	if err := gs.StoreLog(&hraft.Log{Index: 100, Term: 1, Data: []byte("seal")}); err != nil {
		t.Fatalf("seal: %v", err)
	}
	syncBarrier(t, w)
	before := segmentFileCount(t, dir)
	triggerReclaim(t, w, gs)
	if after := segmentFileCount(t, dir); after != before {
		t.Fatal("segment above scavenge threshold was reclaimed")
	}
}

func TestScavengeFsyncFailureLeavesVictimIntact(t *testing.T) {
	fail := &atomic.Bool{}
	w, dir := openTestWAL(t, Config{
		ScavengeMaxLiveBytes: 512,
		SegmentSync: func(f *os.File) error {
			if fail.Load() {
				return errHarnessSyncFail
			}
			return f.Sync()
		},
	})
	gs := w.GroupStore("grp")
	if err := gs.SetUint64([]byte("CurrentTerm"), 7); err != nil {
		t.Fatalf("set: %v", err)
	}
	fillAndDrain(t, gs, 1, 60, 64)
	if err := gs.StoreLog(&hraft.Log{Index: 100, Term: 7, Data: []byte("seal")}); err != nil {
		t.Fatalf("seal: %v", err)
	}
	syncBarrier(t, w) // drained bulk unlinked; the pinned oldest remains
	before := segmentFileCount(t, dir)

	fail.Store(true)
	// The trigger batch's own fsync fails (SegmentSync covers batch fsync
	// too), so the pass never runs; a scavenge attempted by a later
	// successful batch is exercised by clearing the failure below. What
	// must hold under injected failure: victim intact, index unchanged.
	_ = gs.DeleteRange(1, 0) // returns the injected error; ignore it
	fail.Store(false)
	syncBarrier(t, w)
	if after := segmentFileCount(t, dir); after < before {
		t.Fatal("victim unlinked despite fsync failure")
	}
	assertLiveBytesInvariant(t, w, "after failed trigger")
	if v, err := gs.GetUint64([]byte("CurrentTerm")); err != nil || v != 7 {
		t.Errorf("stable read = %d, %v", v, err)
	}

	triggerReclaim(t, w, gs) // clean retry scavenges and unlinks
	if after := segmentFileCount(t, dir); after >= before {
		t.Fatal("retry after clearing fsync failure reclaimed nothing")
	}
	assertLiveBytesInvariant(t, w, "after retried scavenge")
}

// The Raft pattern reclamation is built for: append a batch, snapshot, then
// truncate the whole log prefix. Repeated cycles must keep the WAL bounded —
// a reclamation gate that never opens leaves it growing without bound as
// truncations pile up. The exact steady state is an implementation detail;
// staying far below the unreclaimed count is the invariant.
func TestReclaimKeepsWALBoundedAcrossTruncationCycles(t *testing.T) {
	w, dir := openTestWAL(t, Config{
		SegmentTargetSize:    1024,
		ScavengeMaxLiveBytes: 512,
	})
	gs := w.GroupStore("grp")

	payload := make([]byte, 256)
	const cycles = 12
	const perCycle = 10

	idx := uint64(0)
	peak := 0
	for range cycles {
		for range perCycle {
			idx++
			if err := gs.StoreLog(&hraft.Log{Index: idx, Term: 1, Data: payload}); err != nil {
				t.Fatalf("store %d: %v", idx, err)
			}
		}
		// Truncate to the newest entry, as a Raft group does after a
		// snapshot: the head genuinely drains, cycle after cycle.
		if err := gs.DeleteRange(1, idx-1); err != nil {
			t.Fatalf("delete range ending at %d: %v", idx, err)
		}
		syncBarrier(t, w)
		if n := segmentFileCount(t, dir); n > peak {
			peak = n
		}
	}

	// 12 cycles x 10 entries x 256B against a 1 KiB target is tens of
	// segments when nothing is reclaimed.
	if peak > 8 {
		t.Errorf("peak data-bearing segments = %d, want <= 8 (the WAL is not being reclaimed)", peak)
	}
	assertLiveBytesInvariant(t, w, "after truncation cycles")

	// The survivor must still read back after all that reclamation.
	var lg hraft.Log
	if err := gs.GetLog(idx, &lg); err != nil {
		t.Fatalf("GetLog(%d) after %d truncation cycles: %v", idx, cycles, err)
	}
	if len(lg.Data) != len(payload) {
		t.Errorf("payload len = %d, want %d", len(lg.Data), len(payload))
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

// Crash window: copies fsynced, index never repointed, victim never
// unlinked. Replay applies victim then copies (idempotent duplicates, later
// wins), leaves the victim drained, and the Open-time pass unlinks it.
//
// ScavengeMaxLiveBytes: 1 keeps the wired pass from scavenging the victim
// on its own; the manual steps below ARE the scavenge, halted mid-way.
// syncBarrier quiesces the writer before the test touches writer-owned
// segment fields via appendScavenge.
func TestCrashAfterScavengeCopiesBeforeSwap(t *testing.T) {
	cfg := Config{SegmentTargetSize: 2048, ScavengeMaxLiveBytes: 1}
	w, dir := openTestWAL(t, cfg)
	gs := w.GroupStore("grp")
	if err := gs.SetUint64([]byte("CurrentTerm"), 7); err != nil {
		t.Fatalf("set: %v", err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 7, Data: []byte{byte(i)}}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	fillAndDrain(t, gs, 4, 60, 64)
	if err := gs.StoreLog(&hraft.Log{Index: 100, Term: 7, Data: []byte("seal")}); err != nil {
		t.Fatalf("seal: %v", err)
	}
	syncBarrier(t, w)

	w.stateMu.RLock()
	victim := w.oldestSealedSegment()
	w.stateMu.RUnlock()
	records, err := w.collectScavenge(victim)
	if err != nil {
		t.Fatalf("collectScavenge: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("expected live records in the oldest segment")
	}
	if _, err := w.appendScavenge(records); err != nil {
		t.Fatalf("appendScavenge: %v", err)
	}
	victimPath := w.segmentPath(victim)
	if err := w.Close(); err != nil { // crash: no swap, no unlink
		t.Fatalf("close: %v", err)
	}
	if _, err := os.Stat(victimPath); err != nil {
		t.Fatalf("victim must still exist at crash point: %v", err)
	}

	// Reopen with a workable threshold: replay makes the copies the live
	// index (later wins), the victim reads drained, Open's pass unlinks it.
	w2, err := Open(dir, Config{SegmentTargetSize: 2048, ScavengeMaxLiveBytes: 512})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	assertLiveBytesInvariant(t, w2, "after crash replay")

	if _, err := os.Stat(victimPath); !os.IsNotExist(err) {
		t.Errorf("victim not reclaimed at Open: stat err = %v", err)
	}
	gs2 := w2.GroupStore("grp")
	var lg hraft.Log
	if err := gs2.GetLog(2, &lg); err != nil || lg.Term != 7 {
		t.Errorf("GetLog(2) = %+v, %v", lg, err)
	}
	if v, err := gs2.GetUint64([]byte("CurrentTerm")); err != nil || v != 7 {
		t.Errorf("GetUint64 = %d, %v", v, err)
	}
}

// A DeleteRange whose targets were scavenged forward replays against emptier
// state than it originally saw (the mask applies before the copies). The
// first/last bounds bookkeeping must converge once the copies apply.
func TestMaskReplaysAgainstEmptierState(t *testing.T) {
	cfg := Config{SegmentTargetSize: 2048, ScavengeMaxLiveBytes: 1024}
	w, dir := openTestWAL(t, cfg)
	gs := w.GroupStore("grp")

	for i := uint64(1); i <= 30; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: make([]byte, 48)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	// Suffix truncation (AppendEntries-conflict shape) then prefix
	// truncation: survivors are 11..19, in the oldest segments.
	if err := gs.DeleteRange(20, 30); err != nil {
		t.Fatalf("suffix delete: %v", err)
	}
	if err := gs.DeleteRange(1, 10); err != nil {
		t.Fatalf("prefix delete: %v", err)
	}
	if err := gs.StoreLog(&hraft.Log{Index: 100, Term: 2, Data: []byte("seal")}); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Each pass scavenges at most one segment; drive passes until the
	// survivors have been carried forward past both DeleteRange records.
	for range 8 {
		triggerReclaim(t, w, gs)
	}
	assertLiveBytesInvariant(t, w, "after reclaim cycles")

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	w2, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	gs2 := w2.GroupStore("grp")

	first, err := gs2.FirstIndex()
	if err != nil || first != 11 {
		t.Errorf("FirstIndex = %d, %v; want 11", first, err)
	}
	last, err := gs2.LastIndex()
	if err != nil || last != 100 {
		t.Errorf("LastIndex = %d, %v; want 100", last, err)
	}
	var lg hraft.Log
	for i := uint64(11); i <= 19; i++ {
		if err := gs2.GetLog(i, &lg); err != nil {
			t.Errorf("GetLog(%d): %v", i, err)
		}
	}
	for _, i := range []uint64{1, 10, 20, 30} {
		if err := gs2.GetLog(i, &lg); err != hraft.ErrLogNotFound {
			t.Errorf("GetLog(%d) = %v, want ErrLogNotFound", i, err)
		}
	}
	assertLiveBytesInvariant(t, w2, "after replay")
}

// Repeated scavenge cycles across restarts: registrations and stable keys
// migrate forward indefinitely without loss or duplication.
func TestRepeatedScavengeCyclesSurviveRestarts(t *testing.T) {
	cfg := Config{SegmentTargetSize: 2048, ScavengeMaxLiveBytes: 512}
	dir := t.TempDir()
	next := uint64(1)
	for cycle := 0; cycle < 4; cycle++ {
		w, err := Open(dir, cfg)
		if err != nil {
			t.Fatalf("cycle %d open: %v", cycle, err)
		}
		gs := w.GroupStore("grp")
		if err := gs.SetUint64([]byte("CurrentTerm"), uint64(cycle+1)); err != nil {
			t.Fatalf("cycle %d set: %v", cycle, err)
		}
		fillAndDrain(t, gs, next, next+40, 64)
		next += 41
		if err := gs.StoreLog(&hraft.Log{Index: 1000 + uint64(cycle), Term: uint64(cycle + 1), Data: []byte("live")}); err != nil {
			t.Fatalf("cycle %d live: %v", cycle, err)
		}
		triggerReclaim(t, w, gs)
		assertLiveBytesInvariant(t, w, "cycle reclaim")
		if err := w.Close(); err != nil {
			t.Fatalf("cycle %d close: %v", cycle, err)
		}
	}
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("final open: %v", err)
	}
	defer func() { _ = w.Close() }()
	gs := w.GroupStore("grp")
	if v, err := gs.GetUint64([]byte("CurrentTerm")); err != nil || v != 4 {
		t.Errorf("CurrentTerm = %d, %v; want 4", v, err)
	}
	var lg hraft.Log
	for cycle := uint64(0); cycle < 4; cycle++ {
		if err := gs.GetLog(1000+cycle, &lg); err != nil {
			t.Errorf("GetLog(%d): %v", 1000+cycle, err)
		}
	}
	assertLiveBytesInvariant(t, w, "final")
}
