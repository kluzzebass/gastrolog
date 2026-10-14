# Raft WAL Prefix Scavenging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the raftwal monolithic compaction (which blocks every Raft group's appends for up to 12 s) with incremental, oldest-first segment reclamation whose writer-inline work is bounded by a 4 MiB scavenge threshold.

**Architecture:** Per-segment live-payload-bytes accounting maintained at the existing index-mutation sites under `stateMu`; a reclamation pass on the batch-writer goroutine (strictly after waiter notification) that unlinks drained oldest segments and scavenges at most one nearly-drained one per pass by re-appending its live records through the normal write path. A pre-unlink verification scan distrusts the counter. Spec: `docs/raftwal-scavenging-design.md`. Issue: gastrolog-23iln4.

**Tech Stack:** Go 1.26+, hashicorp/raft, `go test` (in-package white-box tests, `package raftwal`).

## Global Constraints

- Branch: `fix/gastrolog-23iln4-raftwal-prefix-scavenging` (already checked out). All commits go here.
- **NEVER touch the user's running cluster.** In-process `go test` only.
- Run Go commands from `backend/` (module root). Build only via `just build`; never ad-hoc `go build -o`.
- No issue IDs anywhere in code, comments, test names, or log messages — commit messages only.
- Comments describe present-tense behavior only; no history, no "previously"; comment only where the code cannot say it.
- Single-writer invariant is sacred: no new goroutine may touch `seg`, `segPath`, `segSize`, `segSeq`, `sparePath`. Everything in this plan runs on the batch writer, in `Open` before the writer starts, or under `stateMu`.
- Never gate a test on wall-clock elapsed time; assert on state and ordering. Waiting = a write's round-trip (the writer is serial), never a sleep.
- New slow tests get `testing.Short()` skips with a one-line reason.
- Final gates (Task 7): `go test -race ./internal/raftwal/`, `just test`, `just backend test-full`.
- On-disk record format is unchanged; scavenging reuses `entryLog`, `entryStableSet`, `entryGroupReg`.

## Task order rationale (read before Task 1)

Compaction is deleted and reclamation wired in Task 3, BEFORE the scavenge path exists. Two invariants force this order:

1. Legacy compaction swaps index locations without maintaining the new live-bytes counters. Any test triggering it after Task 2 would corrupt the counters. Tasks 1–2 therefore disable it in their test configs (`CompactionMinSegments: 1 << 30`) and Task 3 removes it entirely.
2. Once reclamation is wired into `flushBatch`, calling `w.reclaimPass()` from a test goroutine would race the writer's own pass on the writer-owned segment fields. So after Task 3, tests never call `reclaimPass` directly — they fire the wired trigger and use a write round-trip as a barrier (`triggerReclaim` helper, defined in Task 3).

Between Tasks 3 and 4, registration-pinned segments are simply retained (unlink-only reclamation) — a space-laziness window with zero correctness impact, closed when scavenging lands.

---

### Task 1: Stable-key and registration location tracking

Pure groundwork: the in-memory index gains durable-location tracking for stable keys and group registrations, so later tasks can account and scavenge them. No behavior change.

**Files:**
- Modify: `backend/internal/raftwal/wal.go` (groupState, `applyToMemory`, `collectCompactionSnapshot`)
- Modify: `backend/internal/raftwal/groupstore.go` (`Get`, `GetUint64`)
- Test: `backend/internal/raftwal/reclaim_test.go` (new file)

**Interfaces:**
- Consumes: existing `logLoc{seg, off, length}`, `applyToMemory(groupID, typ, payload, seg, payloadOff)`.
- Produces: `type stableVal struct { value []byte; loc logLoc }`; `groupState.stable map[string]stableVal`; `groupState.regName string`; `groupState.regLoc logLoc`. Later tasks rely on these exact names.

- [ ] **Step 1: Write the failing test**

Create `backend/internal/raftwal/reclaim_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run from `backend/`: `go test ./internal/raftwal/ -run TestStableAndRegLocationsTracked -v`
Expected: compile FAILURE — `state.regName undefined`, `sv.value undefined` (stable is `map[string][]byte`).

- [ ] **Step 3: Implement the type change**

In `wal.go`:

1. Add above `groupState`:

```go
// stableVal is a stable-store value plus the durable location of the record
// that last set it, so reclamation can tell which segment the live copy
// occupies.
type stableVal struct {
	value []byte
	loc   logLoc
}
```

2. In `groupState`, change `stable map[string][]byte` to `stable map[string]stableVal` and add:

```go
	// Live registration record for this group: the name and the durable
	// location of the entryGroupReg record replay would use.
	regName string
	regLoc  logLoc
```

3. Update `newGroupState` to `stable: make(map[string]stableVal)`.

4. In `applyToMemory`, compute `loc := logLoc{seg: seg, off: payloadOff, length: len(payload)}` at the top, then:
   - `entryStableSet`: `gs.stable[key] = stableVal{value: val, loc: loc}`
   - `entryStableUint64`: `gs.stable[key] = stableVal{value: buf, loc: loc}`
   - `entryGroupReg`: alongside the existing name/ID bookkeeping, `gs.regName = name; gs.regLoc = loc` (the `gs` fetch/create block at the top of the function already runs for every entry type).

5. In `collectCompactionSnapshot` (alive until Task 3), fix the stable read: `val := gs.stable[key].value`.

6. In `groupstore.go` `Get`: `sv, ok := gs.stable[string(key)]`, copy and return `sv.value`. In `GetUint64`: same, with the existing `len < 8` check against `sv.value`.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/raftwal/ -run TestStableAndRegLocationsTracked -v` — expected PASS.
Run: `go test ./internal/raftwal/` — expected PASS (pure refactor; all existing tests green).

- [ ] **Step 5: Commit**

```bash
git add backend/internal/raftwal/wal.go backend/internal/raftwal/groupstore.go backend/internal/raftwal/reclaim_test.go
git commit -m "feat(raftwal): track durable locations for stable keys and group registrations (gastrolog-23iln4)"
```

---

### Task 2: Per-segment live-bytes accounting

**Files:**
- Modify: `backend/internal/raftwal/wal.go` (WAL struct, `Open`, `applyToMemory`, `applyLogEntry`, `applyDeleteRange`, `rotateSegment`, `replaySegment`)
- Test: `backend/internal/raftwal/reclaim_test.go`

**Interfaces:**
- Consumes: Task 1's `stableVal`, `regLoc`, `regName`.
- Produces: `WAL.segLive map[int]int64` (segment seq → live payload bytes, guarded by `stateMu`); `(w *WAL) recomputeSegLive() map[int]int64` (caller holds `stateMu`); `(w *WAL) liveRefsForSegment(seq int) int` (caller holds `stateMu`); `(w *WAL) registerSegment(seq int)`; `applyLogEntry`/`applyDeleteRange` become WAL methods: `(w *WAL) applyLogEntry(gs *groupState, payload []byte, loc logLoc)` and `(w *WAL) applyDeleteRange(gs *groupState, payload []byte)`.

- [ ] **Step 1: Write the failing invariant test**

Append to `reclaim_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/raftwal/ -run TestLiveBytesAccounting -v`
Expected: compile FAILURE — `w.recomputeSegLive undefined`, `w.segLive undefined`.

- [ ] **Step 3: Implement accounting**

In `wal.go`:

1. WAL struct, next to `groups`:

```go
	// segLive tracks, per segment, the payload bytes of records the index
	// still references (live log entries, current stable values, group
	// registrations). Masks (entryDeleteRange) never count. Zero means no
	// live references. Guarded by stateMu.
	segLive map[int]int64
```

2. `Open`: add `segLive: make(map[int]int64),` to the struct literal (before `replay()` runs).

3. Segment registration so a data segment with zero live records is still a reclamation candidate:

```go
// registerSegment ensures a live-bytes counter exists for a data segment so
// a segment whose records all die (or that holds only masks) still becomes
// a reclamation candidate. Runs on the writer or during single-threaded
// replay; takes stateMu because readers may scan segLive concurrently.
func (w *WAL) registerSegment(seq int) {
	w.stateMu.Lock()
	if _, ok := w.segLive[seq]; !ok {
		w.segLive[seq] = 0
	}
	w.stateMu.Unlock()
}
```

Call it: in `rotateSegment` after `w.segSize = 0`, and as the first line of `replaySegment`. **Do not** call it from any context already holding `stateMu` (`rotateSegment` and `replaySegment` never do).

4. Convert `applyLogEntry` and `applyDeleteRange` from `groupState` methods to WAL methods, and update their callers in `applyToMemory` (cases `entryLog`, `entryLogBatch` inside `forEachBatchEntry`, and `entryDeleteRange`) to `w.applyLogEntry(gs, ...)` / `w.applyDeleteRange(gs, payload)`:

```go
// applyLogEntry indexes one encoded raft.Log at its durable location, moves
// live-bytes accounting off any overwritten record, and admits the payload
// to the recent window. Caller holds stateMu for writing.
func (w *WAL) applyLogEntry(gs *groupState, payload []byte, loc logLoc) {
	var log hraft.Log
	if err := decodelog(payload, &log); err != nil {
		return
	}
	if old, ok := gs.logs[log.Index]; ok {
		w.segLive[old.seg] -= int64(old.length)
	}
	w.segLive[loc.seg] += int64(loc.length)
	gs.logs[log.Index] = loc
	if gs.firstIndex == 0 || log.Index < gs.firstIndex {
		gs.firstIndex = log.Index
	}
	if log.Index > gs.lastIndex {
		gs.lastIndex = log.Index
	}
	gs.cacheStore(log.Index, payload, w.cfg.LogCacheBudgetBytes)
}
```

`applyDeleteRange`: keep today's body, add inside the deletion loop, before `delete(gs.logs, i)`:

```go
		if old, ok := gs.logs[i]; ok {
			w.segLive[old.seg] -= int64(old.length)
		}
```

5. `applyToMemory` accounting for the other live record kinds (using Task 1's `loc`):
   - `entryStableSet` / `entryStableUint64`: before assigning, `if old, ok := gs.stable[key]; ok { w.segLive[old.loc.seg] -= int64(old.loc.length) }`; after assigning, `w.segLive[loc.seg] += int64(loc.length)`.
   - `entryGroupReg`: `if gs.regName != "" { w.segLive[gs.regLoc.seg] -= int64(gs.regLoc.length) }` then `w.segLive[loc.seg] += int64(loc.length)` alongside setting `regName`/`regLoc`.
   - `entryDeleteRange` and unknown types: no credit — masks are never live.

6. Scan helpers at the bottom of `wal.go`:

```go
// recomputeSegLive rebuilds live-bytes counters by full index scan. Test and
// verification support. Caller holds stateMu.
func (w *WAL) recomputeSegLive() map[int]int64 {
	out := make(map[int]int64, len(w.segLive))
	for _, gs := range w.groups {
		for _, loc := range gs.logs {
			out[loc.seg] += int64(loc.length)
		}
		for _, sv := range gs.stable {
			out[sv.loc.seg] += int64(sv.loc.length)
		}
		if gs.regName != "" {
			out[gs.regLoc.seg] += int64(gs.regLoc.length)
		}
	}
	return out
}

// liveRefsForSegment counts index references into segment seq. Caller holds
// stateMu.
func (w *WAL) liveRefsForSegment(seq int) int {
	refs := 0
	for _, gs := range w.groups {
		for _, loc := range gs.logs {
			if loc.seg == seq {
				refs++
			}
		}
		for _, sv := range gs.stable {
			if sv.loc.seg == seq {
				refs++
			}
		}
		if gs.regName != "" && gs.regLoc.seg == seq {
			refs++
		}
	}
	return refs
}
```

Known, accepted mid-series state: legacy compaction does NOT maintain `segLive` (it is deleted in Task 3); the huge `CompactionMinSegments` in test configs keeps it from firing until then.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/raftwal/ -run TestLiveBytesAccounting -v` — expected PASS.
Run: `go test ./internal/raftwal/` — expected PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/internal/raftwal/wal.go backend/internal/raftwal/reclaim_test.go
git commit -m "feat(raftwal): per-segment live-bytes accounting with full-scan invariant test (gastrolog-23iln4)"
```

---

### Task 3: Retire compaction; wire drained-segment unlink with verification

The mechanism swap. Compaction is deleted, unlink-only reclamation is wired into `flushBatch` and `Open`, the legacy test suite is ported, and the app callbacks are rewired. The tree is red mid-task and green at the end.

**Files:**
- Create: `backend/internal/raftwal/reclaim.go`
- Modify: `backend/internal/raftwal/wal.go` (delete compaction, wire reclamation, Config)
- Delete: `backend/internal/raftwal/compaction_test.go`
- Modify: `backend/internal/raftwal/harness_test.go:22,219`, `reserve_test.go:279,308`, `retention_test.go:298,315-316,407`, `wal_invariants_test.go:180`, `wal_test.go:1436,1472,1536`, `reclaim_test.go` (drop the `CompactionMinSegments: 1 << 30` lines)
- Modify: `backend/internal/app/raft.go:189-218`, `backend/internal/app/app.go:1664-1665`
- Test: `backend/internal/raftwal/reclaim_test.go`

**Interfaces:**
- Consumes: Task 2's `segLive`, `liveRefsForSegment`, `registerSegment`.
- Produces (later tasks and app wiring depend on these exact names):
  - `type ReclaimStats struct { Seq int; ReclaimedBytes int64; ScavengedBytes int64; Duration time.Duration }`
  - `Config.ScavengeMaxLiveBytes int64` (default constant `scavengeMaxLiveBytes = 4 << 20`)
  - `Config.OnReclaim func(ReclaimStats)`, `Config.OnReclaimAnomaly func(seq int, liveRefs int)`
  - `(w *WAL) reclaimPass()` (unlink loop; Task 4 adds the scavenge call), `(w *WAL) oldestSealedSegment() int`, `(w *WAL) unlinkOldestDrained(scavengedBytes int64, start time.Time) bool`
  - Test helpers `triggerReclaim(t, w, gs)` and `syncBarrier(t, w)` — the ONLY way tests drive reclamation from here on.
  - `flushBatch` ordering: write → apply → fsync → **notify** → reclaim.
  - App helpers `walReclaimLog(logger *slog.Logger, walName string) func(raftwal.ReclaimStats)`, `walReclaimAnomalyAlarm(alerts *alert.Collector, logger *slog.Logger, walName string) func(seq, liveRefs int)`.

- [ ] **Step 1: Write the failing tests**

Append to `reclaim_test.go`:

```go
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

	// The registration record lands in segment 1 and pins it (until the
	// scavenge path exists); everything behind it drains and unlinks. Seal
	// the drained segments behind a live append, then let the wired pass run.
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
	for _, s := range reclaimed {
		if s.ReclaimedBytes <= 0 {
			t.Errorf("ReclaimedBytes = %d, want > 0", s.ReclaimedBytes)
		}
		if s.ScavengedBytes != 0 {
			t.Errorf("pure unlink reported ScavengedBytes = %d", s.ScavengedBytes)
		}
	}
	mu.Unlock()
	if got == 0 {
		t.Fatal("OnReclaim never invoked")
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
```

Add `"os"`, `"strings"`, `"sync"`, `"sync/atomic"` to the file's imports.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/raftwal/ -run 'TestReclaim' -v`
Expected: compile FAILURE — `ReclaimStats`, `OnReclaim`, `oldestSealedSegment` undefined.

- [ ] **Step 3: Implement reclaim.go and the Config surface**

Create `reclaim.go`:

```go
package raftwal

import (
	"os"
	"time"
)

// ReclaimStats describes one reclaimed segment.
type ReclaimStats struct {
	Seq            int
	ReclaimedBytes int64 // file bytes released by the unlink
	ScavengedBytes int64 // live payload bytes rewritten first (0 for a pure unlink)
	Duration       time.Duration
}

// reclaimPass reclaims dead WAL space. Runs on the batch writer strictly
// after batch waiters are notified: unlinks every drained oldest segment.
// The scavenge step for nearly-drained segments extends this pass.
func (w *WAL) reclaimPass() {
	for w.unlinkOldestDrained(0, time.Now()) {
	}
}

// oldestSealedSegment returns the lowest tracked segment sequence below the
// active segment, or 0 when none exists. Caller holds stateMu.
func (w *WAL) oldestSealedSegment() int {
	oldest := 0
	for seq := range w.segLive {
		if seq >= w.segSeq {
			continue
		}
		if oldest == 0 || seq < oldest {
			oldest = seq
		}
	}
	return oldest
}

// unlinkOldestDrained removes the oldest sealed segment if its counter reads
// drained AND a verification scan confirms no index reference survives — the
// counter only nominates. A disagreement quarantines the segment and raises
// OnReclaimAnomaly once. Returns whether a segment was removed.
func (w *WAL) unlinkOldestDrained(scavengedBytes int64, start time.Time) bool {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	seq := w.oldestSealedSegment()
	if seq == 0 || w.segLive[seq] != 0 {
		return false
	}
	if _, bad := w.quarantined[seq]; bad {
		return false
	}
	if refs := w.liveRefsForSegment(seq); refs > 0 {
		w.quarantined[seq] = struct{}{}
		if w.cfg.OnReclaimAnomaly != nil {
			w.cfg.OnReclaimAnomaly(seq, refs)
		}
		return false
	}

	path := w.segmentPath(seq)
	var reclaimedBytes int64
	if info, err := os.Stat(path); err == nil {
		reclaimedBytes = info.Size()
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return false
	}
	w.closeSegmentReadersUpTo(seq)
	delete(w.segLive, seq)

	if w.cfg.OnReclaim != nil {
		w.cfg.OnReclaim(ReclaimStats{
			Seq:            seq,
			ReclaimedBytes: reclaimedBytes,
			ScavengedBytes: scavengedBytes,
			Duration:       time.Since(start),
		})
	}
	return true
}
```

In `wal.go`:

1. WAL struct: add (initialize in `Open`):

```go
	// quarantined holds segments whose drained counter contradicted the
	// verification scan. They are excluded from reclamation — the
	// contradiction is a counter bug, and a restart rebuilds counters from
	// replay. Guarded by stateMu.
	quarantined map[int]struct{}
```

2. Config: add after `OnReserveState`:

```go
	// ScavengeMaxLiveBytes bounds the live payload bytes the writer will
	// rewrite in one reclamation pass to free the oldest segment. It is the
	// hard ceiling on reclamation's inline cost: about 6% of a default
	// segment and single-digit milliseconds of sequential write. Segments
	// above the bound are retained until truncation drains them further.
	// Default: 4 MiB.
	ScavengeMaxLiveBytes int64

	// OnReclaim, if non-nil, is invoked after each reclaimed segment.
	// Invoked from the batch-writer goroutine: must not block.
	OnReclaim func(ReclaimStats)

	// OnReclaimAnomaly, if non-nil, is invoked when the pre-unlink
	// verification scan finds live references in a segment whose counter
	// reads drained. The segment is retained and reclamation halts on it;
	// a restart rebuilds counters from replay. Invoked from the
	// batch-writer goroutine: must not block.
	OnReclaimAnomaly func(seq int, liveRefs int)
```

3. Constant next to `segmentTargetSize`:

```go
	// scavengeMaxLiveBytes is the default ceiling on live bytes rewritten to
	// reclaim the oldest segment in one pass.
	scavengeMaxLiveBytes = 4 << 20 // 4 MiB
```

4. `withDefaults`: `if c.ScavengeMaxLiveBytes <= 0 { c.ScavengeMaxLiveBytes = scavengeMaxLiveBytes }`.

- [ ] **Step 4: Delete compaction and wire reclamation**

In `wal.go`:

1. Delete: `compactSegments`, `collectCompactionSnapshot`, `writeCompactionSnapshot`, `appendCompactedEntry`, `snapshotRecord`, `compactionLogRef`, `compactionSnapshot`, `CompactionStats`, `LastCompactionStats`, the `lastCompaction` field, `Config.CompactionMinSegments`, `Config.OnCompaction`, and the `CompactionMinSegments` default in `withDefaults`. Update the package doc comment and the `stateMu` field comment (both name compaction).

2. Rewrite `flushBatch` — waiters first, then reclaim, triggered by truncation or rotation:

```go
// flushBatch writes all ops to the segment, fsyncs once, notifies callers,
// then reclaims dead segments. Segment I/O and fsync run without stateMu so
// reads can proceed concurrently; waiters are notified before reclamation so
// an already-fsynced op never waits on space management.
func (w *WAL) flushBatch(batch []writeOp) {
	if len(batch) == 0 {
		return
	}
	segSeqBefore := w.segSeq

	applied, writeErr, sawDeleteRange := w.appendBatchToSegment(batch)

	if len(applied) > 0 {
		w.stateMu.Lock()
		for _, rec := range applied {
			w.applyToMemory(rec.op.groupID, rec.op.typ, rec.op.payload, rec.seg, rec.payloadOff)
		}
		w.stateMu.Unlock()
	}

	// Single fsync for the entire batch — no stateMu held.
	syncErr := w.syncActiveSegment()

	w.notifyBatchWaiters(batch, syncErr)

	if syncErr == nil && writeErr == nil && (sawDeleteRange || w.segSeq != segSeqBefore) {
		w.reclaimPass()
	}
}
```

3. In `Open`, after `rotateSegment()` succeeds and BEFORE `go w.batchWriter()`:

```go
	// Collect segments a crash left drained (e.g. an interrupted scavenge
	// whose copies were fsynced). Single-threaded: the writer has not
	// started.
	w.reclaimPass()
```

- [ ] **Step 5: Port the legacy test suite**

- `rm backend/internal/raftwal/compaction_test.go` (reclamation semantics are covered by reclaim_test.go; the trigger-floor tests are obsolete with the trigger gone).
- `reclaim_test.go`: delete the `CompactionMinSegments` default in `openTestWAL` and the `CompactionMinSegments: 1 << 30` reopen literals (the field no longer exists).
- `harness_test.go:22`: replace `CompactionMinSegments: 2,` with `ScavengeMaxLiveBytes: 512,`. Line ~219 (`w.LastCompactionStats()`): rewrite the harness verification to record reclamation via an `OnReclaim` counter in the harness config; keep the harness's intent (reclamation actually ran).
- `reserve_test.go:279,308`: config gains `ScavengeMaxLiveBytes: 512`; the `LastCompactionStats` assertion becomes an `OnReclaim`-callback counter. The reserve assertions stay untouched — reclamation must keep them true.
- `retention_test.go:298,315-316,407`, `wal_invariants_test.go:180`, `wal_test.go:1436,1472,1536`: same substitution pattern.
- Ported tests asserting "space was reclaimed" after full truncation still pass with unlink-only reclamation (fully drained segments unlink); a test asserting a segment-count of exactly N may need N+1 until Task 4 lands, because the registration-pinned oldest segment is retained — prefer asserting `<=` a small structural bound and add a note ONLY if the bound had to change.
- In every ported test, add `assertLiveBytesInvariant(t, w, "...")` after the reclamation-triggering phase.

- [ ] **Step 6: Rewire app**

In `backend/internal/app/raft.go`, replace `walCompactionLog` (lines 189-206) with:

```go
// walReclaimLog reports each reclaimed WAL segment. Reclamation runs on the
// shared batch writer (bounded per pass), so the log line is the only
// operator-visible record of space being returned.
func walReclaimLog(logger *slog.Logger, walName string) func(raftwal.ReclaimStats) {
	if logger == nil {
		return nil
	}
	return func(s raftwal.ReclaimStats) {
		logger.Info("raft WAL segment reclaimed",
			"wal", walName,
			"segment", s.Seq,
			"duration", s.Duration,
			"reclaimed_bytes", s.ReclaimedBytes,
			"scavenged_bytes", s.ScavengedBytes)
	}
}

// walReclaimAnomalyAlarm raises a storage alarm when the reclamation
// verification scan contradicts the live-bytes counter: the segment is
// retained and reclamation halts on it until a restart rebuilds the
// counters.
func walReclaimAnomalyAlarm(alerts *alert.Collector, logger *slog.Logger, walName string) func(seq, liveRefs int) {
	return func(seq, liveRefs int) {
		if logger != nil {
			logger.Error("raft WAL reclamation halted — live-bytes counter contradicts verification scan",
				"wal", walName, "segment", seq, "live_refs", liveRefs)
		}
		if alerts != nil {
			alerts.Raise("wal-reclaim-anomaly", walName, fmt.Sprintf(
				"Raft WAL (%s) reclamation halted: segment %d nominated as drained still holds %d live references; restart the node to rebuild counters",
				walName, seq, liveRefs))
		}
	}
}
```

Wire both WAL opens — `openRaftClusterCtlStore` in `raft.go`:

```go
	wal, err := raftwal.Open(walDir, raftwal.Config{
		OnReserveState:   walReserveAlarm(opts.Alerts, opts.Logger, "cluster-ctl"),
		OnReclaim:        walReclaimLog(opts.Logger, "cluster-ctl"),
		OnReclaimAnomaly: walReclaimAnomalyAlarm(opts.Alerts, opts.Logger, "cluster-ctl"),
	})
```

and `setupMultiRaft` in `app.go` with the `"vault-ctl"` equivalents (`alerts`, `logger` are in scope there).

- [ ] **Step 7: Run the full gate**

Run: `go test ./internal/raftwal/ -run 'TestReclaim' -v` — expected PASS (3 tests).
Run: `go test ./internal/raftwal/` — expected PASS (legacy suite ported).
Run: `go build ./...` then `go test -short ./...` from `backend/` — expected PASS.
Verify: `grep -rn "CompactionMinSegments\|OnCompaction\|LastCompactionStats\|CompactionStats\|compactSegments" backend/ --include="*.go"` → zero hits.

- [ ] **Step 8: Commit**

```bash
git add -A backend/internal/raftwal backend/internal/app
git commit -m "feat(raftwal): retire monolithic compaction; wire drained-segment reclamation after waiter notification (gastrolog-23iln4)"
```

---

### Task 4: Scavenge path

**Files:**
- Modify: `backend/internal/raftwal/reclaim.go`
- Test: `backend/internal/raftwal/reclaim_test.go`

**Interfaces:**
- Consumes: Task 3's `reclaimPass`, `unlinkOldestDrained`, `triggerReclaim`, `syncBarrier`; Task 1's `stableVal`/`regLoc`; existing `appendEntry`, `rotateSegment`, `syncActiveSegment`, `readPayload`, `encodeStableSet`.
- Produces: `(w *WAL) scavengeOldest()` decomposed into `(w *WAL) collectScavenge(victim int) []scavRecord`, `(w *WAL) appendScavenge(records []scavRecord) ([]logLoc, error)`, `(w *WAL) swapScavenged(victim int, records []scavRecord, locs []logLoc) int64`; `type scavRecord struct { gid uint32; typ entryType; payload []byte; isLog bool; idx uint64; key string }`. Task 5's crash-window tests call the three steps individually.

- [ ] **Step 1: Write the failing tests**

Append to `reclaim_test.go`:

```go
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
```

(`errHarnessSyncFail` already exists in `harness_test.go`, same package.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/raftwal/ -run 'TestScavenge' -v`
Expected: `TestScavengeReclaimsNearlyDrainedSegment` FAILS at "no scavenge reported" (unlink-only pass never frees the pinned segment). `TestScavengeSkipsSegmentAboveThreshold` passes vacuously; the fsync test fails at the retry assertion.

- [ ] **Step 3: Implement scavenging**

Append to `reclaim.go` (add `"slices"` and `"sort"` to its imports):

```go
// scavRecord is one live record carried out of a segment being scavenged.
type scavRecord struct {
	gid     uint32
	typ     entryType
	payload []byte
	isLog   bool
	idx     uint64 // raft index when isLog
	key     string // stable key when typ == entryStableSet
}

// scavengeOldest reclaims the oldest sealed segment when its live remainder
// is at or below Config.ScavengeMaxLiveBytes: re-append the live records
// through the normal write path, fsync, repoint the index, then unlink. Any
// error aborts with the index untouched — the copies are idempotent
// duplicates on replay and the pass retries later. Runs on the batch writer.
func (w *WAL) scavengeOldest() {
	start := time.Now()

	w.stateMu.RLock()
	victim := w.oldestSealedSegment()
	eligible := victim != 0 && w.segLive[victim] > 0 &&
		w.segLive[victim] <= w.cfg.ScavengeMaxLiveBytes
	if eligible {
		if _, bad := w.quarantined[victim]; bad {
			eligible = false
		}
	}
	w.stateMu.RUnlock()
	if !eligible {
		return
	}

	records := w.collectScavenge(victim)
	if len(records) == 0 {
		return
	}
	locs, err := w.appendScavenge(records)
	if err != nil {
		return
	}
	scavenged := w.swapScavenged(victim, records, locs)
	w.unlinkOldestDrained(scavenged, start)
}

// collectScavenge gathers the victim segment's live records: group
// registrations, current stable values, and surviving log entries. Log
// payloads come from the recent window when cached, otherwise from the
// victim file (still on disk). Deterministic order: group, then kind, then
// key/index.
func (w *WAL) collectScavenge(victim int) []scavRecord {
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()

	gids := make([]uint32, 0, len(w.groups))
	for gid := range w.groups {
		gids = append(gids, gid)
	}
	slices.Sort(gids)

	var records []scavRecord
	for _, gid := range gids {
		gs := w.groups[gid]
		if gs == nil {
			continue
		}
		if gs.regName != "" && gs.regLoc.seg == victim {
			records = append(records, scavRecord{
				gid: gid, typ: entryGroupReg, payload: []byte(gs.regName),
			})
		}
		keys := make([]string, 0, len(gs.stable))
		for k, sv := range gs.stable {
			if sv.loc.seg == victim {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			records = append(records, scavRecord{
				gid: gid, typ: entryStableSet,
				payload: encodeStableSet(k, gs.stable[k].value), key: k,
			})
		}
		var idxs []uint64
		for idx, loc := range gs.logs {
			if loc.seg == victim {
				idxs = append(idxs, idx)
			}
		}
		slices.Sort(idxs)
		for _, idx := range idxs {
			payload, ok := gs.cache[idx]
			if !ok {
				var err error
				payload, err = w.readPayload(gs.logs[idx])
				if err != nil {
					return nil // victim unreadable: abort, retry later
				}
			}
			records = append(records, scavRecord{
				gid: gid, typ: entryLog, payload: payload, isLog: true, idx: idx,
			})
		}
	}
	return records
}

// appendScavenge writes the records through the normal append path on the
// active segment (rotating on size like any append) and fsyncs. Runs on the
// batch writer.
func (w *WAL) appendScavenge(records []scavRecord) ([]logLoc, error) {
	locs := make([]logLoc, len(records))
	for i, r := range records {
		entrySize := int64(headerSize + len(r.payload))
		if w.segSize > 0 && w.segSize+entrySize > w.cfg.SegmentTargetSize {
			if err := w.rotateSegment(); err != nil {
				return nil, err
			}
		}
		locs[i] = logLoc{seg: w.segSeq, off: w.segSize + headerSize, length: len(r.payload)}
		if err := w.appendEntry(r.gid, r.typ, r.payload); err != nil {
			return nil, err
		}
	}
	if err := w.syncActiveSegment(); err != nil {
		return nil, err
	}
	return locs, nil
}

// swapScavenged repoints the index at the fsynced copies and moves the
// live-bytes accounting, draining the victim. Copies never enter the recent
// window: re-caching cold records would evict the hot tail. Returns the
// payload bytes moved.
func (w *WAL) swapScavenged(victim int, records []scavRecord, locs []logLoc) int64 {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	var moved int64
	for i, r := range records {
		gs := w.groups[r.gid]
		if gs == nil {
			continue
		}
		newLoc := locs[i]
		switch {
		case r.isLog:
			if cur, ok := gs.logs[r.idx]; ok && cur.seg == victim {
				gs.logs[r.idx] = newLoc
				w.segLive[victim] -= int64(cur.length)
				w.segLive[newLoc.seg] += int64(newLoc.length)
				moved += int64(newLoc.length)
			}
		case r.typ == entryStableSet:
			if cur, ok := gs.stable[r.key]; ok && cur.loc.seg == victim {
				gs.stable[r.key] = stableVal{value: cur.value, loc: newLoc}
				w.segLive[victim] -= int64(cur.loc.length)
				w.segLive[newLoc.seg] += int64(newLoc.length)
				moved += int64(newLoc.length)
			}
		case r.typ == entryGroupReg:
			if gs.regLoc.seg == victim {
				w.segLive[victim] -= int64(gs.regLoc.length)
				gs.regLoc = newLoc
				w.segLive[newLoc.seg] += int64(newLoc.length)
				moved += int64(newLoc.length)
			}
		}
	}
	return moved
}
```

Extend `reclaimPass`:

```go
func (w *WAL) reclaimPass() {
	for w.unlinkOldestDrained(0, time.Now()) {
	}
	w.scavengeOldest()
}
```

Accounting subtlety the implementer must NOT "fix": a stable value re-encoded by scavenge (`encodeStableSet(key, sv.value)`) can differ in length from the record that originally set it (values set via `entryStableUint64` re-encode as `entryStableSet`); the swap debits the OLD length and credits the NEW length, so the invariant holds exactly.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/raftwal/ -run 'TestScavenge|TestReclaim|TestLiveBytes' -v` — expected PASS.
Run: `go test ./internal/raftwal/` — expected PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/internal/raftwal/reclaim.go backend/internal/raftwal/reclaim_test.go
git commit -m "feat(raftwal): scavenge nearly-drained oldest segments through the write path (gastrolog-23iln4)"
```

---

### Task 5: Crash-window and replay-ordering tests

Tests only; drives out any latent replay bugs. Uses Task 4's decomposed scavenge steps to stop at exact crash points. The manual-step tests touch writer-owned fields, so they establish quiescence with `syncBarrier` first and suppress the wired auto-scavenge with a tiny threshold.

**Files:**
- Test: `backend/internal/raftwal/reclaim_test.go`

**Interfaces:**
- Consumes: `collectScavenge`, `appendScavenge`, `swapScavenged`, `oldestSealedSegment`, `triggerReclaim`, `syncBarrier`, `assertLiveBytesInvariant`.

- [ ] **Step 1: Write the tests**

Append to `reclaim_test.go`:

```go
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
	records := w.collectScavenge(victim)
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
```

- [ ] **Step 2: Run tests**

Run: `go test ./internal/raftwal/ -run 'TestCrashAfterScavenge|TestMaskReplays|TestRepeatedScavenge' -v`
Expected: PASS. Any failure here is a real replay or accounting bug — debug the code, never weaken the assertion.

- [ ] **Step 3: Commit**

```bash
git add backend/internal/raftwal/reclaim_test.go
git commit -m "test(raftwal): crash-window, mask-reordering and restart-cycle coverage for scavenging (gastrolog-23iln4)"
```

---

### Task 6: Concurrency and multi-node integration coverage

**Files:**
- Test: `backend/internal/raftwal/reclaim_test.go` (race), `backend/internal/raftwal/integration_test.go` (4-node raft)

**Interfaces:**
- Consumes: everything landed; hashicorp/raft test helpers in `integration_test.go` (`counterFSM`, and `TestWALBackedRaftSnapshotAndRestore` for transport/snapshot/bootstrap patterns).

- [ ] **Step 1: Write the concurrent-readers race test**

Append to `reclaim_test.go`:

```go
// Readers hammer GetLog/FirstIndex/LastIndex while the writer truncates and
// reclaims via the wired passes. Correctness is the race detector plus never
// observing a read error other than ErrLogNotFound.
func TestConcurrentReadsDuringReclamation(t *testing.T) {
	w, _ := openTestWAL(t, Config{ScavengeMaxLiveBytes: 512})
	gs := w.GroupStore("grp")

	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var lg hraft.Log
			for !stop.Load() {
				first, _ := gs.FirstIndex()
				last, _ := gs.LastIndex()
				if first == 0 || last < first {
					continue
				}
				for i := first; i <= last; i++ {
					if err := gs.GetLog(i, &lg); err != nil && err != hraft.ErrLogNotFound {
						t.Errorf("GetLog(%d): %v", i, err)
						return
					}
				}
			}
		}()
	}

	next := uint64(1)
	for round := 0; round < 30; round++ {
		for i := 0; i < 40; i++ {
			if err := gs.StoreLog(&hraft.Log{Index: next, Term: 1, Data: make([]byte, 64)}); err != nil {
				t.Fatalf("store %d: %v", next, err)
			}
			next++
		}
		if err := gs.DeleteRange(1, next-10); err != nil {
			t.Fatalf("truncate: %v", err)
		}
	}
	stop.Store(true)
	wg.Wait()
	syncBarrier(t, w)
	assertLiveBytesInvariant(t, w, "after concurrent load")
}
```

- [ ] **Step 2: Run it under the race detector**

Run: `go test -race ./internal/raftwal/ -run TestConcurrentReadsDuringReclamation -v`
Expected: PASS, no race reports. A race here means a lock was skipped in reclaim.go — fix the code, not the test.

- [ ] **Step 3: Write the 4-node raft integration test**

Append to `integration_test.go`. Mirror `TestWALBackedRaftSnapshotAndRestore` exactly for the per-node setup (config via `newTestRaftConfig`-equivalent inline settings, `hraft.NewInmemTransport`, `transport.Connect` between all pairs, `hraft.NewFileSnapshotStore` under `t.TempDir()`, `counterFSM`), extended to four voters bootstrapped in one `hraft.Configuration`. Shape:

```go
// Four raft nodes, each on its own shared WAL, under sustained apply +
// forced-snapshot cycles: post-snapshot truncation drains old segments and
// reclamation keeps every node's WAL directory bounded while the cluster
// keeps electing and applying.
func TestFourNodeRaftSustainedTruncationReclaims(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft convergence run")
	}
	// Per node: raftwal.Open(dir, Config{SegmentTargetSize: 4096,
	// ScavengeMaxLiveBytes: 1024}); one GroupStore("shard") used as both
	// LogStore and StableStore. SnapshotThreshold: 8192 so ONLY the forced
	// Snapshot() calls truncate (deterministic truncation points).
	//
	// 20 rounds:
	//   1. Apply 50 entries of ~200 bytes through the leader, waiting on
	//      each future.
	//   2. Force snapshot: r.Snapshot().Error() on the leader — hraft
	//      truncates the log on every node's store.
	//   3. Apply one more entry and wait its future (cluster still works).
	//
	// After the rounds: for each node, assert
	// segmentFileCount(t, nodeWALDir) <= 6 (without reclamation this
	// workload accumulates dozens of 4 KiB segments), and
	// assertLiveBytesInvariant(t, nodeWAL, "final").
	//
	// No sleeps: every step waits on raft futures; assertions are on
	// final state.
}
```

The comment block is the specification — write the real code in its place; every named primitive exists in `integration_test.go` or `reclaim_test.go`.

- [ ] **Step 4: Run it**

Run: `go test ./internal/raftwal/ -run TestFourNodeRaft -v` (not `-short`)
Expected: PASS. Then `go test -race ./internal/raftwal/` — full package race-clean.

- [ ] **Step 5: Commit**

```bash
git add backend/internal/raftwal/reclaim_test.go backend/internal/raftwal/integration_test.go
git commit -m "test(raftwal): reader-vs-reclamation race coverage and 4-node truncation-reclaim integration (gastrolog-23iln4)"
```

---

### Task 7: Documentation and final gates

**Files:**
- Modify: `backend/internal/raftwal/README.md`
- Modify: `docs/ubiquitous_language.md`

- [ ] **Step 1: Update the raftwal README**

Rewrite the compaction-related content to describe reclamation as it now is (present tense, no history):
- "Segment Rotation" section: replace the compaction paragraph — live-bytes tracking, oldest-first unlink/scavenge, `ScavengeMaxLiveBytes`, one scavenge per pass.
- "Limitations": replace the "Segment compaction" bullet — reclamation is lazy (the space floor depends on snapshot cadence and the scavenge threshold); a group that never snapshots retains its segments.
- "Concurrency Model": the reclamation pass runs on the batch writer strictly after waiter notification, bounded per pass.
- "Failure Modes": add the interrupted-scavenge window (copies fsynced, victim retained → drained at replay, unlinked at Open).
- Update the Mermaid rotation flowchart if it references compaction.

- [ ] **Step 2: Extend the ubiquitous language**

In `docs/ubiquitous_language.md`, in the bounded context covering cluster consensus/storage (follow the doc's existing table format), add:
- **reclamation** — returning dead space in the shared Raft WAL by removing drained segments, oldest-first.
- **scavenge** — the reclamation path that re-appends a nearly-drained oldest segment's live records through the write path before unlinking it.
- **drained** — a WAL segment with zero live payload bytes (no index references).
Add a consistency-rules row: "WAL compaction" → reclamation / scavenge (retired term).

- [ ] **Step 3: Run every gate**

- From `backend/`: `go test -race ./internal/raftwal/` — expected PASS.
- From repo root: `just test` — expected PASS.
- From repo root: `just backend test-full` — expected PASS (includes the 4-node test).
- `grep -rn "compact" backend/internal/raftwal/ --include="*.go"` — expected: zero hits (identifiers and comments; the README describes reclamation, not compaction).

- [ ] **Step 4: Commit**

```bash
git add backend/internal/raftwal/README.md docs/ubiquitous_language.md
git commit -m "docs(raftwal): describe segment reclamation; add reclamation vocabulary (gastrolog-23iln4)"
```

- [ ] **Step 5: Hand off**

Set the issue to review — `dcat update --status in_review gastrolog-23iln4` — and report to the user for cluster validation. Do NOT close, merge, or push without explicit user instruction.
