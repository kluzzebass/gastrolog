# Raft WAL Reclamation: Prefix Scavenging

Issue: gastrolog-23iln4. Status: approved design, pre-implementation.

Retires monolithic WAL compaction. Dead-space reclamation becomes an
incremental, oldest-first process on the batch-writer goroutine, with no
work unit larger than one segment's live remainder.

## Problem

The shared Raft WAL (`backend/internal/raftwal`) multiplexes every Raft
group on a node onto one segmented log with a single batch-writer
goroutine. Compaction — rewrite every live entry of every group into fresh
segments, fsync twice, rotate, preallocate 64 MiB spares — runs inline on
that goroutine, inside `flushBatch`, before batch waiters are notified.
Observed on the dev cluster: compactions up to 12 seconds, during which
every group's `StoreLogs` blocks, heartbeats miss, and elections fire.

The trigger-gate fix (gastrolog-44yl6w) made compactions rare. It did not
make them short: one run still stalls the node's entire consensus plane for
its whole duration. The defect is not where compaction runs — it is that a
monolithic "rewrite the whole live log" work unit exists at all.

## Alternative considered: concurrent compactor

Keep whole-log compaction as a job, run it on its own goroutine: the writer
rotates past a reserved segment-sequence gap, the compactor rewrites live
state into the gap, fsyncs, swaps locations under `stateMu`, deletes old
files. Rejected: it adds a second segment-writing goroutine, a
prepare/handoff/abort protocol, and a gap-allocation scheme to the highest-
blast-radius code in the repo — to preserve a job whose payoff (tightly
repacking partially-live segments) reclaims almost nothing, because Raft's
dead space is prefix-shaped. After a snapshot, `DeleteRange` kills whole old
segments outright; the live tail does not need rewriting.

## Design

### 1. Live-bytes accounting per sealed segment

The WAL maintains, per segment, the total payload bytes of records the
in-memory index still references:

- **Log entries**: each `gs.logs[idx]` location contributes `loc.length` to
  its segment. Batch records (`entryLogBatch`) are accounted per sub-entry,
  since each sub-entry has its own location.
- **Stable keys**: `groupState.stable` grows a location per key
  (`map[string]stableVal{value, loc}`). The current value's encoded record
  contributes to its segment; an overwrite decrements the superseded
  segment and credits the new one.
- **Group registrations**: the WAL tracks each group's registration-record
  location; re-registration (scavenge copy) moves the contribution.
- **Masks contribute nothing**: `entryDeleteRange` records are never
  referenced by the index and count zero from birth.

Decrement sites are exactly the index mutation sites, all already under
`stateMu` or on the writer: `applyLogEntry` overwriting an existing index,
`applyDeleteRange`, stable overwrite, registration move. Replay rebuilds
the counters from scratch, so restart cannot inherit drift.

Live bytes measure *referenced payload*, not file size. Zero live bytes
means zero live references (no record type has an empty payload).

### 2. Reclamation is strictly oldest-first

Only sealed segments (sequence below the active segment) are candidates,
and only the oldest data segment is ever reclaimed. Two paths:

- **Drained (live bytes = 0)**: unlink. Under `stateMu.Lock`: verify (see
  §4), remove the file, close its cached read handle, drop its counter.
  Microseconds; no rewrite I/O. This is the dominant post-snapshot case.
- **Nearly drained (live bytes ≤ `ScavengeMaxLiveBytes`)**: scavenge. The
  writer re-appends the segment's live records — surviving log entries as
  individual `entryLog` records, current stable values, the group
  registrations it carries — through `appendEntry` on the active segment,
  fsyncs, then updates the index locations and counters under
  `stateMu.Lock`, which drains the segment to zero; it is then unlinked in
  the same pass. Bounded by the threshold: single-digit milliseconds.

A reclamation pass runs on the writer at the end of `flushBatch`, strictly
**after** `notifyBatchWaiters` — waiters never wait on reclamation. Three
triggers: a flushed batch that contained a `DeleteRange`; a rotation
sealing a segment; once at `Open` after replay (to collect segments a
crash left drained). A pass unlinks as many drained segments as it finds
(each is cheap) but scavenges at most one segment, capping inline rewrite
work per flush at the threshold.

Accepted trade-off: reclamation is lazy. The disk floor depends on Raft
snapshot cadence and the threshold, not on a "compact now" operation — a
half-live oldest segment is retained until truncation drains it below the
threshold. Rewriting such a segment would reclaim almost nothing anyway,
and a group that never snapshots defeats whole-log compaction equally.

Scavenge copies bypass `applyToMemory`: they update `gs.logs`/stable/
registration locations directly, without touching the recent-payload cache
— re-caching cold entries would evict the hot window.

`ScavengeMaxLiveBytes` defaults to 4 MiB: about 6% of a 64 MiB segment, and
roughly 4 ms of sequential write on the dev cluster's NVMe — the same
order as the existing 1 ms batch window, so a scavenge is invisible next
to a normal flush.

### 3. Replay safety

Replay applies segments in sequence order; correctness rests on one
invariant:

> A record that masks another — an overwrite of the same Raft index, a
> `DeleteRange`, a newer stable value — is always written later, therefore
> in a later or equal segment. Oldest-first removal can never delete a mask
> while the masked record survives.

Scavenging moves copies *forward past existing masks* (a copy of segment
1's survivors lands in segment 9, replaying after a segment-2
`DeleteRange`). This is safe because a copy carries only state that is
live at scavenge time: every mask in existence has already been applied to
it, so no existing mask targets it, and masks created later sort after it
naturally.

Crash windows:

- **Before the scavenge fsync**: copies lost, old segment intact — replay
  sees the original state.
- **After fsync, before unlink**: both old records and copies replay; the
  copies are later, so they win with identical values. The rebuilt counters
  show the old segment drained (the index points at the copies), and the
  first pass after `Open` unlinks it.
- **Mid-unlink**: unlink is a single `os.Remove`.

One consequence needs its own tests: masks can replay against emptier
state than they originally saw (a `DeleteRange` whose targets were
unlinked applies to nothing, and its `firstIndex`/`lastIndex` adjustments
in `applyDeleteRange` run from zero-state). The bounds bookkeeping must
converge once the forward copies apply.

### 4. Unlink verification (counter distrust)

Live-bytes is a derived aggregate with four writers; drift toward zero
would gate an irreversible `os.Remove` on a segment the index still
references. The counter therefore only *nominates*: before unlinking, the
writer scans the live index under `stateMu` — every group's log locations,
stable locations, registration locations — and asserts none reference the
victim segment. On mismatch the unlink is aborted, the segment retained
(wasted space, never lost data), and the condition surfaced via the
`OnReclaimAnomaly` callback so the node raises an alert: drift is a bug,
not an operational state. The scan is a bounded in-memory walk, runs once
per reclaimed segment, and an invariant test recomputes live bytes by full
scan after every mutation and diffs against the incremental counters.

### 5. What the writer no longer does

Compaction's rotations, double fsyncs, snapshot capture, and whole-log
rewrite disappear. The space-reserve machinery (`sparePath`,
`reconcileReserve`, `ensureSpare`) is untouched: reclamation never
rotates the active segment and never touches the spare. A scavenge's
re-appends can trigger an ordinary size-based rotation through the normal
path, which is already correct. Unlinking a preallocated segment releases
its physical blocks.

### 6. API and vocabulary changes

Removed: `compactSegments`, `collectCompactionSnapshot`,
`writeCompactionSnapshot`, `appendCompactedEntry`, `CompactionStats`,
`LastCompactionStats`, `Config.CompactionMinSegments`,
`Config.OnCompaction`, and the `raft WAL compacted` log line.

Added:

- `Config.ScavengeMaxLiveBytes` (default 4 MiB).
- `Config.OnReclaim(ReclaimStats)` — invoked per reclaimed segment from
  the writer (must not block), carrying reclaimed bytes, scavenged live
  bytes, and duration. Wired to a `raft WAL segment reclaimed` log line in
  `app` (replacing `walCompactionLog` at `app/raft.go` and `app/app.go`),
  which the self ingester captures — events are logs.
- `Config.OnReclaimAnomaly(seq int, liveRefs int)` — verification-scan
  mismatch on a segment nominated for unlink, wired to an alert.

Vocabulary: **reclamation** is the umbrella (returning dead WAL space),
**scavenge** is the rewrite-remainder path, **drained** describes a
segment with zero live bytes. `docs/ubiquitous_language.md` gains these
terms in the implementation PR; "WAL compaction" is retired.

The on-disk format is unchanged — scavenging reuses existing record types
(formats stay V1; no migration concerns regardless).

Out of scope: group deregistration. A removed vault's group pins its
registration and stable keys forever (they scavenge forward indefinitely).
Pre-existing gap, filed as gastrolog-3zkxa6.

### 7. Acceptance criteria (restated for this shape)

The issue's criteria were written for a concurrent-compactor design; they
restate as:

1. No reclamation work unit exceeds one segment's live remainder
   (≤ `ScavengeMaxLiveBytes` of rewrite); appends from other groups queued
   behind a reclamation pass are delayed by at most that bound. Asserted
   structurally (work accounting), not by elapsed-time thresholds.
2. Batch waiters are notified before any reclamation work runs.
3. Single-writer ownership is preserved: no new goroutine touches
   `seg`/`segPath`/`segSize`/`segSeq`/`sparePath`; all reclamation runs on
   the batch writer.
4. The space-reserve invariant holds: reclamation never touches the spare;
   `OnReserveState` transitions remain correct under scavenge-triggered
   rotation.
5. Replay safety and restart survival hold across every crash window in
   §3, including mask-replay against emptier state, and oldest-first
   discipline is never violated (property test: no segment is ever removed
   while an older data segment exists).
6. Live-bytes counters match a full-scan recomputation after every
   mutation type (overwrite, delete-range, batch sub-entries, stable
   overwrite, registration move, replay).
7. `go test -race ./internal/raftwal/` clean; single-node and multi-node
   (4+) coverage, happy and unhappy paths; `just test` and
   `just backend test-full` pass.

## Testing dimensions

- **Unit**: counter exactness property test (invariant recomputation);
  drained-unlink; scavenge of each record kind (log, batch sub-entries,
  stable, registration); threshold boundary; oldest-first discipline;
  verification-scan abort path (injected drift).
- **Crash/replay**: interrupted scavenge at each window in §3; mask replay
  against emptier state (`DeleteRange` bounds bookkeeping); restart after
  repeated scavenge cycles; torn scavenge copies (existing torn-entry
  handling applies).
- **Concurrency**: `GetLog`/`FirstIndex`/`LastIndex` racing reclamation
  (read handles, location updates); race detector.
- **Integration**: hashicorp/raft snapshot-truncate-reclaim cycles;
  multi-node (4+) via the existing multinode harness with sustained
  truncation traffic on several groups.
- **Unhappy path**: fsync failure during scavenge (segment retained, no
  loss); ENOSPC during scavenge re-append (degraded reserve path
  unchanged); reclamation with a group that never truncates (its segments
  are simply retained).
