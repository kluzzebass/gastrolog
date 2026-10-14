# Raft WAL Reclamation: Prefix Scavenging

Status: implemented (`backend/internal/raftwal/reclaim.go`, `wal.go`). The
package README (`backend/internal/raftwal/README.md`) carries the
operational detail; this document states the design and the invariants it
rests on.

The shared Raft WAL returns dead space incrementally and oldest-first, on
the batch-writer goroutine, with no work unit larger than one segment's
live remainder. There is no whole-log compaction: a monolithic "rewrite
every live entry" job stalls every group's `StoreLogs` for its whole
duration, and Raft's dead space is prefix-shaped — after a snapshot,
`DeleteRange` kills whole old segments outright, so the live tail never
needs rewriting.

## Context

The shared Raft WAL (`backend/internal/raftwal`) multiplexes every Raft
group on a node onto one segmented log with a single batch-writer
goroutine. Anything that runs on that goroutine delays every group's
appends, heartbeats and elections behind it, so reclamation is bounded by
construction rather than by tuning.

## 1. Live-bytes accounting per sealed segment

The WAL maintains, per segment, the total payload bytes of records the
in-memory index still references:

- **Log entries**: each `gs.logs[idx]` location contributes `loc.length` to
  its segment. Batch records (`entryLogBatch`) are accounted per sub-entry,
  since each sub-entry has its own location.
- **Stable keys**: `groupState.stable` holds a location per key
  (`map[string]stableVal{value, loc}`). The current value's encoded record
  contributes to its segment; an overwrite decrements the superseded
  segment and credits the new one.
- **Group registrations**: each group's registration-record location
  (`regLoc`) contributes; a scavenge copy moves the contribution.
- **Masks contribute nothing**: `entryDeleteRange` records are never
  referenced by the index and count zero from birth.

Decrement sites are exactly the index mutation sites, all under `stateMu`
or on the writer: `applyLogEntry` overwriting an existing index,
`applyDeleteRange`, stable overwrite, registration move, group drop.
Replay rebuilds the counters from scratch, so a restart cannot inherit
drift.

Live bytes measure *referenced payload*, not file size. Zero live bytes
means zero live references (no record type has an empty payload).

## 2. Reclamation is strictly oldest-first

Only sealed segments (sequence below the active segment) are candidates,
and only the oldest sealed segment is ever reclaimed. Two paths:

- **Drained (live bytes = 0)**: unlink. Under `stateMu.Lock`: verify (see
  §4), remove the file, close its cached read handles, drop its counter.
  No rewrite I/O. This is the dominant post-snapshot case.
- **Nearly drained (live bytes ≤ `ScavengeMaxLiveBytes`)**: scavenge. The
  writer re-appends the segment's live records — surviving log entries as
  individual `entryLog` records, current stable values, the group
  registrations it carries — through `appendEntry` on the active segment,
  fsyncs, then updates the index locations and counters under
  `stateMu.Lock`, which drains the segment to zero; it then unlinks in the
  same pass. A transient error collecting or re-appending aborts the pass
  with the index untouched; the next trigger retries.

A reclamation pass (`reclaimPass`) runs on the writer at the end of
`flushBatch`, strictly **after** `notifyBatchWaiters` — waiters never wait
on reclamation. Three triggers: a flushed batch that contained a
`DeleteRange` or a group drop; a rotation sealing a segment; once at
`Open` after replay (to collect segments a crash left drained). A pass
unlinks the whole oldest-first run of drained segments it finds
(`unlinkDrainedBurst`, under one `stateMu.Lock` hold and at most one index
scan) and then scavenges at most one segment (`scavengeOldest`), so the
threshold caps inline rewrite work per pass.

Reclamation is lazy. The disk floor depends on Raft snapshot cadence and
the threshold, not on a "reclaim now" operation: a segment whose live
remainder sits above the threshold — and every sealed segment behind it,
whichever group they belong to — is retained until truncation drains it
below the threshold. A group that never snapshots never fires a
`DeleteRange`, so its segments are retained.

Scavenge copies bypass `applyToMemory`: they update `gs.logs`/stable/
registration locations directly, without touching the recent-payload cache
— re-caching cold entries would evict the hot window.

`ScavengeMaxLiveBytes` defaults to 4 MiB (`scavengeMaxLiveBytes` in
`wal.go`): about 6% of a 64 MiB segment, the same order of write cost as
the 1 ms batch window, so a scavenge is invisible next to a normal flush.

## 3. Replay safety

Replay applies segments in sequence order; correctness rests on one
invariant:

> A record that masks another — an overwrite of the same Raft index, a
> `DeleteRange`, a newer stable value, a group drop — is always written
> later, therefore in a later or equal segment. Oldest-first removal can
> never delete a mask while the masked record survives.

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
  pass after `Open` unlinks it.
- **Mid-unlink**: unlink is a single `os.Remove`.

Masks can replay against emptier state than they originally saw (a
`DeleteRange` whose targets were unlinked applies to nothing, and its
`firstIndex`/`lastIndex` adjustments in `applyDeleteRange` run from
zero-state); the bounds bookkeeping converges once the forward copies
apply.

## 4. Unlink verification and quarantine

Live bytes is a derived aggregate with several writers; drift toward zero
would gate an irreversible `os.Remove` on a segment the index still
references. The counter therefore only *nominates*: before unlinking, the
writer scans the live index under `stateMu` — every group's log locations,
stable locations, registration locations — and asserts none reference the
victim segment. Drift in the other direction is caught by the scavenge
path: a segment eligible for scavenge whose collection finds no live
records.

Either disagreement **quarantines** the segment: it is retained (wasted
space, never lost data), `Config.OnReclaimAnomaly(seq, liveRefs)` fires
once, and — because reclamation is oldest-first — every sealed segment
behind it is retained too, until a restart rebuilds the counters from
replay. The node raises the latched software-fault alarm
`wal-reclaim-anomaly:<wal>`: drift is a bug, not an operational state. A
counter overstating a segment's live bytes past the scavenge threshold
makes it eligible for neither path, so that drift direction goes
undetected.

The scan is a bounded in-memory walk, taken at most once per drained run
plus once for a scavenged victim. An invariant test
(`wal_invariants_test.go`) recomputes live bytes by full scan after every
mutation and diffs against the incremental counters.

A failing `os.Remove` is not an anomaly: the counter and index are right,
only the filesystem is wrong. `Config.OnUnlinkError(seq, err)` reports it,
the segment stays nominated, and every later pass retries it; nothing
sealed behind it reclaims until the removal succeeds. The node raises the
non-latching `wal-unlink-error:<wal>` alarm, cleared by the next
successful removal.

## 5. Reclamation and the space reserve

Reclamation never rotates the active segment and never touches the
preallocated spare (`sparePath`, `reconcileReserve`, `ensureSpare`). A
scavenge's re-appends can trigger an ordinary size-based rotation through
the normal path, and `OnReserveState` transitions apply to it unchanged.
Unlinking a preallocated segment releases its physical blocks.

## 6. API

`raftwal.Config`:

- `ScavengeMaxLiveBytes` (default 4 MiB).
- `OnReclaim(ReclaimStats)` — invoked per reclaimed segment with the
  segment sequence, reclaimed file bytes, scavenged live bytes and
  duration. `app` wires it to the `raft WAL segment reclaimed` log line
  (`app/raft.go`), which the self ingester captures — events are logs.
- `OnReclaimAnomaly(seq int, liveRefs int)` — a quarantined segment, wired
  to `wal-reclaim-anomaly`.
- `OnUnlinkError(seq int, err error)` — a failed removal, wired to
  `wal-unlink-error`.

Callbacks are collected during a pass and invoked after `stateMu` is
released, so a slow notification never blocks reads. The cluster-ctl and
vault-ctl WALs are wired identically (`app/raft.go`, `app/app.go`).

Vocabulary: **reclamation** is the umbrella (returning dead WAL space),
**scavenge** is the rewrite-remainder path, **drained** describes a
segment with zero live bytes. The on-disk format has no reclamation-only
record type: scavenging reuses the existing record types.

## 7. Group drop

Group deregistration rides on the same discipline: `DropGroup` appends a
group-drop record (`entryGroupDrop`) that masks everything earlier for the
group — its live bytes are released, it is never carried by a scavenge,
and replay applies the drop in physical order so nothing resurrects. The
drop record stays live only until its own segment becomes the oldest;
everything it masks is gone by then, so the scavenge pass discards it
instead of carrying it. Vault deletion decommissions the vault's
control-plane group this way on every node
(`GroupManager.DecommissionGroup`), without a final snapshot, and removes
the group's directory; a vault merely reassigned away from a node stops
its group without dropping, so the state recovers when the group
re-ensures.

## 8. Guarantees

1. No reclamation work unit rewrites more than one segment's live
   remainder (≤ `ScavengeMaxLiveBytes`); appends from other groups queued
   behind a pass are delayed by at most that rewrite plus the drained-run
   unlinks.
2. Batch waiters are notified before any reclamation work runs.
3. Single-writer ownership holds: no goroutine other than the batch writer
   touches `seg`/`segPath`/`segSize`/`segSeq`/`sparePath`.
4. The space-reserve invariant holds: reclamation never touches the spare.
5. Replay safety and restart survival hold across every crash window in
   §3, and no segment is removed while an older data segment exists.
6. Live-bytes counters match a full-scan recomputation after every
   mutation type (overwrite, delete-range, batch sub-entries, stable
   overwrite, registration move, group drop, replay).
