# Retention Route Fan-Out Under Disk Protect

Issue: gastrolog-5ct2av. Status: approved design, pre-implementation.

## Problem

The disk guard's admission gate governs retention route fan-out. On a
route-disposition vault, retention is the only mechanism that frees space, so
the guard disables the only process that can satisfy its own resume condition:

1. Free space falls below the floor. Ingest admission suspends, pipeline
   builds and pulls pause, and retention route fan-out defers.
2. Admission and fan-out resume only above the admission gate's bar
   (~1.25×warn — 58.2 GiB in the observed incident, against a 14 GiB floor).
3. Nothing frees space toward that bar, because the only freer is deferred.
4. The vault freezes permanently: chunks hours past a 3-minute TTL, byte-
   identical state histograms across sweeps, admission refused "until
   retention drains it."

Observed live 2026-07-16 on the dev cluster: first-vault frozen at 368
chunks / 97 GiB under a 3m retention policy, with three unrelated WARN lines
(admission suspended, builds paused, fan-out deferred) as the only operator
signal.

The guard itself is correct in isolation (gastrolog-5034va: fanning out while
admission rejects everything destroys chunks unrouted). The defect is which
gate governs the fan-out, and the absence of any escape or signal.

## Constraint discovered during design

Retention fan-out records pass through the routing engine and can match
routes into multiple destination vaults. Two consequences shape everything
below:

- **Whole-record rejection is load-bearing.** Routing already nacks the
  whole record when any matched destination is admission-gated
  (`routing.Manager.route`): delivering to the healthy subset and retaining
  the chunk would re-route the same records on the next sweep and duplicate
  them unboundedly at the healthy destinations (routing has no dedup). The
  retention worker treats that nack as a terminal abort and retains the
  chunk. This stays.
- **Queues jam in ways gates cannot see.** `sink.deliver` is a blocking send
  into each destination's bounded segmentation queue. A destination that
  passes its admission gate but stops draining parks routing workers; parked
  workers fill the shared routing input; `Submit` then blocks for every
  producer. Retention pours ~1M records per chunk, so an unbounded fan-out
  into a jammed pipeline both suffers and amplifies a global stall.

## Decisions

### 1. Fan-out follows the drain gate, not the admission gate

The disk guard has two staged gates (see `disk_guard.go`):

| Gate | Governs | Engages | Resumes |
|------|---------|---------|---------|
| Admission gate (`protect`) | ingest admission, catch-up pulls | below floor | above ~1.25×warn |
| Drain gate (`deferWrites`) | chunking builds, collection pulls | below floor | above 1.25×floor |

Retention route fan-out is drain work, not new ingest: each routed-and-
destroyed chunk is net space-negative, and its transient cost (segment
writes before the source chunk is destroyed) is safe exactly when chunking
runs to absorb it — which is the drain gate's resume condition.

Changes:

- The pre-check in `fireRetentionEvent` gates on `diskDeferWrites()` instead
  of `diskProtectActive()`.
- `SubmitRetentionRecord` uses a drain submit path on the pipeline
  supervisor that skips only the node-global admission gate (`admit()`).
  Per-destination admission (vault disk protect, max-size cap, backlog
  budget — local and peer via NodeStats), routing semantics, and ack
  behavior are unchanged.

Effect: in the incident's frozen band (free between 1.25×floor and
1.25×warn) retention drains; live ingest stays suspended, so retention is
nearly the only pipeline producer there. Below the floor everything still
stops — the floor's purpose is Raft WAL survival and this design does not
touch it. The destination backlog budget paces the drain at the rate
chunking absorbs it. No new thresholds, no new configuration.

### 2. Bounded-wait fan-out watchdog

`fireRetentionEvent` gains a progress bound: every accepted submit resets
it; if no record is accepted for a generous fixed window, the existing
`abort()` path fires with a stall cause. The chunk is retained, the one-shot
route flag is not consumed, and the sweep returns instead of hanging.

- The bound is an internal constant, not configuration. It gates only
  abort-and-retry (idempotent by the 5034va ordering), never a correctness
  decision.
- The gate-nack abort path (terminal errors: `ErrDiskProtect`,
  `ErrVaultDiskProtect`, `ErrVaultMaxSize`, `ErrVaultBacklogBudget`,
  pipeline stopped, shutdown) stays exactly as is.

### 3. Deferral alarm and operator lever

The retention runner counts consecutive deferred sweeps per vault — a pure
count, in memory, nothing persisted. At N consecutive deferrals (internal
constant, small) it raises one High alarm, new catalog type
`retention-route-deferred`, whose detail names in a single line:

- the vault,
- the cause of the most recent deferral (drain gate engaged / destination
  vault X gated / fan-out stalled),
- how much is waiting (chunks and bytes past policy),
- what resumes it.

Any chunk fully routed and destroyed resets the count and clears the alarm.
This replaces the incident's three-unrelated-warnings experience with one
line that names the deadlock.

There is no automatic destroy-unrouted, ever — that trades the cardinal
rule against availability and is the operator's call. The sanctioned lever
is flipping the vault's retention disposition to `delete`, which already
exists in config and makes the next sweep destroy without routing.
(gastrolog-2ebvl — disposition toggling leaves orphan active chunks — must
be fixed for this lever to be safe; it remains a separate issue.)

### 4. Deliberate non-changes

- **Builds stay paused below the floor.** A GLCB build consumes disk
  transiently; below the floor the priority is WAL survival. The staged
  guard already resumes builds at 1.25×floor — confirmed working in the
  incident log (212 sealing chunks all sealed within minutes of free space
  crossing that bar).
- **Whole-record rejection on any gated destination** stays (see
  constraint above).
- **Delete-disposition retention** stays ungated — it never fans out and
  only frees space.
- **The 5034va ordering** (route before marking retention-pending; aborted
  fan-out does not consume the one-shot) stays.

## Cluster considerations

- The guard is per-node state; the sweep runs on the vault's placement
  leader. The drain-gate check consults the sweeping node's local
  `deferWrites` — correct, because the fan-out's segment writes land on the
  sweeping node.
- Destination health is cluster-wide: `vaultAdmissionGate` already merges
  local guard state with every peer's via the NodeStats broadcast, so a
  destination capped on a remote node defers the sweep wherever it runs.
- The fix must not depend on which node runs the sweep (acceptance
  criterion); the multi-node tests below exercise sweep-node placement
  explicitly.

## Recovery path for an already-wedged cluster

Documented for operators (and required by the issue's acceptance criteria):

1. **Preferred: free space or raise capacity** on the starved volume until
   free clears 1.25×floor — the drain gate releases, builds seal the
   backlog, and retention drains without further action. (After this
   design, that bar is 1.25×floor, not the admission gate's ~1.25×warn.)
2. **Destination-capped deadlocks:** drain or raise the destination vault's
   max-size budget, or shorten its retention.
3. **Last resort, trades routed records for availability:** flip the source
   vault's retention disposition to `delete`. The next sweep destroys
   expired chunks without routing. This discards the operator's route
   disposition for those chunks and is intentionally manual.

## Testing

Unit (single-node):

- Fan-out proceeds when `deferWrites` is clear while the admission gate
  (`protect`) is still engaged — the incident's frozen band.
- Fan-out defers below the floor (both gates engaged).
- Abort classification per cause: gated destination, pipeline stopped,
  stall.
- Watchdog: abort on no-progress via injected progress signaling — no
  real-clock races; timing is not tested with timing.
- Alarm: raises at N consecutive deferrals, carries the cause, clears on
  the first successful chunk.

Multi-node (4+ nodes, file-backed vaults, real transferrers,
`setupMultiNode` harness):

- Guard engaged on the sweep node, healthy peers: retention drains in the
  drain band.
- Destination vault capped on a remote node: sweep defers locally, alarm
  names the remote cause.
- Sweep-node independence: same outcome regardless of which node holds
  placement leadership.

Unhappy paths and edges:

- Destination permanently capped → deferral alarm persists; disposition
  flip to `delete` drains (guarded by gastrolog-2ebvl status).
- Space freed after deferral → drain resumes without operator action;
  alarm clears.
- Restart mid-deferral → counters restart cleanly; alarm re-raises after N
  fresh deferrals.
- Empty vault / no matching routes → unchanged behavior (counted drop).

Slow multi-node convergence tests carry `testing.Short()` skips with a
one-line reason. `just backend test-full` gates completion.

## Ubiquitous language

`docs/ubiquitous_language.md` gains the disk-guard vocabulary this design
leans on (currently only in code comments, some of it in banned form —
gastrolog-16hruo): **disk guard**, **admission gate**, **drain gate**,
**retention deferral**. The `disk_guard.go` comments that currently use the
banned word for these two gates are rewritten to this vocabulary in the same
PR, since this change touches exactly those seams.
