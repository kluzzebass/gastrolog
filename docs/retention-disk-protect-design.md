# Retention Fan-Out Under Disk Protect

Status: implemented (`backend/internal/orchestrator/retention.go`,
`retention_watchdog.go`, `retention_transfer.go`, `disk_guard.go`,
`pipeline.go`, `pipeline/supervisor.go`).

How retention's non-delete dispositions — route fan-out and transfer —
behave while the disk guard is protecting a node, and how a stalled drain
is bounded and named.

## The constraint

On a route- or transfer-disposition vault, retention is the only mechanism
that frees the vault's space. If the gate that suspends ingest also
suspended retention, the guard would disable the only process that can
satisfy its own resume condition: free space below the floor suspends
admission, admission resumes only above the warn band, and nothing frees
space toward that bar. The vault would freeze with chunks far past their
retention bound.

Two properties of the fan-out shape everything below:

- **Whole-record rejection is load-bearing.** Retention fan-out records
  pass through the routing engine and can match routes into several
  destination vaults. Routing nacks the whole record when any matched
  destination is admission-gated (`routing.Manager.route`): delivering to
  the healthy subset and retaining the chunk would re-route the same
  records on the next sweep and duplicate them at the healthy destinations
  (routing has no dedup). `SubmitRetentionRecord` therefore carries the
  durability ack and waits for it: a completed fan-out means durably
  committed at every matched destination, and a gate rejection is a
  terminal abort that retains the chunk.
- **Queues jam in ways gates cannot see.** Delivery is a blocking send into
  each destination's bounded segmentation queue. A destination that passes
  its admission gate but stops draining parks routing workers; parked
  workers fill the shared routing input. A chunk holds up to millions of
  records, so an unbounded fan-out into a jammed pipeline both suffers and
  amplifies a global stall.

## 1. Fan-out follows the drain gate, not the admission gate

The disk guard has two staged gates (`disk_guard.go`):

| Gate | Governs | Engages | Resumes |
|------|---------|---------|---------|
| Admission gate (`protect`) | ingest admission, GLCB catch-up pulls | below floor | above max(1.25×warn, 1.25×floor) (`admissionResumeAbove`) |
| Drain gate (`deferWrites`) | chunking builds, collection pulls, retention fan-out and transfer | below floor | above 1.25×floor |

Retention fan-out is drain work, not new ingest: each routed-and-destroyed
chunk is net space-negative, and its transient cost (segment writes before
the source chunk is destroyed) is safe exactly when chunking runs to absorb
it — which is the drain gate's resume condition.

- `fireRetentionEvent` (route) and `fireTransferEvent` (transfer) gate on
  `diskDeferWrites()`. Below the floor they defer, record a retention
  deferral, and retain the chunk.
- `SubmitRetentionRecord` submits through `Supervisor.SubmitDrain`, which
  skips only the node-global admission gate (`admit()`). Per-destination
  admission (storage disk protect, max-size bound, age/count bounds,
  backlog budget — local and peer via NodeStats), routing semantics, and
  ack behavior are those of any routed record.

Effect: between 1.25×floor and the admission resume level, retention
drains while live ingest stays suspended, so retention is nearly the only
pipeline producer there. Below the floor everything stops — the floor
protects the Raft WAL. The destination backlog budget paces the drain at
the rate chunking absorbs it.

Delete-disposition retention is not gated: it never fans out and only
frees space. Chunking builds stay paused below the floor: a GLCB build
consumes disk transiently, and below the floor the priority is WAL
survival; builds resume at 1.25×floor.

## 2. Bounded-wait fan-out watchdog

A route fan-out aborts when no submit completes for
`retentionFanOutStallWindow` (2 minutes, `retention_watchdog.go`). Progress
is any completed submit — accepted-and-committed, dropped per record, or
unmatched; only a blocked submit is not progress. On a stall the existing
`abort()` path fires: the chunk is retained, the one-shot route flag is not
consumed, and the sweep returns instead of hanging. A transfer applies the
same bound to its destination-receipts wait
(`transferReceiptsMaxStallTicks` × `transferReceiptsPollInterval`).

The window is an internal constant, not configuration. It gates only
abort-and-retry (idempotent: route before marking retention-pending, and
an aborted fan-out does not consume the one-shot), never a correctness
decision.

Terminal submit errors abort the whole fan-out: `pipeline.ErrNotRunning`,
context cancellation, `ErrDiskProtect`, `ErrStorageDiskProtect`,
`ErrVaultMaxSize`, `ErrVaultBacklogBudget`, `ErrVaultAgeBound`,
`ErrVaultChunkCountBound`, and orchestrator shutdown. Any other per-record
error is a deliberate partial-fan-out tolerance — one bad record must not
strand a chunk forever: the record is counted, and one warn line per chunk
reports the dropped count when the chunk is destroyed.

## 3. Deferral alarm and operator lever

The retention runner keeps a per-vault streak of consecutive deferred
sweeps — a pure in-memory count, nothing persisted. Route fan-out and
transfer feed the same streak. At `retentionDeferralAlarmAfter` (3)
consecutive deferrals it raises the High alarm `retention-deferred:<vault>`,
whose detail names in one line the vault, the disposition, the cause of
the most recent deferral (drain gate engaged, destination or target vault
gated, fan-out or receipts stalled, …), how many chunks past policy are
waiting, and what resumes it. A sweep in which any chunk completes its
disposition resets the streak and clears the alarm.

Retention never destroys a chunk unrouted on its own — that would trade
the cardinal rule for availability, and it is the operator's call. The
lever is setting the vault's retention disposition to `delete`, which makes
the next sweep destroy expired chunks without routing.

## Accepted systemic property: a full drive can stall the cluster

A full volume on one node can stall admission far beyond that node, and
this is accepted, not a defect to engineer away. It follows from the
durability guarantee: a record accepted for a vault must be durably
placeable on that vault's placements, so when a placement's volume cannot
take it, refusal at admission is the only response that keeps the promise
— accepting into a doomed write, silently degrading the replica count, or
delivering a partial fan-out are all worse.

The stall does not stay scoped to records destined for the gated vault. A
nacked record is not dropped (cardinal rule) and its source cannot release
later records past it without breaking source ordering, so one gated-vault
record halts its entire source stream — healthy destinations included —
while retries recirculate through the shared routing input and fill it
with records that can never proceed. The shared queues are one conveyor:
a single gated vault eventually stops routing for everything. Spreading
placements and routes delays this; it does not bound it. What the system
owes the operator is that the stall is bounded in time and named
(drain-gate recovery, the deferral alarm), never permanent and silent.

## Cluster behavior

- The guard is per-node state; the sweep runs on the node leading the
  vault (only leader instances get a retention runner). The drain-gate check consults the sweeping node's local
  `deferWrites` — correct, because the fan-out's segment writes land on
  the sweeping node.
- Destination health is cluster-wide: `vaultAdmissionGate` merges local
  guard state with every peer's via the NodeStats broadcast, so a
  destination capped on a remote node defers the sweep wherever it runs.

## Recovering a wedged vault

1. **Preferred: free space or raise capacity** on the starved volume until
   free clears 1.25×floor — the drain gate releases, builds seal the
   backlog, and retention drains without further action.
2. **Destination-capped deferral:** drain or raise the destination vault's
   bound, or shorten its retention.
3. **Last resort, trades routed records for availability:** set the source
   vault's retention disposition to `delete`. The next sweep destroys
   expired chunks without routing. This discards the operator's route
   disposition for those chunks and is intentionally manual.

Vocabulary: **disk guard**, **admission gate**, **drain gate** and
**retention deferral** are defined in `docs/ubiquitous_language.md`.
