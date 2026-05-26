# Fan-Out V2 Spool State Machine

Status: draft. Spool **accept model** locked in [write-path-lock.md](write-path-lock.md).

This document defines a strict lifecycle for spool-based ingestion so V2 can be split into implementation issues without ambiguity.

Related:

- [write-path-lock.md](write-path-lock.md)
- [phase-rework-map.md](phase-rework-map.md)

## Scope

This state machine is per `(vault, node)` write path and per sealed output boundary.

It models:

- spool append durability,
- fence creation,
- spool-to-chunk materialization,
- reconcile completion for sealed chunks.

It does not prescribe wire protocol details for cross-node pull; that belongs in reconcile design.

This document focuses on lifecycle execution and crash/recovery behavior. Architectural rationale and decision ownership live in `architecture-overview.md`.

## Interface migration scope note

Current chunk write interfaces are synchronous around immediate chunk identity
(`ChunkManager.Append` returning `ChunkID`) and downstream anchors like
`RecordRef = (ChunkID, Pos)` used by context-query flows.

Under V2 spool-first materialization, chunk IDs are minted asynchronously at
materialization time. Implementation scope must therefore include interface
shape updates so write-time anchors no longer assume immediate final chunk ID.

This is a migration-scope item to plan explicitly before issue decomposition.

## Terms

- **Spool**: pre-chunk durable store on each replica (lifecycle container).
- **Sequence window**: on-disk unit covering one allocator swath range `[start..end]` for a vault on this replica.
- **Slot**: one `(VaultSeq, record)` write inside a sequence window — the spool accept primitive.
- **Fence**: immutable upper bound over assigned **`VaultSeq`** space that defines a materialization batch.
- **Ingest high watermark (`H`)**: highest accepted **`VaultSeq`** on the reference acceptance view (see high-watermark-contract.md).
- **Acceptance sequence (`VaultSeq`)**: vault-wide accept label assigned on the ingesting router before replica fan-out; distinct from `EventID`.
- **Materialization watermark (`M_r`)**: highest **`VaultSeq`** fully materialized on this replica.
- **Converge-sealed**: sealed chunk whose canonical record set (by **`EventID`**) matches across required replicas.

## Spool windows, slots, and crash-safety

Spool reuse the chunk crash model **per slot/window**:

- payload bytes written before index commit marker for the slot,
- recovery trusts index; truncate uncommitted tails on restart.

Window layout (file-backed example):

```text
spool/windows/w-<start>-<end>/
  idx    # VaultSeq → offset (sparse within window)
  raw / attr
```

Requirements:

- slot write keyed by **`VaultSeq`**; **out-of-order** arrival within a window is required behavior,
- window identity follows **allocator swath range**, not ingesting node,
- window metadata records at least `start`, `end`, and durability footer/checksum,
- **`ReadByVaultSeq`** resolves through slot index (plus optional in-memory cache).

Reclaim rule:

- window reclaimable when `window.end <= reclaim_watermark_seq`,
- gated by materialization + reconcile safety (do not delete slots still needed as reconcile source).

## Invariants

- Hot-path write acknowledgement is based on spool **slot** durability (`W-of-N`), not chunk creation.
- Fence ordering is monotonic per vault replica.
- A logical record is materialized into local chunk set at most once (**`EventID`** canonicalization at materialize).
- Watermark never moves backwards.
- Upload/archive/final sealed-read shortcuts require converge-sealed state.

## State Machine

Primary lifecycle for one fence-defined batch:

1. `SpoolOpen`
2. `Fenced`
3. `Materializing`
4. `MaterializedLocal`
5. `SealedPendingReconcile`
6. `ConvergeSealed`

Error and retry substates:

- `MaterializeFailed` (retryable)
- `ReconcileFailed` (retryable)

## State Definitions

## `SpoolOpen`

The replica accepts new records into spool **slots** (within sequence windows).

- Entry: vault initialized or previous fence batch completed.
- Exit trigger: rotation policy decides to cut a fence (does not require stopping slot writes globally).

## `Fenced`

A fence is durably recorded. It freezes the upper bound for one batch.

- New writes continue to later accepted sequence values.
- Fence metadata includes at least `(vault, fence_id, upper_bound_seq, created_at)`.
- Fence must survive restart before leaving this state.

## `Materializing`

Background worker reads **`VaultSeq` range** `(prev_watermark_seq, fence.upper_bound_seq]` via **`ReadByVaultSeq`**, writes chunk records locally.

- Chunk IDs are minted here, not on ingest hot path.
- **Dedup by `EventID`** when emitting sealed output (default: lowest **`VaultSeq`** wins).
- Partial progress is allowed; restart resumes from durable watermark/checkpoint.

## `MaterializedLocal`

Local replica has fully materialized the fenced range.

- Local chunk(s) are sealed from this replica's perspective.
- Replica is ready to participate in set reconcile for the newly sealed chunk set.

## `SealedPendingReconcile`

Chunk is sealed locally but not yet converge-sealed cluster-wide.

- Reads must follow policy for non-converged sealed data (fan-out or explicit downgraded mode).
- Upload/archive/final "single replica is enough" behavior remains gated off.

## `ConvergeSealed`

Reconcile confirms record-set equivalence by `EventID` across required replicas.

- This is the only state that grants full sealed semantics.
- Post-seal actions that assume convergence are now allowed.

## Transition Table

| From | Event | To | Durable write required before ack |
|---|---|---|---|
| `SpoolOpen` | slot fsynced | `SpoolOpen` | spool slot durability |
| `SpoolOpen` | rotation policy emits fence | `Fenced` | fence metadata persisted |
| `Fenced` | materializer worker starts | `Materializing` | worker lease/checkpoint persisted |
| `Materializing` | local batch complete | `MaterializedLocal` | watermark advanced to fence |
| `Materializing` | unrecoverable local write error | `MaterializeFailed` | failure marker + reason |
| `MaterializeFailed` | retry scheduled | `Materializing` | retry intent persisted |
| `MaterializedLocal` | seal publish step complete | `SealedPendingReconcile` | sealed metadata persisted |
| `SealedPendingReconcile` | reconcile success | `ConvergeSealed` | convergence marker persisted |
| `SealedPendingReconcile` | reconcile error/timeout | `ReconcileFailed` | failure marker + reason |
| `ReconcileFailed` | retry scheduled | `SealedPendingReconcile` | retry intent persisted |
| `ConvergeSealed` | next batch active | `SpoolOpen` | next fence context initialized |

## Crash Recovery Rules

On restart, recovery logic must:

1. Reload last durable materialization watermark.
2. Reload outstanding fences ordered by creation.
3. Re-enter `Materializing` for any fence with `upper_bound_seq > watermark_seq`.
4. Resume materialize for outstanding fences; **dedup by `EventID`** at emit time.
5. Re-enter `SealedPendingReconcile` for locally sealed batches missing convergence marker.

Never infer success from in-memory progress; only durable markers drive recovery.

## Write Acknowledgement Contract

Write path:

- ingesting router assigns **`VaultSeq`**, fans out to peer replicas,
- each replica **slot-writes** on arrival,
- acknowledge success only after configured `W-of-N` replicas report durable slot write.

No fence, chunk naming, or reconcile action may block this hot-path acknowledgement.

## Read Contract By State

- `SpoolOpen` / `Fenced` / `Materializing`: active queries must include spool-visible records per configured read mode.
- `MaterializedLocal` / `SealedPendingReconcile`: reads treat output as sealed-local but non-converged globally.
- `ConvergeSealed`: normal sealed-chunk read behavior allowed.

## Operational Signals

Minimum operator-visible metrics/alerts:

- spool append latency and backlog bytes,
- materialization lag (`fence.upper_bound_seq - watermark_seq`),
- oldest outstanding fence age,
- count of batches in `MaterializeFailed` / `ReconcileFailed`,
- time-to-convergence per sealed batch.

## Issue-Splitting Boundaries

A clean issue stack can follow state boundaries:

1. Spool storage + durable append ack plumbing (`SpoolOpen`).
2. Fence generation and persistence (`Fenced`).
3. Materializer and watermark protocol (`Materializing` -> `MaterializedLocal`).
4. Sealed-pending-reconcile publish path.
5. Reconcile completion markers (`SealedPendingReconcile` -> `ConvergeSealed`).
6. Read-path integration across spool + chunks.
7. Recovery and fault-injection tests across all transitions.

Each issue should include both single-node and 4+ node tests for its transitions.

## No-Go Conditions

This state machine should be rejected if any proposed implementation:

- reintroduces chunk naming into write ack path,
- allows watermark advancement without durable materialization proof,
- treats locally sealed as globally converged without reconcile proof,
- exposes provisional counts/durability as authoritative.
