# Fan-Out V2 Router Delivery Queue

Status: draft (2026-05-26). **Phase 12** (`gastrolog-2qrec`) — post P11 cutover. Design locked; implementation deferred.

Related:

- [write-path-lock.md](write-path-lock.md) — vault accept path (runs **after** delivery)
- [architecture-overview.md](architecture-overview.md) — decision authority map
- [spool-state-machine.md](spool-state-machine.md) — vault replica spool (not this layer)

## System contract

**Nothing that is ingested is lost** — provided a route destination captures the record.

If routing finds no matching destination, the record is dropped by design (same as today: silent drop when `RouteSet` has no match). Once a record **has** a destination, loss from transient delivery failure is a bug:

- vault metadata not ready yet,
- peers temporarily unreachable,
- ingesting node holds no vault replica,
- process restart before delivery completes,
- or any other retryable fault on the path to vault accept.

Today the orchestrator tries once in `writeLoop` and logs `write failed`. That violates the contract for every transient failure mode above.

## Design rule: cluster-first

**Cluster-first** (every feature must work on every node) is a **design rule**, not the ingest durability contract. It shapes *how* we build — no privileged ingest node, no "connect to the replica holder" requirement — but it is not itself the promise to operators.

The router delivery queue satisfies the **system contract** above. Cluster-first is one reason the queue must exist on **every** ingest-capable node, not an alternate statement of the guarantee.

## Problem statement

The locked sequenced write path ([write-path-lock.md](write-path-lock.md)) assumes the **ingesting router** can immediately:

1. assign `VaultSeq` from a local swath,
2. fan out labeled slots to **vault replicas**, and
3. decide W-of-N acceptance.

Current code additionally requires `vault.Instance != nil` on the ingesting node — treating the router as a vault replica. That blocks asymmetric ingest (e.g. scatterbox on a node with no placement) and offers no retry across partition or bootstrap windows.

Chunk-append has a partial escape hatch: cross-node `RecordForwarder` buffers for remote vault routes. Sequenced vaults bypass that path entirely. Neither path persists undelivered records across restart.

## What this layer is

A **router delivery queue** is a **persistent, per-node, pre-vault buffer** owned by the orchestrator — **not** vault spool, **not** RF, **not** queryable as vault data.

| | Router delivery queue | Vault spool |
|---|---|---|
| Scope | One queue per orchestrator (per node) | One spool store per vault **replica** |
| Holds | Digested records **before** successful delivery | `(VaultSeq, record)` **after** assign, on replicas |
| Durability | Node-local persistent (fsync before upstream ack policy) | W-of-N cluster accept semantics |
| Replication | None — single-node best-effort until delivery | RF fan-out to vault replicas |
| Drives | Drain into write path | `H`, fences, materialization, reconcile |
| On full | Bounded rolling eviction (drop oldest / reject enqueue) | N/A (accept contract) |

**Delivery** means: the destination write path completed successfully — sequenced assign + replica slot writes + W-of-N for sequenced vaults; placement-leader append (or forward) for chunk-append vaults.

Until delivery succeeds, the record stays in the queue across process restart.

## What this layer is not

- **Not** a vault replica or placement member.
- **Not** a substitute for W-of-N after delivery — vault spool accept rules unchanged.
- **Not** an ingest-idempotency or assign-time dedup map — `EventID` dedup remains at materialize/search choke points.
- **Not** a RAM reorder buffer for out-of-order `VaultSeq` on replicas (explicit non-goal in write-path-lock).

## Placement in the pipeline

```text
Ingester → digest → ROUTER DELIVERY QUEUE (persist, fsync)
                         ↓
                    drain worker (retry with backoff)
                         ↓
              dispatchDestinationWrite / forward
                         ↓
         sequenced: assign VaultSeq → replica fan-out → W-of-N
         chunk-append: local append or cross-node forward
                         ↓
              on success: dequeue / mark delivered
```

The queue sits **before** the locked write path. Vault spool slots are written only after a record leaves the queue via successful delivery.

## Sequenced vaults on non-replica ingest nodes

Sequenced ingest from nodes without a vault replica requires two coordinated changes (design + implementation):

1. **Router delivery queue** (this doc) — persist until delivery succeeds.
2. **Acceptance without local replica** — ingesting router assigns from a local swath (vault-ctl is independent of placement) and fans out to **vault replicas only**; W-of-N counts replica slot durability, not a local spool slot on a node that holds no replica.

Current `appendLocalSequenced` (`vault.Instance == nil` gate, `1 + followerSuccesses` math) must change as part of the implementation issue; this doc defines the buffer layer only.

## Persistence contract

Minimum requirements:

- **Durability before ack policy**: enqueue must reach stable storage (append + fsync or equivalent) before the ingester acknowledges upstream, unless an explicit operator policy opts into weaker ack (documented, not default).
- **Crash recovery**: on restart, reload undelivered entries and resume drain from head (or checkpoint cursor).
- **Idempotent drain**: redelivery after crash may re-assign a new `VaultSeq` — allowed by product story (no ingest idempotency); materialize/search dedup by `EventID`.
- **Bounded disk**: configurable byte/count cap per node; when full, evict oldest undelivered entries with operator-visible telemetry (drops are data loss at the buffer tier — same severity as upstream loss, but bounded and observable).

Suggested on-disk layout (implementation detail, not locked):

```text
<node-home>/router-queue/
  meta          # format version, head/tail cursors, byte cap
  log/          # append-only segments of serialized digested records + route context
```

Use the same index-last / truncate-uncommitted-tail discipline as spool windows where practical.

## Drain worker

Background loop per orchestrator:

- Dequeue head (or peek until success).
- Evaluate readiness: vault exists, route still valid, seq assign reachable, peers reachable (respect replica circuit breaker with retry, not permanent skip).
- Run delivery (sequenced or chunk-append path).
- On success: advance cursor / delete segment entry.
- On transient failure: backoff; entry stays in queue.
- On permanent failure (vault deleted, route disabled): dead-letter or drop with alert — policy TBD in implementation.

Pressure integration:

- Register queue depth + disk bytes on the orchestrator **PressureGate** so ingesters throttle when the queue is elevated (same pattern as forward-buffer backpressure, gastrolog-27zvt).

## Operator visibility

Minimum inspect surfaces (CLI + UI parity per Phase 8/9):

- queue depth (entries, bytes),
- oldest undelivered age,
- drain rate / last error,
- cap utilization,
- cumulative drops from eviction.

## Testing expectations

Every implementation PR must cover:

- **Single-node**: enqueue survives restart; drain succeeds when vault ready; cap eviction observable.
- **Multi-node (4+)**: asymmetric ingest — scatterbox on node **without** vault replica; partition peer replicas; heal on rejoin; no silent loss while queue has capacity.
- **Unhappy path**: disk full, corrupt tail truncate, vault deleted while queued, route disabled mid-drain.
- **Edge cases**: concurrent ingest + drain, shutdown drain, vault-not-ready bootstrap window.

## Authority

| Decision | Owner | Location |
|---|---|---|
| Enqueue before delivery | Orchestrator on ingesting node | Persistent router queue |
| Delivery retry/backoff | Orchestrator drain worker | Same node |
| Assign + replica fan-out | Ing ingesting router | After dequeue (write-path-lock) |
| Vault accept (`H`, W-of-N) | Vault replicas + ingesting router aggregator | Unchanged post-delivery |

## Open implementation notes

- Retention-route replays through the same queue vs direct dispatch — likely same queue for uniform durability.
- Whether ack-gated ingesters block on **delivery** (vault W-of-N) vs **enqueue** (router durable) — default should match operator expectation: ack-gated waits for delivery, not merely queue persist.
- Relationship to existing `RecordForwarder` per-peer buffers: forwarder handles in-flight cross-node chunk routes; router queue owns **all** undelivered work including local sequenced dispatch failures. Consolidation vs layering is an implementation choice; semantics must not double-drop.

## No-go conditions

Reject designs that:

- store undelivered records only in RAM or pipeline channels,
- conflate router queue with vault spool or RF,
- require ingest traffic to land only on vault replica nodes,
- ack upstream before persistent enqueue without explicit weak-ack policy,
- treat "no matching route" as a delivery failure (unrouted drop remains intentional).
