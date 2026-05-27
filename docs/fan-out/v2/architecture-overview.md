# Fan-Out V2 Architecture Overview

Status: draft. **Write-path ingest/assign/spool accept:** see locked [write-path-lock.md](write-path-lock.md).

This document is the single architectural overview for fan-out v2.

## Design intent

V2 keeps the leaderless traffic shape and removes hot-path synchronization on chunk identity.

### System contract (ingest)

Nothing that is ingested is lost — **provided a route destination captures the record.** No matching route → intentional drop (unchanged). Once routed, transient delivery failure (partition, vault-not-ready, no local replica, restart mid-write) must not drop the record.

That contract requires a **persistent router delivery queue** on each node (pre-vault, not RF) — see [router-delivery-queue.md](router-delivery-queue.md). Today this layer does not exist; routed records are dropped after a single failed delivery attempt.

### Design rule: cluster-first

Every feature must work on every node — no assumption that the user or upstream is connected to a particular node or vault replica holder. This is an **architectural design rule**, not the ingest durability contract itself. It explains why delivery buffering is per-node rather than centralized.

The write path is:

- digest → **persist to router delivery queue** (survives restart; bounded on disk),
- drain → route fan-out (source record → destination vault set),
- per-router **seq swath** from vault-ctl allocator leader,
- local **`VaultSeq` assign** and attach before replica fan-out,
- per-destination replica fan-out with `W-of-N` durability,
- asynchronous fence / materialize / reconcile for sealed output.

**Identity:** strong `EventID`; dedup at materialize, search, and other choke points — **not** assign-time ingest idempotency.

## V1 vs V2 At A Glance

| Topic | V1 (rewound) | V2 (current direction) |
|---|---|---|
| Hot-path chunk identity | Coordinated on write path | Removed from write path; handled asynchronously at seal/materialize time |
| Canonical inclusion boundary | Ambiguous under churn | Deterministic sequence-range fences (`prev < seq <= curr`) |
| Ordering model | Emergent from local timing/state | Vault-wide **`VaultSeq`** from allocator swaths |
| Fan-out semantics | Mechanically present but overloaded with synchronous invariants | Route fan-out and replica fan-out explicitly separated |
| Reconcile role | Correctness + ambiguity cleanup | Coverage healing + **EventID** canonicalization at seal |
| Decision ownership | Mixed/implicit in places | Explicit authority map (router, allocator leader, materializer, reconciler) |
| Implementation posture | Design-first with gate + contract + lifecycle docs | Write-path lock + phase rework (see [phase-rework-map.md](phase-rework-map.md)) |

## Two fan-outs (different layers)

- **Route fan-out**: router sends one source record to multiple destination vaults (each vault gets its own `VaultSeq`).
- **Replica fan-out**: ingesting router sends one labeled write to **all** replicas of a destination vault (same `VaultSeq` everywhere).

These are separate operations and must not be conflated.

## Placement-leader field migration scope

`VaultPlacement.Leader` is currently wired through multiple subsystems (replication,
retention, forwarder target derivation, and related tests). Under V2's leaderless
write model, that bit cannot remain the write-path authority as-is.

V2 implementation scope must include one explicit migration decision:

- remove the write-path meaning of `VaultPlacement.Leader`, or
- redefine it to a non-write-path role with clear semantics.

Sequenced vaults **do not** relay full records through placement residency for assign/spool accept. See [write-path-lock.md](write-path-lock.md).

## Core pipeline

For each source record on the ingesting node:

1. Router **persists** the digested record to the local delivery queue (fsync before upstream ack unless weak-ack policy).
2. Drain worker computes destination vault set and attempts delivery.
3. For each destination vault:
   - ensure local seq swath (refill from vault-ctl allocator leader when empty),
   - assign `VaultSeq` locally (`swath.next++`),
   - attach `VaultSeq` to record,
   - replica fan-out in parallel to all vault replicas,
   - resolve write success/failure from `W-of-N`.
4. On delivery success, dequeue; on transient failure, retain in queue for retry.
5. Fence coordinator cuts fence boundaries (`F_n`) by policy on assigned seq space.
6. Materializer reads spool slots by `VaultSeq`, **dedups by `EventID`**, writes sealed chunks.
7. Reconcile converges sealed record sets by `EventID`.

Spool implementation note (locked):

- spool stores **slots** keyed by `VaultSeq` inside **sequence windows** (allocator swath ranges),
- out-of-order slot arrival is normal; no RAM reorder buffer,
- write durability visibility follows index-last commit semantics per slot/window,
- restart recovery truncates uncommitted tails per window crash rules.

## Locked sequencing rule

- `VaultSeq` is assigned on the **ingesting router** from a local swath **before** replica fan-out.
- The same `VaultSeq` is carried on the wire to every replica.
- Replicas and materializer treat `VaultSeq` as part of the write payload, not a local post-write annotation.

## Decision Authority Matrix

| Decision | Decisionmaker | Execution location | Decision time | Durable source of truth |
|---|---|---|---|---|
| Pre-vault ingest persistence | Orchestrator on ingesting node | Router delivery queue (node-local disk) | Before delivery attempt; before upstream ack (default) | Node-local queue log + cursors |
| Route destination vault set | Router on the node processing the source record | Routing evaluation pipeline on processing node | During ingest or retention-route evaluation | Route config + per-write routing context |
| Destination-vault seq swath allocation | Vault-ctl leader for that destination vault | Allocator on vault-ctl leader | On swath request / refill | Vault-ctl Raft log (allocator state + grants) |
| Destination-vault per-record `VaultSeq` assignment | Ing ingesting router (using local swath) | Router on processing node | After destination selection, before replica fan-out | Attached to fan-out payload; spool slot on each replica |
| Destination-vault replica fan-out target snapshot (`N`) | Ing ingesting router | Replica dispatch on processing node | At write dispatch | In-flight write context + replica slot writes |
| Destination-vault write success/failure (`W-of-N`) | Ing ingesting router | Fan-out result aggregator | After responses or timeout | Ack outcome + durability/error telemetry |
| Fence cut boundary (`F_n`) | Vault-ctl leader (authoritative fence coordinator) | Fence coordinator path on vault-ctl leader | On policy trigger evaluation (count/time/age) | Vault-ctl Raft log + persisted fence record |
| Fence trigger evidence (`FenceHint`) | Data-bearing replica nodes | Replica hint emitter path | When local policy evidence crosses trigger threshold | Ephemeral hint channel only (not authoritative) |
| Materialization progression (`M_r`) | Local materializer on each replica | Local spool-to-chunk materializer | During batch materialization | Local durable checkpoint/watermark state |
| Reconcile completion (`ConvergeSealed`) | Reconcile coordinator + participating replicas | Reconcile worker + local apply paths | After hole classification + fill completion | Convergence marker / reconcile completion metadata |
| Retention-route destination sequencing | Ing ingesting router for destination vault | Retention route → same write path | When routing retained records into destination vault(s) | New `VaultSeq` per destination vault accept |

## Why this fixes v1 failure mode

V1 failed primarily on ambiguous chunk membership under churn.

V2 makes membership explicit:

- deterministic inclusion rule: `prev_fence < VaultSeq <= curr_fence`,
- chunk naming moved off hot path,
- **EventID** canonicalization at materialize/search, not synchronous ingest agreement.

## What remains asynchronous by design

- per-replica slot arrival timing (OOO within seq space),
- per-replica spool/materialization progress,
- reconcile completion timing,
- duplicate accepts at different seq until materialize/search dedup.

These are expected and modeled; they are not correctness failures by default.

## Read behavior before convergence (policy scope)

Before `ConvergeSealed`, the system policy must specify:

- coverage set: which replicas/spool ranges are queried,
- visibility contract: whether missing non-converged records are acceptable,
- merge behavior: how spool and chunk sources combine in one result stream,
- labeling: how counts/results are marked as provisional vs authoritative.

## Source docs

- [write-path-lock.md](write-path-lock.md) — **authoritative write path**
- [router-delivery-queue.md](router-delivery-queue.md) — Phase 12 persistent pre-vault buffer (post-cutover)
- [phase-rework-map.md](phase-rework-map.md) — stack/branch rework order
- [feasibility-gate.md](feasibility-gate.md)
- [high-watermark-contract.md](high-watermark-contract.md)
- [spool-state-machine.md](spool-state-machine.md)
