# Fan-out data-plane docs

Design set for the fan-out data-plane architecture — the leaderless
"every Receiving replica appends + fans out" write model for vault chunks.

## Status: v1 archived, v2 pending

The **v1 implementation was rewound off `main`** on 2026-05-25. It was judged
not salvageable: the design layered **synchronous global invariants** onto a
fundamentally asynchronous system (single-Active chunk invariant, FSM-mediated
chunk identity/rotation on the hot path, synchronous W-of-N acks, a single
authoritative live durable count). Each synchronization point spawned the next
complication — implement-and-discover, repeatedly — and the result wedged under
repeated burst load and could not even be measured reliably (every count path
disagreed with the FSM manifest).

What is **not** wrong: the leaderless fan-out *traffic* model itself. A single
writer / leader-per-vault was rejected because it causes substantial overhead
and unbalanced traffic. v2 keeps the fan-out shape and removes the
synchronization, not the fan-out.

## Where the code went

- **`archive/fan-out`** branch — the full merged v1 epic (`gastrolog-2ujjh`,
  72 commits). The implementation, for reference.
- **`gastrolog-69vv0`** branch (local) — v1 + the unmerged stabilization stack
  (orphan-loss fix `1l46z`, the rotation/birth deadlock fix `23ups`, the
  single-writer transport and copy-on-write RouteSet rework, etc.) and
  `docs/write-path-rework.md`.

## v2 direction

Keep leaderless every-Receiver-appends fan-out; remove **every** synchronization
point. The raw material is already here: records carry globally-unique
EventIDs, so identity needs no agreement → chunks can be node-local, multiple
actives can coexist, and durability + dedup become **reconciled,
eventually-consistent** properties computed asynchronously rather than
invariants enforced synchronously. The reconcile path in
[obsoleted/fan-out-data-plane-design.md](obsoleted/fan-out-data-plane-design.md) (EventID Merkle
summaries, divergence-tolerance contracts) already leans this way; v2 extends
the same async treatment to sealing, acks, and counting.

**Design-first**: the v1 failure was implementation-driven. v2 works the full
async design and its failure modes out on an epic branch *before* any code, and
merges to `main` only when the whole system is validated (repeated-burst
survival + a durable-count measurement that agrees with the FSM manifest).

## Terminology note

V2 documents use two different fan-out terms:

- **Route fan-out**: one source record routed into multiple destination vaults.
- **Replica fan-out**: one destination-vault write replicated to that vault's replicas under `W-of-N`.

Do not use these interchangeably.

## Documents

### Current v2 set

- [v2/write-path-lock.md](v2/write-path-lock.md) — **locked** write path (swaths, slots, fan-out)
- [v2/phase-rework-map.md](v2/phase-rework-map.md) — phase/branch rework order
- [v2/architecture-overview.md](v2/architecture-overview.md) — canonical v2
  architecture narrative and authority map.
- [v2/feasibility-gate.md](v2/feasibility-gate.md) — v2 go/no-go gate and
  validation checklist.
- [v2/high-watermark-contract.md](v2/high-watermark-contract.md) — sequence
  allocation, high watermark/fence semantics, hole classification, and
  retention-route destination sequencing.
- [v2/spool-state-machine.md](v2/spool-state-machine.md) — spool lifecycle,
  transitions, recovery rules, and issue-splitting boundaries.

### Obsoleted v1 design

- [obsoleted/fan-out-data-plane-design.md](obsoleted/fan-out-data-plane-design.md)
  — v1 proposed architecture (write path, seal-time reconcile,
  Receiving/Holding FSM split, W-of-N, set-diff reconcile, divergence
  tolerance). Kept for historical context.
- [obsoleted/pull-records-design.md](obsoleted/pull-records-design.md) — v1-era
  pull-based record catch-up/reconcile design notes.
