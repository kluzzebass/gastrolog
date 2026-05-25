# Fan-out data-plane design

Design documents for the fan-out data-plane architecture — the leaderless
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
[fan-out-data-plane-design.md](fan-out-data-plane-design.md) (EventID Merkle
summaries, divergence-tolerance contracts) already leans this way; v2 extends
the same async treatment to sealing, acks, and counting.

**Design-first**: the v1 failure was implementation-driven. v2 works the full
async design and its failure modes out on an epic branch *before* any code, and
merges to `main` only when the whole system is validated (repeated-burst
survival + a durable-count measurement that agrees with the FSM manifest).

## Documents

- [fan-out-data-plane-design.md](fan-out-data-plane-design.md) — the v1 proposed
  architecture (write path, seal-time reconcile, Receiving/Holding FSM split,
  W-of-N, set-diff reconcile, divergence tolerance). The starting point for v2;
  read it knowing the synchronous mechanisms are what v2 must redesign.
- [pull-records-design.md](pull-records-design.md) — the pull-based record
  catch-up / reconcile mechanism.
