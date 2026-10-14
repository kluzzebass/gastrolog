# Fan-Out V2 Design Set

This directory contains the active fan-out v2 design documents.

## Reading order

1. **[write-path-lock.md](write-path-lock.md)** — **locked** ingest / swath / slot spool contract (start here)
2. [phase-rework-map.md](phase-rework-map.md) — phase + branch rework order
3. [architecture-overview.md](architecture-overview.md)
4. [feasibility-gate.md](feasibility-gate.md)
5. [high-watermark-contract.md](high-watermark-contract.md)
6. [spool-state-machine.md](spool-state-machine.md)
7. [router-delivery-queue.md](router-delivery-queue.md) — persistent pre-vault buffer (ingest durability rule)
8. [implementation-plan.md](implementation-plan.md)
9. [verification-ladder.md](verification-ladder.md) — Phase 10 runbooks and evidence ladder
10. [anchor-model.md](anchor-model.md) — locked GetContext anchor contract (Phase 0.5)
11. [placement-leader-migration.md](placement-leader-migration.md) — locked placement-leader direction (Phase 0.6)
12. [router-delivery-queue.md](router-delivery-queue.md) — Phase 12 ingest durability (post-cutover)

## Purpose of each doc

- `write-path-lock.md`: authoritative write path — swaths, router assign, slot spool, non-goals.
- `phase-rework-map.md`: which phases/branches/code change after the lock.
- `architecture-overview.md`: single architectural narrative and decision-authority map.
- `feasibility-gate.md`: go/no-go criteria, risk checklist, and validation expectations.
- `high-watermark-contract.md`: `VaultSeq` / fence / watermark / hole semantics (ingest idempotency sections withdrawn).
- `spool-state-machine.md`: spool lifecycle, transitions, crash/recovery progression.
- `router-delivery-queue.md`: Phase 12 — persistent per-node pre-vault buffer; nothing ingested is lost once routed.
- `implementation-plan.md`: phased migration plan including backend, CLI/UI, and k8s validation ladder.
- `verification-ladder.md`: reproducible commands and evidence template for Phase 10 sign-off.
