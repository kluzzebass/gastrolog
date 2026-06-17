# Backend architecture audit

Epic: **gastrolog-2p313** — full `backend/` tree against eight criteria (see plan in Cursor or epic description).

## Artifacts

| File | Purpose |
|------|---------|
| [coverage.md](./coverage.md) | Package × phase checklist — **47/47 internal+cmd+api packages reviewed** |
| [invariant-ledger.md](./invariant-ledger.md) | Cross-phase invariant → violator map |
| [findings/](./findings/) | All audit findings — per-phase memos + cross-cutting rollups |

## Finding ID format

`audit-<phase>-<nnn>` — filed as dcat child issues when not already tracked.

## Severity

- **P0** — wrong results, cluster node matters, data loss, silent FSM/disk divergence
- **P1** — order-of-magnitude perf / scale
- **P2** — layering, duplication, dead code, doc drift

## Status

| Phase | BC | Memo | Coverage |
|-------|-----|------|----------|
| 0 | Setup | — | done |
| 1 | Storage | [01-storage.md](./findings/01-storage.md) | done |
| 2 | Ingestion | [02-ingestion.md](./findings/02-ingestion.md) | done |
| 3 | Query | [03-query.md](./findings/03-query.md) | done |
| 4 | Cluster | [06-cluster.md](./findings/06-cluster.md) | done |
| 5 | Orchestration | [05-orchestration.md](./findings/05-orchestration.md) | done |
| 6 | Replication | [06-replication.md](./findings/06-replication.md) | done (cross-ref) |
| 7 | Transport | [07-transport.md](./findings/07-transport.md) | done |
| 8 | Observability | [08-observability.md](./findings/08-observability.md) | done |
| 9 | Shared libs | [09-shared.md](./findings/09-shared.md) | done |
| 10 | API + cmd | [10-api-cmd.md](./findings/10-api-cmd.md) | done |
| 11 | system/config | [11-system.md](./findings/11-system.md) | done |
| — | Sweeps (rollup) | [04-sweep-compensators.md](./findings/04-sweep-compensators.md) | done |
| 12 | Cross-cut | [12-crosscut.md](./findings/12-crosscut.md) | done |
| 13 | Synthesis | [13-synthesis.md](./findings/13-synthesis.md) | done |

## Finding count (approx.)

| Phase | P0 | P1 | P2 |
|-------|----|----|-----|
| Storage | 2 | 6 | 6 |
| Ingestion | 1 | 8 | 9 |
| Query | 2 | 6 | 10 |
| Cluster | 1 | 4 | 9 |
| Orchestration | 3 | 10 | 9 |
| Transport | 0 | 6 | 8 |
| Observability | 0 | 1 | 4 |
| Shared | 0 | 0 | 5 |
| API+cmd | 0 | 2 | 14 |
| system | 0 | 2 | 6 |

**Next:** User review of audit docs; epic gastrolog-2p313 → `in_review`. Remediation via epics A–F in [13-synthesis.md](./findings/13-synthesis.md).
