# Phase 6 — Replication & forwarding (cross-ref)

**Scope:** Replication paths spanning `cluster/`, `orchestrator/replication_catchup.go`, vault-ctl receipt protocol, chunk transfer.  
**Epic:** gastrolog-2p313

Phase 6 is **not a separate package set** — replication behavior is audited in Phases 4–5. This memo indexes cross-cutting replication findings.

## Primary findings (by phase)

| Topic | Finding IDs | Memo |
|-------|-------------|------|
| Vault-ctl forward without apply barrier | audit-cluster-001 | [06-cluster.md](./06-cluster.md) |
| ForwardRPC streaming mismatch | audit-cluster-003 | [06-cluster.md](./06-cluster.md) |
| WaitVaultReady poll | audit-cluster-004, sweep-011 | [06-cluster.md](./06-cluster.md) |
| Missing replica catchup | audit-orch-005 (`SweepMissingReplicas` @ 20s) | [05-orchestration.md](./05-orchestration.md) |
| Snapshot cloud projection | audit-orch-002 | [05-orchestration.md](./05-orchestration.md) |
| Peer fan-out partial merges | audit-server-003, -004 | [07-transport.md](./07-transport.md) |
| FSM vs local chunk meta | audit-orch-001, audit-query-008 | [05-orchestration.md](./05-orchestration.md), [03-query.md](./03-query.md) |

## Compensator chain (replication)

```
Replication RPC failure / missed apply
  → vaultCatchupSweepAll 20s (SweepMissingReplicas, stale FSM, …)
  → cloud backfill 5s (upload announce miss)
  → placement 15s (FollowerTargets refresh)
```

## Status

Replication **reviewed** via cluster + orchestrator + server memos. No additional package-only pass required unless `replication_catchup.go` grows new paths.
