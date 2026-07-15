# Phase 4 — Cluster / Raft audit

**Packages:** `cluster/`, `multiraft/`, `raftgroup/`, `raftwal/`, `vaultraft/`, `system/raftfsm/`, `system/raftstore/`, `system/command/` (Raft paths)  
**Epic:** gastrolog-2p313

## Clean

`multiraft/` (transport, encode/decode), `raftgroup/leader_loop.go`, `raftgroup/vault_group_id.go`, `system/command/` (marshal/unmarshal only).

`raftwal/` has strong harness tests but is **not** clean (P1 heap + per-log submit).

## Findings

| ID | Crit | Sev | Location | Violation | Correct pattern | Workaround? | dcat |
|----|------|-----|----------|-----------|-----------------|-------------|------|
| audit-cluster-001 | 3,4,8 | P0 | `vault_apply_forwarder.go`, `forward.go` `forwardVaultApply` | Vault-ctl forward returns on RPC success **without** local FSM catch-up (no `applied_index` vs `ForwardApply` / gastrolog-2nxij) | Mirror system path: follower polls `AppliedIndex` before return | Y | **gastrolog-4l24u** |
| audit-cluster-002 | 1,2,5 | P1 | `raftwal/groupstore.go` `StoreLogs` | Per-log `submit()` + full payload in `gs.logs` until `DeleteRange` | Batch per `StoreLogs`; snapshot cadence documented | Y | TBD |
| audit-cluster-003 | 2,3,4 | P1 | `forward_rpc.go` | Streaming API vs unary-only handler (≤4MB buffer) | Implement streaming or narrow API | Y | TBD |
| audit-cluster-004 | 4,6 | P1 | `chunk_transferrer.go` `WaitVaultReady` | 100ms poll `ForwardListChunks` | Vault-ready signal on peer | Y | **sweep-011** |
| audit-cluster-005 | 6,8 | P1 | `app/peer_cache_reconcile.go` + `ReconcilePeers` | 30s purge when `PeerObservation` missed | Reliable snapshot-install signal | Y | **sweep-009** |
| audit-cluster-006 | 6,4 | P2 | `raftstore/store.go` `waitForLocalApply` | 5ms poll after config forward | FSM-apply notify | Y | **sweep-010** |
| audit-cluster-007 | 3,8 | P2 | `forward.go` search stream + `isMissingLocalChunkFileError` | Partial results, no error on missing file | Retryable error / resume | Y | TBD |
| audit-cluster-008 | 7,8 | P2 | `cluster.go` / `forward.go` | 15+ `Set*Executor` callbacks | Thinner transport | Y | gastrolog-3pf9w |
| audit-cluster-009 | 7,8 | P2 | `vaultraft/fsm.go` vs `vaultctlfsm/fsm.go` `Ready()` | Two readiness models | Single contract | Y | TBD |
| audit-cluster-010 | 2,8 | P2 | `raftwal/wal.go` replay | Stale comment; silent decode skip on index update | Fix comment; fail replay | N | TBD |
| audit-cluster-011 | 8 | P2 | `raftwal/wal.go` `syncCh` | Unused field | Remove or implement | N | — |
| audit-cluster-012 | 8 | P2 | `raftgroup/groupmanager.go` comment | Says BoltDB; requires WAL | Update comment | N | — |
| audit-cluster-013 | 3,6,7 | P2 | `vaultctlfsm/announcer.go` | Best-effort apply; reconcile-on-load | Durable queue | Y | TBD |
| audit-cluster-014 | 8 | P2 | `raftfsm/fsm.go` `Restore` orphan filter | Migration filter for old snapshots | Steady-state shouldn't need | N | gastrolog-485u1 |

## WAL correctness (summary)

Coherent: `DeleteRange` semantics, multi-group isolation, `wal_invariants_test.go` / harness / fuzz. Residual: audit-cluster-002, -010.

## Cluster-first forwarding (summary)

System config path has read-after-write barrier. **P0 gap:** vault-ctl path (audit-cluster-001).

See sweeps **sweep-009**, **sweep-010**, **sweep-011** in [`04-sweep-compensators.md`](./04-sweep-compensators.md).
