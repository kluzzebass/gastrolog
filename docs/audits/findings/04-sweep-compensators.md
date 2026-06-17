# Cross-cutting findings — periodic sweep compensators

**Criterion:** 6 (compensating workarounds), often 8 (incoherent patterns)  
**Epic:** gastrolog-2p313

Every row is a **formal finding**. Remediation: fix upstream + remove/narrow tick — not tune the interval.

Sweep findings discovered in a phase are also listed in that phase memo (`01-storage.md`, `02-ingestion.md`, …). This file is the **rollup index** across phases.

| ID | Interval | Location | Tick does | Upstream gap (root cause) | Phase | dcat |
|----|----------|----------|-----------|---------------------------|-------|------|
| sweep-001 | 2s | `pipeline/distribution/manager.go` `rescanInterval` | Rescan `completed/`, republish stranded segments | Non-blocking send on `completed` chan in `segmentation/writer.go` | 2 | **gastrolog-2i62e** |
| sweep-002 | 2s | `pipeline/collection/manager.go` `recollectInterval` | Retry collect assignments | FSM publish wake insufficient; pull/receipt races | 2 | **gastrolog-5vwav** |
| sweep-003 | 2s | `pipeline/chunking/manager.go` `replanInterval` | Plan/build catch-up on every home | Incomplete event graph; segment pull has no vault-ctl event | 2 | **gastrolog-12gue** |
| sweep-004 | 15s | `pipeline/chunking/manager.go` `sealRetryInterval` | Rate-limit `CmdSealChunk` retries | Forward/apply failures while `sealedManifest` pending | 2 | **gastrolog-12gue** |
| sweep-005 | 15s | `app/placement.go` + `orchestrator/rotationsweep.go` | Placement / FollowerTargets / routing refresh | Missed leadership/NSC/config events | 5, 7 | TBD |
| sweep-006 | 30s | `orchestrator/vault_ctl_leader_manager.go` | Membership reconcile safety net | `desiredChanged` signal missed | 5 | TBD |
| sweep-007 | varies | `app/raft.go` `WaitForFSMCatchup` / `WaitForLeader` | Follower startup FSM / leader polls | No apply-complete event API | 4, 7 | TBD |
| sweep-008 | 5ms | `system/raftstore/store.go` `waitForLocalApply` | Poll until follower `AppliedIndex` catches leader | No post-forward FSM wake (gastrolog-2nxij partial) | 4 | extends gastrolog-2nxij |
| sweep-009 | 30s | `app/peer_cache_reconcile.go` | `ReconcilePeers` on peer caches | `PeerObservation` missed on snapshot install | 4, 7 | gastrolog-9ohip |
| sweep-010 | 20s | `orchestrator/retention.go` `vaultCatchupSweepAll` | Six reconciler sweeps (orphans, replicas, stale FSM, …) | Missed apply callbacks, snapshot gaps, RPC failures | 5 | TBD |
| sweep-011 | 100ms | `cluster/chunk_transferrer.go` `WaitVaultReady` | Poll `ForwardListChunks` until vault on peer | No cross-node vault-ready signal | 4 | TBD |
| sweep-012 | 60s | `orchestrator/cache_eviction.go` | LRU/TTL warm-cache eviction | Duplicates retention-tick `EvictCache` (audit-orch-010) | 5 | gastrolog-2idw8 |
| sweep-013 | 5s | `orchestrator/cloud_health.go` `backfillCloudUploads` | Schedule cloud upload jobs | Missed `AnnounceUpload`; snapshot restore gap | 5 | gastrolog-68fqk |
| sweep-014 | 1 min | `orchestrator/retention.go` `retentionSweepAll` | Rule eval + memory budget + duplicate evict | Policy (legitimate) + eviction duplication | 5 | — |
| sweep-015 | 30s | `app/unreachable_sweep.go` | Live↔Unreachable from PeerState | Heartbeat not wired to FSM | 7 | TBD |
| sweep-016 | 30s | `app/cluster_ctl_learner_promoter.go` | Promote caught-up learners | Join-as-learner promotion gate | 7 | TBD |
| sweep-017 | hourly | `orchestrator/archival_sweep.go` | Cloud storage-class transitions | Verify policy vs missed events | 5 | TBD |
| sweep-018 | daily 03:00 | `orchestrator/archival_sweep.go` `reconcileSweepAll` | Remove cloud index entries with missing blobs | Hot path never removes suspect index | 5 | TBD |

## Legitimate ticks (not compensators — document only)

- `statscollector` 1s heartbeat / 5s stats broadcast
- `multiraft` 1ms heartbeat batch window
- hashicorp `SnapshotInterval` 30s in `raftgroup/groupmanager.go`
- `orchestrator` progress notifier 1s (UI throttle)
- `vault_readiness` 500ms cache for `/readyz`

## Filing rule

For each new sweep finding:

1. Row in **this file** (`findings/04-sweep-compensators.md`)
2. Detail in the **phase memo** where it was found (`findings/0N-….md`)
3. **dcat child** under gastrolog-2p313
