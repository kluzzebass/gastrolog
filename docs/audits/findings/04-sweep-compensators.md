# Cross-cutting findings — periodic sweep compensators

**Criterion:** 6 (compensating workarounds), often 8 (incoherent patterns)  
**Epic:** gastrolog-2p313 → remediation **gastrolog-8gmd0** (Compensator retirement)

Every row is a **formal finding**. Remediation: fix upstream + remove/narrow tick — not tune the interval.

| ID | Interval | Location | Tick does | Upstream gap (root cause) | dcat |
|----|----------|----------|-----------|---------------------------|------|
| sweep-001 | 2s | `pipeline/distribution/manager.go` `rescanInterval` | Rescan `completed/`, republish stranded segments | Non-blocking send on `completed` chan in `segmentation/writer.go` | **gastrolog-2i62e** |
| sweep-002 | 2s | `pipeline/collection/manager.go` `recollectInterval` | Retry collect assignments | FSM publish wake insufficient; pull/receipt races | **gastrolog-5vwav** |
| sweep-003 | 2s | `pipeline/chunking/manager.go` `replanInterval` | Plan/build catch-up on every home | Incomplete event graph; segment pull has no vault-ctl event | **gastrolog-12gue** |
| sweep-004 | 15s | `pipeline/chunking/manager.go` `sealRetryInterval` | Rate-limit `CmdSealChunk` retries | Forward/apply failures while `sealedManifest` pending | **gastrolog-12gue** |
| sweep-005 | 15s | `app/placement.go` + `orchestrator/rotationsweep.go` | Placement / FollowerTargets / routing refresh | Missed leadership/NSC/config events | **gastrolog-29xpy** |
| sweep-006 | 30s | `orchestrator/vault_ctl_leader_manager.go` | Membership reconcile safety net | `desiredChanged` signal missed | **gastrolog-3oram** |
| sweep-007 | varies | `app/raft.go` `WaitForFSMCatchup` / `WaitForLeader` | Follower startup FSM / leader polls | No apply-complete event API | **gastrolog-1go57** |
| sweep-008 | 5ms | `system/raftstore/store.go` `waitForLocalApply` | Poll until follower `AppliedIndex` catches leader | No post-forward FSM wake (gastrolog-2nxij partial) | **gastrolog-3klg1** |
| sweep-009 | 30s | `app/peer_cache_reconcile.go` | `ReconcilePeers` on peer caches | `PeerObservation` missed on snapshot install | **gastrolog-1loe9** (see **gastrolog-9ohip** closed) |
| sweep-010 | 20s | `orchestrator/retention.go` `vaultCatchupSweepAll` | Six reconciler sweeps | Missed apply callbacks, snapshot gaps, RPC failures | **gastrolog-3fu9t** |
| sweep-011 | 100ms | `cluster/chunk_transferrer.go` `WaitVaultReady` | Poll `ForwardListChunks` until vault on peer | No cross-node vault-ready signal | **gastrolog-3sdnn** |
| sweep-012 | 60s | `orchestrator/cache_eviction.go` | LRU/TTL warm-cache eviction | Duplicates retention-tick `EvictCache` | **gastrolog-1a18r** (see **gastrolog-2idw8** closed) |
| sweep-013 | 5s | `orchestrator/cloud_health.go` `backfillCloudUploads` | Schedule cloud upload jobs | Missed `AnnounceUpload`; snapshot restore gap | **gastrolog-576bm** |
| sweep-014 | 1 min | `orchestrator/retention.go` `retentionSweepAll` | Rule eval + memory budget | Policy (legitimate) + eviction duplication | — |
| sweep-015 | 30s | `app/unreachable_sweep.go` | Live↔Unreachable from PeerState | Heartbeat not wired to FSM | **gastrolog-48o1r** |
| sweep-016 | 30s | `app/cluster_ctl_learner_promoter.go` | Promote caught-up learners | Join-as-learner promotion gate | **gastrolog-4vg17** |
| sweep-017 | hourly | `orchestrator/archival_sweep.go` | Cloud storage-class transitions | Verify policy vs missed events | **gastrolog-15nn1** |
| sweep-018 | daily 03:00 | `orchestrator/archival_sweep.go` `reconcileSweepAll` | Remove cloud index entries with missing blobs | Hot path never removes suspect index | **gastrolog-2iwai** |

## Legitimate ticks (not compensators — document only)

- `statscollector` 1s heartbeat / 5s stats broadcast
- `multiraft` 1ms heartbeat batch window
- hashicorp `SnapshotInterval` 30s in `raftgroup/groupmanager.go`
- `orchestrator` progress notifier 1s (UI throttle)
- `vault_readiness` 500ms cache for `/readyz`
- sweep-014 retention rule evaluation (policy-driven)

## Filing rule

1. Row in **this file**
2. Detail in phase memo where found
3. **dcat child** under gastrolog-8gmd0 (or root-cause epic gastrolog-36wys for upstream fix)

See [issue-map.md](../issue-map.md).
