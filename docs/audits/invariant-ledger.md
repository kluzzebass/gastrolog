# Invariant ledger

Cross-phase map: broken invariants → compensating code. Epic: **gastrolog-2p313**.

| Invariant | Authoritative source | Consumers | Violators / compensators |
|-----------|---------------------|-----------|--------------------------|
| Sealed chunk TS-ordered search uses mmap ITSI rank/position, not heap decode of full index | `index/file/tsidx.MmapView`, `FindIngestRank` | `query.buildTSOrderedScanner` | **F-01**: `LoadIngestEntries` heap slurp — **gastrolog-2o9e9** |
| Sealed chunk with local/warm GLCB never buffers all records for TS order | ITSI in GLCB + position scanner | `reorderByTSWithBounds` | **F-02**: cold/missing index → full buffer — **gastrolog-2o9e9**, gastrolog-1dg3i |
| Cloud chunk index read uses FSM TOC offsets / range fetch when blob not local | FSM `IngestIdxOffset`, `blobstore.DownloadRange` | `chunk/file.openCloudCursor` | **F-03**: full blob download — audit-storage-003 |
| Histogram IngestTS rank available on every node that serves vault-ctl FSM | `manifest.IndexReader` + FSM | `histogram`, `query` | **F-06**: local vault instance gate → FSM proportional estimate |
| FSM sealed manifest ↔ local cm chunk set converge | vault-ctl Raft + reconciler | query, retention, inspector | **F-08**: dual meta; `OverlayFromFSM`; **gastrolog-3ukgz** |
| Attr breakdown reads attrs only, not full records | `ScanAttrs` / idx+attr path | histogram `timechartChunkGroups` | **F-04**: `scanAttrsViaGLCB` full `ReadRecord` |
| Sidecar indexes exist for sealed file chunks before cloud flip | `PostSealProcess` | `index/file` openers | **F-11**: `CloudBacked` skip → runtime scan fallback |
| Every completed segment reaches vault-ctl registry promptly | Segmentation close + distribution publish | `distribution.rescanStranded` (2s) | audit-ingestion-002 — **gastrolog-2i62e** |
| Collect assignment follows FSM segment publish | vault-ctl apply + wake | `collection.recollectInterval` (2s) | audit-ingestion-003 — **gastrolog-5vwav** |
| Chunk plan/build follows segment availability events | vault-ctl events | `chunking.replanInterval` (2s) | audit-ingestion-004 — **gastrolog-12gue** |
| Seal retries only on explicit forward/apply failure | `CmdSealChunk` error path | `chunking.sealRetryInterval` (15s) | audit-ingestion + **gastrolog-12gue** |
| Ingester BC decoupled from orchestrator | `pipeline/ingestion` types | `ingesterAdapter` + all `ingester/*` | audit-ingestion-005, L1 layering |
| GLCB visible to local query immediately after seal | FSM seal + reconciler | `registerPipelineGLCB` triple path | audit-orch-003 / audit-ingestion-009 |
| Vault-ctl follower apply visible before RPC returns | `AppliedIndex` barrier | `ForwardVaultApply` | **F-12**: missing vs system `ForwardApply` — audit-cluster-001 |
| System config follower apply visible before RPC returns | `ForwardApplyResponse.applied_index` | `raftstore.forwardAndWait` | gastrolog-2nxij (fixed for system Raft) |
| WAL `StoreLogs` batches amortize fsync | `raftwal` batch writer | hashicorp `LogStore` | **F-13**: per-log submit — audit-cluster-002 |
| Peer cache reflects current Raft membership | `PeerObservation` on config change | `peer_cache_reconcile` 30s | sweep-009 — gastrolog-9ohip |
| Placement / FollowerTargets refresh on config apply | FSM config dispatch | `placementSweep` 15s | sweep-005 — audit-orch-006 |
| Vault-ctl membership matches desired on leadership change | `SetDesiredMembers` + barrier | `vault_ctl_leader_manager` 30s | sweep-006 — audit-orch-007 |
| Cloud upload announced → cm.cloudIdx updated | `CmdUploadChunk` apply | `backfillCloudUploads` 5s | sweep-013 — audit-orch-008 |
| Receipt-protocol delete is sole delete executor | vault-ctl reconciler | `wireVaultFSMOnDelete` legacy | audit-orch-011 — gastrolog-51gme |
| Steady-state FSM apply suffices without periodic re-walk | apply callbacks + snapshot restore hooks | `vaultCatchupSweepAll` 20s (6 sweeps) | sweep-010 — audit-orch-005 |
| Ingester test/trigger runs on assignment node | placement + assignment | local-only RPC handlers | audit-server-001/002 |
| Inspector fan-out marks missing peers | peer health | silent partial merge | audit-server-003 — gastrolog-csspr |
| Storage class match is strict for placement | `eligibleStorages` | `StorageIDForNode` fallback to `[0]` | audit-system-001/002 |
| Node storage config merge uses consistent NodeId encoding | proto `bytes` UTF-8 GLID string | `add-storage` uses `glid.FromBytes` | audit-cmd-002 |

## Compensator chain (ingestion → orchestration)

```
completed chan drop → distribution 2s rescan (sweep-001)
  → collection 2s recollect (sweep-002)
  → chunking 2s replan (sweep-003)
  → seal 15s retry (sweep-004)

FSM apply miss / snapshot gap
  → vaultCatchup 20s: 6 reconciler sweeps (sweep-010)
  → cloud backfill 5s (sweep-013)
  → placement 15s (sweep-005)
  → vault-ctl membership 30s (sweep-006)
```

Remediation rule: fix the **upstream gap** in the invariant column, then remove the sweep row from [`04-sweep-compensators.md`](./findings/04-sweep-compensators.md).
