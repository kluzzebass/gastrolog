# Audit: Subsystems Treating Disk as Authority

**Status:** Audit deliverable for [gastrolog-5dfv7](dcat://gastrolog-5dfv7) under epic [gastrolog-2i1g9](dcat://gastrolog-2i1g9). This document inventories code paths that treat local disk state as authoritative for information that has migrated (or is migrating) to the vault-ctl FSM. Each entry becomes a cleanup target tracked as its own implementation issue under the migration epic.

## What this audits

For each candidate site, the test is:
1. Does it consult local disk (`os.ReadDir`, on-disk meta files, the chunk-manager in-memory map populated from disk) for "what chunks does this node have"?
2. Does it make a **cluster-coordinated decision** based on that answer — replication targets, retention scheduling, search routing, RPC responses?
3. If yes to both → the disk read should become **evidence** to reconcile against the FSM, not the **authority**.

Sites that pass (1) but fail (2) — purely local operations like file-handle management, GLCB existence checks, temp-file cleanup — are not findings. They legitimately operate on local-only concerns.

## Findings

### F1 — `loadExisting` is the root disk-authority site

[backend/internal/chunk/file/manager.go:1037-1103](backend/internal/chunk/file/manager.go#L1037) `Manager.loadExisting()`.

`os.ReadDir(m.cfg.Dir)` scans the vault directory at startup. Every subdirectory whose name parses as a `chunk.ChunkID` becomes an entry in `m.metas`. This in-memory map is the chunk manager's complete view of "what chunks does this node have."

The function already partially honors FSM authority: a comment at [line 1077-1093](backend/internal/chunk/file/manager.go#L1077) notes that sealed-state projection has been moved to FSM via `VaultLifecycleReconciler.onSeal` + `ReconcileFromSnapshot` (gastrolog-51gme step 8). So sealed/unsealed is FSM-driven. But the **existence** question (is chunk X a thing for this vault on this node?) is still disk-derived.

**Reframing under FSM authority:** the chunk manager should learn the set of chunks-it-should-have from the FSM (residency = `placement_set - pendingDeletes.ExpectedFrom`, filtered to chunks where this node is in the placement set). The disk scan becomes a reconciliation pass — files matching FSM-expected chunks load normally; files for chunks not in the expected set raise orphan signals; chunks in the expected set with no files raise catchup signals.

**Related prior art (do not duplicate):** [`SweepMissingReplicas`](backend/internal/orchestrator/vault_lifecycle_reconciler.go#L686) and [`SweepLocalOrphans`](backend/internal/orchestrator/vault_lifecycle_reconciler.go#L580) already implement the diff-against-FSM pattern at the orchestrator level. The cleanup here is to extend that pattern into the chunk manager's startup itself, so the disk-derived state never claims to be authoritative even briefly.

### F2 — `m.metas` is consulted as the local truth across the chunk manager

Same file. Reads of `m.metas[id]` occur at [lines 748, 1167, 1386, 2994, 3110, 3545, 3666, 3827, 3905, 4085, 4139](backend/internal/chunk/file/manager.go#L748). Writes at [lines 1070, 1817, 2278, 3708](backend/internal/chunk/file/manager.go#L1070). Public surface via [`HasLocalContent`](backend/internal/chunk/file/manager.go#L2702) reads `m.metas` and is consulted by callers including [query/histogram.go:475](backend/internal/query/histogram.go#L475).

Because `m.metas` is built by F1, every consumer inherits the "disk is truth" framing. After F1's reframing, `m.metas` becomes the in-memory cache of the reconciled-against-FSM view; reads are unchanged in shape but the values are now FSM-authoritative.

**Cleanup target:** scoped to F1; no separate code surface to change. Listed as a separate finding because the consumer set is large and the audit should make their lineage explicit.

### F3 — `RemoveVault` empty check uses disk-derived view

[backend/internal/orchestrator/orchestrator.go:140-166](backend/internal/orchestrator/orchestrator.go#L140) `RemoveVault`. Reads `vaultInst.Chunks.List()` (disk-derived via F1) to check whether the vault has data. Refuses removal if `RecordCount > 0` or any chunk unsealed. Does **not** consult the FSM manifest for the same question.

**Failure mode:** a vault could have FSM-recorded chunks that this node doesn't hold locally (followers, post-recovery state). Local view says "empty," but the FSM still tracks data. `RemoveVault` would succeed and remove the local vault instance while the cluster still expects this node to participate.

**Reframing under FSM authority:** consult FSM manifest first; emptiness is "FSM has no chunks for this vault AND no pendingDeletes obligations." Local chunk list is corroborating evidence at most.

### F4 — Archival sweep iterates from disk view, overlays FSM state

[backend/internal/orchestrator/archival_sweep.go:110-160](backend/internal/orchestrator/archival_sweep.go#L110) `archivalSweepVault`. Iterates `vaultInst.Chunks.List()` and applies `OverlayFromFSM(meta)` before gating on `meta.Sealed && meta.CloudBacked`.

The overlay pattern is the right direction — it acknowledges FSM authority for state — but the iteration is one-sided: it only sees chunks the local node has on disk. Chunks the FSM manifest knows about but the local node doesn't have are silently invisible to the archival sweep.

**Failure mode:** in a multi-node scenario where the archival sweep runs on a node that has FSM membership but partial chunk presence, chunks that should be archived but live elsewhere are not seen by this node's sweep. In practice the original holder's sweep handles it, but the per-node asymmetry creates a non-obvious dependency on which node runs the sweep first.

**Reframing under FSM authority:** iterate FSM manifest as the source of truth for "what chunks belong to this vault." Use local disk presence as a per-chunk routing input ("am I the right node to archive this one?") rather than as the iteration domain.

### F5 — Orchestrator `ListChunkMetas` is the disk-derived RPC surface

[backend/internal/orchestrator/vault_ops.go:180-204](backend/internal/orchestrator/vault_ops.go#L180) `Orchestrator.ListChunkMetas`. Returns the disk-derived chunk list for this node's view of a vault. Consumed by:

- [server/vault_chunks.go:42](backend/internal/server/vault_chunks.go#L42), [:433](backend/internal/server/vault_chunks.go#L433) — `ListChunks` RPC local-collection step
- [server/query.go:810](backend/internal/server/query.go#L810) — search engine chunk discovery
- [server/vault_info.go:383](backend/internal/server/vault_info.go#L383) — vault info display
- [server/vault_operations.go:94](backend/internal/server/vault_operations.go#L94), [:142](backend/internal/server/vault_operations.go#L142) — vault operations
- [app/executors.go:359](backend/internal/app/executors.go#L359), [:439](backend/internal/app/executors.go#L439) — query executors

All of these inherit the disk-derived view via F1 → F2 → F5.

**Mixed semantics:** some of these consumers want "what chunks exist locally on this node" (search engine, executors — disk view is correct), others want "what chunks exist for this vault according to the cluster" (vault info display, ListChunks RPC top-level — should be FSM-authoritative).

**Reframing under FSM authority:** split `ListChunkMetas` into two API surfaces:
- `ListLocalChunkMetas(vault)` — disk-derived; explicit per-node; used by local query paths
- `ListClusterChunkMetas(vault)` — FSM-authoritative; used by RPC surfaces and display

Audit each consumer and route to the right one.

### F6 — `rebuildVaultIndexes` iterates local chunks

[backend/internal/orchestrator/lifecycle.go:512-542](backend/internal/orchestrator/lifecycle.go#L512) `rebuildVaultIndexes`. Iterates `vaultInst.Chunks.List()` and rebuilds incomplete indexes for sealed chunks.

**This is correct under both authority models.** Index rebuild is an inherently local operation — it builds files on this node's disk for chunks this node holds. The disk view is the right iteration domain. The FSM overlay (line 530-535) handles the sealed-state gating correctly.

**Status:** Not a finding. Listed for completeness so future readers don't mistake it for a cleanup target.

### F7 — Cloud health check iterates local chunks

[backend/internal/orchestrator/cloud_health.go:42-73](backend/internal/orchestrator/cloud_health.go#L42) `cloudHealthCheckVault`. Iterates `vaultInst.Chunks.List()` to verify cloud-backed chunks against their blob storage.

Same shape as F6 — verification is intrinsically per-node (each node checks its own knowledge of cloud blobs). Disk view is acceptable iteration domain. The follow-up question is whether a node that *doesn't* hold a chunk locally but is in its placement set should also be checking cloud health for it — but that's an active-design question, not a current-correctness issue.

**Status:** Not a finding. Worth noting in case future scope shifts.

### F8 — FSM Restore paths are correct

[backend/internal/vaultraft/fsm.go:164-225](backend/internal/vaultraft/fsm.go#L164) `FSM.Restore`, [vaultraft/vaultctlfsm/fsm.go:638](backend/internal/vaultraft/vaultctlfsm/fsm.go#L638) `FSM.Restore`. Replace FSM state from snapshot; fire `onAfterRestore` callback which triggers [`VaultLifecycleReconciler.ReconcileFromSnapshot`](backend/internal/orchestrator/vault_lifecycle_reconciler.go#L183).

The reconciler then runs [`projectAllSealedFromFSM`](backend/internal/orchestrator/vault_lifecycle_reconciler.go#L227), which iterates FSM entries (FSM-authoritative) and projects state onto local chunks. This is the **correct FSM-authority pattern**: FSM is the source, disk is the destination.

**Status:** Not a finding. Listed as prior art and as the model for F1's reframing.

### F9 — `SweepLocalOrphans` and `SweepMissingReplicas` are correct

[backend/internal/orchestrator/vault_lifecycle_reconciler.go:580](backend/internal/orchestrator/vault_lifecycle_reconciler.go#L580), [:686](backend/internal/orchestrator/vault_lifecycle_reconciler.go#L686).

Both sweeps already implement the diff-against-FSM pattern: list local (disk view) + list FSM (authority), diff, act. The reconciliation infrastructure is in place; the migration's job is to extend the same pattern into other consumer call sites.

**Status:** Not a finding. Prior art; the pattern other findings should converge on.

### Sites scanned and not flagged

- [chunk/file/manager.go:1109](backend/internal/chunk/file/manager.go#L1109) `cleanOrphanTempFiles` — temp file cleanup; per-node, not authoritative
- [chunk/file/manager.go:2416](backend/internal/chunk/file/manager.go#L2416), [:2440](backend/internal/chunk/file/manager.go#L2440) `computeDiskBytes` / `computeTotalLogicalBytes` — measuring disk usage for known chunks
- [chunk/file/manager.go:2495](backend/internal/chunk/file/manager.go#L2495) `chunkDirHasFiles` — local cleanup check
- [chunk/file/manager.go:3140](backend/internal/chunk/file/manager.go#L3140) `hasLocalGLCB` — local cache existence; per-node operational concern
- [chunk/file/move.go:34-43](backend/internal/chunk/file/move.go#L34) — file movement plumbing
- [server/upload.go:243-269](backend/internal/server/upload.go#L243) `ManagedFileExists` / `ManagedFileIDs` — managed files (TLS certs, lookup tables) are per-node local data; trust-the-disk is correct here

## Cleanup issues to open

The findings translate to four cleanup implementation issues under [gastrolog-2i1g9](dcat://gastrolog-2i1g9):

1. **Reframe `Manager.loadExisting` as reconciliation against FSM authority** (F1 + F2 — single change, single PR)
2. **Reframe `RemoveVault` empty check to consult FSM manifest** (F3 — small, independent)
3. **Reframe `archivalSweepVault` to iterate FSM manifest instead of local list** (F4 — moderate, needs care around per-node sweep semantics)
4. **Split `ListChunkMetas` into `ListLocalChunkMetas` + `ListClusterChunkMetas` and route each consumer to the right API** (F5 — largest, touches many call sites)

These should depend on the residency tracking implementation (issue opened against [docs/node-lifecycle-design.md](docs/node-lifecycle-design.md) — Option A: constrained placement) being in place, because the cleanup targets need a well-defined "what does the FSM say about residency" answer to consult.

## Out of scope (deferred to follow-up audits)

- The `app/` package (executors, dispatch) was sampled via grep but not fully audited. The consumers of `ListChunkMetas` documented in F5 cover the main call sites, but a thorough sweep of `app/` for other disk-derived state would be a useful follow-up if time permits.
- Index files (`*.idx`, `*.tsidx`) and cloud-blob caches were treated as per-node operational concerns. If future authority migration extends to index-completeness as a cluster-coordinated concern, those sites need a separate audit.
- The `home/` and `system/` packages were not audited; they handle node-local configuration which is not within the chunk-residency migration's scope.

## Relationship to the epic

- Parent: [gastrolog-2i1g9](dcat://gastrolog-2i1g9) (FSM-authority migration epic)
- Companion design: [docs/node-lifecycle-design.md](docs/node-lifecycle-design.md) (Option A: constrained placement; soft-offline; learners)
- Companion design: [gastrolog-5rh68](dcat://gastrolog-5rh68) (resolved to Option A by the node-lifecycle design)

The four cleanup issues described above are the implementation-layer follow-ups that, together with the design-layer changes, complete the migration. Each can be picked up as its own focused PR once the design issues close.
