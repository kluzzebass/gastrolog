# Vault-level control-plane architecture

**Planning artifact:** dcat `gastrolog-k1ej7`  
**Parent:** Architectural redesign — orchestration and metadata consistency (`gastrolog-3eghu`)  
**Unblocks:** `gastrolog-5xxbd` (vault-level Raft + FSM namespacing), `gastrolog-554s3` (explicit vault → instance ownership)

**Acceptance criterion (k1ej7):** A **uniform Raft group model** — system/config and vault groups share **lifecycle and on-disk conventions**. Any exception is a **named compatibility shim** with an explicit **sunset** (no undocumented privileged directory layout).

**Terminal state (5xxbd complete):** There are **no** per-vault-instance metadata Raft groups. **One vault-scoped control-plane Raft** (plus cluster-ctl Raft for cluster-global config) owns every cross-node chunk metadata mutation. **Vault instances** (chunk/index managers) remain the data plane and **do not** host their own Raft groups.

**Vocabulary:** canonical terms follow [`ubiquitous_language.md`](./ubiquitous_language.md). In particular, this architecture uses **vault-ctl Raft** (one group per vault, authoritative for that vault's chunk metadata) and **instance FSM** (the per-vault-instance sub-state-machine inside the vault-ctl FSM, implemented in `vaultctlfsm`).

---

## Table of contents

1. [Inventory: today (as implemented)](#inventory-today-as-implemented)
2. [A) Vocabulary (normative)](#a-vocabulary-normative)
3. [B) Authority model (normative)](#b-authority-model-normative)
4. [C) Uniform Raft group model (target)](#c-uniform-raft-group-model-target)
5. [D) Vault readiness semantics](#d-vault-readiness-semantics-normative--testable)
6. [E) Legal state machine (chunk / vault)](#e-legal-state-machine-chunk--vault)
7. [F) Non-goals and open questions](#f-non-goals-and-open-questions)
8. [G) Handoff checklists](#g-handoff-checklists)
9. [Code anchors (maintenance)](#code-anchors-maintenance)

---

## Inventory: today (as implemented)

### Cluster-ctl Raft

| Aspect | Detail |
|--------|--------|
| **Entry** | `openRaftClusterCtlStore` in [`backend/internal/app/raft.go`](../backend/internal/app/raft.go) |
| **Persistence** | Its own WAL: `raftwal.Open(Home.ClusterCtlWALDir())` (`raft/wal`) with `GroupStore("cluster-ctl")`; file snapshot store `hraft.NewFileSnapshotStore(Home.RaftGroupDir("cluster-ctl"), …)` (`raft/groups/cluster-ctl`). Layout helpers in [`backend/internal/home/home.go`](../backend/internal/home/home.go) |
| **Transport** | `cluster.Server.Transport()` — the multiraft transport scoped to group ID `config` (`cluster.ConfigGroupID`); not a `GroupManager` group |
| **Apply path** | `cluster.SetApplyFn` → `raftstore.Store.ApplyRaw` (config mutations) |

### Vault control-plane Raft (per vault, `vault/<id>/ctl`)

| Aspect | Detail |
|--------|--------|
| **Entry** | `ensureVaultControlPlaneRaftGroup` in [`backend/internal/orchestrator/reconfig_vaults.go`](../backend/internal/orchestrator/reconfig_vaults.go) |
| **FSM** | [`backend/internal/vaultraft/`](../backend/internal/vaultraft/) — holds per-instance sub-FSMs ([`vaultctlfsm.FSM`](../backend/internal/vaultraft/vaultctlfsm/)) namespaced by `OpVaultChunkFSM` commands |
| **Cross-node apply** | `orchestrator.ApplyVaultControlPlane` → `cluster.VaultApplyForwarder` when `PeerConns` is set, else local `Raft.Apply`; RPC path: `ForwardVaultApply` + `cluster.SetGroupApplyFn` ([`backend/internal/orchestrator/vault_ctl_apply.go`](../backend/internal/orchestrator/vault_ctl_apply.go), [`backend/internal/cluster/vault_apply_forwarder.go`](../backend/internal/cluster/vault_apply_forwarder.go), [`backend/internal/cluster/forward.go`](../backend/internal/cluster/forward.go), [`backend/internal/app/app.go`](../backend/internal/app/app.go)) |
| **Client forwarder** | [`backend/internal/cluster/vault_apply_forwarder.go`](../backend/internal/cluster/vault_apply_forwarder.go) (`VaultApplyForwarder`) |

### Vault chunk metadata (cluster mode)

Replicated chunk metadata uses the **vault control-plane** group only (`OpVaultChunkFSM` wrapping `vaultctlfsm` wire payloads). Entry: `ensureVaultCtlMetadata` in [`backend/internal/orchestrator/reconfig_vaults.go`](../backend/internal/orchestrator/reconfig_vaults.go). Cross-node apply uses `ForwardVaultApply` with the vault-ctl `group_id` and wrapped command bytes (`cluster.NewVaultCtlChunkApplyForwarder`).

### Orchestrator

Vault / chunk operations and routing: [`backend/internal/orchestrator/vault_ops.go`](../backend/internal/orchestrator/vault_ops.go) and related orchestrator code; replicated metadata callbacks from `ensureVaultCtlMetadata` / `vaultRaftCallbacks` read the per-instance sub-FSM inside [`backend/internal/vaultraft/`](../backend/internal/vaultraft/).

### Status

1. **Authority scope:** Replicated chunk metadata is on **vault control-plane Raft** (cluster mode).
2. **Snapshot layout:** Every group snapshots under `raft/groups/<groupID>/` — `raft/groups/cluster-ctl` for cluster-ctl, `raft/groups/vault/<vaultID>/ctl` for each vault-ctl group.
3. **WALs:** Two per node. Cluster-ctl has its own `raftwal` at `raft/wal` (`Home.ClusterCtlWALDir`) so cluster heartbeats are not coupled to vault append/fsync batching; every vault-ctl group on the node shares one `raftwal` at `raft/groups/wal` (`Home.VaultCtlWALDir`, opened in `setupMultiRaft`).
4. **Readiness / partial peers:** `Vault.ReadinessErr` ([`vault_readiness.go`](../backend/internal/orchestrator/vault_readiness.go)) gates on the instance's `IsFSMReady` callback, which `buildVaultRaftCallbacks` wires to `r.AppliedIndex() > 0`.
5. **Apply entrypoints:** **One** `cluster.Server.SetGroupApplyFn`; `ForwardVaultApply` is the single cross-node apply RPC.

---

## A) Vocabulary (normative)

| Term | Definition |
|------|------------|
| **Control plane** | Replicated metadata and decisions that must survive restart and agree cluster-wide (Raft-backed configuration, placement-derived invariants that affect safety, chunk metadata that must not fork across nodes). |
| **Data plane** | Bytes on disk / object store / cloud blobs and record streams; may be locally authoritative under placement rules but **must not** contradict committed control-plane state. |
| **Authority** | The sole writer path for a given decision (typically through the Raft leader for that group). |
| **Cache / hint** | Read models that may lag (peer discovery, placement views). **MUST** be labeled as such; **MUST NOT** be the only source for unsafe actions. |
| **Vault readiness** | Predicate bundle (section D) for whether this node may execute **vault-scoped** control-plane responsibilities for a vault. |
| **Instance readiness** | Whether this node may operate a **specific vault instance** (local placement) — may depend on vault readiness + local placement + resource state. |
| **Node readiness** | Process-level (gRPC up, cluster-ctl Raft available, etc.) — orthogonal to vault readiness. |

---

## B) Authority model (normative)

**MUST**

- Every mutation that changes **cross-node** interpretation of vault/chunk metadata **MUST** go through a **defined Raft group** (system or vault-scoped per target topology), or through an explicitly documented non-Raft path that remains safe under partition (rare; default is “no”).
- **Cluster-ctl Raft** remains the home for **cluster-global** configuration and membership that is not vault-owned (exact list to be enumerated during implementation; default: anything not naturally keyed by vault ID stays system-scoped until moved).
- **Vault-scoped** control-plane state **MUST** be keyed and replicated under a **vault-scoped Raft identity** in the target architecture (section C), not ad hoc sub-Raft groups for vault-wide invariants.

**MUST NOT**

- Introduce a second silent writer path for the same metadata (e.g. direct boltdb / local files that diverge from what Raft would apply).
- Treat placement caches, peer tables, or “last known leader” hints as authoritative for commits that need consensus.

**Partial membership / unresolved peers**

- **MUST** inherit the safety principle: **do not bootstrap** a new Raft group with an incomplete voter set when that would bake a bad configuration.
- **MUST** define whether **deferred creation** means “vault not ready” vs “instance not ready” vs both — and what user-visible behavior is (backpressure, skip, retry).

---

## C) Uniform Raft group model (target)

**Goal:** System and vault groups follow the **same lifecycle and storage conventions**. Any exception is a **named compatibility shim** with a **sunset** (version gate or explicit removal deadline).

### Target topology (conceptual)

- **SystemRaft:** Global cluster config / membership (existing role, possibly narrowed as vault scope grows).
- **VaultRaft (per vault):** Authoritative for **all** vault/chunk control-plane metadata. The single per-vault Raft hosts the chunk-FSM as a sub-FSM (`vaultctlfsm`).
- **Chunk path (data plane):** Consumes committed state from VaultRaft + system; **does not** host Raft; **does not** own consensus for cross-node metadata.

### Identifiers and routing

- String `GroupID` remains the routing key for `multiraft.GroupTransport` ([`backend/internal/multiraft/transport.go`](../backend/internal/multiraft/transport.go)).
- Target naming convention **MUST** be explicit. **Vault control-plane Raft** uses `vault/<vaultGLID>/ctl` (`raftgroup.VaultControlPlaneGroupID`) and is the **only** vault-scoped multiraft group for replicated chunk metadata. Cluster-ctl Raft routes on the multiraft transport as group `config` (`cluster.ConfigGroupID`); its on-disk name is `cluster-ctl`.

### Persistence (target rules)

- **WAL:** Split WALs with an identical semantic contract: cluster-ctl on `raft/wal`, all vault-ctl groups sharing `raft/groups/wal`. The split exists for I/O isolation — cluster-ctl heartbeats must not wait on vault append/fsync batching.
- **Snapshots:** One directory pattern for all group kinds: `raft/groups/<groupID>/`, with cluster-ctl at `raft/groups/cluster-ctl`. **Greenfield only:** no on-disk upgrade path from older layouts; wipe or replace the raft tree when the layout changes during development.

### Lifecycle

- **Create / seed / restart:** Vault-ctl groups are created, seeded and restarted through `GroupManager` ([`backend/internal/raftgroup/groupmanager.go`](../backend/internal/raftgroup/groupmanager.go)). Cluster-ctl Raft is not a `GroupManager` group: `openRaftClusterCtlStore` opens it directly, and `setupMultiRaft` leaves `GroupManagerConfig.ShutdownLast` empty for that reason.
- **Shutdown:** Preserve ordering already relied upon (Raft shutdown before transport removal; see `DestroyGroup`). Vault redesign **MUST NOT** weaken shutdown invariants.

### Authority direction (target)

```mermaid
flowchart TB
  sysRaft[SystemRaft_clusterScope]
  vaultRaft[VaultRaft_perVault]
  orch[Orchestrator]
  chunkMgr[ChunkManagers_dataPlane]
  sysRaft -->|"committed_global_config"| orch
  vaultRaft -->|"committed_vault_metadata"| orch
  orch --> chunkMgr
```

---

## D) Vault readiness semantics (normative + testable)

Predicate names are illustrative; exact symbols belong in implementation.

| Predicate | Intent |
|-----------|--------|
| **VaultRaft_LocalReplicaReady** | Local process has joined/replayed the vault group far enough that **reads** required for safe local work are valid (define minimum: e.g. snapshot + committed config applied, or stricter). |
| **Vault_ControlPlaneReady** (node-local) | AND of: node process ready, `VaultRaft_LocalReplicaReady`, and any **dependency** declared mandatory (cluster-ctl Raft catchup, peer resolvability, etc.). |
| **Vault_ServeIngestReady** / **Vault_ServeQueryReady** | **MAY** be stricter than control-plane ready; **MUST** be explicit if ingest is allowed before full FSM catch-up. |

**Consistency**

- Metadata read from VaultRaft-backed state **MUST** be **linearizable** via normal Raft reads / barriers where this spec says “strong”.
- Caches **MAY** be eventually consistent only where this spec lists allowed staleness and the failure mode is bounded.

**WAL / replay / partial peers**

- **MUST** state whether the prior style of **defer create** becomes **vault-group defer create**, and how that surfaces in readiness bits and logs.

---

## E) Legal state machine (chunk / vault)

Documents the transitions that flow through `vaultctlfsm` / vault-ctl Raft applies (`reconfig_vaults.go` callbacks) and which transitions are **vault Raft commands** vs **local instance** bookkeeping.

Chunk substates (delete pending, tombstone, etc.) are mapped to: **vault-committed**, **instance-local**, or **system-committed**.

### Chunk lifecycle (pipeline vaults)

A chunk traverses three FSM-tracked states (`chunk.ChunkState` on the vault-ctl `ManifestEntry`). The state is `vault-committed` (replicated through the per-vault control-plane Raft). The vault-ctl leader plans chunks as open chunk manifests over published segments; every home builds its own GLCB from the sealed manifest.

```mermaid
stateDiagram-v2
  direction LR
  [*] --> Active: AddOpenChunkSegmentRef (first ref of an open manifest)
  Active --> Sealing: SealOpenChunkManifest (leader seals the manifest)
  Sealing --> Sealed: CmdSealChunk (leader, after its own GLCB build)
  Sealed --> [*]: FinalizeDelete (delete receipt protocol)
```

| State    | Producer trigger | Bytes on each home | Reads |
|----------|------------------|--------------------|-------|
| Active   | `applyAddOpenChunkSegmentRef` ([`vaultctlfsm/open_chunk.go`](../backend/internal/vaultraft/vaultctlfsm/open_chunk.go)) creates the `ManifestEntry` in `Active` when the first segment ref lands in the open manifest | Segments in `head/`; no GLCB | `OpenPipelineChunkCursor` reads the manifest-listed segment spans |
| Sealing  | `applySealOpenChunkManifest` moves the open manifest onto the FIFO sealed-manifest queue and the entry to `Sealing`; proposed by the chunking leader's `applySealOpenManifest` ([`pipeline/chunking/leader.go`](../backend/internal/pipeline/chunking/leader.go)) on rotation policy or operator seal | Each home builds `data.glcb` and its secondary indexes from the sealed manifest; until its build finishes it still has the segments | `OpenPipelineChunkCursor` opens the local GLCB when present, else the segment spans |
| Sealed   | `proposeSealOnce` ([`pipeline/chunking/build_pass.go`](../backend/internal/pipeline/chunking/build_pass.go)) — only the vault-ctl leader commits `CmdSealChunk`, after its own build; `applySeal` pops the sealed-manifest head | `data.glcb` on every home that built or pulled it; holder receipts (`AckChunkHolder` / `RevokeChunkHolder`) record which homes hold it | The local GLCB |

**Every home builds.** No home receives another's sealed bytes as the normal path. A follower home that finishes its build before `CmdSealChunk` commits keeps its GLCB and waits for the leader's seal. A home that misses a build (down or wedged while the segments were released) pulls the GLCB from a peer home: the lifecycle reconciler's `syncPipelineSealedGLCBs` ([`vault_lifecycle_reconciler.go`](../backend/internal/orchestrator/vault_lifecycle_reconciler.go)) calls `pullMissingGLCB` ([`glcb_catchup.go`](../backend/internal/orchestrator/glcb_catchup.go)) for every Sealed entry without local bytes, then reconciles this node's holder receipt against what is on disk.

**Gates on Sealed.** Metas are grounded in the FSM through `groundChunkMeta` ([`grounded_meta.go`](../backend/internal/orchestrator/grounded_meta.go)), which sets `meta.Sealed` from `State == Sealed`. Every producer-side gate that ships, retires, or otherwise irreversibly acts on a chunk reads the grounded meta and skips anything not yet Sealed: retention (`retentionRunner.sweep`), archival sweep (`archivalSweepVault`), cloud backfill/upload (`backfillCloudUploads`), and vault drain (`drainVaultChunks`, `drainSealed`). `ListAllChunkMetas` ([`vault_ops.go`](../backend/internal/orchestrator/vault_ops.go)) projects every chunk from the FSM manifest (including open and sealing entries) the same way.

**Crash recovery.** A home that built its GLCB but stopped before `CmdSealChunk` applied recovers through chunking's `RecoverOnce` ([`pipeline/chunking/recover.go`](../backend/internal/pipeline/chunking/recover.go)): for the sealed-manifest head and for every Active or Sealing entry whose GLCB is present locally, it re-reads the build result from the GLCB and proposes the seal (the leader commits; followers wait). It runs when the pipeline registers the vault on a home and after every vault-ctl snapshot restore (`reconcilePipelineAfterCtlRestore`). The build writes through a temp file and an atomic rename, so re-running after a partial build is safe; `RecoverOnce` sweeps orphaned build temp files first.

### Vault state diagram

```mermaid
stateDiagram-v2
  direction LR
  [*] --> VaultConfigured
  VaultConfigured --> VaultRaftRunning: seed_or_replay
  VaultRaftRunning --> VaultReady: predicates_D
  VaultRaftRunning --> VaultDegraded: partial_peers_or_lagging
  VaultDegraded --> VaultRaftRunning: heal
  VaultReady --> InstanceActive: placement_exports
  InstanceActive --> InstanceDraining: operator_or_policy
  InstanceDraining --> InstanceInactive: drain_complete
```

---

## F) Non-goals and open questions

These require explicit product/architecture decisions:

1. **One VaultRaft vs multiple raft groups per vault** (e.g. separate hot/cold metadata): pick one default.
2. **Cutover** to **no per-instance Raft**: ~~move chunk-FSM commands and apply-forwarding onto vault control-plane Raft, then remove the legacy per-instance Raft groups, group store names, and apply-fn split~~ — **done** (gastrolog-5xxbd + gastrolog-2ze8j). `ensureVaultCtlMetadata` is the canonical entry; the single `SetGroupApplyFn` replaces the prior split apply-fns.
3. **Cluster-ctl Raft shrink/grow:** Which config moves to vault Raft vs stays global; backward compatibility for existing clusters.
4. **Snapshot layout:** Converged — every group snapshots under `raft/groups/<groupID>/` (no legacy on-disk compatibility).
5. **Feature flags:** Whether vault Raft rolls out behind a flag per vault or cluster-wide.

---

## G) Handoff checklists

### For gastrolog-5xxbd — vault-level Raft groups and FSM namespacing

- [x] Implement `GroupID` scheme and `GroupStore` / `CreateGroup` wiring chosen in C/F. — done (5xxbd).
- [x] Align snapshot directories with the chosen uniform pattern (greenfield; remove old `raft/` tree if layout changes). — done (5xxbd).
- [x] FSM boundary: chunk-metadata commands run as a sub-FSM (`vaultctlfsm`) inside `vaultraft.FSM`, dispatched via `OpVaultChunkFSM` envelopes (`vaultraft.MarshalVaultChunkCommand`). Done (5xxbd).
- [x] **RPC / naming cleanup:** consolidated apply entrypoints — done (2ze8j): single `SetGroupApplyFn`; `ForwardVaultApply` is the single cross-node apply RPC.
- [x] Bootstrap/defer rules from D for partial membership; tests for "no bad initial config". — done (5xxbd).
- [x] WAL: vault-ctl groups share the per-node vault-ctl `raftwal` (`raft/groups/wal`); cluster-ctl has its own (`raft/wal`).
- [x] Shutdown: preserve `DestroyGroup` ordering vs transport; add vault group to shutdown sequence in `app` if needed. — done (5xxbd).

### For gastrolog-554s3 — explicit vault → instance ownership

- [x] Replace implicit "find chunk manager" shortcuts with APIs that take **vault context** first (`vault_ops.go` and call sites). — done (554s3 + 2ze8j): orchestrator shortcuts retired in favor of vault-scoped lookups.
- [x] Document and enforce: chunk managers are **owned** under a vault umbrella; no orphan instance lifetimes. — done (554s3).
- [x] Forwarding (vault apply path and record/search forwarding) must use stable vault identity keys consistent with C. — done (2ze8j): `ForwardVaultApply` is the single apply RPC, `groupApplyFn` is the single apply path.
- [x] Readiness plumbing: orchestrator checks **Vault_ControlPlaneReady** before operations that require it (per D). — `Vault.ReadinessErr` gates on the instance's `IsFSMReady` callback (`r.AppliedIndex() > 0`, wired in `buildVaultRaftCallbacks`).
- [x] Remove or quarantine legacy paths called out during inventory once vault Raft is live. — done (2ze8j): legacy per-instance Raft group-id helpers and tests deleted.

---

## Code anchors (maintenance)

| Area | Paths |
|------|--------|
| Cluster-ctl Raft | `backend/internal/app/raft.go` |
| Multi-Raft setup, unified group apply | `backend/internal/app/app.go` (`setupMultiRaft`, `wireClusterRaftApplies`, `SetGroupApplyFn`) |
| Vault control-plane Raft | `backend/internal/orchestrator/reconfig_vaults.go` (`ensureVaultControlPlaneRaftGroup`), `backend/internal/orchestrator/vault_ctl_apply.go`, `backend/internal/vaultraft/`, `backend/internal/cluster/vault_apply_forwarder.go`, `backend/internal/cluster/vault_ctl_chunk_apply_forwarder.go` |
| Vault ctl metadata wiring | `backend/internal/orchestrator/reconfig_vaults.go` (`ensureVaultControlPlaneRaftGroup`, `ensureVaultCtlMetadata`) |
| Group manager | `backend/internal/raftgroup/groupmanager.go` |
| WAL | `backend/internal/raftwal/` |
| Transport | `backend/internal/multiraft/transport.go` |
| Orchestrator | `backend/internal/orchestrator/vault_ops.go` |

**Normative text:** This file is the readable **source of truth** for k1ej7 scope. Implementation issues should link here; scope changes **MUST** update this document rather than reinterpret informally.

**Tracker:** dcat issue `gastrolog-k1ej7` tracks the planning task and links child issues (`gastrolog-5xxbd`, `gastrolog-554s3`).
