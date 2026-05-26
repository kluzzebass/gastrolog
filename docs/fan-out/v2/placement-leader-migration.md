# Fan-Out V2 Placement-Leader Migration (Locked)

Status: locked in Phase 0.6 (gastrolog-3c35d). Runtime migration in Phase 7 (gastrolog-38rtk).

## Decision

Under V2, **`VaultPlacement.Leader` is not write-path authority.**

V2 ingest accepts writes on any destination-vault replica that can durably spool the record (`W-of-N`). Ordering, fence cuts, and allocator leases are owned by the **vault-ctl Raft leader**, not the config placement leader node.

The placement `Leader` bit remains during the transition but its meaning changes:

| Write model | `VaultPlacement.Leader` meaning (transitional) | Write-path gate? |
|---|---|---|
| **V1** (`writeModel` empty or `v1`) | Config-resident primary replica for active-chunk append + replica fan-out source | **Yes** — legacy behavior unchanged until Phase 7 cleanup |
| **V2** (`writeModel` `v2`) | **Bootstrap / residency hint only** — which node hosts the initial placement row; not an ingest mutex | **No** — hot path must not reject writes based on `IsFollower` or placement leader node ID |

Phase 7 may rename or remove the field once V1 is gone; Phase 0.6 only locks direction.

## Authority split (V2)

| Concern | Authority | Durable source |
|---|---|---|
| Destination-vault sequence allocation | vault-ctl leader | vault-ctl Raft |
| Fence cuts | vault-ctl leader | vault-ctl Raft |
| Ingest accept / spool append | Any replica in destination set (`W-of-N`) | Local spool + replica fan-out |
| Placement reconcile / RF topology | Placement manager (cluster-ctl) | `Runtime.VaultPlacements` |
| Retention sweep driver (legacy) | Placement leader node today | Config placement + instance role |

Retention and sealed-chunk replication may still *reference* placement topology during V1/V2 coexistence; they must not become V2 **ingest** write gates.

## Coexistence rules (mandatory until Phase 7)

1. **Per-vault dispatch:** read `VaultConfig.ResolveWriteModel()` before applying placement-leader write gates.
2. **V1 vaults:** existing `VaultInstance.IsFollower` / `LeaderNodeID` / `LeaderNodeID()` routing behavior stays as-is.
3. **V2 vaults:** ingest and destination write dispatch (`dispatchDestinationWrite`) must not require this node to be the placement leader. Use `PlacementLeaderIsWriteAuthority` (or equivalent) at every legacy gate.
4. **Mixed cluster:** one node may be placement follower for a vault but still accept V2 spool writes for that vault when RF routing includes it.
5. **Fence/allocator paths (Phase 4+):** use vault-ctl leader checks only — never conflate with `VaultPlacement.Leader`.

## Phase 7 migration targets (execution checklist)

Primary write-path and role wiring to audit/refactor:

| Area | Files | Current placement-leader coupling |
|---|---|---|
| Instance role derivation | `backend/internal/orchestrator/reconfig_vaults.go` | Sets `IsFollower`, `LeaderNodeID`, `FollowerTargets` from `LeaderNodeID()` / `LeaderStorageID()` |
| Active-chunk append / replica fan-out | `backend/internal/orchestrator/vault_ops.go`, `ingest.go` | `ShouldForwardToFollowers`, follower append rejection |
| Route remote resolution | `backend/internal/orchestrator/reconfig_routes.go` | `LeaderNodeID` for remote vault node |
| Retention driver | `backend/internal/orchestrator/retention.go` | Only placement leader runs retention sweep |
| Registry / query leader shortcuts | `backend/internal/orchestrator/registry.go` | `!IsFollower` filters for query/forward |
| Placement reconcile | `backend/internal/app/placement.go` | Elects/maintains `VaultPlacement.Leader` bit |
| Config dispatch | `backend/internal/app/dispatch.go` | Refreshes `LeaderNodeID` on placement change |
| Remote query routing | `backend/internal/server/query_remote.go`, `query_context.go` | `LeaderNodeID` for remote vault |
| Cluster catchup | `backend/internal/cluster/forward.go`, `chunk_replicator.go` | Placement-leader RPC handlers |
| Schema / helpers | `backend/internal/system/storage.go` | `VaultPlacement.Leader`, `LeaderNodeID()`, `LeaderStorageID()` |
| Vault config surface | `backend/internal/system/vault.go`, `config.go` | Placement mirrored on `VaultConfig` |
| Proto | `backend/api/proto/gastrolog/v1/system.proto` | `VaultPlacement.leader` field comment |

Phase 7 deliverable: V2 paths stop consulting placement leader for ingest; V1 paths remain until explicit V1 removal.

## Verification (Phase 0.6)

- `PlacementLeaderIsWriteAuthority` encodes the per-vault rule in code.
- Guardrail tests assert V1=true, V2=false.
- This document is linked from `docs/fan-out/v2/README.md`.

## Out of scope here

- Removing or renaming `VaultPlacement.Leader` in proto/config (Phase 7).
- Rewiring retention/replication to vault-ctl leader (follow-up issues as needed).
- UI copy changes for “leader” labels (Phase 9).
