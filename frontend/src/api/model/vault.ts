// Vault domain model.
//
// Joins the runtime `VaultInfo` (from ListVaults — has chunk/record counts
// and the cluster-visible flags) with the durable `VaultConfig` (from
// GetSystem — has the typed enum, cloud-service binding, retention rules,
// and the authoritative placement set). Either side may be missing during
// transient states (e.g. a newly created vault arrives in config before
// its first ListVaults entry).

import type { VaultInfo } from "../gen/gastrolog/v1/vault_pb";
import type { VaultConfig } from "../gen/gastrolog/v1/system_pb";
import type { NodeStorageConfig } from "../gen/gastrolog/v1/storage_pb";
import { VaultType } from "../gen/gastrolog/v1/system_pb";
import { type EntityID, idFromBytes, isEmptyID } from "./id";
import { leaderNodeId, followerNodeIds } from "../../utils/placement";

// Re-exported so components (no direct api/gen imports outside
// src/api/model/ and src/api/hooks/) can name the enum/message without
// reaching into the generated proto layer themselves.
export { VaultAdmissionCause } from "../gen/gastrolog/v1/vault_pb";
import type { VaultAdmissionRefusal } from "../gen/gastrolog/v1/vault_pb";
export type { VaultAdmissionRefusal } from "../gen/gastrolog/v1/vault_pb";

export class Vault {
  readonly id: EntityID;
  readonly info: VaultInfo | null;
  readonly config: VaultConfig | null;

  constructor(info: VaultInfo | null, config: VaultConfig | null) {
    const idBytes = info?.id ?? config?.id;
    this.id = idFromBytes(idBytes);
    this.info = info;
    this.config = config;
  }

  /** Display name: VaultInfo.name → VaultConfig.name → id. */
  get name(): string {
    return this.info?.name || this.config?.name || this.id;
  }

  get displayLabel(): string {
    return this.name;
  }

  /** Runtime type string ("memory", "file", "cloud", "jsonl") from VaultInfo. */
  get typeLabel(): string {
    return this.info?.type ?? "";
  }

  /** Typed VaultType enum from VaultConfig (only available when config is joined). */
  get configType(): VaultType {
    return this.config?.type ?? VaultType.UNSPECIFIED;
  }

  /** Enabled state: prefer config when available, fall back to runtime. */
  get enabled(): boolean {
    if (this.config) return this.config.enabled;
    if (this.info) return this.info.enabled;
    return false;
  }

  /** True when the vault is on a different node than the API node. */
  get isRemote(): boolean {
    return this.info?.remote ?? false;
  }

  /** Chunk count from the runtime overlay. */
  get chunkCount(): bigint {
    return this.info?.chunkCount ?? 0n;
  }

  /** Record count from the runtime overlay. */
  get recordCount(): bigint {
    return this.info?.recordCount ?? 0n;
  }

  /** Backing cloud-service ID, if this vault is cloud-backed. */
  get cloudServiceId(): EntityID {
    return idFromBytes(this.config?.cloudServiceId);
  }

  /** True if this vault is backed by a cloud storage service. */
  get isCloudBacked(): boolean {
    return !isEmptyID(this.cloudServiceId);
  }

  /**
   * Currently-applicable admission-refusal causes, each paired with the
   * backend's own detail text for it (which storage and its free-vs-floor
   * numbers, which bound and value), as reported by the responding node's
   * own admission-causes collector (local disk guard + live-peer
   * broadcasts) — a first-class backend signal, not a client-side
   * derivation from alarm state. Empty when the vault admits normally.
   */
  get admissionRefused(): readonly VaultAdmissionRefusal[] {
    return this.info?.admissionRefused ?? [];
  }

  /**
   * Leader node ID, resolved from `config.placements` against the cluster's
   * NodeStorageConfig array. Returns the supplied fallback for vaults with
   * no placements yet (memory vaults, freshly created vaults before the
   * placement manager has assigned storage).
   */
  placementNodeId(nscs: readonly NodeStorageConfig[], localFallback: EntityID): EntityID {
    if (this.config?.placements && this.config.placements.length > 0) {
      const leader = leaderNodeId(this.config, nscs);
      if (leader !== "") return leader as EntityID;
    }
    return localFallback;
  }

  /**
   * All member node IDs for this vault (leader + followers), resolved from
   * `config.placements`. Returns `[localFallback]` when no placements are
   * registered yet, so the locally-connected node tab still shows
   * pre-placement vaults instead of going empty.
   */
  placementNodeIds(nscs: readonly NodeStorageConfig[], localFallback: EntityID): EntityID[] {
    if (this.config?.placements && this.config.placements.length > 0) {
      const ids: EntityID[] = [];
      const leader = leaderNodeId(this.config, nscs);
      if (leader !== "") ids.push(leader as EntityID);
      for (const f of followerNodeIds(this.config, nscs)) {
        ids.push(f as EntityID);
      }
      if (ids.length > 0) return ids;
    }
    return isEmptyID(localFallback) ? [] : [localFallback];
  }

  /**
   * True when this vault has a placement (leader OR follower) on the target
   * node. Uses the placement set from `config.placements`, NOT the
   * `info.nodeId` single-field model, which the backend never populates.
   */
  isOn(
    targetNodeId: EntityID,
    nscs: readonly NodeStorageConfig[],
    localFallback: EntityID,
  ): boolean {
    return this.placementNodeIds(nscs, localFallback).includes(targetNodeId);
  }
}
