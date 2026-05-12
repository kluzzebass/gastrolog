// Vault domain model.
//
// Joins the runtime `VaultInfo` (from ListVaults — has chunk/record counts
// and the current placement nodeId) with the durable `VaultConfig` (from
// GetSystem — has the typed enum, cloud-service binding, retention rules).
// Either side may be missing during transient states (e.g. a newly created
// vault arrives in config before its first ListVaults entry).

import type { VaultInfo } from "../gen/gastrolog/v1/vault_pb";
import type { VaultConfig } from "../gen/gastrolog/v1/system_pb";
import { VaultType } from "../gen/gastrolog/v1/system_pb";
import { type EntityID, idFromBytes, EMPTY_ID, isEmptyID } from "./id";

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
   * Placement node ID. Vaults without an explicit nodeId (e.g. memory vaults
   * that haven't been pinned) fall back to the supplied local node ID — the
   * same convention the inspector's filters used inline.
   */
  placementNodeId(localFallback: EntityID): EntityID {
    const fromInfo = idFromBytes(this.info?.nodeId);
    return isEmptyID(fromInfo) ? localFallback : fromInfo;
  }

  /** True when this vault is placed on (or implicitly belongs to) the target node. */
  isOn(targetNodeId: EntityID, localFallback: EntityID): boolean {
    return this.placementNodeId(localFallback) === targetNodeId;
  }
}
