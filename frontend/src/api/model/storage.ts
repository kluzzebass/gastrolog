// Storage domain model — wraps the published StorageState wire message
// (ListStorages / NodeStats.storages / WatchSystemStatusResponse.storages).
//
// Operator directive (gastrolog-9akebz, extended to storages by
// gastrolog-3cobq4): every threshold and verdict here is rendered verbatim
// from the wire — free/total, warn/floor bytes, warn/protect verdicts, and
// the inherited-vs-explicit flags are all server-computed. This model does
// no derivation of its own; it only reshapes proto bytes into typed
// EntityID values for the UI layer, the same role Vault plays for
// VaultInfo.

import type { StorageState } from "../gen/gastrolog/v1/storage_pb";
import { type EntityID, idFromBytes } from "./id";
import type { ProtoTimestamp } from "../../utils/temporal";

export class Storage {
  readonly id: EntityID;
  readonly state: StorageState;

  constructor(state: StorageState) {
    this.id = idFromBytes(state.id);
    this.state = state;
  }

  get name(): string {
    return this.state.name || this.id;
  }

  get displayLabel(): string {
    return this.name;
  }

  get path(): string {
    return this.state.path;
  }

  /** Operator-facing node display name, pre-resolved server-side. */
  get nodeName(): string {
    return this.state.nodeName;
  }

  /**
   * The owning node's raw ID — the stable join key for grouping/filtering
   * storages by node (gastrolog-3cobq4 review: nodeName alone collides on
   * rename/duplicate names). nodeName stays the display label; this is the
   * key.
   */
  get nodeId(): EntityID {
    return idFromBytes(this.state.nodeId);
  }

  get storageClass(): number {
    return this.state.storageClass;
  }

  get warnExpr(): string {
    return this.state.warnExpr;
  }

  get floorExpr(): string {
    return this.state.floorExpr;
  }

  get warnInherited(): boolean {
    return this.state.warnInherited;
  }

  get floorInherited(): boolean {
    return this.state.floorInherited;
  }

  get warnBytes(): bigint {
    return this.state.warnBytes;
  }

  get floorBytes(): bigint {
    return this.state.floorBytes;
  }

  get freeBytes(): bigint {
    return this.state.freeBytes;
  }

  get totalBytes(): bigint {
    return this.state.totalBytes;
  }

  get sampledAt(): ProtoTimestamp | undefined {
    return this.state.sampledAt;
  }

  get warnVerdict(): boolean {
    return this.state.warnVerdict;
  }

  get protectVerdict(): boolean {
    return this.state.protectVerdict;
  }

  get placedVaultIds(): readonly EntityID[] {
    return this.state.placedVaultIds.map((b) => idFromBytes(b));
  }

  /**
   * True once a real statfs sample has landed on the owning node. Before
   * the first sample (a storage just added to config), the backend's
   * fallback publishes identity + placements with honestly-zero live
   * fields — never fabricated — so the UI must gate on this rather than
   * treating totalBytes === 0 as "a full disk."
   */
  get hasSample(): boolean {
    return this.totalBytes > 0n;
  }
}
