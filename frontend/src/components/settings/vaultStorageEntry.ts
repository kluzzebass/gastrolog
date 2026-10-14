import { VaultType } from "../../api/gen/gastrolog/v1/system_pb";
import { transferSourceIneligibility } from "./transferEligibility";

// A cloud-backed vault is a file vault with cloudServiceId set; cloud-ness
// is derived via isCloudBacked() rather than a separate type discriminator.
export type VaultTypeLabel = "memory" | "file";

/** Returns true if this vault is cloud-backed (file vault with a cloud service binding). */
export function isCloudBacked(v: { type: VaultTypeLabel; cloudServiceId: string }): boolean {
  return v.type === "file" && v.cloudServiceId !== "";
}

export interface StorageEntry {
  key: string;
  type: VaultTypeLabel;
  storageClass: string;
  cloudServiceId: string;
  cacheEviction: string;
  cacheBudget: string;
  cacheTTL: string;
  memoryBudget: string;
  rotationPolicyId: string;
  retentionPolicyIds: string[]; // one RetentionRule per policy, in this order
  retentionDisposition: string; // "delete" (default) | "route" | "transfer"
  retentionTransferTarget: string; // vault ID — required when retentionDisposition is "transfer"
  replicationFactor: string;
}

export function vaultTypeEnum(t: VaultTypeLabel): VaultType {
  switch (t) {
    case "memory":
      return VaultType.MEMORY;
    case "file":
      return VaultType.FILE;
  }
}

export function isStorageComplete(s: StorageEntry, _hasCloudServices: boolean): boolean {
  return storageIncompleteReason(s) === undefined;
}

/** Why the storage shape cannot be saved yet, or undefined when it can. */
export function storageIncompleteReason(s: StorageEntry): string | undefined {
  if (s.retentionDisposition === "transfer") {
    const sourceReason = transferSourceIneligibility(s);
    if (sourceReason !== undefined) return `Transfer is not available: ${sourceReason}`;
    if (s.retentionTransferTarget === "") return "Pick a transfer target";
  }
  switch (s.type) {
    case "memory":
      return undefined;
    case "file":
      // Single storage class for both local-only and cloud-backed file
      // vaults — the active chunk and cached copies live at the same chunkDir
      // path, so no separate "active" or "cache" class is meaningful.
      return s.storageClass === "" ? "Pick a storage class" : undefined;
  }
}
