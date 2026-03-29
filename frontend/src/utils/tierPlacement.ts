/**
 * Resolves tier placement information from TierConfig.placements and
 * NodeStorageConfig arrays. Replaces the removed TierConfig.nodeId and
 * TierConfig.secondaryNodeIds fields.
 */

interface Placement {
  storageId: string;
  primary: boolean;
}

interface StorageRef {
  id: string;
}

interface NSC {
  nodeId: string;
  fileStorages: StorageRef[];
}

/** Returns the node ID that owns the given file storage, or "" if not found. */
export function nodeIdForStorage(storageId: string, nscs: readonly NSC[]): string {
  for (const nsc of nscs) {
    if (nsc.fileStorages.some((a) => a.id === storageId)) return nsc.nodeId;
  }
  return "";
}

/** Returns the node ID of the primary placement, or "" if none. */
export function primaryNodeId(
  tier: { placements: readonly Placement[] },
  nscs: readonly NSC[],
): string {
  const p = tier.placements.find((pl) => pl.primary);
  if (!p) return "";
  return nodeIdForStorage(p.storageId, nscs);
}

/** Returns the node IDs of all secondary (non-primary) placements. */
export function secondaryNodeIds(
  tier: { placements: readonly Placement[] },
  nscs: readonly NSC[],
): string[] {
  return tier.placements
    .filter((pl) => !pl.primary)
    .map((pl) => nodeIdForStorage(pl.storageId, nscs))
    .filter((id) => id !== "");
}
