/**
 * Resolves tier placement information from TierConfig.placements and
 * NodeStorageConfig arrays. Replaces the removed TierConfig.nodeId and
 * TierConfig.followerNodeIds fields.
 */

interface Placement {
  storageId: string;
  leader: boolean;
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

/** Returns the node ID of the leader placement, or "" if none. */
export function leaderNodeId(
  tier: { placements: readonly Placement[] },
  nscs: readonly NSC[],
): string {
  const p = tier.placements.find((pl) => pl.leader);
  if (!p) return "";
  return nodeIdForStorage(p.storageId, nscs);
}

/** Returns the node IDs of all follower (non-leader) placements. */
export function followerNodeIds(
  tier: { placements: readonly Placement[] },
  nscs: readonly NSC[],
): string[] {
  return tier.placements
    .filter((pl) => !pl.leader)
    .map((pl) => nodeIdForStorage(pl.storageId, nscs))
    .filter((id) => id !== "");
}
