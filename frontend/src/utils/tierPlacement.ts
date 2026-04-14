/**
 * Resolves tier placement information from TierConfig.placements and
 * NodeStorageConfig arrays. Replaces the removed TierConfig.nodeId and
 * TierConfig.followerNodeIds fields.
 */

import { encode } from "../api/glid";

interface Placement {
  storageId: Uint8Array;
  leader: boolean;
}

interface StorageRef {
  id: Uint8Array;
}

interface NSC {
  nodeId: Uint8Array;
  fileStorages: StorageRef[];
}

/** Returns the encoded node ID that owns the given file storage, or "" if not found. */
export function nodeIdForStorage(storageId: string, nscs: readonly NSC[]): string {
  for (const nsc of nscs) {
    if (nsc.fileStorages.some((a) => encode(a.id) === storageId)) return encode(nsc.nodeId);
  }
  return "";
}

/** Returns the encoded node ID of the leader placement, or "" if none. */
export function leaderNodeId(
  tier: { placements: readonly Placement[] },
  nscs: readonly NSC[],
): string {
  const p = tier.placements.find((pl) => pl.leader);
  if (!p) return "";
  return nodeIdForStorage(encode(p.storageId), nscs);
}

/** Returns the encoded node IDs of all follower (non-leader) placements. */
export function followerNodeIds(
  tier: { placements: readonly Placement[] },
  nscs: readonly NSC[],
): string[] {
  return tier.placements
    .filter((pl) => !pl.leader)
    .map((pl) => nodeIdForStorage(encode(pl.storageId), nscs))
    .filter((id) => id !== "");
}
