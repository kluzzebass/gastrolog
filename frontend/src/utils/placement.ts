/**
 * Placement helpers: resolve leader / follower node IDs from a placement
 * list (VaultConfig.placements or TierConfig.placements — same shape)
 * and the cluster's NodeStorageConfig array.
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

const SYNTHETIC_STORAGE_PREFIX = "node:";

/** Returns the encoded node ID that owns the given storage, or "" if not found.
 * Handles synthetic storage IDs ("node:<nodeId>") used by memory tiers on nodes
 * without file storages — see system.SyntheticStorageID. */
export function nodeIdForStorage(storageId: string, nscs: readonly NSC[]): string {
  if (storageId.startsWith(SYNTHETIC_STORAGE_PREFIX)) {
    return storageId.slice(SYNTHETIC_STORAGE_PREFIX.length);
  }
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
