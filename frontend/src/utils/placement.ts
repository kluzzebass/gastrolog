/**
 * Placement helpers: resolve canonical / peer node IDs from
 * VaultConfig.placements and the cluster's NodeStorageConfig array.
 *
 * Under the fan-out data plane every placement member is symmetric — the
 * "leader" / "follower" distinction at the data-path level is gone. The
 * first placement in the slice is the deterministic canonical used by
 * routing-layer code that needs to pick a stable target; every other
 * placement is an equally authoritative peer.
 */

import { encode } from "../api/glid";

interface Placement {
  storageId: Uint8Array;
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
 * Handles synthetic storage IDs ("node:<nodeId>") used by memory vaults on nodes
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

/** Returns the encoded node ID of the first (canonical) placement, or "" if none. */
export function leaderNodeId(
  vault: { placements: readonly Placement[] },
  nscs: readonly NSC[],
): string {
  const first = vault.placements[0];
  if (!first) return "";
  return nodeIdForStorage(encode(first.storageId), nscs);
}

/** Returns the encoded node IDs of every placement except the first (canonical). */
export function followerNodeIds(
  vault: { placements: readonly Placement[] },
  nscs: readonly NSC[],
): string[] {
  if (vault.placements.length <= 1) return [];
  return vault.placements
    .slice(1)
    .map((pl) => nodeIdForStorage(encode(pl.storageId), nscs))
    .filter((id) => id !== "");
}
