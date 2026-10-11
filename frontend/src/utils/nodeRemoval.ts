/**
 * Client-side mirror of the leader's node-removal gates, so the Nodes tab
 * can show a refusal before the operator asks for it. The leader remains
 * the authority: this reads the same placements and node storage configs
 * it does, and names the same vaults.
 */

import { encode } from "../api/glid";
import { NodeState, VaultType } from "../api/gen/gastrolog/v1/system_pb";

interface Placement {
  storageId: Uint8Array;
}

interface StorageRef {
  id: Uint8Array;
  storageClass: number;
}

interface NSC {
  nodeId: Uint8Array;
  fileStorages: StorageRef[];
}

interface Vault {
  name: string;
  type: VaultType;
  storageClass: number;
  replicationFactor: number;
  placements: readonly Placement[];
}

interface Member {
  id: string;
  state: NodeState;
}

export interface RemovalEffect {
  /** Vaults whose only copy is on the node: removal loses their data. */
  orphaned: string[];
  /** Vaults left below their replication factor with no live node to re-place onto. */
  belowRF: { name: string; rf: number; surviving: number }[];
  /** Vaults holding a copy on the node that survive elsewhere and get re-placed. */
  rePlaced: string[];
}

const SYNTHETIC_STORAGE_PREFIX = "node:";

function nodeIdForStorage(storageId: string, nscs: readonly NSC[]): string {
  if (storageId.startsWith(SYNTHETIC_STORAGE_PREFIX)) {
    return storageId.slice(SYNTHETIC_STORAGE_PREFIX.length);
  }
  for (const nsc of nscs) {
    if (nsc.fileStorages.some((fs) => encode(fs.id) === storageId)) return encode(nsc.nodeId);
  }
  return "";
}

function holderNodeIds(vault: Vault, nscs: readonly NSC[]): Set<string> {
  const holders = new Set<string>();
  for (const p of vault.placements) {
    const nid = nodeIdForStorage(encode(p.storageId), nscs);
    if (nid !== "") holders.add(nid);
  }
  return holders;
}

function nodeHasStorageClass(nscs: readonly NSC[], nodeId: string, storageClass: number): boolean {
  const nsc = nscs.find((n) => encode(n.nodeId) === nodeId);
  return nsc?.fileStorages.some((fs) => fs.storageClass === storageClass) ?? false;
}

// A node can take a new copy of a memory vault always, of a file vault when
// it has a file storage of the vault's class, and of a JSONL sink never.
function nodeEligibleForVault(vault: Vault, nodeId: string, nscs: readonly NSC[]): boolean {
  switch (vault.type) {
    case VaultType.MEMORY:
      return true;
    case VaultType.FILE:
      return nodeHasStorageClass(nscs, nodeId, vault.storageClass);
    default:
      return false;
  }
}

/** What removing `targetNodeId` does to every vault, given the cluster as the UI sees it. */
export function removalEffect(
  targetNodeId: string,
  vaults: readonly Vault[],
  nscs: readonly NSC[],
  members: readonly Member[],
): RemovalEffect {
  const memberIds = new Set(members.map((m) => m.id));
  const liveOthers = members
    .filter((m) => m.id !== targetNodeId && (m.state === NodeState.LIVE || m.state === NodeState.UNSPECIFIED))
    .map((m) => m.id);
  const effect: RemovalEffect = { orphaned: [], belowRF: [], rePlaced: [] };
  for (const vault of vaults) {
    const holders = holderNodeIds(vault, nscs);
    if (holders.size === 0 || !holders.has(targetNodeId)) continue;
    const surviving = [...holders].filter((nid) => nid !== targetNodeId && memberIds.has(nid));
    if (surviving.length === 0) {
      effect.orphaned.push(vault.name);
      continue;
    }
    const rf = vault.replicationFactor > 0 ? vault.replicationFactor : 1;
    if (surviving.length < rf) {
      const eligible = liveOthers.filter(
        (nid) => !surviving.includes(nid) && nodeEligibleForVault(vault, nid, nscs),
      ).length;
      if (surviving.length + eligible < rf) {
        effect.belowRF.push({ name: vault.name, rf, surviving: surviving.length });
        continue;
      }
    }
    effect.rePlaced.push(vault.name);
  }
  return effect;
}

/** One line stating why the leader refuses the removal, or undefined when it would not. */
export function removalRefusalReason(effect: RemovalEffect): string | undefined {
  if (effect.orphaned.length > 0) {
    return `Holds the only copy of ${effect.orphaned.join(", ")} — move or delete ${effect.orphaned.length === 1 ? "it" : "them"} first`;
  }
  if (effect.belowRF.length > 0) {
    const parts = effect.belowRF.map((v) => `${v.name} (${String(v.surviving)} of ${String(v.rf)} copies would remain)`);
    return `Would leave ${parts.join(", ")} below its replication factor with no node to re-place onto`;
  }
  return undefined;
}
