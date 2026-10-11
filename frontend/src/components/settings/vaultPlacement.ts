// Placement derivations for the vault settings surfaces: how high the
// replication factor can go and why, and how a vault's placements compare
// to it. Every number here is read straight off the cluster config.

export type VaultShape = { type: "memory" | "file" | "jsonl"; storageClass: string };

/** The replication factor cap and the fact it derives from. */
export interface RFCap {
  /** Upper bound for the RF spinner; never below 1. */
  max: number;
  /** Storages (or nodes) that can hold a replica; 0 when none qualifies. */
  eligible: number;
  cause: string;
}

export function plural(n: number, noun: string): string {
  return `${String(n)} ${noun}${n === 1 ? "" : "s"}`;
}

/** File storages per storage class, across every node. Same-node storages count: a replica per storage. */
export function classStorageCounts(
  nodeStorageConfigs: readonly { fileStorages: readonly { storageClass: number }[] }[],
): Map<number, number> {
  const counts = new Map<number, number>();
  for (const nsc of nodeStorageConfigs) {
    for (const fs of nsc.fileStorages) {
      counts.set(fs.storageClass, (counts.get(fs.storageClass) ?? 0) + 1);
    }
  }
  return counts;
}

export function rfCap(shape: VaultShape, counts: ReadonlyMap<number, number>, totalNodes: number): RFCap {
  if (shape.type === "memory") {
    return { max: Math.max(1, totalNodes), eligible: totalNodes, cause: `${plural(totalNodes, "node")} in the cluster` };
  }
  if (shape.type === "jsonl") {
    return { max: 1, eligible: 1, cause: "a JSONL sink writes on one node" };
  }
  const sc = parseInt(shape.storageClass, 10) || 0;
  if (sc === 0) {
    return { max: 1, eligible: 0, cause: "no storage class selected" };
  }
  const n = counts.get(sc) ?? 0;
  if (n === 0) {
    return { max: 1, eligible: 0, cause: `no file storage has class ${String(sc)}` };
  }
  return { max: n, eligible: n, cause: `${plural(n, "file storage")} with class ${String(sc)}` };
}

export interface PlacementStatus {
  placed: number;
  rf: number;
  cap: RFCap;
  /** Why the vault has no leader; undefined while a leader is placed. */
  unplacedCause?: string;
  /** What raises the placement count; undefined while placed >= rf. */
  action?: string;
}

export function placementStatus(
  vault: { replicationFactor: number; placements: readonly { leader: boolean }[] },
  shape: VaultShape,
  counts: ReadonlyMap<number, number>,
  totalNodes: number,
): PlacementStatus {
  const rf = vault.replicationFactor || 1;
  const placed = vault.placements.length;
  const cap = rfCap(shape, counts, totalNodes);
  const hasLeader = vault.placements.some((p) => p.leader);
  const status: PlacementStatus = { placed, rf, cap };
  if (!hasLeader) {
    status.unplacedCause = cap.eligible === 0 ? cap.cause : "no leader assigned yet";
  }
  if (placed < rf) {
    status.action = placementAction(shape, cap, rf);
  }
  return status;
}

function placementAction(shape: VaultShape, cap: RFCap, rf: number): string {
  if (cap.eligible >= rf) return "placement pending";
  const missing = rf - cap.eligible;
  if (shape.type === "memory") return `add ${plural(missing, "node")}`;
  if (shape.type === "file" && cap.eligible === 0 && (parseInt(shape.storageClass, 10) || 0) === 0) {
    return "select a storage class";
  }
  return `add ${plural(missing, "file storage")} with class ${shape.storageClass}`;
}
