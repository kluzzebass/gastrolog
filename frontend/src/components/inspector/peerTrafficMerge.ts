// eslint-disable-next-line no-restricted-imports -- passthrough proto types from Node.stats
import type { NodeStats, PeerConnStat } from "../../api/gen/gastrolog/v1/cluster_pb";
import { type EntityID, asEntityID } from "../../api/model/id";

/** One logical cluster-port lane between viewNode and peer, both directions. */
export interface MergedPeerLane {
  lane: string;
  poolIndex: number;
  groupId: string;
  purposes: string[];
  bytesSent: number;
  bytesReceived: number;
  txBytesPerSec: number;
  rxBytesPerSec: number;
  txSpark: number[];
  rxSpark: number[];
}

export interface MergedPeerTraffic {
  peerId: EntityID;
  lanes: MergedPeerLane[];
  total: MergedPeerLane;
}

function laneMergeKey(row: PeerConnStat): string {
  if (row.lane === "raft") {
    return `raft\u0000${row.groupId}`;
  }
  return `service\u0000${row.poolIndex}`;
}

function mergeSparks(a: readonly number[], b: readonly number[]): number[] {
  const len = Math.max(a.length, b.length);
  const out: number[] = [];
  for (let i = 0; i < len; i++) {
    out.push((a[i] ?? 0) + (b[i] ?? 0));
  }
  return out;
}

function addRow(map: Map<string, MergedPeerLane>, row: PeerConnStat): void {
  if (row.lane === "inbound") {
    return;
  }
  const key = laneMergeKey(row);
  const existing = map.get(key);
  const purposes = new Set(existing?.purposes);
  for (const p of row.purposes) {
    if (p) {
      purposes.add(p);
    }
  }
  const lane = row.lane === "raft" ? "raft" : "service";
  map.set(key, {
    lane,
    poolIndex: row.lane === "raft" ? 0 : row.poolIndex,
    groupId: row.lane === "raft" ? row.groupId : "",
    purposes: [...purposes].sort(),
    bytesSent: (existing?.bytesSent ?? 0) + Number(row.bytesSent),
    bytesReceived: (existing?.bytesReceived ?? 0) + Number(row.bytesReceived),
    txBytesPerSec: (existing?.txBytesPerSec ?? 0) + row.txBytesPerSec,
    rxBytesPerSec: (existing?.rxBytesPerSec ?? 0) + row.rxBytesPerSec,
    txSpark: mergeSparks(existing?.txSpark ?? [], row.txSpark),
    rxSpark: mergeSparks(existing?.rxSpark ?? [], row.rxSpark),
  });
}

function sumLanes(lanes: readonly MergedPeerLane[]): MergedPeerLane {
  const purposes = new Set<string>();
  let bytesSent = 0;
  let bytesReceived = 0;
  let txBytesPerSec = 0;
  let rxBytesPerSec = 0;
  let txSpark: number[] = [];
  let rxSpark: number[] = [];
  for (const lane of lanes) {
    for (const p of lane.purposes) {
      purposes.add(p);
    }
    bytesSent += lane.bytesSent;
    bytesReceived += lane.bytesReceived;
    txBytesPerSec += lane.txBytesPerSec;
    rxBytesPerSec += lane.rxBytesPerSec;
    txSpark = mergeSparks(txSpark, lane.txSpark);
    rxSpark = mergeSparks(rxSpark, lane.rxSpark);
  }
  return {
    lane: "total",
    poolIndex: 0,
    groupId: "",
    purposes: [...purposes].sort(),
    bytesSent,
    bytesReceived,
    txBytesPerSec,
    rxBytesPerSec,
    txSpark,
    rxSpark,
  };
}

function laneSortOrder(lane: string): number {
  if (lane === "raft") {
    return 0;
  }
  if (lane === "service") {
    return 1;
  }
  return 2;
}

/** Extract vault entity id from a vault control-plane raft group id. */
export function vaultIdFromCtlGroup(groupId: string): EntityID | null {
  if (!groupId.startsWith("vault/") || !groupId.endsWith("/ctl")) {
    return null;
  }
  const id = groupId.slice("vault/".length, -"/ctl".length);
  return id ? asEntityID(id) : null;
}

/** One display string per lane: vault name for raft, subsystem labels for service. */
export function laneDetailText(
  lane: MergedPeerLane,
  opts?: {
    isTotal?: boolean;
    vaultNameOf?: (vaultId: EntityID) => string | undefined;
  },
): { label: string; title: string } {
  const isTotal = opts?.isTotal ?? false;
  if (isTotal || lane.lane === "total") {
    return { label: "—", title: "" };
  }
  if (lane.lane === "raft") {
    if (!lane.groupId) {
      return { label: "—", title: "" };
    }
    const vaultId = vaultIdFromCtlGroup(lane.groupId);
    const name = vaultId ? opts?.vaultNameOf?.(vaultId) : undefined;
    if (name) {
      return { label: name, title: lane.groupId };
    }
    return { label: "—", title: lane.groupId };
  }
  const purposes = lane.purposes
    .filter((p) => p && p !== "unknown")
    .sort();
  if (purposes.length === 0) {
    return { label: "—", title: "" };
  }
  const label = purposes.join(", ");
  return { label, title: label };
}

type PeerConnStatsView = Pick<NodeStats, "peerConnections">;

/** Merge viewNode→peer and peer→viewNode peer_connections into link lanes. */
export function mergePeerTraffic(
  viewNodeId: EntityID,
  viewStats: PeerConnStatsView | null | undefined,
  peerId: EntityID,
  peerStats: PeerConnStatsView | null | undefined,
): MergedPeerTraffic | null {
  const map = new Map<string, MergedPeerLane>();

  for (const row of viewStats?.peerConnections ?? []) {
    if (asEntityID(row.peer) !== peerId) {
      continue;
    }
    addRow(map, row);
  }
  for (const row of peerStats?.peerConnections ?? []) {
    if (asEntityID(row.peer) !== viewNodeId) {
      continue;
    }
    addRow(map, row);
  }

  if (map.size === 0) {
    return null;
  }

  const lanes = [...map.values()].sort((a, b) => {
    const oa = laneSortOrder(a.lane);
    const ob = laneSortOrder(b.lane);
    if (oa !== ob) {
      return oa - ob;
    }
    if (a.lane === "raft") {
      return a.groupId.localeCompare(b.groupId);
    }
    return a.poolIndex - b.poolIndex;
  });

  return {
    peerId,
    lanes,
    total: sumLanes(lanes),
  };
}

/** All peers with traffic to/from viewNode, merged both ways when peer stats exist. */
export function mergeAllPeerTraffic(
  viewNodeId: EntityID,
  viewStats: PeerConnStatsView | null | undefined,
  peerStatsById: ReadonlyMap<EntityID, PeerConnStatsView | null | undefined>,
): MergedPeerTraffic[] {
  const peerIds = new Set<EntityID>();
  for (const row of viewStats?.peerConnections ?? []) {
    peerIds.add(asEntityID(row.peer));
  }
  for (const [id, stats] of peerStatsById) {
    if (id === viewNodeId) {
      continue;
    }
    for (const row of stats?.peerConnections ?? []) {
      if (asEntityID(row.peer) === viewNodeId) {
        peerIds.add(id);
      }
    }
  }

  const out: MergedPeerTraffic[] = [];
  for (const peerId of peerIds) {
    const merged = mergePeerTraffic(
      viewNodeId,
      viewStats,
      peerId,
      peerStatsById.get(peerId),
    );
    if (merged) {
      out.push(merged);
    }
  }
  return out;
}
