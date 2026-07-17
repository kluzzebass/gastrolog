import { useClusterStatus } from "./useClusterStatus";
import type { SystemAlert } from "../gen/gastrolog/v1/cluster_pb";
import { encode } from "../glid";

export { AlarmPriority } from "../gen/gastrolog/v1/cluster_pb";

export interface NodeAlert extends SystemAlert {
  nodeId: string;
  nodeName: string;
}

/** Type ID of the alarm-flood meta-alarm (see backend alert.FloodTypeID). */
export const FLOOD_TYPE_ID = "alarm-flood";

/** One node whose alarm system is flooding: the node and its rolling
 *  10-minute activation rate (NodeStats.alarmRate10m). The flood condition
 *  is per-node — the collector and its rate monitor live on each node — so
 *  attribution names the node, never a cluster aggregate. */
export interface NodeFlood {
  nodeId: string;
  nodeName: string;
  rate: number;
}

/** A display group of alarms: same node, same alarm type. Length 1 renders
 *  as a plain row; longer groups are the flood-mode collapse (one row with
 *  a count, expandable to the instances). Collapsing happens here on the
 *  aggregation side — the wire keeps full per-node, per-instance truth. */
export interface AlertGroup {
  key: string;
  alerts: NodeAlert[];
}

/** Collapse same-type alarms of flooding nodes into groups, preserving the
 *  incoming sort order (a group sits where its first member sorted). Alarms
 *  of non-flooding nodes and the alarm-flood alarm itself stay singletons. */
export function collapseFloodAlerts(
  alerts: readonly NodeAlert[],
  floodingNodeIds: ReadonlySet<string>,
): AlertGroup[] {
  const groups: AlertGroup[] = [];
  const byKey = new Map<string, AlertGroup>();
  for (const a of alerts) {
    const collapsible = floodingNodeIds.has(a.nodeId) && a.typeId !== FLOOD_TYPE_ID;
    const key = collapsible ? `${a.nodeId}:${a.typeId}` : `${a.nodeId}:${encode(a.id)}`;
    const existing = byKey.get(key);
    if (existing) {
      existing.alerts.push(a);
      continue;
    }
    const group: AlertGroup = { key, alerts: [a] };
    byKey.set(key, group);
    groups.push(group);
  }
  return groups;
}

/** Display rank for an alarm: software faults sit outside the priority
 *  scale but demand the same visual weight as Critical. */
export function alarmRank(a: Pick<NodeAlert, "priority" | "softwareFault">): number {
  if (a.softwareFault) return 4;
  return a.priority; // CRITICAL=3, HIGH=2, LOW=1, UNSPECIFIED=0
}

export function useAlerts() {
  const { data: cluster } = useClusterStatus();
  if (!cluster) return { alerts: [] as NodeAlert[], maxRank: 0, floods: [] as NodeFlood[] };

  const alerts: NodeAlert[] = [];
  const floods: NodeFlood[] = [];
  for (const node of cluster.nodes) {
    const nodeAlerts = node.stats?.alerts ?? [];
    if (nodeAlerts.length === 0) continue;
    const nid = encode(node.id);
    const nodeName = node.name || nid.slice(0, 8);
    for (const a of nodeAlerts) {
      const na = a.clone() as NodeAlert;
      na.nodeId = nid;
      na.nodeName = nodeName;
      alerts.push(na);
      // Flooding is a per-node fact: this node's own alarm-flood alarm.
      if (na.typeId === FLOOD_TYPE_ID) {
        floods.push({ nodeId: nid, nodeName, rate: node.stats?.alarmRate10m ?? 0 });
      }
    }
  }
  // Highest rank first, oldest first within a rank.
  alerts.sort((a, b) => {
    const rank = alarmRank(b) - alarmRank(a);
    if (rank !== 0) return rank;
    const aTime = Number(a.firstSeen?.seconds ?? 0n);
    const bTime = Number(b.firstSeen?.seconds ?? 0n);
    return aTime - bTime;
  });

  let maxRank = 0;
  for (const a of alerts) {
    maxRank = Math.max(maxRank, alarmRank(a));
  }

  return { alerts, maxRank, floods };
}
