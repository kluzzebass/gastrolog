import { useClusterStatus } from "./useClusterStatus";
import type { SystemAlert } from "../gen/gastrolog/v1/cluster_pb";
import { encode } from "../glid";

export { AlarmPriority } from "../gen/gastrolog/v1/cluster_pb";

export interface NodeAlert extends SystemAlert {
  nodeId: string;
  nodeName: string;
}

/** Display rank for an alarm: software faults sit outside the priority
 *  scale but demand the same visual weight as Critical. */
export function alarmRank(a: Pick<NodeAlert, "priority" | "softwareFault">): number {
  if (a.softwareFault) return 4;
  return a.priority; // CRITICAL=3, HIGH=2, LOW=1, UNSPECIFIED=0
}

/** Sort in place: highest rank first, oldest first within a rank. */
export function sortAlerts(alerts: NodeAlert[]): NodeAlert[] {
  alerts.sort((a, b) => {
    const rank = alarmRank(b) - alarmRank(a);
    if (rank !== 0) return rank;
    const aTime = Number(a.firstSeen?.seconds ?? 0n);
    const bTime = Number(b.firstSeen?.seconds ?? 0n);
    return aTime - bTime;
  });
  return alerts;
}

/** The cluster's standing alarms, aggregated from every node's broadcast and
 *  attributed to the raising node. A flat, priority-sorted list — an alarm
 *  is standing or it is not. */
export function useAlerts() {
  const { data: cluster } = useClusterStatus();
  if (!cluster)
    return {
      alerts: [] as NodeAlert[],
      maxRank: 0,
    };

  const alerts: NodeAlert[] = [];
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
    }
  }
  sortAlerts(alerts);

  // The header pill reflects the most severe standing condition.
  let maxRank = 0;
  for (const a of alerts) maxRank = Math.max(maxRank, alarmRank(a));

  return { alerts, maxRank };
}
