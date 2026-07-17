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

export function useAlerts() {
  const { data: cluster } = useClusterStatus();
  if (!cluster) return { alerts: [] as NodeAlert[], maxRank: 0 };

  const alerts: NodeAlert[] = [];
  for (const node of cluster.nodes) {
    for (const a of node.stats?.alerts ?? []) {
      const na = a.clone() as NodeAlert;
      const nid = encode(node.id);
      na.nodeId = nid;
      na.nodeName = node.name || nid.slice(0, 8);
      alerts.push(na);
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

  return { alerts, maxRank };
}
