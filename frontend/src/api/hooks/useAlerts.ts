import { useClusterStatus } from "./useClusterStatus";
import { AlertSeverity } from "../gen/gastrolog/v1/cluster_pb";
import type { SystemAlert } from "../gen/gastrolog/v1/cluster_pb";

export interface NodeAlert extends SystemAlert {
  nodeId: string;
  nodeName: string;
}

export function useAlerts() {
  const { data: cluster } = useClusterStatus();
  if (!cluster) return { alerts: [] as NodeAlert[], maxSeverity: null as AlertSeverity | null };

  const alerts: NodeAlert[] = [];
  for (const node of cluster.nodes) {
    for (const a of node.stats?.alerts ?? []) {
      const na = a.clone() as NodeAlert;
      na.nodeId = node.id;
      na.nodeName = node.name || node.id.slice(0, 8);
      alerts.push(na);
    }
  }
  alerts.sort((a, b) => {
    const aTime = Number(a.firstSeen?.seconds ?? 0n);
    const bTime = Number(b.firstSeen?.seconds ?? 0n);
    return aTime - bTime;
  });

  const maxSeverity = alerts.some((a) => a.severity === AlertSeverity.ERROR)
    ? AlertSeverity.ERROR
    : alerts.length > 0
      ? AlertSeverity.WARNING
      : null;

  return { alerts, maxSeverity };
}
