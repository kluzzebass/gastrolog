import { useConfig } from "../../api/hooks";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useSettings } from "../../api/hooks/useConfig";
import { Badge } from "../Badge";

export function NodeBadge({
  nodeId,
  dark,
}: Readonly<{ nodeId: string; dark: boolean }>) {
  const { data: clusterStatus } = useClusterStatus();
  const { data: config } = useConfig();
  const { data: settings } = useSettings();

  if (!nodeId) return null;

  const multiNode =
    clusterStatus?.clusterEnabled ||
    (config?.nodeConfigs && config.nodeConfigs.length > 1);
  if (!multiNode) return null;

  const localNodeId = settings?.nodeId ?? "";
  const isLocal = nodeId === localNodeId;
  const node = config?.nodeConfigs?.find((n) => n.id === nodeId);
  const label = node?.name || nodeId;

  return (
    <>
      {isLocal && <Badge variant="copper" dark={dark}>this node</Badge>}
      <Badge variant="muted" dark={dark}>{label}</Badge>
    </>
  );
}
