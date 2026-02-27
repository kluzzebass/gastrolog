import { useThemeClass } from "../../hooks/useThemeClass";
import { useConfig } from "../../api/hooks";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";

export function NodeBadge({
  nodeId,
  dark,
}: Readonly<{ nodeId: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data: clusterStatus } = useClusterStatus();
  const { data: config } = useConfig();

  if (!nodeId) return null;

  const multiNode =
    clusterStatus?.clusterEnabled ||
    (config?.nodeConfigs && config.nodeConfigs.length > 1);
  if (!multiNode) return null;

  const node = config?.nodeConfigs?.find((n) => n.id === nodeId);
  const label = node?.name || nodeId;

  return (
    <span
      className={`px-1.5 py-0.5 text-[0.75em] font-mono rounded ${c(
        "bg-ink-hover text-text-muted",
        "bg-light-hover text-light-text-muted",
      )}`}
      title={`Node: ${label}`}
    >
      {label}
    </span>
  );
}
