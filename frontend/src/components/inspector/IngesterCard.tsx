import { useThemeClass } from "../../hooks/useThemeClass";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { useIngesterStatus, useConfig } from "../../api/hooks";
import { formatBytes } from "../../utils/units";
import { Badge } from "../Badge";
import { CogIcon } from "../icons";
import { ExpandableCard } from "../settings/ExpandableCard";
import { NodeBadge } from "../settings/NodeBadge";
import { CrossLinkBadge } from "./CrossLinkBadge";
import { encode } from "../../api/glid";
import type { BadgeVariant } from "../Badge";

type StatusVariant = Extract<BadgeVariant, "info" | "warn" | "error">;

interface IngesterCardProps {
  ingester: { id: Uint8Array; name: string; type: string; running: boolean; enabled: boolean; nodeIds: Uint8Array[]; nodeStatus: { [key: string]: boolean } };
  liveNodeIds: Set<string>;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  showNodeBadge?: boolean;
  onOpenSettings?: () => void;
}

export function IngesterCard({
  ingester,
  liveNodeIds,
  dark,
  expanded,
  onToggle,
  showNodeBadge = true,
  onOpenSettings,
}: Readonly<IngesterCardProps>) {
  const ingId = encode(ingester.id);
  const selected = ingester.nodeIds.length;
  const running = Object.values(ingester.nodeStatus).filter(Boolean).length;

  let statusVariant: StatusVariant = "info";
  if (selected > 0 && running < selected) {
    const hasDeadNode = ingester.nodeIds.some((nid) => !liveNodeIds.has(encode(nid)));
    statusVariant = hasDeadNode ? "error" : "warn";
  }

  return (
    <ExpandableCard
      id={ingester.name || ingId}
      typeBadge={ingester.type}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      headerRight={
        <span className="flex items-center gap-1.5">
          {showNodeBadge && ingester.nodeIds.length > 0 && <NodeBadge nodeId={encode(ingester.nodeIds[0]!)} dark={dark} />}
          <IngesterStatusBadge selected={selected} running={running} variant={statusVariant} enabled={ingester.enabled} dark={dark} />
          {onOpenSettings && (
            <CrossLinkBadge dark={dark} title="Open in Settings" onClick={onOpenSettings}>
              <CogIcon className="w-3 h-3" />
            </CrossLinkBadge>
          )}
        </span>
      }
    >
      <IngesterDetail id={ingId} nodeIds={ingester.nodeIds} nodeStatus={ingester.nodeStatus} liveNodeIds={liveNodeIds} dark={dark} />
    </ExpandableCard>
  );
}

function IngesterStatusBadge({ selected, running, variant, enabled, dark }: Readonly<{
  selected: number; running: number; variant: StatusVariant; enabled: boolean; dark: boolean;
}>) {
  if (!enabled) return <Badge variant="muted" dark={dark}>stopped</Badge>;
  if (selected > 0) return <Badge variant={variant} dark={dark}>{`${String(running)}/${String(selected)}`}</Badge>;
  return <Badge variant="muted" dark={dark}>stopped</Badge>;
}

function IngesterDetail({ id, nodeIds, nodeStatus, liveNodeIds, dark }: Readonly<{
  id: string;
  nodeIds: Uint8Array[];
  nodeStatus: { [key: string]: boolean };
  liveNodeIds: Set<string>;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useIngesterStatus(id);
  const { data: config } = useConfig();

  if (isLoading) {
    return <LoadingPlaceholder dark={dark} className="px-4 py-3" />;
  }

  if (!data) {
    return (
      <div
        className={`px-4 py-3 text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
      >
        No status available.
      </div>
    );
  }

  const stats = [
    {
      label: "Messages ingested",
      value: Number(data.messagesIngested).toLocaleString(),
    },
    { label: "Bytes ingested", value: formatBytes(Number(data.bytesIngested)) },
    {
      label: "Dropped",
      hint: "No vault filter matched, or storage I/O failed",
      value: Number(data.errors).toLocaleString(),
      isError: Number(data.errors) > 0,
    },
  ];

  return (
    <div className={`px-4 py-3 ${c("bg-ink-raised", "bg-light-bg")}`}>
      <div
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
      >
        Metrics
      </div>
      <div className="flex flex-col gap-1.5">
        {stats.map((stat) => (
          <div
            key={stat.label}
            className="flex items-start gap-3 text-[0.85em]"
          >
            <div className="w-36">
              <span
                className={c("text-text-muted", "text-light-text-muted")}
              >
                {stat.label}
              </span>
              {stat.hint && (
                <div className={`text-[0.8em] leading-tight mt-0.5 ${c("text-text-muted", "text-light-text-muted")}`}>
                  {stat.hint}
                </div>
              )}
            </div>
            <span
              className={`font-mono ${
                stat.isError
                  ? "text-severity-error"
                  : c("text-text-bright", "text-light-text-bright")
              }`}
            >
              {stat.value}
            </span>
          </div>
        ))}
      </div>
      {nodeIds.length > 0 && (
        <div className="mt-3">
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Nodes
          </div>
          <div className="flex flex-wrap gap-1.5">
            {nodeIds.map((nid) => {
              const nodeId = encode(nid);
              const alive = nodeStatus[nodeId] ?? false;
              const dead = !liveNodeIds.has(nodeId);
              const nodeCfg = config?.nodeConfigs.find((n) => encode(n.id) === nodeId);
              const label = nodeCfg?.name || nodeId;
              let variant: StatusVariant = "info";
              if (dead) variant = "error";
              else if (!alive) variant = "warn";
              return <Badge key={nodeId} variant={variant} dark={dark}>{label}</Badge>;
            })}
          </div>
        </div>
      )}
    </div>
  );
}
