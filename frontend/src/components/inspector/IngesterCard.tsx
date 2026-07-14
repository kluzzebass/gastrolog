import { useThemeClass } from "../../hooks/useThemeClass";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { useIngesterStatus, useIngesterAlive, useNodeRegistry } from "../../api/hooks";
import { formatBytes } from "../../utils/units";
import { Badge } from "../Badge";
import { CogIcon } from "../icons";
import { ExpandableCard } from "../settings/ExpandableCard";
import { NodeBadge } from "../settings/NodeBadge";
import { CrossLinkBadge } from "./CrossLinkBadge";
import { type EntityID } from "../../api/model/id";
import type { Ingester, NodeStatusMap } from "../../api/model/ingester";

interface IngesterCardProps {
  ingester: Ingester;
  liveNodeIds: ReadonlySet<EntityID>;
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
  const aliveMap = useIngesterAlive();
  const nodeStatus: NodeStatusMap = aliveMap.get(ingester.id) ?? {};

  const selected = ingester.selectedCount(liveNodeIds);
  const running = ingester.runningCount(aliveMap, liveNodeIds);
  const variant = ingester.statusVariant(aliveMap, liveNodeIds);

  return (
    <ExpandableCard
      id={ingester.displayLabel}
      typeBadge={ingester.type}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      headerRight={
        <span className="flex items-center gap-1.5">
          {showNodeBadge && !ingester.allNodes && ingester.pinnedNodeIds.length > 0 && (
            <NodeBadge nodeId={ingester.pinnedNodeIds[0]!} dark={dark} />
          )}
          <IngesterStatusBadge selected={selected} running={running} variant={variant} dark={dark} />
          {onOpenSettings && (
            <CrossLinkBadge dark={dark} title="Open in Settings" onClick={onOpenSettings}>
              <CogIcon className="w-3 h-3" />
            </CrossLinkBadge>
          )}
        </span>
      }
    >
      <IngesterDetail ingester={ingester} nodeStatus={nodeStatus} liveNodeIds={liveNodeIds} dark={dark} />
    </ExpandableCard>
  );
}

function IngesterStatusBadge({ selected, running, variant, dark }: Readonly<{
  selected: number; running: number; variant: "info" | "warn" | "error" | "muted"; dark: boolean;
}>) {
  if (variant === "muted") return <Badge variant="muted" dark={dark}>stopped</Badge>;
  return <Badge variant={variant} dark={dark}>{`${String(running)}/${String(selected)}`}</Badge>;
}

function IngesterDetail({ ingester, nodeStatus, liveNodeIds, dark }: Readonly<{
  ingester: Ingester;
  nodeStatus: NodeStatusMap;
  liveNodeIds: ReadonlySet<EntityID>;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useIngesterStatus(ingester.id);
  const nodes = useNodeRegistry();

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
      label: "Errors",
      hint: "Errors reported by this ingester (decode failures, rejected writes)",
      value: Number(data.errors).toLocaleString(),
      isError: Number(data.errors) > 0,
    },
  ];

  const nodesToShow = ingester.nodesToDisplay(liveNodeIds);

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
      {nodesToShow.length > 0 && (
        <div className="mt-3">
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Nodes {ingester.allNodes && <span className="font-normal normal-case tracking-normal opacity-70">— all nodes</span>}
          </div>
          <div className="flex flex-wrap gap-1.5">
            {nodesToShow.map((nodeId) => {
              const alive = nodeStatus[nodeId] ?? false;
              const dead = !liveNodeIds.has(nodeId);
              const label = nodes.nameOf(nodeId);
              let variant: "info" | "warn" | "error" = "info";
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
