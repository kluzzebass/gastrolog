import { useThemeClass } from "../../hooks/useThemeClass";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { useIngesterStatus, useIngesterAlive, useNodeRegistry } from "../../api/hooks";
import { formatBytes } from "../../utils/units";
import { Badge } from "../Badge";
import { HelpButton } from "../HelpButton";
import { LiveValue } from "../LiveValue";
import { CogIcon } from "../icons";
import { ExpandableCard } from "../settings/ExpandableCard";
import { NodeBadge } from "../settings/NodeBadge";
import { CrossLinkBadge } from "./CrossLinkBadge";
import { type EntityID } from "../../api/model/id";
import type { Ingester, IngesterStatusVariant, NodeStatusMap } from "../../api/model/ingester";

export const INGESTER_STATUS_HELP_REF = "inspector-ingesters#status";
export const INGESTER_NODES_HELP_REF = "inspector-ingesters#nodes";

export interface IngesterStatusBadgeSpec {
  text: string;
  title: string;
}

/** The header badge for an ingester: disabled, nowhere to run, or how many of its eligible nodes run it. */
export function ingesterStatusBadge(args: {
  enabled: boolean;
  selected: number;
  running: number;
  variant: IngesterStatusVariant;
}): IngesterStatusBadgeSpec {
  if (!args.enabled) {
    return { text: "disabled", title: "disabled — runs on no node; enable it in Settings" };
  }
  if (args.selected === 0) {
    return {
      text: "no eligible node",
      title: "no live node may run this ingester — pin a live node or allow all nodes",
    };
  }
  const text = `${String(args.running)}/${String(args.selected)} running`;
  switch (args.variant) {
    case "info":
      return { text, title: "running on every eligible node (running / eligible)" };
    case "error":
      return { text, title: "an eligible pinned node is unreachable (running / eligible)" };
    default:
      return { text, title: "running on some eligible nodes only (running / eligible) — the rest have not started it" };
  }
}

/** The hover text of one node chip in the Nodes section. */
export function nodeChipTitle(name: string, status: { alive: boolean; dead: boolean }): string {
  if (status.dead) return `${name}: unreachable — the cluster has not heard from it`;
  if (!status.alive) return `${name}: eligible but not running the ingester`;
  return `${name}: running the ingester`;
}

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
  const nodes = useNodeRegistry();
  const nodeStatus: NodeStatusMap = aliveMap.get(ingester.id) ?? {};

  const selected = ingester.selectedCount(liveNodeIds);
  const running = ingester.runningCount(aliveMap, liveNodeIds);
  const variant = ingester.statusVariant(aliveMap, liveNodeIds);
  const pinned = ingester.allNodes ? [] : ingester.pinnedNodeIds;

  return (
    <ExpandableCard
      id={ingester.displayLabel}
      typeBadge={ingester.type}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      headerRight={
        <span className="flex items-center gap-1.5">
          {showNodeBadge && pinned.length === 1 && (
            <NodeBadge nodeId={pinned[0]!} dark={dark} />
          )}
          {showNodeBadge && pinned.length > 1 && (
            <Badge
              variant="muted"
              dark={dark}
              title={`pinned to ${pinned.map((id) => nodes.nameOf(id)).join(", ")}`}
              helpRef={INGESTER_NODES_HELP_REF}
            >
              {`${String(pinned.length)} nodes`}
            </Badge>
          )}
          <IngesterStatusBadge
            spec={ingesterStatusBadge({ enabled: ingester.enabled, selected, running, variant })}
            variant={variant}
            dark={dark}
          />
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

function IngesterStatusBadge({ spec, variant, dark }: Readonly<{
  spec: IngesterStatusBadgeSpec; variant: IngesterStatusVariant; dark: boolean;
}>) {
  return (
    <Badge variant={variant} dark={dark} title={spec.title} helpRef={INGESTER_STATUS_HELP_REF}>
      {spec.text}
    </Badge>
  );
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
        No status — no node is reporting this ingester.
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
      hint: "Failed ingester runs — the ingester exited with an error and was restarted",
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
            <LiveValue dark={dark} tone={stat.isError ? "error" : "bright"} align="start">
              {stat.value}
            </LiveValue>
          </div>
        ))}
      </div>
      {nodesToShow.length > 0 && (
        <div className="mt-3">
          <div
            className={`flex items-center gap-1.5 text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Nodes {ingester.allNodes && <span className="font-normal normal-case tracking-normal opacity-70">— eligible on every live node</span>}
            <HelpButton helpRef={INGESTER_NODES_HELP_REF} />
          </div>
          <div className="flex flex-wrap gap-1.5">
            {nodesToShow.map((nodeId) => {
              const alive = nodeStatus[nodeId] ?? false;
              const dead = !liveNodeIds.has(nodeId);
              const label = nodes.nameOf(nodeId);
              let variant: "info" | "warn" | "error" = "info";
              if (dead) variant = "error";
              else if (!alive) variant = "warn";
              return (
                <Badge key={nodeId} variant={variant} dark={dark} title={nodeChipTitle(label, { alive, dead })}>
                  {label}
                </Badge>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
