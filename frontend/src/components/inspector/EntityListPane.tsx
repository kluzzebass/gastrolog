import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { useVaults, useIngesters, useNodeRegistry } from "../../api/hooks";
import { type EntityID, idFromBytes } from "../../api/model/id";
import { useWatchJobs } from "../../api/hooks";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useConfig } from "../../api/hooks/useSystem";
import type { Job } from "../../api/model/job";
import { toastError } from "../Toast";
import { Badge } from "../Badge";
import { NodeStateBadge } from "../NodeStateBadge";
import { ExpandableCard } from "../settings/ExpandableCard";
import { HelpButton } from "../HelpButton";
import { VaultCard } from "./VaultCard";
import { IngesterCard } from "./IngesterCard";
import { protoToInstant, formatTimestamp, elapsed, countdown } from "../../utils/temporal";
import { useTick } from "./JobCard";
import { SystemStatsView, ClusterSummaryView } from "./SystemStatsView";
import { RouteStatsView } from "./RouteStatsView";
import { groupByNode } from "./groupByNode";
import type { EntityType } from "./InspectorDialog";
import { encode } from "../../api/glid";

interface EntityListPaneProps {
  entityType: EntityType;
  dark: boolean;
  onOpenSettings?: (tab: string, entityName?: string) => void;
  expandTarget?: string | null;
}

export function EntityListPane({ entityType, dark, onOpenSettings, expandTarget }: Readonly<EntityListPaneProps>) {
  switch (entityType) {
    case "vaults":
      return <VaultsList dark={dark} onOpenSettings={onOpenSettings} expandTarget={expandTarget} />;
    case "ingesters":
      return <IngestersList dark={dark} onOpenSettings={onOpenSettings} expandTarget={expandTarget} />;
    case "routes":
      return <RouteStatsList dark={dark} />;
    case "jobs":
      return <JobsList dark={dark} />;
    case "system":
      return <SystemList dark={dark} />;
  }
}

function useToggleSet() {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const toggle = (id: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  const add = (id: string) =>
    setExpanded((prev) => {
      if (prev.has(id)) return prev;
      const next = new Set(prev);
      next.add(id);
      return next;
    });
  return { expanded, toggle, add };
}

// ---- Node context helper ----
//
// Thin shim over useNodeRegistry that exposes the legacy `nodeNames` Map
// shape (EntityID → display name) used by `groupByNode`.

function useNodeContext() {
  const registry = useNodeRegistry();
  const nodeNames = new Map<EntityID, string>();
  for (const n of registry.all) {
    if (n.cluster?.name) nodeNames.set(n.id, n.cluster.name);
    else if (n.config?.name) nodeNames.set(n.id, n.config.name);
  }
  return {
    localNodeId: registry.localNodeId,
    multiNode: registry.multiNode,
    nodeNames,
  };
}

// ---- Vaults ----

function VaultsList({ dark, onOpenSettings, expandTarget }: Readonly<{ dark: boolean; onOpenSettings?: (tab: string, entityName?: string) => void; expandTarget?: string | null }>) {
  const { data: vaults, isLoading } = useVaults();
  const { expanded, toggle, add } = useToggleSet();

  // Auto-expand a vault when deep-linked from settings.
  const [consumedTarget, setConsumedTarget] = useState<string | null>(null);
  if (expandTarget && expandTarget !== consumedTarget && vaults.length > 0) {
    setConsumedTarget(expandTarget);
    const match = vaults.find((v) => v.displayLabel === expandTarget);
    if (match) add(match.id);
  }

  if (isLoading) return <Loading dark={dark} />;
  if (vaults.length === 0) return <Empty dark={dark}>No vaults configured.</Empty>;

  const sorted = [...vaults].sort((a, b) => a.displayLabel.localeCompare(b.displayLabel));

  return (
    <div className="flex flex-col gap-3">
      <EntityHeader title="Vaults" helpTopicId="inspector-vaults" dark={dark} />
      {sorted.map((vault) => (
        <VaultCard
          key={vault.id}
          vault={vault}
          dark={dark}
          expanded={expanded.has(vault.id)}
          onToggle={() => toggle(vault.id)}
          onOpenSettings={onOpenSettings ? () => onOpenSettings("vaults", vault.displayLabel) : undefined}
        />
      ))}
    </div>
  );
}

// ---- Ingesters ----

function IngestersList({ dark, onOpenSettings, expandTarget }: Readonly<{ dark: boolean; onOpenSettings?: (tab: string, entityName?: string) => void; expandTarget?: string | null }>) {
  const { isLoading } = useConfig();
  const ingesters = useIngesters();
  const { data: cluster } = useClusterStatus();
  const liveNodeIds: ReadonlySet<EntityID> = new Set((cluster?.nodes ?? []).map((n) => idFromBytes(n.id)));
  const { expanded, toggle, add } = useToggleSet();

  // Auto-expand an ingester when deep-linked from settings.
  const [consumedTarget, setConsumedTarget] = useState<string | null>(null);
  if (expandTarget && expandTarget !== consumedTarget && ingesters.length > 0) {
    setConsumedTarget(expandTarget);
    const match = ingesters.find((i) => i.displayLabel === expandTarget);
    if (match) add(match.id);
  }

  if (isLoading) return <Loading dark={dark} />;
  if (ingesters.length === 0) return <Empty dark={dark}>No ingesters configured.</Empty>;

  const sorted = [...ingesters].sort((a, b) => a.displayLabel.localeCompare(b.displayLabel));

  return (
    <div className="flex flex-col gap-3">
      <EntityHeader title="Ingesters" helpTopicId="inspector-ingesters" dark={dark} />
      {sorted.map((ing) => (
        <IngesterCard
          key={ing.id}
          ingester={ing}
          liveNodeIds={liveNodeIds}
          dark={dark}
          expanded={expanded.has(ing.id)}
          onToggle={() => toggle(ing.id)}
          onOpenSettings={onOpenSettings ? () => onOpenSettings("ingesters", ing.displayLabel) : undefined}
        />
      ))}
    </div>
  );
}

// ---- Routes ----

function RouteStatsList({ dark }: Readonly<{ dark: boolean }>) {
  return (
    <div className="flex flex-col gap-3">
      <EntityHeader title="Routes" helpTopicId="inspector-routes" dark={dark} />
      <RouteStatsView dark={dark} />
    </div>
  );
}

// ---- Jobs ----

function JobsList({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { jobs, connected, reconnecting } = useWatchJobs({ onError: toastError });
  const { localNodeId, multiNode, nodeNames } = useNodeContext();
  const [expandedNodes, setExpandedNodes] = useState<Record<string, boolean>>({});

  if (!connected && !reconnecting && jobs.length === 0) return <Loading dark={dark} />;

  const tasks = jobs.filter((j) => j.isTask);
  const scheduled = jobs.filter((j) => j.isScheduled);

  // Single-node: flat list.
  if (!multiNode) {
    return (
      <div className="flex flex-col gap-5">
        <div className="flex items-center gap-2">
          <EntityHeader title="Jobs" helpTopicId="inspector-jobs" dark={dark} />
          {reconnecting && <Badge variant="warn" dark={dark}>reconnecting</Badge>}
        </div>
        {scheduled.length > 0 && (
          <section>
            <SectionLabel dark={dark}>Scheduled</SectionLabel>
            <ScheduledHeader dark={dark} />
            <div className="flex flex-col">
              {scheduled.map((job) => (
                <ScheduledRow key={job.id} job={job} dark={dark} />
              ))}
            </div>
          </section>
        )}
        {tasks.length > 0 && (
          <section>
            <SectionLabel dark={dark}>Tasks</SectionLabel>
            <div className="flex flex-col gap-1">
              {tasks.map((job) => (
                <JobRow key={job.id} job={job} dark={dark} />
              ))}
            </div>
          </section>
        )}
        {tasks.length === 0 && scheduled.length === 0 && (
          <Empty dark={dark}>No active or scheduled jobs.</Empty>
        )}
      </div>
    );
  }

  // Multi-node: one ExpandableCard per node, flat job rows inside.
  // Merge tasks + scheduled into per-node groups.
  const allJobs = [...tasks, ...scheduled];
  const groups = groupByNode(allJobs, nodeNames, localNodeId);

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center gap-2">
        <EntityHeader title="Jobs" helpTopicId="inspector-jobs" dark={dark} />
        {reconnecting && <Badge variant="warn" dark={dark}>reconnecting</Badge>}
      </div>

      {groups.length === 0 && <Empty dark={dark}>No active or scheduled jobs.</Empty>}

      {groups.map((group) => {
        const nodeTasks = group.items.filter((j) => j.isTask);
        const nodeScheduled = group.items.filter((j) => j.isScheduled);

        return (
          <ExpandableCard
            key={group.nodeId}
            id={group.nodeName}
            dark={dark}
            monoTitle={false}
            expanded={expandedNodes[group.nodeId] ?? true}
            onToggle={() =>
              setExpandedNodes((prev) => ({ ...prev, [group.nodeId]: !(prev[group.nodeId] ?? true) }))
            }
            headerRight={
              <span className="flex items-center gap-1.5">
                {group.nodeId === localNodeId && <Badge variant="copper" dark={dark}>this node</Badge>}
                <Badge variant="muted" dark={dark}>{group.items.length}</Badge>
              </span>
            }
          >
            <div className="flex flex-col">
              {nodeScheduled.length > 0 && (
                <div className="px-4 pt-2">
                  <SectionLabel dark={dark}>Scheduled</SectionLabel>
                </div>
              )}
              {nodeScheduled.length > 0 && <ScheduledHeader dark={dark} />}
              {nodeScheduled.map((job, i) => (
                <div
                  key={job.id}
                  className={i > 0 ? `border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}` : ""}
                >
                  <ScheduledRow job={job} dark={dark} />
                </div>
              ))}
              {nodeTasks.length > 0 && (
                <div className={`px-4 pt-2 ${nodeScheduled.length > 0 ? "border-t " + c("border-ink-border-subtle", "border-light-border-subtle") : ""}`}>
                  <SectionLabel dark={dark}>Tasks</SectionLabel>
                </div>
              )}
              {nodeTasks.map((job, i) => (
                <div
                  key={job.id}
                  className={i > 0 ? `border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}` : ""}
                >
                  <JobRow job={job} dark={dark} />
                </div>
              ))}
            </div>
          </ExpandableCard>
        );
      })}
    </div>
  );
}

function JobRow({ job, dark }: Readonly<{ job: Job; dark: boolean }>) {
  const c = useThemeClass(dark);
  const now = useTick();
  return (
    <div className="flex items-center gap-3 px-4 py-2.5 text-[0.85em]">
      <span className={`font-mono font-medium truncate ${c("text-text-bright", "text-light-text-bright")}`}>
        {job.displayLabel}
      </span>
      <JobStatusBadge job={job} dark={dark} />
      {Number(job.chunksTotal) > 0 && (
        <span className={`font-mono text-[0.9em] shrink-0 ${c("text-text-muted", "text-light-text-muted")}`}>
          {Number(job.chunksDone)}/{Number(job.chunksTotal)} chunks
        </span>
      )}
      {job.startedAt && (
        <span className={`ml-auto font-mono text-[0.9em] shrink-0 ${c("text-text-muted", "text-light-text-muted")}`}>
          {elapsed(protoToInstant(job.startedAt), now)}
        </span>
      )}
    </div>
  );
}

const scheduledGrid = "grid grid-cols-[minmax(0,1fr)_8rem_7rem_7rem] gap-3";

function ScheduledHeader({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`${scheduledGrid} px-4 py-1.5 text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-muted", "text-light-text-muted")}`}
    >
      <span>Job</span>
      <span>Schedule</span>
      <span className="text-right">Last run</span>
      <span className="text-right">Next run</span>
    </div>
  );
}

function ScheduledRow({ job, dark }: Readonly<{ job: Job; dark: boolean }>) {
  const c = useThemeClass(dark);
  const now = useTick();
  return (
    <div className={`${scheduledGrid} px-4 py-2 text-[0.85em]`}>
      <span
        className={`font-mono truncate min-w-0 ${c("text-text-bright", "text-light-text-bright")}`}
        title={job.scheduleLabel}
      >
        {job.scheduleLabel}
      </span>
      <span
        className={`font-mono text-[0.9em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
        title={job.displaySchedule ? `Schedule: ${job.displaySchedule}` : undefined}
      >
        {job.displaySchedule}
      </span>
      <span
        className={`font-mono text-[0.9em] text-right ${c("text-text-muted", "text-light-text-muted")}`}
        title={job.lastRun ? formatTimestamp(protoToInstant(job.lastRun)) : ""}
      >
        {job.lastRun ? elapsed(protoToInstant(job.lastRun), now) : "\u2014"}
      </span>
      <span
        className={`font-mono text-[0.9em] text-right ${c("text-text-muted", "text-light-text-muted")}`}
        title={job.nextRun ? formatTimestamp(protoToInstant(job.nextRun)) : ""}
      >
        {job.nextRun ? countdown(protoToInstant(job.nextRun), now) : "\u2014"}
      </span>
    </div>
  );
}

function JobStatusBadge({ job, dark }: Readonly<{ job: Job; dark: boolean }>) {
  const label = job.statusLabel;
  if (!label) return null;
  return <Badge variant={job.statusVariant} dark={dark}>{label}</Badge>;
}

// ---- System ----

function SystemList({ dark }: Readonly<{ dark: boolean }>) {
  const { localNodeId, multiNode } = useNodeContext();
  const { data: cluster } = useClusterStatus();
  const registry = useNodeRegistry();
  const [expandedNodes, setExpandedNodes] = useState<Record<string, boolean>>({});

  // Single-node: show local stats directly.
  if (!multiNode) {
    const localStats = registry.byId.get(localNodeId)?.stats ?? null;
    return (
      <div className="flex flex-col gap-3">
        <EntityHeader title="System" helpTopicId="inspector-system" dark={dark} />
        <SystemStatsView nodeStats={localStats} dark={dark} />
      </div>
    );
  }

  // Multi-node: one ExpandableCard per node.
  const nodes = cluster?.nodes
    ? [...cluster.nodes].sort((a, b) => {
        if (idFromBytes(a.id) === localNodeId) return -1;
        if (idFromBytes(b.id) === localNodeId) return 1;
        return (a.name || "").localeCompare(b.name || "");
      })
    : [];

  return (
    <div className="flex flex-col gap-3">
      <EntityHeader title="System" helpTopicId="inspector-system" dark={dark} />
      {nodes.length > 0 && (
        <ExpandableCard
          id="cluster-summary"
          dark={dark}
          monoTitle={false}
          expanded={expandedNodes["__cluster"] ?? true}
          onToggle={() =>
            setExpandedNodes((prev) => ({ ...prev, __cluster: !(prev.__cluster ?? true) }))
          }
          headerRight={<Badge variant="copper" dark={dark}>{nodes.length} nodes</Badge>}
        >
          <div className="p-3">
            <ClusterSummaryView nodes={nodes} dark={dark} />
          </div>
        </ExpandableCard>
      )}
      {nodes.length === 0 && <Empty dark={dark}>No cluster data available.</Empty>}
      {nodes.map((node) => {
        const nid = idFromBytes(node.id);
        const isLocal = nid === localNodeId;
        return (
          <ExpandableCard
            key={nid}
            id={node.name || nid}
            dark={dark}
            monoTitle={false}
            expanded={expandedNodes[nid] ?? isLocal}
            onToggle={() =>
              setExpandedNodes((prev) => ({ ...prev, [nid]: !(prev[nid] ?? isLocal) }))
            }
            headerRight={
              <div className="flex items-center gap-1.5">
                <NodeStateBadge state={node.state} stateSince={node.stateSince} dark={dark} />
                {isLocal && <Badge variant="copper" dark={dark}>this node</Badge>}
              </div>
            }
          >
            <div className="p-3">
              <SystemStatsView nodeStats={node.stats ?? null} dark={dark} />
            </div>
          </ExpandableCard>
        );
      })}
    </div>
  );
}

// ---- Shared building blocks ----

function EntityHeader({
  title,
  helpTopicId,
  dark,
}: Readonly<{ title: string; helpTopicId: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  return (
    <div className="flex items-center gap-2 mb-2">
      <h2
        className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
      >
        {title}
      </h2>
      <HelpButton topicId={helpTopicId} />
    </div>
  );
}

function SectionLabel({ dark, children }: Readonly<{ dark: boolean; children: React.ReactNode }>) {
  const c = useThemeClass(dark);
  return (
    <div className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}>
      {children}
    </div>
  );
}

function Loading({ dark }: Readonly<{ dark: boolean }>) {
  return <LoadingPlaceholder dark={dark} />;
}

function Empty({ dark, children }: Readonly<{ dark: boolean; children: React.ReactNode }>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`flex items-center justify-center h-full text-[0.9em] ${c("text-text-muted", "text-light-text-muted")}`}
    >
      {children}
    </div>
  );
}
