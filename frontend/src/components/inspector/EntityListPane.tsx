import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useVaults, useIngesters } from "../../api/hooks";
import { useWatchJobs } from "../../api/hooks";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useSettings, useConfig } from "../../api/hooks/useConfig";
import { JobKind, JobStatus } from "../../api/gen/gastrolog/v1/job_pb";
import type { Job } from "../../api/gen/gastrolog/v1/job_pb";
import { toastError } from "../Toast";
import { Badge } from "../Badge";
import { ExpandableCard } from "../settings/ExpandableCard";
import { HelpButton } from "../HelpButton";
import { VaultCard } from "./VaultCard";
import { IngesterCard } from "./IngesterCard";
import { formatTimestamp, elapsed, countdown, useTick } from "./JobCard";
import { LocalSystemStats, SystemStatsView, ClusterSummaryView } from "./SystemStatsView";
import { groupByNode } from "./groupByNode";
import type { EntityType } from "./InspectorDialog";

interface EntityListPaneProps {
  entityType: EntityType;
  dark: boolean;
}

export function EntityListPane({ entityType, dark }: Readonly<EntityListPaneProps>) {
  switch (entityType) {
    case "vaults":
      return <VaultsList dark={dark} />;
    case "ingesters":
      return <IngestersList dark={dark} />;
    case "jobs":
      return <JobsList dark={dark} />;
    case "system":
      return <SystemList dark={dark} />;
  }
}

// ---- Node context helper ----

function useNodeContext() {
  const { data: settingsData } = useSettings();
  const { data: config } = useConfig();
  const { data: cluster } = useClusterStatus();
  const localNodeId = settingsData?.nodeId ?? "";
  const clusterEnabled = cluster?.clusterEnabled ?? false;
  const multiNode = clusterEnabled || (config?.nodeConfigs && config.nodeConfigs.length > 1);

  const nodeNames = new Map<string, string>();
  if (config?.nodeConfigs) {
    for (const nc of config.nodeConfigs) {
      if (nc.name) nodeNames.set(nc.id, nc.name);
    }
  }
  if (cluster?.nodes) {
    for (const n of cluster.nodes) {
      if (n.name) nodeNames.set(n.id, n.name);
    }
  }

  return { localNodeId, multiNode, nodeNames, cluster };
}

// ---- Vaults ----

function VaultsList({ dark }: Readonly<{ dark: boolean }>) {
  const { data: vaults, isLoading } = useVaults();
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  if (isLoading) return <Loading dark={dark} />;
  if (!vaults || vaults.length === 0) return <Empty dark={dark}>No vaults configured.</Empty>;

  const sorted = [...vaults].sort((a, b) => (a.name || a.id).localeCompare(b.name || b.id));

  const toggle = (id: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

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
        />
      ))}
    </div>
  );
}

// ---- Ingesters ----

function IngestersList({ dark }: Readonly<{ dark: boolean }>) {
  const { data: ingesters, isLoading } = useIngesters();
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  if (isLoading) return <Loading dark={dark} />;
  if (!ingesters || ingesters.length === 0) return <Empty dark={dark}>No ingesters configured.</Empty>;

  const sorted = [...ingesters].sort((a, b) => (a.name || a.id).localeCompare(b.name || b.id));

  const toggle = (id: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  return (
    <div className="flex flex-col gap-3">
      <EntityHeader title="Ingesters" helpTopicId="inspector-ingesters" dark={dark} />
      {sorted.map((ing) => (
        <IngesterCard
          key={ing.id}
          ingester={ing}
          dark={dark}
          expanded={expanded.has(ing.id)}
          onToggle={() => toggle(ing.id)}
        />
      ))}
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

  const tasks = jobs.filter((j) => j.kind === JobKind.TASK);
  const scheduled = jobs.filter((j) => j.kind === JobKind.SCHEDULED);

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
        const nodeTasks = group.items.filter((j) => j.kind === JobKind.TASK);
        const nodeScheduled = group.items.filter((j) => j.kind === JobKind.SCHEDULED);

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
                  className={`${i > 0 ? `border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}` : ""}`}
                >
                  <ScheduledRow job={job} dark={dark} />
                </div>
              ))}
              {nodeTasks.length > 0 && (
                <div className={`px-4 pt-2 ${nodeScheduled.length > 0 ? `border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}` : ""}`}>
                  <SectionLabel dark={dark}>Tasks</SectionLabel>
                </div>
              )}
              {nodeTasks.map((job, i) => (
                <div
                  key={job.id}
                  className={`${i > 0 ? `border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}` : ""}`}
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
        {job.description || job.name || job.id}
      </span>
      <JobStatusBadge status={job.status} dark={dark} />
      {Number(job.chunksTotal) > 0 && (
        <span className={`font-mono text-[0.9em] shrink-0 ${c("text-text-ghost", "text-light-text-ghost")}`}>
          {Number(job.chunksDone)}/{Number(job.chunksTotal)} chunks
        </span>
      )}
      {job.startedAt && (
        <span className={`ml-auto font-mono text-[0.9em] shrink-0 ${c("text-text-muted", "text-light-text-muted")}`}>
          {elapsed(job.startedAt.toDate(), now)}
        </span>
      )}
    </div>
  );
}

const scheduledGrid = "grid grid-cols-[1fr_8rem_7rem_7rem] gap-3";

function ScheduledHeader({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`${scheduledGrid} px-4 py-1.5 text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
    >
      <span>Description</span>
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
        className={`font-mono truncate ${c("text-text-bright", "text-light-text-bright")}`}
        title={job.description || job.name || job.id}
      >
        {job.description || job.name || job.id}
      </span>
      <span className={`font-mono text-[0.9em] ${c("text-text-muted", "text-light-text-muted")}`}>
        {job.schedule}
      </span>
      <span
        className={`font-mono text-[0.9em] text-right ${c("text-text-muted", "text-light-text-muted")}`}
        title={job.lastRun ? formatTimestamp(job.lastRun.toDate()) : ""}
      >
        {job.lastRun ? elapsed(job.lastRun.toDate(), now) : "\u2014"}
      </span>
      <span
        className={`font-mono text-[0.9em] text-right ${c("text-text-muted", "text-light-text-muted")}`}
        title={job.nextRun ? formatTimestamp(job.nextRun.toDate()) : ""}
      >
        {job.nextRun ? countdown(job.nextRun.toDate(), now) : "\u2014"}
      </span>
    </div>
  );
}

function JobStatusBadge({ status, dark }: Readonly<{ status: JobStatus; dark: boolean }>) {
  switch (status) {
    case JobStatus.PENDING:
      return <Badge variant="ghost" dark={dark}>pending</Badge>;
    case JobStatus.RUNNING:
      return <Badge variant="info" dark={dark}>running</Badge>;
    case JobStatus.COMPLETED:
      return <Badge variant="copper" dark={dark}>completed</Badge>;
    case JobStatus.FAILED:
      return <Badge variant="error" dark={dark}>failed</Badge>;
    default:
      return null;
  }
}

// ---- System ----

function SystemList({ dark }: Readonly<{ dark: boolean }>) {
  const { localNodeId, multiNode, cluster } = useNodeContext();
  const [expandedNodes, setExpandedNodes] = useState<Record<string, boolean>>({});

  // Single-node: show local stats directly.
  if (!multiNode) {
    return (
      <div className="flex flex-col gap-3">
        <EntityHeader title="System" helpTopicId="inspector-system" dark={dark} />
        <LocalSystemStats dark={dark} />
      </div>
    );
  }

  // Multi-node: one ExpandableCard per node.
  const nodes = cluster?.nodes
    ? [...cluster.nodes].sort((a, b) => {
        if (a.id === localNodeId) return -1;
        if (b.id === localNodeId) return 1;
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
        const isLocal = node.id === localNodeId;
        return (
          <ExpandableCard
            key={node.id}
            id={node.name || node.id}
            dark={dark}
            monoTitle={false}
            expanded={expandedNodes[node.id] ?? isLocal}
            onToggle={() =>
              setExpandedNodes((prev) => ({ ...prev, [node.id]: !(prev[node.id] ?? isLocal) }))
            }
            headerRight={
              isLocal ? <Badge variant="copper" dark={dark}>this node</Badge> : undefined
            }
          >
            <div className="p-3">
              {isLocal ? (
                <LocalSystemStats dark={dark} />
              ) : (
                <SystemStatsView nodeStats={node.stats ?? null} dark={dark} />
              )}
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
    <div className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-ghost", "text-light-text-ghost")}`}>
      {children}
    </div>
  );
}

function Loading({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  return (
    <div className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
      Loading...
    </div>
  );
}

function Empty({ dark, children }: Readonly<{ dark: boolean; children: React.ReactNode }>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`flex items-center justify-center h-full text-[0.9em] ${c("text-text-ghost", "text-light-text-ghost")}`}
    >
      {children}
    </div>
  );
}
