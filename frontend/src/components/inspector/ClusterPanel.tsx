import { useThemeClass } from "../../hooks/useThemeClass";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useSettings } from "../../api/hooks/useConfig";
import { ClusterNodeRole, ClusterNodeSuffrage } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import type { NodeStats } from "../../api/gen/gastrolog/v1/cluster_pb";
import { ExpandableCard } from "../settings/ExpandableCard";
import { useState } from "react";
import { formatBytes } from "../../utils/units";

function roleName(role: ClusterNodeRole): string {
  switch (role) {
    case ClusterNodeRole.LEADER:
      return "Leader";
    case ClusterNodeRole.FOLLOWER:
      return "Follower";
    default:
      return "Unknown";
  }
}

function suffrageName(suffrage: ClusterNodeSuffrage): string {
  switch (suffrage) {
    case ClusterNodeSuffrage.VOTER:
      return "Voter";
    case ClusterNodeSuffrage.NONVOTER:
      return "Nonvoter";
    case ClusterNodeSuffrage.STAGING:
      return "Staging";
    default:
      return "Unknown";
  }
}

function formatUptime(seconds: bigint): string {
  const s = Number(seconds);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) {
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    return m > 0 ? `${h}h ${m}m` : `${h}h`;
  }
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  return h > 0 ? `${d}d ${h}h` : `${d}d`;
}

function StatRow({
  label,
  value,
  mono = false,
  dark,
}: Readonly<{
  label: string;
  value: string | number;
  mono?: boolean;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  return (
    <div className="flex items-baseline justify-between gap-4">
      <span
        className={`text-[0.75em] font-medium uppercase tracking-wider shrink-0 ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        {label}
      </span>
      <span
        className={`text-[0.8em] text-right ${mono ? "font-mono" : ""} ${c("text-text-muted", "text-light-text-muted")}`}
      >
        {value}
      </span>
    </div>
  );
}

function SectionLabel({ label, dark }: Readonly<{ label: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  return (
    <span
      className={`text-[0.7em] font-medium uppercase tracking-wider ${c("text-text-ghost", "text-light-text-ghost")}`}
    >
      {label}
    </span>
  );
}

function Divider({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`border-t my-1 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
    />
  );
}

function NodeStatsSection({
  stats,
  dark,
}: Readonly<{ stats: NodeStats; dark: boolean }>) {
  const c = useThemeClass(dark);
  return (
    <>
      {/* System stats */}
      <Divider dark={dark} />
      <div className="flex flex-col gap-1.5">
        <SectionLabel label="System" dark={dark} />
        <div className="grid grid-cols-2 gap-x-6 gap-y-1">
          <StatRow
            label="CPU"
            value={`${stats.cpuPercent.toFixed(1)}%`}
            mono
            dark={dark}
          />
          <StatRow
            label="Goroutines"
            value={stats.goroutines.toLocaleString()}
            mono
            dark={dark}
          />
          <StatRow
            label="Mem In-Use"
            value={formatBytes(Number(stats.memoryInuse))}
            mono
            dark={dark}
          />
          <StatRow
            label="RSS"
            value={formatBytes(Number(stats.memoryRss))}
            mono
            dark={dark}
          />
          <StatRow
            label="Heap Alloc"
            value={formatBytes(Number(stats.memoryHeapAlloc))}
            mono
            dark={dark}
          />
          <StatRow
            label="Heap Idle"
            value={formatBytes(Number(stats.memoryHeapIdle))}
            mono
            dark={dark}
          />
          <StatRow
            label="Stack"
            value={formatBytes(Number(stats.memoryStackInuse))}
            mono
            dark={dark}
          />
          <StatRow
            label="GC Cycles"
            value={stats.numGc.toLocaleString()}
            mono
            dark={dark}
          />
        </div>
      </div>

      {/* Queue */}
      {stats.ingestQueueCapacity > 0 && (
        <>
          <Divider dark={dark} />
          <div className="flex flex-col gap-1.5">
            <SectionLabel label="Ingest Queue" dark={dark} />
            <div className="grid grid-cols-2 gap-x-6 gap-y-1">
              <StatRow
                label="Depth"
                value={`${stats.ingestQueueDepth} / ${stats.ingestQueueCapacity}`}
                mono
                dark={dark}
              />
              {stats.uptimeSeconds > 0n && (
                <StatRow
                  label="Uptime"
                  value={formatUptime(stats.uptimeSeconds)}
                  dark={dark}
                />
              )}
            </div>
          </div>
        </>
      )}

      {/* Raft stats */}
      {stats.raftState && (
        <>
          <Divider dark={dark} />
          <div className="flex flex-col gap-1.5">
            <SectionLabel label="Raft State" dark={dark} />
            <div className="grid grid-cols-2 gap-x-6 gap-y-1">
              <StatRow label="State" value={stats.raftState} dark={dark} />
              <StatRow
                label="Term"
                value={stats.raftTerm.toString()}
                mono
                dark={dark}
              />
              <StatRow
                label="Applied"
                value={stats.raftAppliedIndex.toString()}
                mono
                dark={dark}
              />
              <StatRow
                label="Commit"
                value={stats.raftCommitIndex.toString()}
                mono
                dark={dark}
              />
              <StatRow
                label="FSM Pending"
                value={stats.raftFsmPending.toString()}
                mono
                dark={dark}
              />
              <StatRow
                label="Last Contact"
                value={stats.raftLastContact || "never"}
                dark={dark}
              />
            </div>
          </div>
        </>
      )}

      {/* Ingesters */}
      {stats.ingesters.length > 0 && (
        <>
          <Divider dark={dark} />
          <div className="flex flex-col gap-1.5">
            <SectionLabel label="Ingesters" dark={dark} />
            {stats.ingesters.toSorted((a, b) => a.name.localeCompare(b.name)).map((ing) => (
              <div key={ing.id} className="flex flex-col gap-1 ml-1">
                <div className="flex items-center gap-2">
                  <span
                    className={`text-[0.75em] ${c("text-text-muted", "text-light-text-muted")}`}
                    title={ing.id}
                  >
                    {ing.name || ing.id}
                  </span>
                  <span
                    className={`px-1 py-0.5 text-[0.65em] font-medium uppercase tracking-wider rounded ${
                      ing.running
                        ? c("bg-emerald-900/40 text-emerald-400", "bg-emerald-100 text-emerald-700")
                        : c("bg-ink-hover text-text-ghost", "bg-light-hover text-light-text-ghost")
                    }`}
                  >
                    {ing.running ? "running" : "stopped"}
                  </span>
                </div>
                <div className="grid grid-cols-3 gap-x-4 gap-y-0.5">
                  <StatRow
                    label="Msgs"
                    value={Number(ing.messagesIngested).toLocaleString()}
                    mono
                    dark={dark}
                  />
                  <StatRow
                    label="Bytes"
                    value={formatBytes(Number(ing.bytesIngested))}
                    mono
                    dark={dark}
                  />
                  <StatRow
                    label="Errors"
                    value={Number(ing.errors).toLocaleString()}
                    mono
                    dark={dark}
                  />
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </>
  );
}

export function ClusterPanel({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useClusterStatus();
  const { data: settingsData } = useSettings();
  const localNodeId = settingsData?.nodeId ?? "";

  const [expandedCards, setExpandedCards] = useState<Record<string, boolean>>({});
  const toggle = (key: string) =>
    setExpandedCards((prev) => ({ ...prev, [key]: !prev[key] }));

  if (isLoading) {
    return (
      <div className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
        Loading...
      </div>
    );
  }

  if (!data?.clusterEnabled) {
    return (
      <div
        className={`border rounded-lg px-4 py-6 text-center ${c(
          "border-ink-border-subtle bg-ink-surface",
          "border-light-border-subtle bg-light-surface",
        )}`}
      >
        <p
          className={`text-[0.95em] font-medium mb-1 ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Single-node mode
        </p>
        <p
          className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          This instance is running as a standalone node. Start with{" "}
          <code className="font-mono text-copper">--cluster-addr</code> and{" "}
          <code className="font-mono text-copper">--cluster-init</code> to
          form a cluster.
        </p>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      {data.nodes.toSorted((a, b) => (a.name || "").localeCompare(b.name || "")).map((node) => {
        const isLocal = node.id === localNodeId;
        const isLeader = node.isLeader;
        const displayName = node.name || "Unnamed Node";

        return (
          <ExpandableCard
            key={node.id}
            id={displayName}
            dark={dark}
            expanded={expandedCards[node.id] ?? isLocal}
            onToggle={() => toggle(node.id)}
            monoTitle={false}
            typeBadge={roleName(node.role)}
            typeBadgeAccent={isLeader}
            status={
              isLocal ? (
                <span
                  className={`px-1.5 py-0.5 text-[0.7em] font-medium uppercase tracking-wider rounded ${c(
                    "bg-ink-hover text-text-muted",
                    "bg-light-hover text-light-text-muted",
                  )}`}
                >
                  this node
                </span>
              ) : undefined
            }
          >
            <div className="flex flex-col gap-3">
              <div className="flex items-baseline gap-2">
                <span
                  className={`text-[0.75em] font-medium uppercase tracking-wider ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  ID
                </span>
                <span
                  className={`text-[0.8em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
                  title={node.id}
                >
                  {node.id.length > 36
                    ? node.id.slice(0, 36) + "\u2026"
                    : node.id}
                </span>
              </div>

              <div className="flex items-baseline gap-2">
                <span
                  className={`text-[0.75em] font-medium uppercase tracking-wider ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Address
                </span>
                <span
                  className={`text-[0.8em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
                >
                  {node.address}
                </span>
              </div>

              <div className="flex items-baseline gap-2">
                <span
                  className={`text-[0.75em] font-medium uppercase tracking-wider ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Suffrage
                </span>
                <span
                  className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}
                >
                  {suffrageName(node.suffrage)}
                </span>
              </div>

              {node.stats ? (
                <NodeStatsSection stats={node.stats} dark={dark} />
              ) : (
                <div
                  className={`text-[0.8em] italic ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Waiting for data...
                </div>
              )}
            </div>
          </ExpandableCard>
        );
      })}
    </div>
  );
}
