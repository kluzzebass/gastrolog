import { useThemeClass } from "../../hooks/useThemeClass";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useSettings } from "../../api/hooks/useConfig";
import { ClusterNodeRole, ClusterNodeSuffrage, type RaftStats } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import { ExpandableCard } from "../settings/ExpandableCard";
import { useState } from "react";

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

function RaftStatsGrid({
  stats,
  dark,
}: Readonly<{ stats: RaftStats; dark: boolean }>) {
  const c = useThemeClass(dark);
  return (
    <>
      <div
        className={`border-t my-1 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
      />
      <div className="flex flex-col gap-1.5">
        <span
          className={`text-[0.7em] font-medium uppercase tracking-wider ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Raft State
        </span>
        <div className="grid grid-cols-2 gap-x-6 gap-y-1">
          <StatRow label="Term" value={stats.term.toString()} mono dark={dark} />
          <StatRow label="State" value={stats.state || "Unknown"} dark={dark} />
          <StatRow label="Applied" value={stats.appliedIndex.toString()} mono dark={dark} />
          <StatRow label="Commit" value={stats.commitIndex.toString()} mono dark={dark} />
          <StatRow label="Last Log" value={stats.lastLogIndex.toString()} mono dark={dark} />
          <StatRow label="FSM Pending" value={stats.fsmPending.toString()} mono dark={dark} />
          <StatRow label="Last Contact" value={stats.lastContact || "never"} dark={dark} />
          <StatRow label="Peers" value={stats.numPeers} dark={dark} />
          <StatRow label="Snapshot" value={stats.lastSnapshotIndex.toString()} mono dark={dark} />
          <StatRow label="Protocol" value={`v${stats.protocolVersion}`} dark={dark} />
        </div>
      </div>
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
      {data.nodes.map((node) => {
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

              {isLocal && data.localStats && (
                <RaftStatsGrid stats={data.localStats} dark={dark} />
              )}
            </div>
          </ExpandableCard>
        );
      })}
    </div>
  );
}
