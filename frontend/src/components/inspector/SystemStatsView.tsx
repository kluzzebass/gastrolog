import { useThemeClass } from "../../hooks/useThemeClass";
import { useHealth, useStats } from "../../api/hooks";
import { Status } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import type { ClusterNode } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import type { NodeStats } from "../../api/gen/gastrolog/v1/cluster_pb";
import { formatBytes } from "../../utils/units";
import { Badge } from "../Badge";
import type { BadgeVariant } from "../Badge";

/**
 * Unified system stats view.
 *
 * - When `localHealth`/`localStats` are provided (local node), shows the full
 *   rich breakdown: status, version, uptime, CPU, memory, and ingest queue.
 * - When only `nodeStats` is provided (remote node via cluster gossip), shows
 *   a compact grid: system, queue, and raft state.
 *
 * Per-vault and per-ingester details are shown in their own entity panes.
 */
interface SystemStatsViewProps {
  nodeStats: NodeStats | null;
  localHealth?: ReturnType<typeof useHealth>["data"];
  localStats?: ReturnType<typeof useStats>["data"];
  dark: boolean;
}

export function SystemStatsView({
  nodeStats,
  localHealth,
  localStats,
  dark,
}: Readonly<SystemStatsViewProps>) {
  const c = useThemeClass(dark);

  if (localHealth || localStats) {
    return <RichView health={localHealth} stats={localStats} dark={dark} />;
  }
  if (nodeStats) {
    return <CompactView stats={nodeStats} dark={dark} />;
  }
  return (
    <div className={`text-[0.85em] italic ${c("text-text-ghost", "text-light-text-ghost")}`}>
      Waiting for data...
    </div>
  );
}

/**
 * Self-contained local node system stats. Calls useHealth/useStats internally
 * so the parent doesn't need to pass data.
 */
export function LocalSystemStats({ dark }: Readonly<{ dark: boolean }>) {
  const health = useHealth();
  const stats = useStats();

  const c = useThemeClass(dark);

  if (health.isLoading && stats.isLoading) {
    return (
      <div className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
        Loading...
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6">
      {health.data && <SystemSection dark={dark} health={health.data} stats={stats.data} />}
      {stats.data && <MemorySection dark={dark} stats={stats.data} />}
      {health.data && <IngestQueueSection dark={dark} health={health.data} />}
    </div>
  );
}

// ---- Shared building blocks ----

function SectionHeader({
  dark,
  children,
}: Readonly<{ dark: boolean; children: React.ReactNode }>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
    >
      {children}
    </div>
  );
}

function StatRow({
  dark,
  label,
  value,
  isError,
}: Readonly<{
  dark: boolean;
  label: string;
  value: React.ReactNode;
  isError?: boolean;
}>) {
  const c = useThemeClass(dark);
  return (
    <div className="flex items-center gap-3 text-[0.85em]">
      <span className={`w-40 ${c("text-text-muted", "text-light-text-muted")}`}>
        {label}
      </span>
      <span
        className={`font-mono ${
          isError
            ? "text-severity-error"
            : c("text-text-bright", "text-light-text-bright")
        }`}
      >
        {value}
      </span>
    </div>
  );
}

// ---- Rich view (local node) ----

function RichView({
  health,
  stats,
  dark,
}: Readonly<{
  health?: { status: Status; version: string; uptimeSeconds: bigint; ingestQueueDepth: bigint; ingestQueueCapacity: bigint };
  stats?: StatsData;
  dark: boolean;
}>) {
  return (
    <div className="flex flex-col gap-6">
      {health && <SystemSection dark={dark} health={health} stats={stats} />}
      {stats && <MemorySection dark={dark} stats={stats} />}
      {health && <IngestQueueSection dark={dark} health={health} />}
    </div>
  );
}

// ---- Compact view (remote node via cluster stats) ----

function CompactView({
  stats,
  dark,
}: Readonly<{ stats: NodeStats; dark: boolean }>) {
  return (
    <div className="flex flex-col gap-4">
      {/* System stats */}
      <section>
        <CompactSectionLabel label="System" dark={dark} />
        <div className="grid grid-cols-2 gap-x-6 gap-y-1">
          <CompactStatRow label="CPU" value={`${stats.cpuPercent.toFixed(1)}%`} mono dark={dark} />
          <CompactStatRow label="Goroutines" value={stats.goroutines.toLocaleString()} mono dark={dark} />
          <CompactStatRow label="Mem In-Use" value={formatBytes(Number(stats.memoryInuse))} mono dark={dark} />
          <CompactStatRow label="RSS" value={formatBytes(Number(stats.memoryRss))} mono dark={dark} />
          <CompactStatRow label="Heap Alloc" value={formatBytes(Number(stats.memoryHeapAlloc))} mono dark={dark} />
          <CompactStatRow label="Heap Idle" value={formatBytes(Number(stats.memoryHeapIdle))} mono dark={dark} />
          <CompactStatRow label="Stack" value={formatBytes(Number(stats.memoryStackInuse))} mono dark={dark} />
          <CompactStatRow label="GC Cycles" value={stats.numGc.toLocaleString()} mono dark={dark} />
        </div>
      </section>

      {/* Queue */}
      {stats.ingestQueueCapacity > 0 && (
        <section>
          <CompactDivider dark={dark} />
          <CompactSectionLabel label="Ingest Queue" dark={dark} />
          <div className="grid grid-cols-2 gap-x-6 gap-y-1">
            <CompactStatRow
              label="Depth"
              value={`${stats.ingestQueueDepth} / ${stats.ingestQueueCapacity}`}
              mono
              dark={dark}
            />
            {stats.uptimeSeconds > BigInt(0) && (
              <CompactStatRow
                label="Uptime"
                value={formatUptime(stats.uptimeSeconds)}
                dark={dark}
              />
            )}
          </div>
        </section>
      )}

      {/* Raft stats */}
      {stats.raftState && (
        <section>
          <CompactDivider dark={dark} />
          <CompactSectionLabel label="Raft State" dark={dark} />
          <div className="grid grid-cols-2 gap-x-6 gap-y-1">
            <CompactStatRow label="State" value={stats.raftState} dark={dark} />
            <CompactStatRow label="Term" value={stats.raftTerm.toString()} mono dark={dark} />
            <CompactStatRow label="Applied" value={stats.raftAppliedIndex.toString()} mono dark={dark} />
            <CompactStatRow label="Commit" value={stats.raftCommitIndex.toString()} mono dark={dark} />
            <CompactStatRow label="FSM Pending" value={stats.raftFsmPending.toString()} mono dark={dark} />
            <CompactStatRow label="Last Contact" value={stats.raftLastContact || "never"} dark={dark} />
          </div>
        </section>
      )}

    </div>
  );
}

// ---- Compact view building blocks (from old ClusterPanel) ----

function CompactStatRow({
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

function CompactSectionLabel({ label, dark }: Readonly<{ label: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  return (
    <span
      className={`text-[0.7em] font-medium uppercase tracking-wider mb-1.5 block ${c("text-text-ghost", "text-light-text-ghost")}`}
    >
      {label}
    </span>
  );
}

function CompactDivider({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`border-t my-1 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
    />
  );
}

// ---- Rich view sections (from old MetricsPanel) ----

function formatUptime(seconds: bigint): string {
  const s = Number(seconds);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (h < 24) return m > 0 ? `${h}h ${m}m` : `${h}h`;
  const d = Math.floor(h / 24);
  const rh = h % 24;
  return rh > 0 ? `${d}d ${rh}h` : `${d}d`;
}

function statusLabel(s: Status): { text: string; variant: BadgeVariant } {
  switch (s) {
    case Status.HEALTHY:
      return { text: "healthy", variant: "info" };
    case Status.DEGRADED:
      return { text: "degraded", variant: "warn" };
    case Status.UNHEALTHY:
      return { text: "unhealthy", variant: "error" };
    default:
      return { text: "unknown", variant: "warn" };
  }
}

function SystemSection({
  dark,
  health,
  stats,
}: Readonly<{ dark: boolean; health: { status: Status; version: string; uptimeSeconds: bigint }; stats?: StatsData }>) {
  const badge = statusLabel(health.status);
  return (
    <section>
      <SectionHeader dark={dark}>System</SectionHeader>
      <div className="flex flex-col gap-1.5">
        <StatRow
          dark={dark}
          label="Status"
          value={<Badge variant={badge.variant} dark={dark}>{badge.text}</Badge>}
        />
        <StatRow dark={dark} label="Version" value={health.version} />
        <StatRow dark={dark} label="Uptime" value={formatUptime(health.uptimeSeconds)} />
        {stats && (
          <StatRow
            dark={dark}
            label="CPU"
            value={`${stats.processCpuPercent.toFixed(1)}%`}
          />
        )}
      </div>
    </section>
  );
}

type MemoryStatsData = {
  rssBytes: bigint;
  heapAllocBytes: bigint;
  heapInuseBytes: bigint;
  heapIdleBytes: bigint;
  heapReleasedBytes: bigint;
  stackInuseBytes: bigint;
  sysBytes: bigint;
  heapObjects: bigint;
  numGc: number;
};

type StatsData = {
  processCpuPercent: number;
  processMemoryBytes: bigint;
  processMemoryStats?: MemoryStatsData;
};

function MemorySection({
  dark,
  stats,
}: Readonly<{ dark: boolean; stats: StatsData }>) {
  const mem = stats.processMemoryStats;
  return (
    <section>
      <SectionHeader dark={dark}>Memory</SectionHeader>
      <div className="flex flex-col gap-1.5">
        <StatRow dark={dark} label="RSS (peak)" value={mem ? formatBytes(Number(mem.rssBytes)) : "\u2014"} />
        <StatRow dark={dark} label="Heap alloc" value={mem ? formatBytes(Number(mem.heapAllocBytes)) : "\u2014"} />
        <StatRow dark={dark} label="Heap in-use" value={mem ? formatBytes(Number(mem.heapInuseBytes)) : "\u2014"} />
        <StatRow dark={dark} label="Heap idle" value={mem ? formatBytes(Number(mem.heapIdleBytes)) : "\u2014"} />
        <StatRow dark={dark} label="Heap released" value={mem ? formatBytes(Number(mem.heapReleasedBytes)) : "\u2014"} />
        <StatRow dark={dark} label="Stack in-use" value={mem ? formatBytes(Number(mem.stackInuseBytes)) : "\u2014"} />
        <StatRow dark={dark} label="Virtual (sys)" value={mem ? formatBytes(Number(mem.sysBytes)) : "\u2014"} />
        <StatRow dark={dark} label="Heap objects" value={mem ? Number(mem.heapObjects).toLocaleString() : "\u2014"} />
        <StatRow dark={dark} label="GC cycles" value={mem ? mem.numGc.toLocaleString() : "\u2014"} />
      </div>
    </section>
  );
}

function IngestQueueSection({
  dark,
  health,
}: Readonly<{ dark: boolean; health: { ingestQueueDepth: bigint; ingestQueueCapacity: bigint } }>) {
  const c = useThemeClass(dark);
  const depth = Number(health.ingestQueueDepth);
  const capacity = Number(health.ingestQueueCapacity);

  if (capacity === 0) return null;

  const pct = (depth / capacity) * 100;
  let barColor: string;
  if (pct >= 90) barColor = "bg-severity-error";
  else if (pct >= 75) barColor = "bg-severity-warn";
  else barColor = "bg-copper";

  return (
    <section>
      <SectionHeader dark={dark}>Ingest Queue</SectionHeader>
      <div className="flex flex-col gap-2">
        <StatRow
          dark={dark}
          label="Depth"
          value={`${depth.toLocaleString()} / ${capacity.toLocaleString()}`}
        />
        <div className="flex items-center gap-3">
          <span className={`w-40 text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}>
            Fill
          </span>
          <div className="flex-1 flex items-center gap-2">
            <div className={`h-2 flex-1 rounded-full overflow-hidden ${c("bg-ink-hover", "bg-light-hover")}`}>
              <div
                className={`h-full rounded-full transition-all ${barColor}`}
                style={{ width: `${Math.min(pct, 100)}%` }}
              />
            </div>
            <span className={`font-mono text-[0.8em] w-10 text-right ${c("text-text-muted", "text-light-text-muted")}`}>
              {pct.toFixed(0)}%
            </span>
          </div>
        </div>
      </div>
    </section>
  );
}

// ---- Cluster aggregate view ----

/**
 * Aggregated cluster-wide summary. Sums stats across all nodes using
 * gossip-broadcast NodeStats — no extra RPCs needed.
 */
export function ClusterSummaryView({
  nodes,
  dark,
}: Readonly<{ nodes: ClusterNode[]; dark: boolean }>) {
  let totalVaults = 0;
  let totalRecords = 0;
  let totalBytes = 0;
  let totalChunks = 0;
  let totalCpu = 0;
  let totalRss = 0;
  let totalHeapAlloc = 0;
  let totalGoroutines = 0;
  let totalQueueDepth = 0;
  let totalQueueCapacity = 0;
  let leaderName = "";

  for (const node of nodes) {
    if (node.isLeader) leaderName = node.name || node.id;
    const s = node.stats;
    if (!s) continue;
    totalCpu += s.cpuPercent;
    totalRss += Number(s.memoryRss);
    totalHeapAlloc += Number(s.memoryHeapAlloc);
    totalGoroutines += s.goroutines;
    totalQueueDepth += s.ingestQueueDepth;
    totalQueueCapacity += s.ingestQueueCapacity;
    for (const v of s.vaults) {
      totalVaults++;
      totalRecords += Number(v.recordCount);
      totalBytes += Number(v.dataBytes);
      totalChunks += Number(v.chunkCount);
    }
  }

  return (
    <div className="flex flex-col gap-4">
      {/* Cluster overview */}
      <section>
        <CompactSectionLabel label="Cluster" dark={dark} />
        <div className="grid grid-cols-2 gap-x-6 gap-y-1">
          <CompactStatRow label="Nodes" value={nodes.length.toString()} mono dark={dark} />
          <CompactStatRow label="Leader" value={leaderName || "none"} dark={dark} />
          <CompactStatRow label="Vaults" value={totalVaults.toLocaleString()} mono dark={dark} />
          <CompactStatRow label="Records" value={totalRecords.toLocaleString()} mono dark={dark} />
          <CompactStatRow label="Data" value={formatBytes(totalBytes)} mono dark={dark} />
          <CompactStatRow label="Chunks" value={totalChunks.toLocaleString()} mono dark={dark} />
        </div>
      </section>

      {/* Aggregate resources */}
      <CompactDivider dark={dark} />
      <section>
        <CompactSectionLabel label="Combined Resources" dark={dark} />
        <div className="grid grid-cols-2 gap-x-6 gap-y-1">
          <CompactStatRow label="CPU" value={`${totalCpu.toFixed(1)}%`} mono dark={dark} />
          <CompactStatRow label="Goroutines" value={totalGoroutines.toLocaleString()} mono dark={dark} />
          <CompactStatRow label="RSS" value={formatBytes(totalRss)} mono dark={dark} />
          <CompactStatRow label="Heap Alloc" value={formatBytes(totalHeapAlloc)} mono dark={dark} />
        </div>
      </section>

      {/* Aggregate ingest queue */}
      {totalQueueCapacity > 0 && (
        <>
          <CompactDivider dark={dark} />
          <section>
            <CompactSectionLabel label="Ingest Queue (all nodes)" dark={dark} />
            <div className="grid grid-cols-2 gap-x-6 gap-y-1">
              <CompactStatRow
                label="Depth"
                value={`${totalQueueDepth.toLocaleString()} / ${totalQueueCapacity.toLocaleString()}`}
                mono
                dark={dark}
              />
            </div>
          </section>
        </>
      )}
    </div>
  );
}


