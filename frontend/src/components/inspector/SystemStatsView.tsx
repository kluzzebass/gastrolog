import { encode } from "../../api/glid";
import { useThemeClass } from "../../hooks/useThemeClass";
// eslint-disable-next-line no-restricted-imports -- ClusterSummaryView still takes raw ClusterNode for the multi-node cluster summary; deferred migration
import type { ClusterNode } from "../../api/gen/gastrolog/v1/lifecycle_pb";
// eslint-disable-next-line no-restricted-imports -- NodeStats is a passthrough type from Node.stats; no model wrap planned
import type { NodeStats } from "../../api/gen/gastrolog/v1/cluster_pb";
import { formatBytes } from "../../utils/units";

/**
 * System stats view for a single node, using gossip-broadcast NodeStats.
 * Used identically for local and remote nodes — no special-casing.
 */
interface SystemStatsViewProps {
  nodeStats: NodeStats | null;
  dark: boolean;
}

export function SystemStatsView({
  nodeStats,
  dark,
}: Readonly<SystemStatsViewProps>) {
  const c = useThemeClass(dark);

  if (nodeStats) {
    return <CompactView stats={nodeStats} dark={dark} />;
  }
  return (
    <div className={`text-[0.85em] italic ${c("text-text-muted", "text-light-text-muted")}`}>
      Waiting for node stats...
    </div>
  );
}

// ---- Compact view (node stats from gossip) ----

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

      {/* Addresses */}
      {(stats.apiAddress || stats.pprofAddress) && (
        <section>
          <CompactDivider dark={dark} />
          <CompactSectionLabel label="Addresses" dark={dark} />
          <div className="grid grid-cols-2 gap-x-6 gap-y-1">
            {stats.apiAddress && (
              <CompactStatRow label="API" value={stats.apiAddress} mono dark={dark} />
            )}
            {stats.pprofAddress && (
              <CompactStatRow label="pprof" value={stats.pprofAddress} mono dark={dark} />
            )}
          </div>
        </section>
      )}

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

      {/* Pipeline throughput: rolling-window rates from the stats broadcast
          (gastrolog-4eh5ns). Section appears once the node has routing
          activity or a local segmentation writer. */}
      {(Number(stats.routeStatsIngested) > 0 || stats.vaults.some((v) => v.appendQueueCapacity > 0)) && (
        <section>
          <CompactDivider dark={dark} />
          <CompactSectionLabel label="Throughput" dark={dark} />
          <div className="grid grid-cols-2 gap-x-6 gap-y-1">
            <CompactStatRow label="Ingested" value={`${formatRate(stats.routeIngestedPerSec)}/s`} mono dark={dark} />
            <CompactStatRow label="Routed" value={`${formatRate(stats.routeRoutedPerSec)}/s`} mono dark={dark} />
          </div>
          {stats.vaults.filter((v) => v.appendQueueCapacity > 0).map((v) => (
            <div key={encode(v.id)} className="grid grid-cols-2 gap-x-6 gap-y-1 mt-1">
              <CompactStatRow
                label={v.name || encode(v.id).slice(0, 8)}
                value={`${formatRate(v.appendRecordsPerSec)}/s · ${formatBytes(v.appendBytesPerSec)}/s`}
                mono
                dark={dark}
              />
              <CompactStatRow
                label="Queue"
                value={`${v.appendQueueDepth} / ${v.appendQueueCapacity}`}
                mono
                dark={dark}
              />
            </div>
          ))}
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

// ---- Compact view building blocks ----

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
        className={`text-[0.75em] font-medium uppercase tracking-wider shrink-0 ${c("text-text-muted", "text-light-text-muted")}`}
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
      className={`text-[0.7em] font-medium uppercase tracking-wider mb-1.5 block ${c("text-text-muted", "text-light-text-muted")}`}
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
  let totalIngestRate = 0;
  let totalRouteRate = 0;
  let totalAppendRate = 0;
  let totalAppendBytesRate = 0;
  let leaderName = "";

  for (const node of nodes) {
    if (node.isLeader) leaderName = node.name || encode(node.id);
    const s = node.stats;
    if (!s) continue;
    totalCpu += s.cpuPercent;
    totalRss += Number(s.memoryRss);
    totalHeapAlloc += Number(s.memoryHeapAlloc);
    totalGoroutines += s.goroutines;
    totalQueueDepth += s.ingestQueueDepth;
    totalQueueCapacity += s.ingestQueueCapacity;
    totalIngestRate += s.routeIngestedPerSec;
    totalRouteRate += s.routeRoutedPerSec;
    for (const v of s.vaults) {
      totalVaults++;
      totalRecords += Number(v.recordCount);
      totalBytes += Number(v.dataBytes);
      totalChunks += Number(v.chunkCount);
      totalAppendRate += v.appendRecordsPerSec;
      totalAppendBytesRate += v.appendBytesPerSec;
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

      {/* Cluster throughput: summed rolling-window rates (gastrolog-4eh5ns) */}
      <CompactDivider dark={dark} />
      <section>
        <CompactSectionLabel label="Throughput" dark={dark} />
        <div className="grid grid-cols-2 gap-x-6 gap-y-1">
          <CompactStatRow label="Ingested" value={`${formatRate(totalIngestRate)}/s`} mono dark={dark} />
          <CompactStatRow label="Routed" value={`${formatRate(totalRouteRate)}/s`} mono dark={dark} />
          <CompactStatRow label="Appended" value={`${formatRate(totalAppendRate)}/s`} mono dark={dark} />
          <CompactStatRow label="Append data" value={`${formatBytes(totalAppendBytesRate)}/s`} mono dark={dark} />
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

function formatRate(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  if (n >= 10) return Math.round(n).toString();
  return n.toFixed(1);
}
