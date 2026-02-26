import { useThemeClass } from "../../hooks/useThemeClass";
import {
  useHealth,
  useStats,
  useIngesters,
  useIngesterStatus,
} from "../../api/hooks";
import { Status } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import { formatBytes } from "../../utils/units";

export function MetricsPanel({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const health = useHealth();
  const stats = useStats();
  const ingesters = useIngesters();

  if (health.isLoading && stats.isLoading) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Loading...
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6">
      <h2
        className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
      >
        Metrics
      </h2>

      {health.data && <SystemSection dark={dark} health={health.data} stats={stats.data} />}
      {stats.data && <MemorySection dark={dark} stats={stats.data} />}
      {health.data && <IngestQueueSection dark={dark} health={health.data} />}
      {stats.data && <StorageSection dark={dark} stats={stats.data} />}
      {ingesters.data && ingesters.data.length > 0 && (
        <IngestionSection dark={dark} ingesters={ingesters.data} />
      )}
    </div>
  );
}

/* ---- Section header ---- */

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

/* ---- Stat row ---- */

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

/* ---- System ---- */

type HealthData = {
  status: Status;
  version: string;
  uptimeSeconds: bigint;
  ingestQueueDepth: bigint;
  ingestQueueCapacity: bigint;
};

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

function statusLabel(s: Status): { text: string; className: string } {
  switch (s) {
    case Status.HEALTHY:
      return {
        text: "Healthy",
        className:
          "bg-severity-info/15 text-severity-info",
      };
    case Status.DEGRADED:
      return {
        text: "Degraded",
        className:
          "bg-severity-warn/15 text-severity-warn",
      };
    case Status.UNHEALTHY:
      return {
        text: "Unhealthy",
        className:
          "bg-severity-error/15 text-severity-error",
      };
    default:
      return {
        text: "Unknown",
        className:
          "bg-severity-warn/15 text-severity-warn",
      };
  }
}

function SystemSection({
  dark,
  health,
  stats,
}: Readonly<{ dark: boolean; health: HealthData; stats?: StatsData }>) {
  const badge = statusLabel(health.status);
  return (
    <section>
      <SectionHeader dark={dark}>System</SectionHeader>
      <div className="flex flex-col gap-1.5">
        <StatRow
          dark={dark}
          label="Status"
          value={
            <span
              className={`px-1.5 py-0.5 text-[0.8em] font-medium rounded ${badge.className}`}
            >
              {badge.text}
            </span>
          }
        />
        <StatRow dark={dark} label="Version" value={health.version} />
        <StatRow
          dark={dark}
          label="Uptime"
          value={formatUptime(health.uptimeSeconds)}
        />
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

/* ---- Memory ---- */

function MemorySection({
  dark,
  stats,
}: Readonly<{ dark: boolean; stats: StatsData }>) {
  const mem = stats.processMemoryStats;
  return (
    <section>
      <SectionHeader dark={dark}>Memory</SectionHeader>
      <div className="flex flex-col gap-1.5">
        <StatRow
          dark={dark}
          label="RSS (peak)"
          value={mem ? formatBytes(Number(mem.rssBytes)) : "\u2014"}
        />
        <StatRow
          dark={dark}
          label="Heap alloc"
          value={mem ? formatBytes(Number(mem.heapAllocBytes)) : "\u2014"}
        />
        <StatRow
          dark={dark}
          label="Heap in-use"
          value={mem ? formatBytes(Number(mem.heapInuseBytes)) : "\u2014"}
        />
        <StatRow
          dark={dark}
          label="Heap idle"
          value={mem ? formatBytes(Number(mem.heapIdleBytes)) : "\u2014"}
        />
        <StatRow
          dark={dark}
          label="Heap released"
          value={mem ? formatBytes(Number(mem.heapReleasedBytes)) : "\u2014"}
        />
        <StatRow
          dark={dark}
          label="Stack in-use"
          value={mem ? formatBytes(Number(mem.stackInuseBytes)) : "\u2014"}
        />
        <StatRow
          dark={dark}
          label="Virtual (sys)"
          value={mem ? formatBytes(Number(mem.sysBytes)) : "\u2014"}
        />
        <StatRow
          dark={dark}
          label="Heap objects"
          value={mem ? Number(mem.heapObjects).toLocaleString() : "\u2014"}
        />
        <StatRow
          dark={dark}
          label="GC cycles"
          value={mem ? mem.numGc.toLocaleString() : "\u2014"}
        />
      </div>
    </section>
  );
}

/* ---- Ingest Queue ---- */

function IngestQueueSection({
  dark,
  health,
}: Readonly<{ dark: boolean; health: HealthData }>) {
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
          <span
            className={`w-40 text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Fill
          </span>
          <div className="flex-1 flex items-center gap-2">
            <div
              className={`h-2 flex-1 rounded-full overflow-hidden ${c("bg-ink-hover", "bg-light-hover")}`}
            >
              <div
                className={`h-full rounded-full transition-all ${barColor}`}
                style={{ width: `${Math.min(pct, 100)}%` }}
              />
            </div>
            <span
              className={`font-mono text-[0.8em] w-10 text-right ${c("text-text-muted", "text-light-text-muted")}`}
            >
              {pct.toFixed(0)}%
            </span>
          </div>
        </div>
      </div>
    </section>
  );
}

/* ---- Storage ---- */

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
  totalVaults: bigint;
  totalChunks: bigint;
  sealedChunks: bigint;
  totalRecords: bigint;
  totalBytes: bigint;
  processCpuPercent: number;
  processMemoryBytes: bigint;
  processMemoryStats?: MemoryStatsData;
  oldestRecord?: { toDate(): Date };
  newestRecord?: { toDate(): Date };
  vaultStats: Array<{
    id: string;
    type: string;
    chunkCount: bigint;
    sealedChunks: bigint;
    recordCount: bigint;
    dataBytes: bigint;
    indexBytes: bigint;
  }>;
};

function StorageSection({
  dark,
  stats,
}: Readonly<{ dark: boolean; stats: StatsData }>) {
  const c = useThemeClass(dark);
  const oldest = stats.oldestRecord?.toDate();
  const newest = stats.newestRecord?.toDate();

  return (
    <section>
      <SectionHeader dark={dark}>Storage</SectionHeader>
      <div className="flex flex-col gap-1.5">
        <StatRow
          dark={dark}
          label="Records"
          value={Number(stats.totalRecords).toLocaleString()}
        />
        <StatRow
          dark={dark}
          label="Size"
          value={formatBytes(Number(stats.totalBytes))}
        />
        <StatRow
          dark={dark}
          label="Chunks"
          value={`${Number(stats.sealedChunks).toLocaleString()} sealed / ${Number(stats.totalChunks).toLocaleString()} total`}
        />
        {oldest && newest && (
          <StatRow
            dark={dark}
            label="Time span"
            value={`${oldest.toLocaleDateString()} \u2013 ${newest.toLocaleDateString()}`}
          />
        )}
      </div>

      {stats.vaultStats.length > 1 && (
        <div className="mt-3 overflow-x-auto">
          <table className="w-full text-[0.8em]">
            <thead>
              <tr
                className={`text-left ${c("text-text-ghost", "text-light-text-ghost")}`}
              >
                <th className="pb-1 pr-3 font-medium">Vault</th>
                <th className="pb-1 pr-3 font-medium">Type</th>
                <th className="pb-1 pr-3 font-medium text-right">Records</th>
                <th className="pb-1 pr-3 font-medium text-right">Data</th>
                <th className="pb-1 font-medium text-right">Chunks</th>
              </tr>
            </thead>
            <tbody>
              {stats.vaultStats.map((ss) => (
                <tr
                  key={ss.id}
                  className={c("text-text-muted", "text-light-text-muted")}
                >
                  <td className="py-0.5 pr-3 font-mono truncate max-w-[12rem]">
                    {ss.id}
                  </td>
                  <td className="py-0.5 pr-3">
                    <span className="px-1 py-0.5 text-[0.75em] rounded bg-copper/10 text-copper">
                      {ss.type}
                    </span>
                  </td>
                  <td className="py-0.5 pr-3 font-mono text-right">
                    {Number(ss.recordCount).toLocaleString()}
                  </td>
                  <td className="py-0.5 pr-3 font-mono text-right">
                    {formatBytes(Number(ss.dataBytes))}
                  </td>
                  <td className="py-0.5 font-mono text-right">
                    {Number(ss.sealedChunks).toLocaleString()}/
                    {Number(ss.chunkCount).toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

/* ---- Ingestion ---- */

type IngesterEntry = {
  id: string;
  name: string;
  type: string;
  running: boolean;
};

function IngestionSection({
  dark,
  ingesters,
}: Readonly<{ dark: boolean; ingesters: IngesterEntry[] }>) {
  return (
    <section>
      <SectionHeader dark={dark}>Ingestion</SectionHeader>
      <div className="flex flex-col gap-2">
        {ingesters.map((ing) => (
          <IngesterRow key={ing.id} dark={dark} ingester={ing} />
        ))}
      </div>
    </section>
  );
}

function IngesterRow({
  dark,
  ingester,
}: Readonly<{ dark: boolean; ingester: IngesterEntry }>) {
  const c = useThemeClass(dark);
  const { data } = useIngesterStatus(ingester.id);

  return (
    <div
      className={`rounded px-3 py-2 ${c("bg-ink-raised", "bg-light-bg")}`}
    >
      <div className="flex items-center gap-2 mb-1.5">
        <span
          className={`text-[0.85em] font-medium ${c("text-text-bright", "text-light-text-bright")}`}
        >
          {ingester.name || ingester.id}
        </span>
        <span className="px-1 py-0.5 text-[0.7em] rounded bg-copper/10 text-copper">
          {ingester.type}
        </span>
        {ingester.running ? (
          <span className="px-1 py-0.5 text-[0.7em] rounded bg-severity-info/15 text-severity-info">
            running
          </span>
        ) : (
          <span
            className={`px-1 py-0.5 text-[0.7em] rounded ${c("bg-ink-hover text-text-ghost", "bg-light-hover text-light-text-ghost")}`}
          >
            stopped
          </span>
        )}
      </div>
      {data && (
        <div className="flex gap-4 text-[0.8em]">
          <span className={c("text-text-muted", "text-light-text-muted")}>
            <span className="font-mono">
              {Number(data.messagesIngested).toLocaleString()}
            </span>{" "}
            msgs
          </span>
          <span className={c("text-text-muted", "text-light-text-muted")}>
            <span className="font-mono">
              {formatBytes(Number(data.bytesIngested))}
            </span>
          </span>
          {Number(data.errors) > 0 && (
            <span className="text-severity-error">
              <span className="font-mono">
                {Number(data.errors).toLocaleString()}
              </span>{" "}
              errors
            </span>
          )}
        </div>
      )}
    </div>
  );
}
