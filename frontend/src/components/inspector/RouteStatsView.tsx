import { useThemeClass } from "../../hooks/useThemeClass";
import { Spark } from "../Spark";
// eslint-disable-next-line no-restricted-imports -- ThroughputRate is a passthrough stats type; no model wrap planned
import type { ThroughputRate } from "../../api/gen/gastrolog/v1/vault_pb";
import { useRouteStats } from "../../api/hooks/useRouteStats";
import { useRoutes, useVaults } from "../../api/hooks";
import { idFromBytes, type EntityID } from "../../api/model/id";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
interface RouteStatsViewProps {
  dark: boolean;
}

export function RouteStatsView({ dark }: Readonly<RouteStatsViewProps>) {
  const c = useThemeClass(dark);
  const { data: stats, isLoading } = useRouteStats();
  const routes = useRoutes();
  const { data: vaults } = useVaults();

  if (isLoading) return <LoadingPlaceholder dark={dark} />;
  if (!stats) return null;

  const vaultLabelById = new Map<EntityID, string>();
  for (const v of vaults) {
    vaultLabelById.set(v.id, v.displayLabel);
  }

  const routeLabelById = new Map<EntityID, string>();
  for (const r of routes) {
    routeLabelById.set(r.id, r.displayLabel);
  }

  const dropRate =
    stats.totalIngested > 0
      ? ((Number(stats.totalDropped) / Number(stats.totalIngested)) * 100).toFixed(1)
      : "0.0";

  const sorted = [...stats.vaultStats].sort(
    (a, b) => Number(b.recordsMatched) - Number(a.recordsMatched),
  );

  // Per-route view comes from the Route models (config joined with stats),
  // so even unmatched routes show up as zero rows when stats lag.
  const sortedRoutes = [...routes].sort(
    (a, b) => Number(b.recordsMatched) - Number(a.recordsMatched),
  );

  return (
    <div className="flex flex-col gap-5">
      {/* Global summary */}
      <div
        className={`rounded-lg border p-4 ${c("border-ink-border bg-ink-well", "border-light-border bg-light-well")}`}
      >
        {!stats.filterSetActive && (
          <div
            className={`mb-3 px-3 py-2 rounded text-[0.85em] font-medium ${c("bg-severity-error/15 text-severity-error border border-severity-error/30", "bg-severity-error/10 text-severity-error border border-severity-error/20")}`}
          >
            Filter set is inactive — no routes compiled. All ingested records are
            being dropped silently.
          </div>
        )}

        <div className="grid grid-cols-4 gap-4">
          <StatBox
            label="Ingested"
            value={formatCount(stats.totalIngested)}
            dark={dark}
            title="Records that entered the routing stage since process start, cluster-wide"
          />
          <StatBox
            label="Routed"
            value={formatCount(stats.totalRouted)}
            dark={dark}
            variant={Number(stats.totalRouted) > 0 ? "ok" : undefined}
            title="Records that matched at least one route and were delivered to a vault (fan-out counts once)"
          />
          <StatBox
            label="Dropped"
            value={formatCount(stats.totalDropped)}
            dark={dark}
            variant={Number(stats.totalDropped) > 0 ? "error" : undefined}
            title="Records that matched no route and were silently discarded — ingested = routed + dropped"
          />
          <StatBox label="Drop rate" value={`${dropRate}%`} dark={dark} />
        </div>
        <div className={`mt-4 pt-3 border-t grid grid-cols-2 gap-4 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
          <RateBox
            label="Ingest rate"
            rate={stats.ingestedRate}
            dark={dark}
            title="Records/s entering the routing stage, summed across all nodes. The gap between this and the route rate is the live drop rate."
          />
          <RateBox
            label="Route rate"
            rate={stats.routedRate}
            dark={dark}
            title="Records/s matched to at least one route, summed across all nodes. Equal to the ingest rate when nothing is dropped."
          />
        </div>
      </div>

      {/* Per-vault breakdown */}
      {sorted.length > 0 && (
        <div>
          <h3
            className={`text-[0.75em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Per-vault delivery
          </h3>
          <div
            className={`rounded-lg border overflow-hidden ${c("border-ink-border", "border-light-border")}`}
          >
            <div
              className={`grid grid-cols-[1fr_7rem] gap-3 px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c("text-text-muted border-ink-border-subtle bg-ink-well", "text-light-text-muted border-light-border-subtle bg-light-well")}`}
            >
              <span>Vault</span>
              <span className="text-right">Matched</span>
            </div>
            {sorted.map((vs) => {
              const vsId = idFromBytes(vs.vaultId);
              const label = vaultLabelById.get(vsId) ?? vsId.slice(0, 8);
              return (
                <div
                  key={vsId}
                  className={`grid grid-cols-[1fr_7rem] gap-3 px-4 py-2.5 text-[0.85em] border-b last:border-b-0 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
                >
                  <span
                    className={`font-mono truncate ${c("text-text-bright", "text-light-text-bright")}`}
                    title={vsId}
                  >
                    {label}
                  </span>
                  <span
                    className={`font-mono text-right ${c("text-text-muted", "text-light-text-muted")}`}
                  >
                    {formatCount(vs.recordsMatched)}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Per-route breakdown */}
      {sortedRoutes.length > 0 && (
        <div>
          <h3
            className={`text-[0.75em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Per-route delivery
          </h3>
          <div
            className={`rounded-lg border overflow-hidden ${c("border-ink-border", "border-light-border")}`}
          >
            <div
              className={`grid grid-cols-[1fr_7rem] gap-3 px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c("text-text-muted border-ink-border-subtle bg-ink-well", "text-light-text-muted border-light-border-subtle bg-light-well")}`}
            >
              <span>Route</span>
              <span className="text-right">Matched</span>
            </div>
            {sortedRoutes.map((route) => (
              <div
                key={route.id}
                className={`grid grid-cols-[1fr_7rem] gap-3 px-4 py-2.5 text-[0.85em] border-b last:border-b-0 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
              >
                <span
                  className={`font-mono truncate ${c("text-text-bright", "text-light-text-bright")}`}
                  title={route.id}
                >
                  {routeLabelById.get(route.id) ?? route.id.slice(0, 8)}
                </span>
                <span
                  className={`font-mono text-right ${c("text-text-muted", "text-light-text-muted")}`}
                >
                  {formatCount(route.recordsMatched)}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {sorted.length === 0 && stats.filterSetActive && (
        <div
          className={`text-center text-[0.9em] py-4 ${c("text-text-muted", "text-light-text-muted")}`}
        >
          No records have been routed yet.
        </div>
      )}
    </div>
  );
}

// RateBox shows one throughput series: instant rate with the server-side
// spark history (the stats collector windows SUMMED cluster counters, so the
// series is system data that survives panel remounts — never client-side
// accumulation), and the ~30s / ~1m trailing averages underneath.
function RateBox({
  label,
  rate,
  dark,
  title,
}: Readonly<{ label: string; rate?: ThroughputRate; dark: boolean; title?: string }>) {
  const c = useThemeClass(dark);
  const instant = rate?.instantPerSec ?? 0;
  const history = rate?.spark ?? [];

  return (
    <div title={title}>
      <div
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1 ${c("text-text-muted", "text-light-text-muted")}`}
      >
        {label}
      </div>
      <div className="flex items-center gap-3">
        <span className={`text-[1.3em] font-mono font-semibold ${c("text-text-bright", "text-light-text-bright")}`}>
          {formatCount(instant)}/s
        </span>
        <span className={c("text-copper/70", "text-copper/60")}>
          <Spark values={history} />
        </span>
      </div>
      <div
        className={`mt-0.5 text-[0.75em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
        title="Unix-load-style EWMAs (1m/5m/15m) — the sustained-rate figures; the big number and spark show instantaneous burst shape"
      >
        1m {formatCount(rate?.avg1mPerSec ?? 0)}/s · 5m {formatCount(rate?.avg5mPerSec ?? 0)}/s · 15m {formatCount(rate?.avg15mPerSec ?? 0)}/s
      </div>
    </div>
  );
}

function StatBox({
  label,
  value,
  dark,
  variant,
  title,
}: Readonly<{
  label: string;
  value: string;
  dark: boolean;
  variant?: "ok" | "error";
  title?: string;
}>) {
  const c = useThemeClass(dark);

  let valueColor = c("text-text-bright", "text-light-text-bright");
  if (variant === "ok") valueColor = "text-severity-info";
  if (variant === "error") valueColor = "text-severity-error";

  return (
    <div title={title}>
      <div
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1 ${c("text-text-muted", "text-light-text-muted")}`}
      >
        {label}
      </div>
      <div className={`text-[1.3em] font-mono font-semibold ${valueColor}`}>
        {value}
      </div>
    </div>
  );
}

function formatCount(n: bigint | number | string): string {
  const num = Number(n);
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`;
  return num.toLocaleString();
}
