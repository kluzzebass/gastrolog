import { useEffect, useState } from "react";
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
          />
          <StatBox
            label="Routed"
            value={formatCount(stats.totalRouted)}
            dark={dark}
            variant={Number(stats.totalRouted) > 0 ? "ok" : undefined}
          />
          <StatBox
            label="Dropped"
            value={formatCount(stats.totalDropped)}
            dark={dark}
            variant={Number(stats.totalDropped) > 0 ? "error" : undefined}
          />
          <StatBox label="Drop rate" value={`${dropRate}%`} dark={dark} />
        </div>
        <div className={`mt-4 pt-3 border-t grid grid-cols-2 gap-4 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
          <RateBox label="Ingest rate" rate={stats.ingestedRate} dark={dark} />
          <RateBox label="Route rate" rate={stats.routedRate} dark={dark} />
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

// useRateHistory accumulates the displayed cluster rate client-side for a
// sparkline. Per-node sparks are server-side; a cluster-level spark cannot be
// summed from them (tick phases differ), so this is the honest history of the
// number actually shown, starting from panel mount.
function useRateHistory(value: number, cap = 20): number[] {
  const [history, setHistory] = useState<number[]>([]);
  useEffect(() => {
    setHistory((h) => [...h.slice(-(cap - 1)), value]);
  }, [value, cap]);
  return history;
}

// RateBox shows one throughput series: instant rate with a sparkline of its
// recent values, and the ~30s / ~1m trailing averages underneath.
function RateBox({
  label,
  rate,
  dark,
}: Readonly<{ label: string; rate?: ThroughputRate; dark: boolean }>) {
  const c = useThemeClass(dark);
  const instant = rate?.instantPerSec ?? 0;
  const history = useRateHistory(instant);

  return (
    <div>
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
      <div className={`mt-0.5 text-[0.75em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
        30s {formatCount(rate?.avg30sPerSec ?? 0)}/s · 1m {formatCount(rate?.avg60sPerSec ?? 0)}/s
      </div>
    </div>
  );
}

function StatBox({
  label,
  value,
  dark,
  variant,
}: Readonly<{
  label: string;
  value: string;
  dark: boolean;
  variant?: "ok" | "error";
}>) {
  const c = useThemeClass(dark);

  let valueColor = c("text-text-bright", "text-light-text-bright");
  if (variant === "ok") valueColor = "text-severity-info";
  if (variant === "error") valueColor = "text-severity-error";

  return (
    <div>
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
