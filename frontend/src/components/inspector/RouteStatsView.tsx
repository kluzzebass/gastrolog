import { useThemeClass } from "../../hooks/useThemeClass";
import { useRouteStats } from "../../api/hooks/useRouteStats";
import { useConfig } from "../../api/hooks/useConfig";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { Badge } from "../Badge";

interface RouteStatsViewProps {
  dark: boolean;
}

export function RouteStatsView({ dark }: Readonly<RouteStatsViewProps>) {
  const c = useThemeClass(dark);
  const { data: stats, isLoading } = useRouteStats();
  const { data: config } = useConfig();

  if (isLoading) return <LoadingPlaceholder dark={dark} />;
  if (!stats) return null;

  const vaultNames = new Map<string, string>();
  if (config?.vaults) {
    for (const v of config.vaults) {
      vaultNames.set(v.id, v.name || v.id.slice(0, 8));
    }
  }

  const routeNames = new Map<string, string>();
  if (config?.routes) {
    for (const r of config.routes) {
      routeNames.set(r.id, r.name || r.id.slice(0, 8));
    }
  }

  const dropRate =
    stats.totalIngested > 0
      ? ((Number(stats.totalDropped) / Number(stats.totalIngested)) * 100).toFixed(1)
      : "0.0";

  const sorted = [...stats.vaultStats].sort(
    (a, b) => Number(b.recordsMatched) - Number(a.recordsMatched),
  );

  const sortedRoutes = [...stats.routeStats].sort(
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
      </div>

      {/* Per-vault breakdown */}
      {sorted.length > 0 && (
        <div>
          <h3
            className={`text-[0.75em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Per-vault delivery
          </h3>
          <div
            className={`rounded-lg border overflow-hidden ${c("border-ink-border", "border-light-border")}`}
          >
            <div
              className={`grid grid-cols-[1fr_7rem_7rem] gap-3 px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c("text-text-ghost border-ink-border-subtle bg-ink-well", "text-light-text-ghost border-light-border-subtle bg-light-well")}`}
            >
              <span>Vault</span>
              <span className="text-right">Matched</span>
              <span className="text-right">Forwarded</span>
            </div>
            {sorted.map((vs) => (
              <div
                key={vs.vaultId}
                className={`grid grid-cols-[1fr_7rem_7rem] gap-3 px-4 py-2.5 text-[0.85em] border-b last:border-b-0 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
              >
                <span
                  className={`font-mono truncate ${c("text-text-bright", "text-light-text-bright")}`}
                  title={vs.vaultId}
                >
                  {vaultNames.get(vs.vaultId) ?? vs.vaultId.slice(0, 8)}
                </span>
                <span
                  className={`font-mono text-right ${c("text-text-muted", "text-light-text-muted")}`}
                >
                  {formatCount(vs.recordsMatched)}
                </span>
                <span className="font-mono text-right">
                  {Number(vs.recordsForwarded) > 0 ? (
                    <Badge variant="info" dark={dark}>
                      {formatCount(vs.recordsForwarded)}
                    </Badge>
                  ) : (
                    <span className={c("text-text-ghost", "text-light-text-ghost")}>
                      0
                    </span>
                  )}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Per-route breakdown */}
      {sortedRoutes.length > 0 && (
        <div>
          <h3
            className={`text-[0.75em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Per-route delivery
          </h3>
          <div
            className={`rounded-lg border overflow-hidden ${c("border-ink-border", "border-light-border")}`}
          >
            <div
              className={`grid grid-cols-[1fr_7rem_7rem] gap-3 px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c("text-text-ghost border-ink-border-subtle bg-ink-well", "text-light-text-ghost border-light-border-subtle bg-light-well")}`}
            >
              <span>Route</span>
              <span className="text-right">Matched</span>
              <span className="text-right">Forwarded</span>
            </div>
            {sortedRoutes.map((rs) => (
              <div
                key={rs.routeId}
                className={`grid grid-cols-[1fr_7rem_7rem] gap-3 px-4 py-2.5 text-[0.85em] border-b last:border-b-0 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
              >
                <span
                  className={`font-mono truncate ${c("text-text-bright", "text-light-text-bright")}`}
                  title={rs.routeId}
                >
                  {routeNames.get(rs.routeId) ?? rs.routeId.slice(0, 8)}
                </span>
                <span
                  className={`font-mono text-right ${c("text-text-muted", "text-light-text-muted")}`}
                >
                  {formatCount(rs.recordsMatched)}
                </span>
                <span className="font-mono text-right">
                  {Number(rs.recordsForwarded) > 0 ? (
                    <Badge variant="info" dark={dark}>
                      {formatCount(rs.recordsForwarded)}
                    </Badge>
                  ) : (
                    <span className={c("text-text-ghost", "text-light-text-ghost")}>
                      0
                    </span>
                  )}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {sorted.length === 0 && stats.filterSetActive && (
        <div
          className={`text-center text-[0.9em] py-4 ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          No records have been routed yet.
        </div>
      )}
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
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1 ${c("text-text-ghost", "text-light-text-ghost")}`}
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
