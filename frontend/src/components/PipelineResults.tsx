import type { TableResult } from "../api/client";
import { useThemeClass } from "../hooks/useThemeClass";
import { ExportButton } from "./ExportButton";
import { TableView } from "./TableView";
import { TimeSeriesChart } from "./TimeSeriesChart";

const POLL_OPTIONS: { label: string; ms: number | null }[] = [
  { label: "Off", ms: null },
  { label: "5s", ms: 5_000 },
  { label: "10s", ms: 10_000 },
  { label: "30s", ms: 30_000 },
  { label: "1m", ms: 60_000 },
];

function AutoRefreshControls({
  pollInterval,
  onPollIntervalChange,
  onRefresh,
  dark,
}: {
  pollInterval: number | null;
  onPollIntervalChange: (ms: number | null) => void;
  onRefresh: () => void;
  dark: boolean;
}) {
  const c = useThemeClass(dark);
  return (
    <div className="flex items-center gap-2">
      <div
        className={`flex items-center rounded overflow-hidden border ${c(
          "border-ink-border-subtle",
          "border-light-border-subtle",
        )}`}
      >
        {POLL_OPTIONS.map((opt) => (
          <button
            key={opt.label}
            onClick={() => onPollIntervalChange(opt.ms)}
            className={`px-2 py-1 text-[0.75em] font-mono transition-colors ${
              pollInterval === opt.ms
                ? `${c("bg-copper/20 text-copper", "bg-copper/20 text-copper")}`
                : `${c(
                    "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                    "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                  )}`
            }`}
          >
            {opt.label}
          </button>
        ))}
      </div>

      <button
        onClick={onRefresh}
        className={`px-2.5 py-1.5 text-[0.8em] font-mono rounded transition-colors ${c(
          "text-text-muted hover:text-copper hover:bg-ink-hover",
          "text-light-text-muted hover:text-copper hover:bg-light-hover",
        )}`}
        title="Refresh now"
      >
        ↻
      </button>
    </div>
  );
}

interface PipelineResultsProps {
  tableResult: TableResult;
  dark: boolean;
  pollInterval: number | null;
  onPollIntervalChange: (ms: number | null) => void;
  onRefresh: () => void;
}

export function PipelineResults({
  tableResult,
  dark,
  pollInterval,
  onPollIntervalChange,
  onRefresh,
}: Readonly<PipelineResultsProps>) {
  const c = useThemeClass(dark);
  const { columns, rows, truncated, resultType } = tableResult;
  const rowData = rows.map((r) => r.values);

  return (
    <div className="flex flex-col flex-1 overflow-hidden">
      {/* Header */}
      <div
        className={`flex justify-between items-center px-5 py-2.5 border-b ${c(
          "border-ink-border-subtle",
          "border-light-border-subtle",
        )}`}
      >
        <div className="flex items-center gap-3">
          <h3
            className={`font-display text-[1.15em] font-semibold ${c(
              "text-text-bright",
              "text-light-text-bright",
            )}`}
          >
            Pipeline Results
          </h3>
          <span
            className={`font-mono text-[0.8em] px-2 py-0.5 rounded ${c(
              "bg-ink-surface text-text-muted",
              "bg-light-hover text-light-text-muted",
            )}`}
          >
            {rowData.length} row{rowData.length !== 1 ? "s" : ""}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <AutoRefreshControls
            pollInterval={pollInterval}
            onPollIntervalChange={onPollIntervalChange}
            onRefresh={onRefresh}
            dark={dark}
          />
          <ExportButton tableData={{ columns, rows: rowData }} dark={dark} />
        </div>
      </div>

      {/* Truncation warning */}
      {truncated && (
        <div
          className={`px-5 py-2 text-[0.8em] font-mono border-b ${c(
            "bg-severity-warn/10 text-severity-warn border-ink-border-subtle",
            "bg-severity-warn/10 text-severity-warn border-light-border-subtle",
          )}`}
        >
          Results truncated — cardinality cap reached.
        </div>
      )}

      {/* Chart or table */}
      <div className="flex-1 overflow-auto app-scroll">
        {resultType === "timeseries" ? (
          <div className="px-5 py-4 relative">
            <TimeSeriesChart columns={columns} rows={rowData} dark={dark} />
          </div>
        ) : (
          <TableView columns={columns} rows={rowData} dark={dark} />
        )}
      </div>
    </div>
  );
}
