import type { TableResult } from "../api/client";
import { useThemeClass } from "../hooks/useThemeClass";
import { TableView } from "./TableView";
import { TimeSeriesChart } from "./TimeSeriesChart";

interface PipelineResultsProps {
  tableResult: TableResult;
  dark: boolean;
}

export function PipelineResults({
  tableResult,
  dark,
}: Readonly<PipelineResultsProps>) {
  const c = useThemeClass(dark);
  const { columns, rows, truncated, resultType } = tableResult;
  const rowData = rows.map((r) => r.values);

  const exportCsv = () => {
    const escape = (v: string) =>
      v.includes(",") || v.includes('"') || v.includes("\n")
        ? `"${v.replace(/"/g, '""')}"`
        : v;
    const lines = [
      columns.map(escape).join(","),
      ...rowData.map((row) => row.map(escape).join(",")),
    ];
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "pipeline-results.csv";
    a.click();
    URL.revokeObjectURL(url);
  };

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
        <button
          onClick={exportCsv}
          className={`px-3 py-1.5 text-[0.8em] font-mono rounded transition-colors ${c(
            "text-text-muted hover:text-copper hover:bg-ink-hover",
            "text-light-text-muted hover:text-copper hover:bg-light-hover",
          )}`}
        >
          Export CSV
        </button>
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
