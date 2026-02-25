import { useState } from "react";
import type { TableResult } from "../api/client";
import { useThemeClass } from "../hooks/useThemeClass";
import { classifyTableResult } from "../utils/classifyTableResult";
import { tableResultToHistogramData } from "../utils/histogramData";
import { AutoRefreshControls } from "./AutoRefreshControls";
import { BarChart } from "./charts/BarChart";
import { DonutChart } from "./charts/DonutChart";
import { WorldMapChart } from "./charts/WorldMapChart";
import { ExportButton } from "./ExportButton";
import { HistogramChart } from "./HistogramChart";
import { TableView } from "./TableView";
import { TimeSeriesChart } from "./TimeSeriesChart";

function ViewModeToggle({
  mode,
  onModeChange,
  dark,
}: {
  mode: "chart" | "table";
  onModeChange: (mode: "chart" | "table") => void;
  dark: boolean;
}) {
  const c = useThemeClass(dark);
  const options = [
    { key: "chart" as const, label: "Chart" },
    { key: "table" as const, label: "Table" },
  ];
  return (
    <div
      className={`flex items-center rounded overflow-hidden border ${c(
        "border-ink-border-subtle",
        "border-light-border-subtle",
      )}`}
    >
      {options.map((opt) => (
        <button
          key={opt.key}
          onClick={() => onModeChange(opt.key)}
          className={`px-2 py-1 text-[0.75em] font-mono transition-colors ${
            mode === opt.key
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
  );
}

function SingleValueDisplay({
  value,
  label,
  dark,
}: {
  value: string;
  label: string;
  dark: boolean;
}) {
  const c = useThemeClass(dark);

  // Format large numbers with locale separators.
  const num = Number(value);
  const formatted = !isNaN(num) ? num.toLocaleString() : value;

  return (
    <div className="flex flex-col items-center justify-center py-12 px-5">
      <span
        className={`font-mono text-[3.5em] font-semibold leading-tight ${c(
          "text-copper",
          "text-copper",
        )}`}
      >
        {formatted}
      </span>
      <span
        className={`font-body text-[1em] mt-2 ${c(
          "text-text-muted",
          "text-light-text-muted",
        )}`}
      >
        {label}
      </span>
    </div>
  );
}

interface PipelineResultsProps {
  tableResult: TableResult;
  dark: boolean;
  pollInterval: number | null;
  onPollIntervalChange: (ms: number | null) => void;
  scrollRef?: React.RefObject<HTMLDivElement | null>;
  footer?: React.ReactNode;
}

export function PipelineResults({
  tableResult,
  dark,
  pollInterval,
  onPollIntervalChange,
  scrollRef,
  footer,
}: Readonly<PipelineResultsProps>) {
  const c = useThemeClass(dark);
  const { columns, rows, truncated, resultType } = tableResult;
  const rowData = rows.map((r) => r.values);
  const [viewMode, setViewMode] = useState<"chart" | "table">("chart");

  // Single-value display: 1 column, 1 row, result type is "table" (not "raw").
  const isSingleValue =
    resultType === "table" && columns.length === 1 && rowData.length === 1;

  // Classify table results for chart selection.
  const tableClassification =
    resultType === "table" ? classifyTableResult(columns, rowData) : "table";
  const hasChartView =
    resultType === "timeseries" ||
    resultType === "timechart" ||
    tableClassification === "bar-chart" ||
    tableClassification === "donut-chart" ||
    tableClassification === "world-map" ||
    tableClassification === "scatter-map";

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
          {hasChartView && (
            <ViewModeToggle
              mode={viewMode}
              onModeChange={setViewMode}
              dark={dark}
            />
          )}
          <AutoRefreshControls
            pollInterval={pollInterval}
            onPollIntervalChange={onPollIntervalChange}
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

      {/* Chart, table, or single value */}
      <div ref={scrollRef} className="flex-1 overflow-auto app-scroll">
        {isSingleValue ? (
          <SingleValueDisplay
            value={rowData[0]![0]!}
            label={columns[0]!}
            dark={dark}
          />
        ) : resultType === "timechart" && viewMode === "chart" ? (
          (() => {
            const histData = tableResultToHistogramData(columns, rows);
            return histData && histData.buckets.length > 0 ? (
              <div className="px-5 py-4">
                <HistogramChart
                  data={histData}
                  dark={dark}
                  barHeight={200}
                  showHeader={false}
                />
              </div>
            ) : (
              <TableView columns={columns} rows={rowData} dark={dark} />
            );
          })()
        ) : resultType === "timeseries" && viewMode === "chart" ? (
          <div className="px-5 py-4 relative">
            <TimeSeriesChart columns={columns} rows={rowData} dark={dark} />
          </div>
        ) : (tableClassification === "world-map" || tableClassification === "scatter-map") && viewMode === "chart" ? (
          <div className="px-5 py-4">
            <WorldMapChart columns={columns} rows={rowData} dark={dark} />
          </div>
        ) : tableClassification === "donut-chart" && viewMode === "chart" ? (
          <div className="px-5 py-4">
            <DonutChart columns={columns} rows={rowData} dark={dark} />
          </div>
        ) : tableClassification === "bar-chart" && viewMode === "chart" ? (
          <div className="px-5 py-4">
            <BarChart columns={columns} rows={rowData} dark={dark} />
          </div>
        ) : (
          <TableView columns={columns} rows={rowData} dark={dark} />
        )}
        {footer}
      </div>
    </div>
  );
}
