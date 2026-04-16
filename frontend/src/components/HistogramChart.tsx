import { useState, useReducer, useRef } from "react";
import ReactEChartsCore from "echarts-for-react/esm/core";
import { echarts } from "./charts/echartsSetup";
import { useThemeClass } from "../hooks/useThemeClass";
import type { HistogramData } from "../utils/histogramData";
import { resolveColor } from "./charts/chartColors";
import { buildColorMap, useHistogramOption } from "../hooks/useHistogramOption";
import {
  chartReducer,
  CHART_INITIAL,
  useHistogramInteraction,
} from "../hooks/useHistogramInteraction";

function HistogramLegend({
  legendKeys,
  hoveredGroup,
  colorMap,
  dark,
  compact = false,
}: Readonly<{
  legendKeys: string[];
  hoveredGroup: string | null;
  colorMap: Map<string, string>;
  dark: boolean;
  compact?: boolean;
}>) {
  const c = useThemeClass(dark);
  if (legendKeys.length === 0) return null;
  const gapCls = compact ? "gap-x-2.5 gap-y-0.5" : "gap-x-3 gap-y-1 mb-2";
  const dotCls = compact ? "w-1.5 h-1.5" : "w-2 h-2";
  const textCls = compact ? "text-[0.65em]" : "text-[0.7em]";
  return (
    <div className={`flex flex-wrap ${gapCls}`}>
      {legendKeys.map((key) => (
        <div
          key={key}
          className={`flex items-center gap-1 transition-opacity ${
            hoveredGroup !== null && hoveredGroup !== key ? "opacity-40" : ""
          }`}
        >
          <span
            className={`inline-block ${dotCls} rounded-full shrink-0`}
            style={{ backgroundColor: colorMap.get(key) ?? "var(--color-copper)" }}
          />
          <span
            className={`${textCls} font-mono ${
              hoveredGroup === key
                ? c("text-text-bright", "text-light-text-bright")
                : c("text-text-muted", "text-light-text-muted")
            }`}
          >
            {key}
          </span>
        </div>
      ))}
    </div>
  );
}

/** Format elapsed time as a short human-readable string. */
function formatElapsed(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(1)}s`;
  return `${ms}ms`;
}

export function HistogramChart({
  data,
  dark,
  barHeight: barHeightProp,
  showHeader = true,
  truncated = false,
  elapsedMs,
  onBrushSelect,
  onPan,
  onSegmentClick,
}: Readonly<{
  data: HistogramData;
  dark: boolean;
  barHeight?: number;
  showHeader?: boolean;
  truncated?: boolean;
  elapsedMs?: number | null;
  onBrushSelect?: (start: Date, end: Date) => void;
  onPan?: (start: Date, end: Date) => void;
  onSegmentClick?: (level: string) => void;
}>) {
  const { buckets } = data;
  const c = useThemeClass(dark);
  const chartContainerRef = useRef<HTMLDivElement>(null);

  const [ix, dix] = useReducer(chartReducer, CHART_INITIAL);

  // Stable tooltip formatter ref: echarts-for-react uses fast-deep-equal to
  // compare the option prop. Function references always fail === comparison,
  // so a fresh tooltipFormatter closure on every render triggers unnecessary
  // setOption calls (replaying entrance animation). The stable wrapper
  // delegates to the latest closure via ref indirection.
  const formatterImplRef = useRef<(params: any) => string>(() => "");
  const [stableFormatter] = useState(() => (params: any) => formatterImplRef.current(params));

  const axisRef = useRef<HTMLDivElement>(null);

  // --- Data derived from buckets (safe for empty arrays) ---
  const colorMap = buckets.length > 0 ? buildColorMap(data) : new Map<string, string>();
  const groupKeys = [...colorMap.keys()];

  const hasOther = groupKeys.length > 0 && buckets.some((b) => {
    const groupSum = Object.values(b.groupCounts).reduce((a, v) => a + v, 0);
    return b.count - groupSum > 0;
  });
  const legendKeys = hasOther ? [...groupKeys, "other"] : groupKeys;

  const firstBucket = buckets[0];
  const lastBucket = buckets.at(-1);
  const totalCount = buckets.reduce((sum, b) => sum + b.count, 0);
  const hasCloud = buckets.some((b) => b.hasCloudData);
  const barHeight = barHeightProp ?? 96;

  const hasGroups = groupKeys.length > 0;
  const baseOpacity = dark ? 0.6 : 0.5;

  // ECharts canvas can't resolve CSS variables — compute copper once.
  const copperColor = resolveColor("var(--color-copper)");

  // Hook 1: ECharts option computation.
  const { option, formatTime } = useHistogramOption({
    buckets,
    dark,
    colorMap,
    groupKeys,
    copperColor,
    baseOpacity,
    hasCloud,
    hasGroups,
    hoveredBar: ix.hoveredBar,
    hoveredGroup: ix.hoveredGroup,
    onSegmentClick,
    stableFormatter,
    formatterImplRef,
  });

  // Hook 2: Interaction handlers (brush, pan, events).
  const {
    onEvents,
    handleMouseDown,
    brushLo,
    brushHi,
    handlePanStep,
    handleAxisMouseDown,
    labelCount,
    labelStep,
    panDeltaMs,
    formatDuration,
  } = useHistogramInteraction({
    buckets,
    firstBucket,
    lastBucket,
    chartContainerRef,
    axisRef,
    ix,
    dix,
    onBrushSelect,
    onPan,
    onSegmentClick,
  });

  if (buckets.length === 0) return null;

  let countTitle: string | undefined;
  if (truncated) countTitle = "Approximate — scan limit reached";
  else if (hasCloud) countTitle = "Approximate — cloud chunk counts are interpolated";

  return (
    <div className="relative">
      {showHeader ? (
        <div className="flex items-baseline justify-between mb-1.5">
          <div className="flex items-baseline gap-3">
            <span
              className={`text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Volume
            </span>
            {legendKeys.length > 0 && (
              <HistogramLegend
                legendKeys={legendKeys}
                hoveredGroup={ix.hoveredGroup}
                colorMap={colorMap}
                dark={dark}
                compact
              />
            )}
          </div>
          <span
            className={`font-mono text-[0.75em] ${c("text-text-muted", "text-light-text-muted")}`}
            title={countTitle}
          >
            {truncated ? "~" : ""}{totalCount.toLocaleString()} records{elapsedMs != null ? ` in ${formatElapsed(elapsedMs)}` : ""}
          </span>
        </div>
      ) : (
        <HistogramLegend
          legendKeys={legendKeys}
          hoveredGroup={ix.hoveredGroup}
          colorMap={colorMap}
          dark={dark}
        />
      )}

      <div
        ref={chartContainerRef}
        className="relative"
        style={{ height: barHeight }}
        role="presentation"
        data-testid="histogram"
        onMouseDown={handleMouseDown}
      >
        {/* Pan delta indicator */}
        {ix.panOffset !== 0 && (
          <div
            className={`absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 px-2 py-0.5 text-[0.7em] font-mono rounded whitespace-nowrap pointer-events-none z-20 ${c(
              "bg-ink-surface text-copper border border-copper/30",
              "bg-light-surface text-copper border border-copper/30",
            )}`}
          >
            {formatDuration(panDeltaMs)}
          </div>
        )}

        {/* ECharts canvas */}
        <ReactEChartsCore
          echarts={echarts}
          option={option}
          style={{ height: barHeight, width: "100%", cursor: onBrushSelect ? "crosshair" : "default" }}
          notMerge
          lazyUpdate
          onEvents={onEvents}
        />

        {/* Brush selection overlay (pointer-events: none, purely visual) */}
        {brushLo !== null && brushHi !== null && (
          <div
            className="absolute top-0 bottom-0 rounded pointer-events-none z-10"
            style={{
              left: `${(brushLo / buckets.length) * 100}%`,
              width: `${((brushHi - brushLo + 1) / buckets.length) * 100}%`,
              backgroundColor: "var(--color-copper)",
              opacity: 0.2,
            }}
          />
        )}
      </div>

      {/* Time axis with pan arrows + draggable labels */}
      <div className="flex items-center mt-1 gap-1 min-h-5">
        {onPan && (
          <button
            onClick={() => handlePanStep(-1)}
            className={`text-[0.7em] px-1 rounded transition-colors shrink-0 ${c(
              "text-text-muted hover:text-text-muted hover:bg-ink-hover",
              "text-light-text-muted hover:text-light-text-muted hover:bg-light-hover",
            )}`}
            aria-label="Pan backward"
            title="Pan backward"
          >
            {"\u25C2"}
          </button>
        )}
        <div
          ref={axisRef}
          role="presentation"
          onMouseDown={handleAxisMouseDown}
          className={`flex-1 flex justify-between overflow-hidden ${onPan ? "cursor-grab active:cursor-grabbing" : ""}`}
          style={
            ix.panOffset ? { transform: `translateX(${ix.panOffset}px)` } : undefined
          }
        >
          {Array.from({ length: labelCount }, (_, labelIdx) => {
            const bucketIdx = Math.min(labelIdx * labelStep, buckets.length - 1);
            const ts = buckets[bucketIdx]!.ts;
            return (
              <span
                key={`tick-${ts.getTime()}`}
                className={`text-[0.65em] font-mono select-none ${c("text-text-muted", "text-light-text-muted")}`}
              >
                {formatTime(ts)}
              </span>
            );
          })}
        </div>
        {onPan && (
          <button
            onClick={() => handlePanStep(1)}
            className={`text-[0.7em] px-1 rounded transition-colors shrink-0 ${c(
              "text-text-muted hover:text-text-muted hover:bg-ink-hover",
              "text-light-text-muted hover:text-light-text-muted hover:bg-light-hover",
            )}`}
            aria-label="Pan forward"
            title="Pan forward"
          >
            {"\u25B8"}
          </button>
        )}
      </div>
    </div>
  );
}
