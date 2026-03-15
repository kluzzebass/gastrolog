import { useState, useReducer, useRef, useEffect } from "react";
import ReactEChartsCore from "echarts-for-react/esm/core";
import { echarts } from "./charts/echartsSetup";
import { useThemeClass } from "../hooks/useThemeClass";
import type { HistogramData } from "../utils/histogramData";
import { formatDateShort, formatTimeHM, formatTimeOnly } from "../utils/temporal";
import { SEVERITY_COLOR_MAP, GROUP_PALETTE, resolveColor } from "./charts/chartColors";
import type { EChartsOption } from "echarts";

/**
 * Builds an ordered color map for all group values found in the data.
 * Severity levels get their theme colors; everything else gets palette colors.
 */
function buildColorMap(data: HistogramData): Map<string, string> {
  const seen = new Set<string>();
  for (const b of data.buckets) {
    for (const key of Object.keys(b.groupCounts)) {
      seen.add(key);
    }
  }

  const colorMap = new Map<string, string>();
  let paletteIdx = 0;
  const severityOrder = ["error", "warn", "info", "debug", "trace"];
  for (const key of severityOrder) {
    if (seen.has(key)) {
      colorMap.set(key, SEVERITY_COLOR_MAP[key]!);
      seen.delete(key);
    }
  }
  // "other" represents records without a level — use the copper theme color.
  if (seen.has("other")) {
    colorMap.set("other", "var(--color-copper)");
    seen.delete("other");
  }
  for (const key of [...seen].sort()) {
    colorMap.set(key, GROUP_PALETTE[paletteIdx % GROUP_PALETTE.length]!);
    paletteIdx++;
  }
  return colorMap;
}

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

/** Set grab cursor on document body for pan dragging. */
function setGrabbingCursor() {
  document.body.style.cursor = "grabbing";
  document.body.style.userSelect = "none";
}

/** Reset document body cursor after pan dragging. */
function clearGrabbingCursor() {
  document.body.style.cursor = "";
  document.body.style.userSelect = "";
}

// ── Interaction state reducer ─────────────────────────────────────────

interface ChartInteraction {
  hoveredGroup: string | null;
  hoveredBar: number | null;
  brushStart: number | null;
  brushEnd: number | null;
  panAxisWidth: number;
  panOffset: number;
}

type ChartAction =
  | { type: "hover"; group: string | null; bar: number | null }
  | { type: "brushStart"; idx: number }
  | { type: "brushMove"; idx: number }
  | { type: "brushEnd" }
  | { type: "panStart"; axisWidth: number }
  | { type: "panMove"; offset: number }
  | { type: "panEnd" };

const CHART_INITIAL: ChartInteraction = {
  hoveredGroup: null, hoveredBar: null,
  brushStart: null, brushEnd: null,
  panAxisWidth: 1, panOffset: 0,
};

function chartReducer(state: ChartInteraction, action: ChartAction): ChartInteraction {
  switch (action.type) {
    case "hover":
      return { ...state, hoveredGroup: action.group, hoveredBar: action.bar };
    case "brushStart":
      return { ...state, brushStart: action.idx, brushEnd: action.idx };
    case "brushMove":
      return { ...state, brushEnd: action.idx };
    case "brushEnd":
      return { ...state, brushStart: null, brushEnd: null };
    case "panStart":
      return { ...state, panAxisWidth: action.axisWidth };
    case "panMove":
      return { ...state, panOffset: action.offset };
    case "panEnd":
      return { ...state, panOffset: 0 };
  }
}

// eslint-disable-next-line sonarjs/cognitive-complexity -- chart component with many interactive features (brush, pan, tooltip, legend)
export function HistogramChart({
  data,
  dark,
  barHeight: barHeightProp,
  showHeader = true,
  truncated = false,
  onBrushSelect,
  onPan,
  onSegmentClick,
}: Readonly<{
  data: HistogramData;
  dark: boolean;
  barHeight?: number;
  showHeader?: boolean;
  truncated?: boolean;
  onBrushSelect?: (start: Date, end: Date) => void;
  onPan?: (start: Date, end: Date) => void;
  onSegmentClick?: (level: string) => void;
}>) {
  const { buckets } = data;
  const c = useThemeClass(dark);
  const chartContainerRef = useRef<HTMLDivElement>(null);

  const [ix, dix] = useReducer(chartReducer, CHART_INITIAL);
  const brushingRef = useRef(false);

  // Stable tooltip formatter ref: echarts-for-react uses fast-deep-equal to
  // compare the option prop. Function references always fail === comparison,
  // so a fresh tooltipFormatter closure on every render triggers unnecessary
  // setOption calls (replaying entrance animation). The stable wrapper
  // delegates to the latest closure via ref indirection.
  const formatterImplRef = useRef<(params: any) => string>(() => "");
  const [stableFormatter] = useState(() => (params: any) => formatterImplRef.current(params));

  // Pan state refs.
  const axisRef = useRef<HTMLDivElement>(null);
  const panStartX = useRef<number>(0);
  const panningRef = useRef(false);

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
  const barHeight = barHeightProp ?? 96;

  const hasGroups = groupKeys.length > 0;
  const baseOpacity = dark ? 0.6 : 0.5;

  // ECharts canvas can't resolve CSS variables — compute copper once.
  const copperColor = resolveColor("var(--color-copper)");

  // Build ECharts series: one bar series per stack key.
  // Opacity is baked per-item so we can brighten the hovered column.
  const stackKeys = hasGroups ? [...groupKeys, "__other"] : ["__total"];
  const categories = buckets.map((_, i) => String(i));

  // Compute per-bucket values for each stack key to determine the topmost segment.
  const valueGrid = stackKeys.map((key) =>
    buckets.map((b) => {
      if (key === "__total") return b.count;
      if (key === "__other") {
        const groupSum = Object.values(b.groupCounts).reduce((a, v) => a + v, 0);
        return Math.max(0, b.count - groupSum);
      }
      return b.groupCounts[key] ?? 0;
    }),
  );

  // For each bucket, find the topmost series (last with value > 0).
  const topSeriesPerBucket = buckets.map((_, i) => {
    for (let s = stackKeys.length - 1; s >= 0; s--) {
      if (valueGrid[s]![i]! > 0) return s;
    }
    return -1;
  });

  const seriesData = stackKeys.map((key, seriesIdx) => {
    const isOther = key === "__other";
    const isTotal = key === "__total";
    let displayName: string;
    if (isTotal) displayName = "count";
    else if (isOther) displayName = "other";
    else displayName = key;
    const color = isTotal || isOther
      ? copperColor
      : resolveColor(colorMap.get(key) ?? copperColor);

    return {
      name: displayName,
      type: "bar" as const,
      stack: "total",
      data: buckets.map((_, i) => ({
        value: valueGrid[seriesIdx]![i],
        itemStyle: {
          color,
          opacity: ix.hoveredBar === i ? 1 : baseOpacity,
          borderRadius: topSeriesPerBucket[i] === seriesIdx ? [2, 2, 0, 0] : 0,
        },
      })),
      emphasis: { disabled: true },
      barCategoryGap: "8%",
      cursor: onSegmentClick && !isTotal ? "pointer" : "crosshair",
    };
  });

  // Add cloud data marker series — small hatched bars where cloud chunks
  // have data but exact counts are unavailable.
  const hasCloudData = buckets.some((b) => b.hasCloudData);
  if (hasCloudData) {
    const maxCount = Math.max(...buckets.map((b) => b.count).filter((c) => c > 0), 1);
    const markerHeight = Math.max(maxCount * 0.04, 1); // 4% of max bar or 1
    seriesData.push({
      name: "cloud data",
      type: "bar" as const,
      stack: "cloud",
      data: buckets.map((b, i) => ({
        value: b.hasCloudData ? markerHeight : 0,
        itemStyle: {
          color: copperColor,
          opacity: b.hasCloudData ? (ix.hoveredBar === i ? 0.5 : 0.2) : 0,
          borderRadius: [1, 1, 0, 0],
          decal: {
            symbol: "none",
            dashArrayX: [1, 0],
            dashArrayY: [2, 2],
            rotation: -Math.PI / 4,
            color: "rgba(255,255,255,0.4)",
          },
        },
      })),
      emphasis: { disabled: true },
      barCategoryGap: "8%",
      cursor: "crosshair",
    });
  }

  // Time formatting.
  const rangeMs = firstBucket && lastBucket
    ? lastBucket.ts.getTime() - firstBucket.ts.getTime()
    : 0;

  const formatTime = (d: Date) => {
    if (rangeMs > 24 * 60 * 60 * 1000) return formatDateShort(d);
    if (rangeMs < 60 * 60 * 1000) return formatTimeOnly(d);
    return formatTimeHM(d);
  };

  // Build a single tooltip line with a colored dot, label, and count.
  const tooltipLine = (color: string, label: string, count: number, isBold: boolean): string => {
    const dot = `<span style="display:inline-block;width:5px;height:5px;border-radius:50%;background:${color};margin-right:5px;"></span>`;
    let style: string;
    if (isBold) style = "font-weight:bold";
    else if (ix.hoveredGroup) style = "opacity:0.5";
    else style = "opacity:0.7";
    const valueStyle = isBold ? "font-weight:bold" : "";
    return `${dot}<span style="${style}">${label}</span> <span style="${valueStyle}">${count.toLocaleString()}</span>`;
  };

  // Build tooltip HTML for a bucket index.
  const tooltipFormatter = (params: any) => {
    const items: any[] = Array.isArray(params) ? params : [params];
    if (items.length === 0) return "";
    const bucketIdx = items[0].dataIndex as number;
    const bucket = buckets[bucketIdx];
    if (!bucket) return "";

    const lines: string[] = [];
    if (hasGroups) {
      const groupSum = Object.values(bucket.groupCounts).reduce((a, b) => a + b, 0);
      const other = bucket.count - groupSum;
      if (other > 0) {
        lines.push(tooltipLine(copperColor, "other", other, ix.hoveredGroup === "other"));
      }
      for (const key of groupKeys.toReversed()) {
        const count = bucket.groupCounts[key];
        if (count && count > 0) {
          lines.push(tooltipLine(resolveColor(colorMap.get(key) ?? copperColor), key, count, ix.hoveredGroup === key));
        }
      }
    }

    const header = `<div style="opacity:0.7">${bucket.count.toLocaleString()} \u00B7 ${formatTime(bucket.ts)}</div>`;
    return header + lines.join("<br/>");
  };

  // Sync formatter ref — must be above the early return so the hook is
  // called unconditionally on every render.
  useEffect(() => {
    formatterImplRef.current = tooltipFormatter;
  });

  if (buckets.length === 0) return null;

  const tooltipBg = dark
    ? resolveColor("var(--color-ink-surface)")
    : resolveColor("var(--color-light-surface)");
  const tooltipBorder = dark
    ? resolveColor("var(--color-ink-border-subtle)")
    : resolveColor("var(--color-light-border-subtle)");
  const tooltipText = dark
    ? resolveColor("var(--color-text-bright)")
    : resolveColor("var(--color-light-text-bright)");

  const option: EChartsOption = {
    animation: true,
    animationDuration: 400,
    animationDurationUpdate: 300,
    animationEasing: "cubicInOut",
    animationEasingUpdate: "cubicInOut",
    grid: {
      top: 4,
      right: 0,
      bottom: 0,
      left: 0,
    },
    xAxis: {
      type: "category",
      data: categories,
      show: false,
    },
    yAxis: {
      type: "value",
      show: false,
      min: 0,
    },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "none" },
      backgroundColor: tooltipBg,
      borderColor: tooltipBorder,
      textStyle: {
        fontFamily: "'IBM Plex Mono', monospace",
        fontSize: 12,
        color: tooltipText,
      },
      padding: [4, 8],
      extraCssText: "border-radius: 4px; box-shadow: 0 2px 8px rgba(0,0,0,0.3);",
      formatter: stableFormatter,
    },
    series: seriesData,
  };

  // ECharts event handlers for hover highlighting and segment click.
  const onEvents = {
    mouseover: (params: any) => {
      const name = params.seriesName as string;
      const group = name === "count" ? null : name;
      dix({ type: "hover", group, bar: params.dataIndex as number });
    },
    mouseout: () => {
      dix({ type: "hover", group: null, bar: null });
    },
    click: (params: any) => {
      if (!onSegmentClick) return;
      const name = params.seriesName as string;
      if (name !== "count") {
        onSegmentClick(name);
      }
    },
  };

  // Brush helpers — use the chart container div for hit-testing.
  const getBucketIndex = (clientX: number): number => {
    const el = chartContainerRef.current;
    if (!el) return 0;
    const rect = el.getBoundingClientRect();
    const x = clientX - rect.left;
    const idx = Math.floor((x / rect.width) * buckets.length);
    return Math.max(0, Math.min(buckets.length - 1, idx));
  };

  const handleMouseDown = (e: React.MouseEvent) => {
    if (!onBrushSelect) return;
    if (e.button !== 0) return;
    e.preventDefault();
    const idx = getBucketIndex(e.clientX);
    dix({ type: "brushStart", idx });
    brushingRef.current = true;

    const onMouseMove = (ev: MouseEvent) => {
      if (!brushingRef.current) return;
      dix({ type: "brushMove", idx: getBucketIndex(ev.clientX) });
    };
    const onMouseUp = (ev: MouseEvent) => {
      if (!brushingRef.current) return;
      brushingRef.current = false;
      const endIdx = getBucketIndex(ev.clientX);
      const lo = Math.min(idx, endIdx);
      const hi = Math.max(idx, endIdx);
      if (lo !== hi) {
        onBrushSelect(buckets[lo]!.ts, buckets[hi]!.ts);
      }
      dix({ type: "brushEnd" });
      globalThis.removeEventListener("mousemove", onMouseMove);
      globalThis.removeEventListener("mouseup", onMouseUp);
    };
    globalThis.addEventListener("mousemove", onMouseMove, { passive: true });
    globalThis.addEventListener("mouseup", onMouseUp);
  };

  const brushLo =
    ix.brushStart !== null && ix.brushEnd !== null
      ? Math.min(ix.brushStart, ix.brushEnd)
      : null;
  const brushHi =
    ix.brushStart !== null && ix.brushEnd !== null
      ? Math.max(ix.brushStart, ix.brushEnd)
      : null;

  // Pan handlers.
  const handlePanStep = (direction: -1 | 1) => {
    if (!onPan || buckets.length < 2) return;
    const windowMs = lastBucket!.ts.getTime() - firstBucket!.ts.getTime();
    const stepMs = windowMs / 2;
    const first = firstBucket!.ts.getTime();
    const last = lastBucket!.ts.getTime();
    onPan(
      new Date(first + direction * stepMs),
      new Date(last + direction * stepMs),
    );
  };

  const handleAxisMouseDown = (e: React.MouseEvent) => {
    if (!onPan || buckets.length < 2) return;
    e.preventDefault();
    panStartX.current = e.clientX;
    dix({ type: "panStart", axisWidth: axisRef.current?.getBoundingClientRect().width || 1 });
    panningRef.current = true;
    setGrabbingCursor();

    const onMouseMove = (ev: MouseEvent) => {
      dix({ type: "panMove", offset: ev.clientX - panStartX.current });
    };
    const onMouseUp = (ev: MouseEvent) => {
      panningRef.current = false;
      clearGrabbingCursor();
      dix({ type: "panEnd" });
      globalThis.removeEventListener("mousemove", onMouseMove);
      globalThis.removeEventListener("mouseup", onMouseUp);

      const el = axisRef.current;
      if (!el) return;
      const deltaX = panStartX.current - ev.clientX;
      const axisWidth = el.getBoundingClientRect().width;
      if (Math.abs(deltaX) < 3) return;
      const windowMs = lastBucket!.ts.getTime() - firstBucket!.ts.getTime();
      const deltaMs = (deltaX / axisWidth) * windowMs;
      const first = firstBucket!.ts.getTime();
      const last = lastBucket!.ts.getTime();
      onPan(new Date(first + deltaMs), new Date(last + deltaMs));
    };
    globalThis.addEventListener("mousemove", onMouseMove, { passive: true });
    globalThis.addEventListener("mouseup", onMouseUp);
  };

  const labelCount = Math.min(5, buckets.length);
  const labelStep = Math.max(1, Math.floor(buckets.length / labelCount));

  const windowMs =
    buckets.length > 1 ? lastBucket!.ts.getTime() - firstBucket!.ts.getTime() : 0;
  const panDeltaMs =
    ix.panOffset !== 0 ? -((ix.panOffset / ix.panAxisWidth) * windowMs) : 0;

  const formatDuration = (ms: number): string => {
    const abs = Math.abs(ms);
    const sign = ms < 0 ? "-" : "+";
    if (abs < 1000) return `${sign}${Math.round(abs)}ms`;
    if (abs < 60_000) return `${sign}${(abs / 1000).toFixed(1)}s`;
    if (abs < 3_600_000) {
      const m = Math.floor(abs / 60_000);
      const s = Math.round((abs % 60_000) / 1000);
      return s > 0 ? `${sign}${m}m ${s}s` : `${sign}${m}m`;
    }
    if (abs < 86_400_000) {
      const h = Math.floor(abs / 3_600_000);
      const m = Math.round((abs % 3_600_000) / 60_000);
      return m > 0 ? `${sign}${h}h ${m}m` : `${sign}${h}h`;
    }
    const d = Math.floor(abs / 86_400_000);
    const h = Math.round((abs % 86_400_000) / 3_600_000);
    return h > 0 ? `${sign}${d}d ${h}h` : `${sign}${d}d`;
  };

  return (
    <div className="relative">
      {showHeader ? (
        <div className="flex items-baseline justify-between mb-1.5">
          <div className="flex items-baseline gap-3">
            <span
              className={`text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
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
            title={truncated ? "Approximate — scan limit reached" : undefined}
          >
            {truncated ? "~" : ""}{totalCount.toLocaleString()} records
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
              "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
              "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
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
                className={`text-[0.65em] font-mono select-none ${c("text-text-ghost", "text-light-text-ghost")}`}
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
              "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
              "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
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
