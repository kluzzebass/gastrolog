import { useState, useRef } from "react";
import ReactEChartsCore from "echarts-for-react/esm/core";
import { echarts } from "./charts/echartsSetup";
import { useThemeClass } from "../hooks/useThemeClass";
import type { HistogramData } from "../utils/histogramData";
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
  for (const key of [...seen].sort()) {
    colorMap.set(key, GROUP_PALETTE[paletteIdx % GROUP_PALETTE.length]!);
    paletteIdx++;
  }
  return colorMap;
}

export function HistogramChart({
  data,
  dark,
  barHeight: barHeightProp,
  showHeader = true,
  onBrushSelect,
  onPan,
  onSegmentClick,
}: Readonly<{
  data: HistogramData;
  dark: boolean;
  barHeight?: number;
  showHeader?: boolean;
  onBrushSelect?: (start: Date, end: Date) => void;
  onPan?: (start: Date, end: Date) => void;
  onSegmentClick?: (level: string) => void;
}>) {
  const { buckets } = data;
  const c = useThemeClass(dark);
  const chartContainerRef = useRef<HTMLDivElement>(null);

  const [hoveredGroup, setHoveredGroup] = useState<string | null>(null);
  const [hoveredBar, setHoveredBar] = useState<number | null>(null);

  // Brush state.
  const [brushStart, setBrushStart] = useState<number | null>(null);
  const [brushEnd, setBrushEnd] = useState<number | null>(null);
  const brushingRef = useRef(false);

  // Pan state.
  const axisRef = useRef<HTMLDivElement>(null);
  const panStartX = useRef<number>(0);
  const [panAxisWidth, setPanAxisWidth] = useState(1);
  const panningRef = useRef(false);
  const [panOffset, setPanOffset] = useState(0);

  if (buckets.length === 0) return null;

  const colorMap = buildColorMap(data);
  const groupKeys = [...colorMap.keys()];

  const hasOther = groupKeys.length > 0 && buckets.some((b) => {
    const groupSum = Object.values(b.groupCounts).reduce((a, v) => a + v, 0);
    return b.count - groupSum > 0;
  });
  const legendKeys = hasOther ? [...groupKeys, "other"] : groupKeys;

  const firstBucket = buckets[0]!;
  const lastBucket = buckets[buckets.length - 1]!;
  const totalCount = buckets.reduce((sum, b) => sum + b.count, 0);
  const barHeight = barHeightProp ?? 48;

  const hasGroups = groupKeys.length > 0;
  const baseOpacity = dark ? 0.6 : 0.5;

  // ECharts canvas can't resolve CSS variables — compute copper once.
  const copperColor = resolveColor("var(--color-copper)");

  // Build ECharts series: one bar series per stack key.
  // Opacity is baked per-item so we can brighten the hovered column.
  const stackKeys = hasGroups ? [...groupKeys, "__other"] : ["__total"];
  const categories = buckets.map((_, i) => String(i));

  const seriesData = stackKeys.map((key) => {
    const isOther = key === "__other";
    const isTotal = key === "__total";
    const displayName = isTotal ? "count" : isOther ? "other" : key;
    const color = isTotal || isOther
      ? copperColor
      : resolveColor(colorMap.get(key) ?? copperColor);

    return {
      name: displayName,
      type: "bar" as const,
      stack: "total",
      data: buckets.map((b, i) => {
        let value: number;
        if (isTotal) value = b.count;
        else if (isOther) {
          const groupSum = Object.values(b.groupCounts).reduce((a, v) => a + v, 0);
          value = Math.max(0, b.count - groupSum);
        } else {
          value = b.groupCounts[key] ?? 0;
        }
        return {
          value,
          itemStyle: {
            color,
            opacity: hoveredBar === i ? 1 : baseOpacity,
            borderRadius: 0,
          },
        };
      }),
      emphasis: { disabled: true },
      barCategoryGap: "8%",
      cursor: onSegmentClick && !isTotal ? "pointer" : "crosshair",
    };
  });

  // Time formatting.
  const rangeMs =
    buckets.length > 1 ? lastBucket.ts.getTime() - firstBucket.ts.getTime() : 0;

  const formatTime = (d: Date) => {
    if (rangeMs > 24 * 60 * 60 * 1000) {
      return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    }
    if (rangeMs < 60 * 60 * 1000) {
      return d.toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      });
    }
    return d.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
  };

  // Build a single tooltip line with a colored dot, label, and count.
  const tooltipLine = (color: string, label: string, count: number, isBold: boolean): string => {
    const dot = `<span style="display:inline-block;width:5px;height:5px;border-radius:50%;background:${color};margin-right:5px;"></span>`;
    const style = isBold ? "font-weight:bold" : hoveredGroup ? "opacity:0.5" : "opacity:0.7";
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
        lines.push(tooltipLine(copperColor, "other", other, hoveredGroup === "other"));
      }
      for (const key of [...groupKeys].reverse()) {
        const count = bucket.groupCounts[key];
        if (count && count > 0) {
          lines.push(tooltipLine(resolveColor(colorMap.get(key) ?? copperColor), key, count, hoveredGroup === key));
        }
      }
    }

    const header = `<div style="opacity:0.7">${bucket.count.toLocaleString()} \u00b7 ${formatTime(bucket.ts)}</div>`;
    return header + lines.join("<br/>");
  };

  const tooltipBg = dark
    ? resolveColor("var(--color-ink-surface)") || "#1a1a1a"
    : resolveColor("var(--color-light-surface)") || "#ffffff";
  const tooltipBorder = dark
    ? resolveColor("var(--color-ink-border-subtle)") || "rgba(255,255,255,0.08)"
    : resolveColor("var(--color-light-border-subtle)") || "rgba(0,0,0,0.08)";
  const tooltipText = dark
    ? resolveColor("var(--color-text-bright)") || "#e5e5e5"
    : resolveColor("var(--color-light-text-bright)") || "#1a1a1a";

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
      formatter: tooltipFormatter,
    },
    series: seriesData,
  };

  // ECharts event handlers for hover highlighting and segment click.
  const onEvents = {
    mouseover: (params: any) => {
      const name = params.seriesName as string;
      const group = name === "count" ? null : name;
      setHoveredGroup(group);
      setHoveredBar(params.dataIndex as number);
    },
    mouseout: () => {
      setHoveredGroup(null);
      setHoveredBar(null);
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
    setBrushStart(idx);
    setBrushEnd(idx);
    brushingRef.current = true;

    const onMouseMove = (ev: MouseEvent) => {
      if (!brushingRef.current) return;
      setBrushEnd(getBucketIndex(ev.clientX));
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
      setBrushStart(null);
      setBrushEnd(null);
      globalThis.removeEventListener("mousemove", onMouseMove);
      globalThis.removeEventListener("mouseup", onMouseUp);
    };
    globalThis.addEventListener("mousemove", onMouseMove);
    globalThis.addEventListener("mouseup", onMouseUp);
  };

  const brushLo =
    brushStart !== null && brushEnd !== null
      ? Math.min(brushStart, brushEnd)
      : null;
  const brushHi =
    brushStart !== null && brushEnd !== null
      ? Math.max(brushStart, brushEnd)
      : null;

  // Pan handlers.
  const handlePanStep = (direction: -1 | 1) => {
    if (!onPan || buckets.length < 2) return;
    const windowMs = lastBucket.ts.getTime() - firstBucket.ts.getTime();
    const stepMs = windowMs / 2;
    const first = firstBucket.ts.getTime();
    const last = lastBucket.ts.getTime();
    onPan(
      new Date(first + direction * stepMs),
      new Date(last + direction * stepMs),
    );
  };

  const handleAxisMouseDown = (e: React.MouseEvent) => {
    if (!onPan || buckets.length < 2) return;
    e.preventDefault();
    panStartX.current = e.clientX;
    setPanAxisWidth(axisRef.current?.getBoundingClientRect().width || 1);
    panningRef.current = true;
    document.body.style.cursor = "grabbing";
    document.body.style.userSelect = "none";

    const onMouseMove = (ev: MouseEvent) => {
      setPanOffset(ev.clientX - panStartX.current);
    };
    const onMouseUp = (ev: MouseEvent) => {
      panningRef.current = false;
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      setPanOffset(0);
      globalThis.removeEventListener("mousemove", onMouseMove);
      globalThis.removeEventListener("mouseup", onMouseUp);

      const el = axisRef.current;
      if (!el) return;
      const deltaX = panStartX.current - ev.clientX;
      const axisWidth = el.getBoundingClientRect().width;
      if (Math.abs(deltaX) < 3) return;
      const windowMs = lastBucket.ts.getTime() - firstBucket.ts.getTime();
      const deltaMs = (deltaX / axisWidth) * windowMs;
      const first = firstBucket.ts.getTime();
      const last = lastBucket.ts.getTime();
      onPan(new Date(first + deltaMs), new Date(last + deltaMs));
    };
    globalThis.addEventListener("mousemove", onMouseMove);
    globalThis.addEventListener("mouseup", onMouseUp);
  };

  const labelCount = Math.min(5, buckets.length);
  const labelStep = Math.max(1, Math.floor(buckets.length / labelCount));

  const windowMs =
    buckets.length > 1 ? lastBucket.ts.getTime() - firstBucket.ts.getTime() : 0;
  const panDeltaMs =
    panOffset !== 0 ? -((panOffset / panAxisWidth) * windowMs) : 0;

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
              <div className="flex flex-wrap gap-x-2.5 gap-y-0.5">
                {legendKeys.map((key) => (
                  <div
                    key={key}
                    className={`flex items-center gap-1 transition-opacity ${
                      hoveredGroup !== null && hoveredGroup !== key ? "opacity-40" : ""
                    }`}
                  >
                    <span
                      className="inline-block w-1.5 h-1.5 rounded-full shrink-0"
                      style={{ backgroundColor: colorMap.get(key) ?? "var(--color-copper)" }}
                    />
                    <span
                      className={`text-[0.65em] font-mono ${
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
            )}
          </div>
          <span
            className={`font-mono text-[0.75em] ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {totalCount.toLocaleString()} records
          </span>
        </div>
      ) : legendKeys.length > 0 ? (
        <div className="flex flex-wrap gap-x-3 gap-y-1 mb-2">
          {legendKeys.map((key) => (
            <div
              key={key}
              className={`flex items-center gap-1 transition-opacity ${
                hoveredGroup !== null && hoveredGroup !== key ? "opacity-40" : ""
              }`}
            >
              <span
                className="inline-block w-2 h-2 rounded-full shrink-0"
                style={{ backgroundColor: colorMap.get(key) ?? "var(--color-copper)" }}
              />
              <span
                className={`text-[0.7em] font-mono ${
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
      ) : null}

      <div
        ref={chartContainerRef}
        className="relative"
        style={{ height: barHeight }}
        onMouseDown={handleMouseDown}
      >
        {/* Pan delta indicator */}
        {panOffset !== 0 && (
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
          onMouseDown={handleAxisMouseDown}
          className={`flex-1 flex justify-between overflow-hidden ${onPan ? "cursor-grab active:cursor-grabbing" : ""}`}
          style={
            panOffset ? { transform: `translateX(${panOffset}px)` } : undefined
          }
        >
          {Array.from({ length: labelCount }, (_, i) => {
            const idx = Math.min(i * labelStep, buckets.length - 1);
            return (
              <span
                key={`tick-${idx}`}
                className={`text-[0.65em] font-mono select-none ${c("text-text-ghost", "text-light-text-ghost")}`}
              >
                {formatTime(buckets[idx]!.ts)}
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
