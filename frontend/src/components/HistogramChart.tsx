import { useState, useRef, useEffect } from "react";
import { scaleBand, scaleLinear } from "@visx/scale";
import { BarStack } from "@visx/shape";
import { AxisBottom } from "@visx/axis";
import { Group } from "@visx/group";
import { ParentSize } from "@visx/responsive";
import { useThemeClass } from "../hooks/useThemeClass";
import { clickableProps } from "../utils";
import type { HistogramData } from "../utils/histogramData";
import { AnimatedBar } from "./charts/AnimatedBar";
import { SEVERITY_COLOR_MAP, GROUP_PALETTE } from "./charts/chartColors";
import { ChartTooltip, useChartTooltip, type TooltipData } from "./charts/ChartTooltip";

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

interface StackDatum {
  tsKey: string;
  ts: Date;
  index: number;
  total: number;
  [group: string]: number | string | Date;
}

const margin = { top: 4, right: 0, bottom: 0, left: 0 };

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
  const tooltip = useChartTooltip();
  const svgRef = useRef<SVGSVGElement>(null);

  // Track which group is hovered for legend highlighting.
  const [hoveredGroup, setHoveredGroup] = useState<string | null>(null);

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

  // Check if any bucket has ungrouped ("other") counts for the legend.
  const hasOther = groupKeys.length > 0 && buckets.some((b) => {
    const groupSum = Object.values(b.groupCounts).reduce((a, v) => a + v, 0);
    return b.count - groupSum > 0;
  });
  const legendKeys = hasOther ? [...groupKeys, "other"] : groupKeys;

  const firstBucket = buckets[0]!;
  const lastBucket = buckets[buckets.length - 1]!;
  const totalCount = buckets.reduce((sum, b) => sum + b.count, 0);
  const barHeight = barHeightProp ?? 48;

  // Build stack data: one datum per bucket, with a key per group + "other".
  const hasGroups = groupKeys.length > 0;
  const stackKeys = hasGroups ? [...groupKeys, "__other"] : ["__total"];

  const stackData: StackDatum[] = buckets.map((b, i) => {
    const d: StackDatum = {
      tsKey: b.ts.toISOString(),
      ts: b.ts,
      index: i,
      total: b.count,
    };
    if (hasGroups) {
      let groupSum = 0;
      for (const key of groupKeys) {
        const count = b.groupCounts[key] ?? 0;
        d[key] = count;
        groupSum += count;
      }
      d.__other = b.count - groupSum;
    } else {
      d.__total = b.count;
    }
    return d;
  });

  const maxCount = Math.max(...buckets.map((b) => b.count), 1);

  const colorScale = (key: string): string => {
    if (key === "__total") return "var(--color-copper)";
    if (key === "__other") return "var(--color-copper)";
    return colorMap.get(key) ?? "var(--color-copper)";
  };

  // Helpers for brush.
  const getBucketIndex = (clientX: number): number => {
    const el = svgRef.current;
    if (!el) return 0;
    const rect = el.getBoundingClientRect();
    const x = clientX - rect.left;
    const idx = Math.floor((x / rect.width) * buckets.length);
    return Math.max(0, Math.min(buckets.length - 1, idx));
  };

  const handleMouseDown = (e: React.MouseEvent) => {
    if (!onBrushSelect) return;
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

  const labelCount = Math.min(5, buckets.length);
  const labelStep = Math.max(1, Math.floor(buckets.length / labelCount));

  // Pan delta.
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

  // Build tooltip data for a bucket (no side effects).
  const buildBucketTooltipData = (
    bucket: HistogramData["buckets"][number],
    highlightGroup?: string,
  ): TooltipData => {
    const items: TooltipData["items"] = [];
    if (hasGroups) {
      // "other" is the topmost stack segment — list it first.
      const groupSum = Object.values(bucket.groupCounts).reduce((a, b) => a + b, 0);
      const other = bucket.count - groupSum;
      if (other > 0) {
        items.push({
          color: "var(--color-copper)",
          label: "other",
          value: other.toLocaleString(),
        });
      }
      // Reversed order to match visual stacking (top-to-bottom).
      for (const key of [...groupKeys].reverse()) {
        const count = bucket.groupCounts[key];
        if (count && count > 0) {
          items.push({
            color: colorMap.get(key) ?? "var(--color-copper)",
            label: key,
            value: count.toLocaleString(),
          });
        }
      }
    }
    const title = `${bucket.count.toLocaleString()} \u00b7 ${formatTime(bucket.ts)}`;
    return { title, items, highlightLabel: highlightGroup };
  };

  // Tooltip for a bucket (position + data).
  const showBucketTooltip = (
    bucket: HistogramData["buckets"][number],
    e: React.MouseEvent,
    highlightGroup?: string,
  ) => {
    tooltip.showTooltip({
      tooltipData: buildBucketTooltipData(bucket, highlightGroup),
      tooltipLeft: e.clientX,
      tooltipTop: e.clientY,
    });
  };

  // Data-only refresh (no position change) for when poll data arrives while hovering.
  const refreshTooltip = (bucketIdx: number, highlightGroup?: string) => {
    const bucket = buckets[bucketIdx];
    if (!bucket) return;
    tooltip.updateData(buildBucketTooltipData(bucket, highlightGroup));
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

      <div className="relative" style={{ height: barHeight }}>
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
        <ParentSize>
          {({ width }) =>
            width > 0 ? (
              <HistogramSVG
                width={width}
                height={barHeight}
                stackData={stackData}
                stackKeys={stackKeys}
                maxCount={maxCount}
                colorScale={colorScale}
                buckets={buckets}
                groupKeys={groupKeys}
                colorMap={colorMap}
                hasGroups={hasGroups}
                dark={dark}
                svgRef={svgRef}
                brushLo={brushLo}
                brushHi={brushHi}
                onMouseDown={handleMouseDown}
                onSegmentClick={onSegmentClick}
                showBucketTooltip={showBucketTooltip}
                refreshTooltip={refreshTooltip}
                hideTooltip={tooltip.hideTooltip}
                onBrushSelect={onBrushSelect}
                onHoverGroup={setHoveredGroup}
              />
            ) : null
          }
        </ParentSize>
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

      {/* Tooltip */}
      {tooltip.tooltipOpen && tooltip.tooltipData && (
        <ChartTooltip
          tooltipRef={tooltip.tooltipRef}
          data={tooltip.tooltipData as TooltipData}
          dark={dark}
        />
      )}
    </div>
  );
}

/** The inner SVG chart — separated so ParentSize can provide width. */
function HistogramSVG({
  width,
  height,
  stackData,
  stackKeys,
  maxCount,
  colorScale,
  buckets,
  groupKeys,
  colorMap,
  hasGroups,
  dark,
  svgRef,
  brushLo,
  brushHi,
  onMouseDown,
  onSegmentClick,
  showBucketTooltip,
  refreshTooltip,
  hideTooltip,
  onBrushSelect,
  onHoverGroup,
}: Readonly<{
  width: number;
  height: number;
  stackData: StackDatum[];
  stackKeys: string[];
  maxCount: number;
  colorScale: (key: string) => string;
  buckets: HistogramData["buckets"];
  groupKeys: string[];
  colorMap: Map<string, string>;
  hasGroups: boolean;
  dark: boolean;
  svgRef: React.RefObject<SVGSVGElement | null>;
  brushLo: number | null;
  brushHi: number | null;
  onMouseDown: (e: React.MouseEvent) => void;
  onSegmentClick?: (level: string) => void;
  showBucketTooltip: (
    bucket: HistogramData["buckets"][number],
    e: React.MouseEvent,
    highlightGroup?: string,
  ) => void;
  refreshTooltip: (bucketIdx: number, highlightGroup?: string) => void;
  hideTooltip: () => void;
  onBrushSelect?: (start: Date, end: Date) => void;
  onHoverGroup: (group: string | null) => void;
}>) {
  const c = useThemeClass(dark);
  const [hoveredBar, setHoveredBar] = useState<number | null>(null);
  const hoveredGroupRef = useRef<string | undefined>(undefined);
  const mouseYRef = useRef<number>(0);

  const xMax = width - margin.left - margin.right;
  const yMax = height - margin.top - margin.bottom;

  // When bucket data changes while hovering, recompute which segment the
  // (stationary) cursor is now over and refresh the tooltip accordingly.
  useEffect(() => {
    if (hoveredBar === null) return;
    const datum = stackData[hoveredBar];
    if (!datum) return;

    // Rebuild yScale from current props to hit-test the new stack layout.
    const ys = scaleLinear<number>({ domain: [0, maxCount], range: [yMax, 0] });
    let cumulative = 0;
    let groupKey: string | undefined;
    for (const key of stackKeys) {
      const val = (datum[key] as number) || 0;
      if (val <= 0) { cumulative += val; continue; }
      const segTop = ys(cumulative + val) ?? 0;
      const segBottom = ys(cumulative) ?? 0;
      if (mouseYRef.current >= segTop && mouseYRef.current <= segBottom) {
        groupKey = key === "__total" ? undefined : key === "__other" ? "other" : key;
        break;
      }
      cumulative += val;
    }

    hoveredGroupRef.current = groupKey;
    onHoverGroup(groupKey ?? null);
    refreshTooltip(hoveredBar, groupKey);
  }, [buckets]);

  const xScale = scaleBand<string>({
    domain: stackData.map((d) => d.tsKey),
    range: [0, xMax],
    padding: 0.08,
  });

  const yScale = scaleLinear<number>({
    domain: [0, maxCount],
    range: [yMax, 0],
  });

  return (
    <svg
      ref={svgRef}
      width={width}
      height={height}
      className={`select-none ${onBrushSelect ? "cursor-crosshair" : ""}`}
      onMouseDown={onMouseDown}
      onMouseLeave={() => {
        setHoveredBar(null);
        onHoverGroup(null);
        hideTooltip();
      }}
    >
      <Group left={margin.left} top={margin.top}>
        {/* Brush overlay */}
        {brushLo !== null && brushHi !== null && (
          <rect
            x={(brushLo / buckets.length) * xMax}
            y={0}
            width={((brushHi - brushLo + 1) / buckets.length) * xMax}
            height={yMax}
            fill="var(--color-copper)"
            opacity={0.2}
            rx={2}
            className="pointer-events-none"
          />
        )}

        <BarStack
          data={stackData}
          keys={stackKeys}
          x={(d) => d.tsKey}
          xScale={xScale}
          yScale={yScale}
          color={colorScale}
        >
          {(barStacks) => {
            // Pre-compute the topmost stack index per column for rounded corners.
            const topStackPerColumn = new Map<number, number>();
            for (let si = barStacks.length - 1; si >= 0; si--) {
              for (const bar of barStacks[si]!.bars) {
                if (bar.height > 0 && !topStackPerColumn.has(bar.index)) {
                  topStackPerColumn.set(bar.index, si);
                }
              }
            }

            return barStacks.map((barStack) =>
              barStack.bars.map((bar) => {
                const bucketIdx = bar.index;
                const bucket = buckets[bucketIdx]!;
                const isHovered = hoveredBar === bucketIdx;
                const groupKey = bar.key === "__total"
                  ? undefined
                  : bar.key === "__other"
                    ? "other"
                    : bar.key;

                if (bar.height <= 0) return null;

                const isTop = topStackPerColumn.get(bucketIdx) === barStack.index;

                return (
                  <AnimatedBar
                    key={`${barStack.index}-${bar.index}`}
                    x={bar.x}
                    y={bar.y}
                    width={bar.width}
                    height={bar.height}
                    fill={bar.color}
                    opacity={isHovered ? 1 : dark ? 0.6 : 0.5}
                    rx={isTop ? 2 : 0}
                    className="transition-opacity"
                    onMouseMove={(e) => {
                      const svg = svgRef.current;
                      if (svg) {
                        mouseYRef.current = e.clientY - svg.getBoundingClientRect().top - margin.top;
                      }
                      setHoveredBar(bucketIdx);
                      hoveredGroupRef.current = groupKey;
                      onHoverGroup(groupKey ?? null);
                      showBucketTooltip(bucket, e, groupKey);
                    }}
                    onMouseDown={
                      onSegmentClick && groupKey
                        ? (e) => e.stopPropagation()
                        : undefined
                    }
                    onClick={
                      onSegmentClick && groupKey
                        ? (e) => {
                            e.stopPropagation();
                            onSegmentClick(groupKey);
                          }
                        : undefined
                    }
                    {...(onSegmentClick && groupKey
                      ? {
                          ...clickableProps(() => onSegmentClick(groupKey)),
                          cursor: "pointer",
                        }
                      : {})}
                  />
                );
              }),
            );
          }}
        </BarStack>
      </Group>
    </svg>
  );
}
