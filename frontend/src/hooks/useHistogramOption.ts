import { useEffect } from "react";
import type { EChartsOption } from "echarts";
import type { HistogramData } from "../utils/histogramData";
import { formatDateShort, formatTimeHM, formatTimeOnly } from "../utils/temporal";
import { SEVERITY_COLOR_MAP, GROUP_PALETTE, resolveColor } from "../components/charts/chartColors";

/**
 * Builds an ordered color map for all group values found in the data.
 * Severity levels get their theme colors; everything else gets palette colors.
 */
export function buildColorMap(data: HistogramData): Map<string, string> {
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

interface HistogramOptionDeps {
  buckets: HistogramData["buckets"];
  dark: boolean;
  colorMap: Map<string, string>;
  groupKeys: string[];
  copperColor: string;
  baseOpacity: number;
  hasCloud: boolean;
  hasGroups: boolean;
  hoveredBar: number | null;
  hoveredGroup: string | null;
  onSegmentClick: ((level: string) => void) | undefined;
  stableFormatter: (params: any) => string;
  formatterImplRef: React.MutableRefObject<(params: any) => string>;
}

interface HistogramOptionResult {
  option: EChartsOption;
  seriesData: any[];
  categories: string[];
  formatTime: (d: Date) => string;
}

export function useHistogramOption(deps: HistogramOptionDeps): HistogramOptionResult {
  const {
    buckets,
    dark,
    colorMap,
    groupKeys,
    copperColor,
    baseOpacity,
    hasCloud,
    hasGroups,
    hoveredBar,
    hoveredGroup,
    onSegmentClick,
    stableFormatter,
    formatterImplRef,
  } = deps;

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
      data: buckets.map((b, i) => {
        // For cloud buckets, subtract the cloud portion from this series —
        // it will be rendered by the separate hatched cloud series below.
        const fullValue = valueGrid[seriesIdx]![i]!;
        const localValue = b.cloudCount > 0 && b.count > 0
          ? Math.max(0, Math.round(fullValue * (1 - b.cloudCount / b.count)))
          : fullValue;
        return {
          value: localValue,
          itemStyle: {
            color,
            opacity: hoveredBar === i ? 1 : baseOpacity,
            borderRadius: topSeriesPerBucket[i] === seriesIdx && b.cloudCount === 0 ? [2, 2, 0, 0] : 0,
          },
        };
      }),
      emphasis: { disabled: true },
      barCategoryGap: "8%",
      cursor: onSegmentClick && !isTotal ? "pointer" : "crosshair",
    };
  });

  // Add hatched cloud data series on top of existing bars.
  if (hasCloud) {
    seriesData.push({
      name: "cloud",
      type: "bar" as const,
      stack: "total",
      data: buckets.map((b, i) => {
        const isHovered = hoveredBar === i;
        const hoveredOpacity = isHovered ? 0.8 : baseOpacity;
        const cloudOpacity = b.cloudCount > 0 ? hoveredOpacity : 0;
        return {
          value: Math.max(b.cloudCount, 0),
          itemStyle: {
            color: copperColor,
            opacity: cloudOpacity,
            borderRadius: b.cloudCount > 0 ? [2, 2, 0, 0] as number | number[] : 0,
          },
        };
      }),
      emphasis: { disabled: true },
      barCategoryGap: "8%",
      cursor: "crosshair",
      itemStyle: {
        decal: {
          symbol: "rect",
          symbolSize: 1,
          dashArrayX: [1, 0],
          dashArrayY: [4, 3],
          rotation: -Math.PI / 4,
          color: "rgba(255,255,255,0.3)",
        },
      },
    } as any);
  }

  // Time formatting.
  const firstBucket = buckets[0];
  const lastBucket = buckets.at(-1);
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
    else if (hoveredGroup) style = "opacity:0.5";
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
        lines.push(tooltipLine(copperColor, "other", other, hoveredGroup === "other"));
      }
      for (const key of groupKeys.toReversed()) {
        const count = bucket.groupCounts[key];
        if (count && count > 0) {
          lines.push(tooltipLine(resolveColor(colorMap.get(key) ?? copperColor), key, count, hoveredGroup === key));
        }
      }
    }

    const header = `<div style="opacity:0.7">${bucket.count.toLocaleString()} \u00B7 ${formatTime(bucket.ts)}</div>`;
    if (bucket.hasCloudData) {
      lines.push(`<div style="opacity:0.5;font-size:0.85em;margin-top:2px">includes interpolated cloud data</div>`);
    }
    return header + lines.join("<br/>");
  };

  // Sync formatter ref — must be called unconditionally on every render.
  useEffect(() => {
    formatterImplRef.current = tooltipFormatter;
  });

  // Tooltip theme resolution — resolve CSS vars for canvas use.
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
    aria: { decal: { show: true } },
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

  return { option, seriesData, categories, formatTime };
}
