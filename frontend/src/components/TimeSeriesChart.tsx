import ReactEChartsCore from "echarts-for-react/esm/core";
import { echarts } from "./charts/echartsSetup";
import { buildThemeOption } from "./charts/echartsTheme";
import { SERIES_COLORS, resolveColor } from "./charts/chartColors";
import type { EChartsOption } from "echarts";

interface TimeSeriesChartProps {
  columns: string[];
  rows: string[][];
  dark: boolean;
}

interface Series {
  name: string;
  points: { x: number; y: number }[];
  color: string;
}

function parseData(columns: string[], rows: string[][]) {
  if (columns.length < 2 || rows.length === 0) {
    return { series: [] as Series[], timestamps: [] as number[], yMin: 0, yMax: 1 };
  }

  const timeCol = 0;
  const timestamps = rows.map((r) => new Date(r[timeCol]!).getTime());

  // Detect 3-column pivot: [time, group, agg]
  if (columns.length === 3 && rows.some((r) => isNaN(Number(r[1])))) {
    const groups = new Map<string, { x: number; y: number }[]>();
    const uniqueTimes = [...new Set(timestamps)].sort((a, b) => a - b);

    for (const row of rows) {
      const t = new Date(row[timeCol]!).getTime();
      const group = row[1] ?? "";
      const raw = row[2] ?? "";
      if (raw === "") continue;
      const val = Number(raw) || 0;
      if (!groups.has(group)) groups.set(group, []);
      groups.get(group)!.push({ x: t, y: val });
    }

    let colorIdx = 0;
    const seriesList: Series[] = [];
    for (const [name, points] of groups) {
      points.sort((a, b) => a.x - b.x);
      seriesList.push({
        name,
        points,
        color: resolveColor(SERIES_COLORS[colorIdx % SERIES_COLORS.length]!),
      });
      colorIdx++;
    }

    let allMin = Infinity;
    let allMax = -Infinity;
    for (const s of seriesList) {
      for (const p of s.points) {
        if (p.y < allMin) allMin = p.y;
        if (p.y > allMax) allMax = p.y;
      }
    }
    if (allMin === Infinity) { allMin = 0; allMax = 1; }
    if (allMin === allMax) { allMin -= 1; allMax += 1; }

    return { series: seriesList, timestamps: uniqueTimes, yMin: allMin, yMax: allMax };
  }

  // Standard: columns 1..N are numeric series
  const aggCols = columns.slice(1);
  const seriesList: Series[] = aggCols.map((name, i) => ({
    name,
    points: rows.flatMap((r, j) => {
      const raw = r[i + 1] ?? "";
      if (raw === "") return [];
      return [{ x: timestamps[j]!, y: Number(raw) || 0 }];
    }),
    color: resolveColor(SERIES_COLORS[i % SERIES_COLORS.length]!),
  }));

  let allMin = Infinity;
  let allMax = -Infinity;
  for (const s of seriesList) {
    for (const p of s.points) {
      if (p.y < allMin) allMin = p.y;
      if (p.y > allMax) allMax = p.y;
    }
  }
  if (allMin === Infinity) { allMin = 0; allMax = 1; }
  if (allMin === allMax) { allMin -= 1; allMax += 1; }

  const uniqueTimes = [...new Set(timestamps)].sort((a, b) => a - b);
  return { series: seriesList, timestamps: uniqueTimes, yMin: allMin, yMax: allMax };
}

const formatYValue = (v: number) => {
  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";
  if (abs >= 1_000_000) return `${sign}${(abs / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${sign}${(abs / 1_000).toFixed(1)}K`;
  return Number.isInteger(v) ? String(v) : v.toFixed(1);
};

export function TimeSeriesChart({ columns, rows, dark }: Readonly<TimeSeriesChartProps>) {
  const theme = buildThemeOption(dark);
  const { series } = parseData(columns, rows);

  if (series.length === 0) return null;

  const singleSeries = series.length === 1;

  const option: EChartsOption = {
    ...theme,
    grid: {
      containLabel: true,
      top: 10,
      right: 4,
      bottom: series.length > 1 ? 40 : 10,
      left: 4,
    },
    xAxis: {
      ...theme.xAxis as object,
      type: "time",
      splitLine: { show: false },
    },
    yAxis: {
      ...theme.yAxis as object,
      type: "value",
      axisLabel: {
        ...(theme.yAxis as any)?.axisLabel,
        formatter: (v: number) => formatYValue(v),
      },
    },
    tooltip: {
      ...theme.tooltip as object,
      trigger: "axis",
      axisPointer: {
        type: "cross",
        crossStyle: { color: dark ? "rgba(255,255,255,0.3)" : "rgba(0,0,0,0.2)" },
      },
      formatter: (params: any) => {
        const items = Array.isArray(params) ? params : [params];
        if (items.length === 0) return "";
        const ts = new Date(items[0].value[0]);
        const timeStr = ts.toLocaleTimeString("en-US", {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
          hour12: false,
        });
        const lines = items.map((p: any) => {
          const dot = `<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:${p.color};margin-right:6px;"></span>`;
          return `${dot}${p.seriesName} <b>${formatYValue(p.value[1] as number)}</b>`;
        });
        return `<div style="opacity:0.7">${timeStr}</div>${lines.join("<br/>")}`;
      },
    },
    legend: series.length > 1 ? {
      bottom: 0,
      textStyle: {
        fontFamily: "'IBM Plex Mono', monospace",
        fontSize: 10,
        color: dark ? "rgba(255,255,255,0.5)" : "rgba(0,0,0,0.5)",
      },
      icon: "roundRect",
      itemWidth: 10,
      itemHeight: 10,
    } : undefined,
    series: series.map((s) => ({
      name: s.name,
      type: "line" as const,
      smooth: true,
      symbol: "circle",
      symbolSize: 4,
      showSymbol: false,
      lineStyle: {
        width: singleSeries ? 2 : 1.5,
        color: s.color,
      },
      itemStyle: { color: s.color },
      areaStyle: singleSeries ? { opacity: 0.15, color: s.color } : undefined,
      emphasis: {
        focus: "series" as const,
        lineStyle: { width: 2.5 },
      },
      data: s.points.map((p) => [p.x, p.y]),
    })),
  };

  const legendHeight = series.length > 1 ? 30 : 0;

  return (
    <ReactEChartsCore
      echarts={echarts}
      option={option}
      style={{ height: 240 + legendHeight, width: "100%" }}
      notMerge
      lazyUpdate
    />
  );
}
