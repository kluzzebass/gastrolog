import ReactEChartsCore from "echarts-for-react/esm/core";
import { echarts } from "./echartsSetup";
import { buildThemeOption } from "./echartsTheme";
import { formatChartValue } from "./chartColors";
import type { EChartsOption } from "echarts";

interface HeatmapChartProps {
  columns: string[];
  rows: string[][];
  dark: boolean;
}

export function HeatmapChart({ columns, rows, dark }: Readonly<HeatmapChartProps>) {
  const theme = buildThemeOption(dark);

  // Columns: [xField, yField, valueField]
  const xLabel = columns[0] ?? "x";
  const yLabel = columns[1] ?? "y";
  const valueLabel = columns[2] ?? "value";

  // Extract unique axis values preserving order of first appearance.
  const xValues: string[] = [];
  const yValues: string[] = [];
  const xSeen = new Set<string>();
  const ySeen = new Set<string>();

  for (const row of rows) {
    const x = row[0] ?? "";
    const y = row[1] ?? "";
    if (!xSeen.has(x)) { xSeen.add(x); xValues.push(x); }
    if (!ySeen.has(y)) { ySeen.add(y); yValues.push(y); }
  }

  // Build data array: [xIndex, yIndex, value]
  let minVal = Infinity;
  let maxVal = -Infinity;
  const data: [number, number, number][] = [];

  for (const row of rows) {
    const xi = xValues.indexOf(row[0] ?? "");
    const yi = yValues.indexOf(row[1] ?? "");
    const val = Number(row[2]) || 0;
    data.push([xi, yi, val]);
    if (val < minVal) minVal = val;
    if (val > maxVal) maxVal = val;
  }

  if (!Number.isFinite(minVal)) minVal = 0;
  if (!Number.isFinite(maxVal)) maxVal = 1;

  // Traditional blue → yellow → red heatmap ramp.
  const colorRange = ["#313695", "#4575b4", "#74add1", "#fee090", "#f46d43", "#d73027"];

  const textGhost = dark ? "rgba(255,255,255,0.35)" : "rgba(0,0,0,0.35)";

  const option: EChartsOption = {
    ...theme,
    grid: {
      containLabel: true,
      top: 10,
      right: 80,
      bottom: 10,
      left: 10,
    },
    tooltip: {
      ...theme.tooltip as object,
      position: "top",
      formatter: (params: any) => {
        const p = params;
        const x = xValues[p.value[0]] ?? "";
        const y = yValues[p.value[1]] ?? "";
        const v = p.value[2] as number;
        return [
          `<div style="opacity:0.7">${xLabel}: ${x}</div>`,
          `<div style="opacity:0.7">${yLabel}: ${y}</div>`,
          `<b>${valueLabel}: ${formatChartValue(v)}</b>`,
        ].join("");
      },
    },
    xAxis: {
      ...theme.xAxis as object,
      type: "category",
      data: xValues,
      splitArea: { show: false },
    },
    yAxis: {
      ...theme.yAxis as object,
      type: "category",
      data: yValues,
      splitArea: { show: false },
    },
    visualMap: {
      min: minVal,
      max: maxVal,
      calculable: true,
      orient: "vertical",
      right: 0,
      top: "center",
      itemHeight: 100,
      inRange: { color: colorRange },
      textStyle: {
        fontFamily: "'IBM Plex Mono', monospace",
        fontSize: 10,
        color: textGhost,
      },
    },
    series: [
      {
        type: "heatmap",
        data,
        itemStyle: {
          borderWidth: 1,
          borderColor: dark ? "rgba(0,0,0,0.4)" : "rgba(255,255,255,0.6)",
        },
        emphasis: {
          itemStyle: {
            borderColor: dark ? "#e5e5e5" : "#1a1a1a",
            borderWidth: 1,
          },
        },
      },
    ],
  };

  // Dynamic height: 32px per y-axis value, min 160, max 500.
  const height = Math.min(500, Math.max(160, yValues.length * 32 + 80));

  return (
    <ReactEChartsCore
      echarts={echarts}
      option={option}
      style={{ height, width: "100%" }}
      notMerge
      lazyUpdate
    />
  );
}
