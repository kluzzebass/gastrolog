import ReactEChartsCore from "echarts-for-react/esm/core";
import { echarts } from "./echartsSetup";
import { buildThemeOption } from "./echartsTheme";
import { resolveColor, formatChartValue, GROUP_PALETTE } from "./chartColors";
import { scatterTooltipHtml } from "./chartTooltips";
import type { EChartsOption } from "echarts";

interface ScatterChartProps {
  columns: string[];
  rows: string[][];
  dark: boolean;
}

export function ScatterChart({ columns, rows, dark }: Readonly<ScatterChartProps>) {
  const theme = buildThemeOption(dark);

  // Find the two numeric columns (x and y) and any remaining label columns.
  // Convention: the last two columns are the numeric axes, earlier columns are labels.
  const xIdx = columns.length - 2;
  const yIdx = columns.length - 1;
  const xLabel = columns[xIdx]!;
  const yLabel = columns[yIdx]!;

  const hasLabels = columns.length > 2;
  const labelCols = columns.slice(0, -2);

  const data = rows.map((row) => {
    const x = Number(row[xIdx]) || 0;
    const y = Number(row[yIdx]) || 0;
    const label = hasLabels ? labelCols.map((_, i) => row[i]).join(" / ") : "";
    return { x, y, label };
  });

  const copperColor = resolveColor("var(--color-copper)");

  const option: EChartsOption = {
    ...theme,
    grid: {
      containLabel: true,
      top: 16,
      right: 16,
      bottom: 28,
      left: 24,
    },
    xAxis: {
      ...theme.xAxis as object,
      type: "value",
      name: xLabel,
      nameLocation: "center",
      nameGap: 28,
      nameTextStyle: { fontSize: 10, fontFamily: "IBM Plex Mono, monospace" },
      axisLabel: {
        ...(theme.xAxis as any)?.axisLabel,
        formatter: (v: number) => formatChartValue(v),
      },
    },
    yAxis: {
      ...theme.yAxis as object,
      type: "value",
      name: yLabel,
      nameLocation: "center",
      nameRotate: 90,
      nameGap: 40,
      nameTextStyle: { fontSize: 10, fontFamily: "IBM Plex Mono, monospace" },
      axisLabel: {
        ...(theme.yAxis as any)?.axisLabel,
        formatter: (v: number) => formatChartValue(v),
      },
    },
    tooltip: {
      ...theme.tooltip as object,
      trigger: "item",
      formatter: (params: any) =>
        scatterTooltipHtml(params, xLabel, yLabel, data.map((d) => d.label)),
    },
    series: [
      {
        type: "scatter",
        data: data.map((d, i) => ({
          value: [d.x, d.y],
          itemStyle: {
            color: hasLabels
              ? resolveColor(GROUP_PALETTE[i % GROUP_PALETTE.length]!)
              : copperColor,
            opacity: 0.75,
          },
          emphasis: {
            itemStyle: { opacity: 1, borderColor: copperColor, borderWidth: 2 },
          },
        })),
        symbolSize: 8,
      },
    ],
  };

  return (
    <ReactEChartsCore
      echarts={echarts}
      option={option}
      style={{ height: 320, width: "100%" }}
      notMerge
      lazyUpdate
    />
  );
}
