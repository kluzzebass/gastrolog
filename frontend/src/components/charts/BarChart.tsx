import ReactEChartsCore from "echarts-for-react/esm/core";
import { echarts } from "./echartsSetup";
import { buildThemeOption } from "./echartsTheme";
import { getColorForCategory, resolveColor, formatChartValue } from "./chartColors";
import { barTooltipHtml } from "./chartTooltips";
import type { EChartsOption } from "echarts";

interface BarChartProps {
  columns: string[];
  rows: string[][];
  dark: boolean;
}

interface Datum {
  label: string;
  value: number;
}

export function BarChart({ columns, rows, dark }: Readonly<BarChartProps>) {
  const theme = buildThemeOption(dark);

  const labelColIdx = columns.length - 2;
  const valueColIdx = columns.length - 1;
  const data: Datum[] = rows.map((row) => ({
    label: columns.length > 2
      ? row.slice(0, columns.length - 1).join(" / ")
      : row[labelColIdx] ?? "",
    value: Number(row[valueColIdx]) || 0,
  }));

  const option: EChartsOption = {
    ...theme,
    grid: {
      containLabel: true,
      top: 10,
      right: 4,
      bottom: 10,
      left: 4,
    },
    xAxis: {
      ...theme.xAxis as object,
      type: "category",
      data: data.map((d) => d.label),
      axisLabel: {
        ...(theme.xAxis as any)?.axisLabel,
        rotate: -35,
        overflow: "truncate",
      },
      splitLine: { show: false },
    },
    yAxis: {
      ...theme.yAxis as object,
      type: "value",
      axisLabel: {
        ...(theme.yAxis as any)?.axisLabel,
        formatter: (v: number) => formatChartValue(v),
      },
    },
    tooltip: {
      ...theme.tooltip as object,
      trigger: "axis",
      axisPointer: { type: "shadow" },
      formatter: (params: any) => barTooltipHtml(params, columns),
    },
    series: [
      {
        type: "bar",
        data: data.map((d, i) => ({
          value: d.value,
          itemStyle: {
            color: resolveColor(getColorForCategory(d.label, i)),
            opacity: 0.85,
            borderRadius: [2, 2, 0, 0],
          },
          emphasis: {
            itemStyle: { opacity: 1 },
          },
        })),
        barMaxWidth: 60,
      },
    ],
  };

  return (
    <ReactEChartsCore
      echarts={echarts}
      option={option}
      style={{ height: 280, width: "100%" }}
      notMerge
      lazyUpdate
    />
  );
}
