import { useState, useRef } from "react";
import ReactEChartsCore from "echarts-for-react/esm/core";
import { echarts } from "./echartsSetup";
import { buildThemeOption } from "./echartsTheme";
import { getColorForCategory, resolveColor, formatChartValue } from "./chartColors";
import { useThemeClass } from "../../hooks/useThemeClass";
import type { EChartsOption } from "echarts";

interface DonutChartProps {
  columns: string[];
  rows: string[][];
  dark: boolean;
}

interface Datum {
  label: string;
  value: number;
  color: string;
}

export function DonutChart({ columns, rows, dark }: Readonly<DonutChartProps>) {
  const c = useThemeClass(dark);
  const chartRef = useRef<ReactEChartsCore>(null);
  const [hoveredLabel, setHoveredLabel] = useState<string | null>(null);
  const theme = buildThemeOption(dark);

  const valueColIdx = columns.length - 1;
  const data: Datum[] = rows.map((row, i) => {
    const label = row.slice(0, columns.length - 1).join(" / ") || row[0] || "";
    return {
      label,
      value: Number(row[valueColIdx]) || 0,
      color: resolveColor(getColorForCategory(label, i)),
    };
  });

  const total = data.reduce((sum, d) => sum + d.value, 0);
  const textGhost = dark ? "rgba(255,255,255,0.35)" : "rgba(0,0,0,0.35)";
  const textBright = dark ? "#e5e5e5" : "#1a1a1a";

  const option: EChartsOption = {
    ...theme,
    tooltip: {
      ...theme.tooltip as object,
      trigger: "item",
      formatter: (params: any) => {
        const p = params;
        const pct = ((p.value / total) * 100).toFixed(1);
        const dot = `<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:${p.color};margin-right:6px;"></span>`;
        return `<div style="opacity:0.7">${p.name}</div>${dot}${columns[valueColIdx]} <b>${formatChartValue(p.value as number)} (${pct}%)</b>`;
      },
    },
    graphic: [
      {
        type: "text",
        left: "center",
        top: "42%",
        style: {
          text: formatChartValue(total),
          fontFamily: "'IBM Plex Mono', monospace",
          fontSize: 22,
          fontWeight: 600,
          fill: textBright,
        },
      },
      {
        type: "text",
        left: "center",
        top: "54%",
        style: {
          text: "total",
          fontFamily: "'IBM Plex Mono', monospace",
          fontSize: 11,
          fill: textGhost,
        },
      },
    ],
    series: [
      {
        type: "pie",
        radius: ["60%", "85%"],
        center: ["50%", "50%"],
        padAngle: 1,
        itemStyle: {
          borderRadius: 3,
          opacity: 0.85,
        },
        emphasis: {
          scaleSize: 4,
          itemStyle: { opacity: 1 },
        },
        label: { show: false },
        data: data.map((d) => ({
          name: d.label,
          value: d.value,
          itemStyle: { color: d.color },
        })),
      },
    ],
  };

  const onEvents = {
    mouseover: (params: any) => {
      setHoveredLabel(params.name as string);
    },
    mouseout: () => {
      setHoveredLabel(null);
    },
  };

  const handleLegendHover = (label: string | null) => {
    setHoveredLabel(label);
    const instance = chartRef.current?.getEchartsInstance();
    if (!instance) return;
    if (label) {
      instance.dispatchAction({
        type: "highlight",
        seriesIndex: 0,
        name: label,
      });
    } else {
      instance.dispatchAction({
        type: "downplay",
        seriesIndex: 0,
      });
    }
  };

  return (
    <div className="flex flex-col items-center gap-4 py-4">
      <ReactEChartsCore
        ref={chartRef}
        echarts={echarts}
        option={option}
        style={{ height: 240, width: 240 }}
        notMerge
        lazyUpdate
        onEvents={onEvents}
      />

      {/* Legend */}
      <div className="flex flex-wrap justify-center gap-x-4 gap-y-1">
        {data.map((d) => (
          <div
            key={d.label}
            className="flex items-center gap-1.5"
            onMouseEnter={() => handleLegendHover(d.label)}
            onMouseLeave={() => handleLegendHover(null)}
          >
            <span
              className="inline-block w-2 h-2 rounded-full shrink-0"
              style={{ backgroundColor: d.color }}
            />
            <span
              className={`text-[0.75em] font-mono ${
                hoveredLabel !== null && hoveredLabel !== d.label
                  ? "opacity-40"
                  : c("text-text-muted", "text-light-text-muted")
              }`}
            >
              {d.label}
            </span>
            <span
              className={`text-[0.75em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              {formatChartValue(d.value)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
