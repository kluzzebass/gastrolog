import { useState } from "react";
import { scaleBand, scaleLinear } from "@visx/scale";
import { AxisBottom, AxisLeft } from "@visx/axis";
import { GridRows } from "@visx/grid";
import { Group } from "@visx/group";
import { ParentSize } from "@visx/responsive";
import { useThemeClass } from "../../hooks/useThemeClass";
import { AnimatedBar } from "./AnimatedBar";
import { getColorForCategory } from "./chartColors";
import { ChartTooltip, useChartTooltip, type TooltipData } from "./ChartTooltip";

interface BarChartProps {
  columns: string[];
  rows: string[][];
  dark: boolean;
}

interface Datum {
  label: string;
  value: number;
}

const margin = { top: 16, right: 16, bottom: 56, left: 60 };

function BarChartInner({
  columns,
  rows,
  dark,
  width,
}: BarChartProps & { width: number }) {
  const c = useThemeClass(dark);
  const height = 280;
  const tooltip = useChartTooltip();
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);

  // Build data: first column(s) are group labels, last column is the numeric aggregate.
  const labelColIdx = columns.length - 2; // last non-numeric column
  const valueColIdx = columns.length - 1;
  const data: Datum[] = rows.map((row) => ({
    label: columns.length > 2
      ? row.slice(0, columns.length - 1).join(" / ")
      : row[labelColIdx] ?? "",
    value: Number(row[valueColIdx]) || 0,
  }));

  const xMax = width - margin.left - margin.right;
  const yMax = height - margin.top - margin.bottom;

  const xScale = scaleBand<string>({
    domain: data.map((d) => d.label),
    range: [0, xMax],
    padding: 0.25,
  });

  const yScale = scaleLinear<number>({
    domain: [0, Math.max(...data.map((d) => d.value), 1) * 1.1],
    range: [yMax, 0],
    nice: true,
  });

  const formatValue = (v: number) => {
    const abs = Math.abs(v);
    const sign = v < 0 ? "-" : "";
    if (abs >= 1_000_000) return `${sign}${(abs / 1_000_000).toFixed(1)}M`;
    if (abs >= 1_000) return `${sign}${(abs / 1_000).toFixed(1)}K`;
    return Number.isInteger(v) ? String(v) : v.toFixed(1);
  };

  return (
    <div className="relative">
      <svg width={width} height={height}>
        <Group left={margin.left} top={margin.top}>
          <GridRows
            scale={yScale}
            width={xMax}
            stroke={dark ? "rgba(255,255,255,0.06)" : "rgba(0,0,0,0.06)"}
            numTicks={5}
          />
          {data.map((d, i) => {
            const barWidth = xScale.bandwidth();
            const barHeight = yMax - (yScale(d.value) ?? 0);
            const barX = xScale(d.label) ?? 0;
            const barY = yMax - barHeight;
            return (
              <AnimatedBar
                key={d.label}
                x={barX}
                y={barY}
                width={barWidth}
                height={barHeight}
                fill={getColorForCategory(d.label, i)}
                opacity={hoveredIdx === null || hoveredIdx === i ? 0.85 : 0.4}
                rx={2}
                onMouseMove={(e) => {
                  setHoveredIdx(i);
                  tooltip.showTooltip({
                    tooltipData: {
                      title: d.label,
                      items: [{
                        color: getColorForCategory(d.label, i),
                        label: columns[valueColIdx]!,
                        value: formatValue(d.value),
                      }],
                    },
                    tooltipLeft: e.clientX,
                    tooltipTop: e.clientY,
                  });
                }}
                onMouseLeave={() => {
                  setHoveredIdx(null);
                  tooltip.hideTooltip();
                }}
              />
            );
          })}
          <AxisLeft
            scale={yScale}
            numTicks={5}
            tickFormat={(v) => formatValue(v as number)}
            stroke="transparent"
            tickStroke="transparent"
            tickLabelProps={{
              className: `text-[0.6em] font-mono ${c("fill-text-ghost", "fill-light-text-ghost")}`,
              textAnchor: "end" as const,
              dx: -4,
              dy: 3,
            }}
          />
          <AxisBottom
            scale={xScale}
            top={yMax}
            stroke="transparent"
            tickStroke="transparent"
            tickLabelProps={{
              className: `text-[0.6em] font-mono ${c("fill-text-ghost", "fill-light-text-ghost")}`,
              textAnchor: "end" as const,
              angle: -35,
              dx: -4,
              dy: 0,
            }}
          />
        </Group>
      </svg>
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

export function BarChart(props: Readonly<BarChartProps>) {
  return (
    <ParentSize>
      {({ width }) =>
        width > 0 ? <BarChartInner {...props} width={width} /> : null
      }
    </ParentSize>
  );
}
