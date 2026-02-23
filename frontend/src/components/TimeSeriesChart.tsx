import { useState } from "react";
import { scaleTime, scaleLinear } from "@visx/scale";
import { LinePath, AreaClosed } from "@visx/shape";
import { AxisBottom, AxisLeft } from "@visx/axis";
import { GridRows } from "@visx/grid";
import { Group } from "@visx/group";
import { ParentSize } from "@visx/responsive";
import { curveMonotoneX } from "@visx/curve";
import { useThemeClass } from "../hooks/useThemeClass";
import { SERIES_COLORS } from "./charts/chartColors";
import { ChartTooltip, useChartTooltip, type TooltipData } from "./charts/ChartTooltip";

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

const margin = { top: 20, right: 20, bottom: 50, left: 60 };

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
        color: SERIES_COLORS[colorIdx % SERIES_COLORS.length]!,
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
    color: SERIES_COLORS[i % SERIES_COLORS.length]!,
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

function TimeSeriesInner({
  columns,
  rows,
  dark,
  width,
}: TimeSeriesChartProps & { width: number }) {
  const c = useThemeClass(dark);
  const height = 240;
  const tooltip = useChartTooltip();
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const { series, timestamps, yMin, yMax } = parseData(columns, rows);
  if (series.length === 0) return null;

  const xMax = width - margin.left - margin.right;
  const yMaxPx = height - margin.top - margin.bottom;

  const yPad = (yMax - yMin) * 0.1 || 1;
  const yLo = yMin - yPad;
  const yHi = yMax + yPad;

  const xScale = scaleTime<number>({
    domain: [timestamps[0]!, timestamps[timestamps.length - 1]!],
    range: [0, xMax],
  });

  const yScale = scaleLinear<number>({
    domain: [yLo, yHi],
    range: [yMaxPx, 0],
    nice: true,
  });

  const xRange = (timestamps[timestamps.length - 1]! - timestamps[0]!) || 1;

  const formatTime = (ms: number) => {
    const d = new Date(ms);
    if (xRange > 24 * 60 * 60 * 1000) {
      return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    }
    if (xRange < 60 * 60 * 1000) {
      return d.toLocaleTimeString("en-US", {
        hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
      });
    }
    return d.toLocaleTimeString("en-US", {
      hour: "2-digit", minute: "2-digit", hour12: false,
    });
  };

  const formatYValue = (v: number) => {
    const abs = Math.abs(v);
    const sign = v < 0 ? "-" : "";
    if (abs >= 1_000_000) return `${sign}${(abs / 1_000_000).toFixed(1)}M`;
    if (abs >= 1_000) return `${sign}${(abs / 1_000).toFixed(1)}K`;
    return Number.isInteger(v) ? String(v) : v.toFixed(1);
  };

  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const mx = e.clientX - rect.left - margin.left;
    const dataX = xScale.invert(mx).getTime();
    let nearest = 0;
    let minDist = Infinity;
    for (let i = 0; i < timestamps.length; i++) {
      const dist = Math.abs(timestamps[i]! - dataX);
      if (dist < minDist) {
        minDist = dist;
        nearest = i;
      }
    }
    setHoverIdx(nearest);

    const hoverTs = timestamps[nearest]!;
    const items = series
      .map((s) => {
        const p = s.points.find((pt) => pt.x === hoverTs);
        return p ? { color: s.color, label: s.name, value: formatYValue(p.y) } : null;
      })
      .filter((x): x is NonNullable<typeof x> => x !== null);

    tooltip.showTooltip({
      tooltipData: { title: formatTime(hoverTs), items },
      tooltipLeft: e.clientX,
      tooltipTop: e.clientY,
    });
  };

  const singleSeries = series.length === 1;
  const legendHeight = series.length > 1 ? 30 : 0;

  return (
    <div className="relative">
      <svg
        width={width}
        height={height + legendHeight}
        className="select-none"
        onMouseMove={handleMouseMove}
        onMouseLeave={() => {
          setHoverIdx(null);
          tooltip.hideTooltip();
        }}
      >
        <Group left={margin.left} top={margin.top}>
          <GridRows
            scale={yScale}
            width={xMax}
            stroke={dark ? "rgba(255,255,255,0.06)" : "rgba(0,0,0,0.06)"}
            numTicks={5}
          />

          {/* Area fill (single series only) */}
          {singleSeries && (
            <AreaClosed
              data={series[0]!.points}
              x={(d) => xScale(d.x) ?? 0}
              y={(d) => yScale(d.y) ?? 0}
              yScale={yScale}
              fill={series[0]!.color}
              opacity={0.15}
              curve={curveMonotoneX}
            />
          )}

          {/* Lines */}
          {series.map((s) => (
            <LinePath
              key={s.name}
              data={s.points}
              x={(d) => xScale(d.x) ?? 0}
              y={(d) => yScale(d.y) ?? 0}
              stroke={s.color}
              strokeWidth={singleSeries ? 2 : 1.5}
              strokeLinejoin="round"
              curve={curveMonotoneX}
            />
          ))}

          {/* Hover crosshair */}
          {hoverIdx !== null && (
            <line
              x1={xScale(timestamps[hoverIdx]!)}
              y1={0}
              x2={xScale(timestamps[hoverIdx]!)}
              y2={yMaxPx}
              stroke={dark ? "rgba(255,255,255,0.3)" : "rgba(0,0,0,0.2)"}
              strokeDasharray="3,3"
            />
          )}

          {/* Hover dots */}
          {hoverIdx !== null &&
            series.map((s) => {
              const p = s.points.find((pt) => pt.x === timestamps[hoverIdx]!);
              if (!p) return null;
              return (
                <circle
                  key={s.name}
                  cx={xScale(p.x)}
                  cy={yScale(p.y)}
                  r={3.5}
                  fill={s.color}
                  stroke={dark ? "#1a1a1a" : "#fff"}
                  strokeWidth={1.5}
                />
              );
            })}

          <AxisLeft
            scale={yScale}
            numTicks={5}
            tickFormat={(v) => formatYValue(v as number)}
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
            top={yMaxPx}
            numTicks={5}
            tickFormat={(v) => formatTime((v as Date).getTime())}
            stroke="transparent"
            tickStroke="transparent"
            tickLabelProps={{
              className: `text-[0.6em] font-mono ${c("fill-text-ghost", "fill-light-text-ghost")}`,
              textAnchor: "middle" as const,
              dy: 4,
            }}
          />
        </Group>

        {/* Legend (multi-series) */}
        {series.length > 1 &&
          series.map((s, i) => {
            const lx = margin.left + i * 100;
            const ly = height + 15;
            return (
              <g key={s.name}>
                <rect x={lx} y={ly - 5} width={10} height={10} rx={2} fill={s.color} />
                <text
                  x={lx + 14}
                  y={ly + 4}
                  className={`text-[0.6em] font-mono ${c("fill-text-muted", "fill-light-text-muted")}`}
                >
                  {s.name}
                </text>
              </g>
            );
          })}
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

export function TimeSeriesChart(props: Readonly<TimeSeriesChartProps>) {
  return (
    <ParentSize>
      {({ width }) =>
        width > 0 ? <TimeSeriesInner {...props} width={width} /> : null
      }
    </ParentSize>
  );
}
