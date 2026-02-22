import { useState, useRef, useMemo, useEffect } from "react";
import { useThemeClass } from "../hooks/useThemeClass";

interface TimeSeriesChartProps {
  columns: string[];
  rows: string[][];
  dark: boolean;
}

// Color cycle for multi-series — uses CSS variables matching the severity palette.
const SERIES_COLORS = [
  "var(--color-copper)",
  "var(--color-severity-error)",
  "var(--color-severity-warn)",
  "var(--color-severity-info)",
  "var(--color-severity-debug)",
  "var(--color-severity-trace)",
];

interface Series {
  name: string;
  points: { x: number; y: number }[];
  color: string;
}

export function TimeSeriesChart({
  columns,
  rows,
  dark,
}: Readonly<TimeSeriesChartProps>) {
  const c = useThemeClass(dark);
  const containerRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(600);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  // Responsive width via ResizeObserver.
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const observer = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width;
      if (w && w > 0) setWidth(w);
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const height = 240;
  const padding = { top: 20, right: 20, bottom: 50, left: 60 };
  const plotW = width - padding.left - padding.right;
  const plotH = height - padding.top - padding.bottom;

  // Parse data into series.
  const { series, timestamps, yMin, yMax } = useMemo(() => {
    if (columns.length < 2 || rows.length === 0) {
      return { series: [] as Series[], timestamps: [] as number[], yMin: 0, yMax: 1 };
    }

    const timeCol = 0;
    const timestamps = rows.map((r) => new Date(r[timeCol]!).getTime());

    // Detect 3-column pivot: [time, group, agg]
    if (
      columns.length === 3 &&
      rows.some((r) => isNaN(Number(r[1])))
    ) {
      // Pivot: group by column 1, aggregate column 2
      const groups = new Map<string, { x: number; y: number }[]>();
      const uniqueTimes = [...new Set(timestamps)].sort((a, b) => a - b);

      for (const row of rows) {
        const t = new Date(row[timeCol]!).getTime();
        const group = row[1] ?? "";
        const val = Number(row[2]) || 0;
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
      points: rows.map((r, j) => ({
        x: timestamps[j]!,
        y: Number(r[i + 1]) || 0,
      })),
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
  }, [columns, rows]);

  if (series.length === 0) return null;

  const xMin = timestamps[0]!;
  const xMax = timestamps[timestamps.length - 1]!;
  const xRange = xMax - xMin || 1;
  // Add 10% padding to y-axis
  const yPad = (yMax - yMin) * 0.1 || 1;
  const yLo = Math.max(0, yMin - yPad);
  const yHi = yMax + yPad;
  const yRange = yHi - yLo || 1;

  const scaleX = (t: number) => padding.left + ((t - xMin) / xRange) * plotW;
  const scaleY = (v: number) => padding.top + plotH - ((v - yLo) / yRange) * plotH;

  const buildPath = (points: { x: number; y: number }[]) =>
    points
      .map((p, i) => `${i === 0 ? "M" : "L"}${scaleX(p.x).toFixed(1)},${scaleY(p.y).toFixed(1)}`)
      .join(" ");

  const buildArea = (points: { x: number; y: number }[]) => {
    if (points.length === 0) return "";
    const baseline = scaleY(yLo);
    const line = points
      .map((p, i) => `${i === 0 ? "M" : "L"}${scaleX(p.x).toFixed(1)},${scaleY(p.y).toFixed(1)}`)
      .join(" ");
    const last = points[points.length - 1]!;
    const first = points[0]!;
    return `${line} L${scaleX(last.x).toFixed(1)},${baseline} L${scaleX(first.x).toFixed(1)},${baseline} Z`;
  };

  // Y-axis ticks (5 ticks).
  const yTicks: number[] = [];
  for (let i = 0; i <= 4; i++) {
    yTicks.push(yLo + (yRange * i) / 4);
  }

  // X-axis ticks (up to 5).
  const xTickCount = Math.min(5, timestamps.length);
  const xTicks: number[] = [];
  for (let i = 0; i < xTickCount; i++) {
    const idx = Math.round((i / (xTickCount - 1 || 1)) * (timestamps.length - 1));
    xTicks.push(timestamps[idx]!);
  }

  const formatTime = (ms: number) => {
    const d = new Date(ms);
    const rangeMs = xMax - xMin;
    if (rangeMs > 24 * 60 * 60 * 1000) {
      return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    }
    if (rangeMs < 60 * 60 * 1000) {
      return d.toLocaleTimeString("en-US", {
        hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
      });
    }
    return d.toLocaleTimeString("en-US", {
      hour: "2-digit", minute: "2-digit", hour12: false,
    });
  };

  const formatYValue = (v: number) => {
    if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
    if (v >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
    return Number.isInteger(v) ? String(v) : v.toFixed(1);
  };

  // Find nearest x-index for hover.
  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const dataX = xMin + ((mx - padding.left) / plotW) * xRange;
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
  };

  const hoverX = hoverIdx !== null ? scaleX(timestamps[hoverIdx]!) : null;
  const singleSeries = series.length === 1;

  return (
    <div ref={containerRef} className="w-full">
      <svg
        width={width}
        height={height + (series.length > 1 ? 30 : 0)}
        className="select-none"
        onMouseMove={handleMouseMove}
        onMouseLeave={() => setHoverIdx(null)}
      >
        {/* Grid lines */}
        {yTicks.map((tick) => (
          <line
            key={tick}
            x1={padding.left}
            y1={scaleY(tick)}
            x2={width - padding.right}
            y2={scaleY(tick)}
            stroke={dark ? "rgba(255,255,255,0.06)" : "rgba(0,0,0,0.06)"}
          />
        ))}

        {/* Area fills (single series only) */}
        {singleSeries && (
          <path
            d={buildArea(series[0]!.points)}
            fill={series[0]!.color}
            opacity={0.15}
          />
        )}

        {/* Lines */}
        {series.map((s) => (
          <path
            key={s.name}
            d={buildPath(s.points)}
            fill="none"
            stroke={s.color}
            strokeWidth={singleSeries ? 2 : 1.5}
            strokeLinejoin="round"
          />
        ))}

        {/* Hover crosshair */}
        {hoverX !== null && (
          <line
            x1={hoverX}
            y1={padding.top}
            x2={hoverX}
            y2={padding.top + plotH}
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
                cx={scaleX(p.x)}
                cy={scaleY(p.y)}
                r={3.5}
                fill={s.color}
                stroke={dark ? "#1a1a1a" : "#fff"}
                strokeWidth={1.5}
              />
            );
          })}

        {/* Y-axis labels */}
        {yTicks.map((tick) => (
          <text
            key={tick}
            x={padding.left - 8}
            y={scaleY(tick) + 4}
            textAnchor="end"
            className={`text-[0.6em] font-mono ${c("fill-text-ghost", "fill-light-text-ghost")}`}
          >
            {formatYValue(tick)}
          </text>
        ))}

        {/* X-axis labels */}
        {xTicks.map((tick, i) => (
          <text
            key={i}
            x={scaleX(tick)}
            y={height - 8}
            textAnchor="middle"
            className={`text-[0.6em] font-mono ${c("fill-text-ghost", "fill-light-text-ghost")}`}
          >
            {formatTime(tick)}
          </text>
        ))}

        {/* Legend (multi-series) */}
        {series.length > 1 &&
          series.map((s, i) => {
            const lx = padding.left + i * 100;
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

      {/* Tooltip */}
      {hoverIdx !== null && (
        <div
          className={`absolute pointer-events-none px-2 py-1 rounded text-[0.75em] font-mono whitespace-nowrap z-10 ${c(
            "bg-ink-surface text-text-bright border border-ink-border-subtle",
            "bg-light-surface text-light-text-bright border border-light-border-subtle",
          )}`}
          style={{
            left: Math.min(hoverX! + 12, width - 150),
            top: padding.top,
          }}
        >
          <div className={c("text-text-ghost", "text-light-text-ghost")}>
            {formatTime(timestamps[hoverIdx]!)}
          </div>
          {series.map((s) => {
            const p = s.points.find((pt) => pt.x === timestamps[hoverIdx]!);
            return p ? (
              <div key={s.name} className="flex items-center gap-1.5">
                <span
                  className="inline-block w-1.5 h-1.5 rounded-full"
                  style={{ backgroundColor: s.color }}
                />
                {series.length > 1 && (
                  <span className="opacity-70">{s.name}</span>
                )}
                <span>{formatYValue(p.y)}</span>
              </div>
            ) : null;
          })}
        </div>
      )}
    </div>
  );
}
