import { useState } from "react";
import { arc as d3Arc } from "d3-shape";
import { useSpring, animated, to } from "@react-spring/web";
import { Pie } from "@visx/shape";
import { Group } from "@visx/group";
import { useThemeClass } from "../../hooks/useThemeClass";
import { getColorForCategory } from "./chartColors";
import { ChartTooltip, useChartTooltip, type TooltipData } from "./ChartTooltip";

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

const SIZE = 240;
const OUTER_RADIUS = SIZE / 2 - 8;
const INNER_RADIUS = OUTER_RADIUS * 0.6;

/**
 * An arc path that smoothly animates angle and radius changes via react-spring.
 */
function AnimatedDonutArc({
  startAngle,
  endAngle,
  innerRadius,
  outerRadius,
  padAngle,
  cornerRadius,
  ...rest
}: {
  startAngle: number;
  endAngle: number;
  innerRadius: number;
  outerRadius: number;
  padAngle: number;
  cornerRadius: number;
} & Omit<React.SVGProps<SVGPathElement>, "d">) {
  const spring = useSpring({
    startAngle,
    endAngle,
    outerRadius,
    config: { tension: 210, friction: 20 },
  });

  return (
    <animated.path
      d={to(
        [spring.startAngle, spring.endAngle, spring.outerRadius],
        (s: number, e: number, r: number) =>
          d3Arc()
            .cornerRadius(cornerRadius)(
            { startAngle: s, endAngle: e, innerRadius, outerRadius: r, padAngle } as any,
          ) ?? "",
      )}
      {...rest}
    />
  );
}

export function DonutChart({ columns, rows, dark }: Readonly<DonutChartProps>) {
  const c = useThemeClass(dark);
  const tooltip = useChartTooltip();
  const [hoveredLabel, setHoveredLabel] = useState<string | null>(null);

  const valueColIdx = columns.length - 1;
  const data: Datum[] = rows.map((row, i) => {
    const label = row.slice(0, columns.length - 1).join(" / ") || row[0] || "";
    return {
      label,
      value: Number(row[valueColIdx]) || 0,
      color: getColorForCategory(label, i),
    };
  });

  const total = data.reduce((sum, d) => sum + d.value, 0);

  const formatValue = (v: number) => {
    if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
    if (v >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
    return Number.isInteger(v) ? String(v) : v.toFixed(1);
  };

  return (
    <div className="flex flex-col items-center gap-4 py-4 relative">
      <svg width={SIZE} height={SIZE}>
        <Group top={SIZE / 2} left={SIZE / 2}>
          <Pie
            data={data}
            pieValue={(d) => d.value}
            outerRadius={OUTER_RADIUS}
            innerRadius={INNER_RADIUS}
            padAngle={0.02}
            cornerRadius={3}
          >
            {(pie) =>
              pie.arcs.map((arc) => (
                <AnimatedDonutArc
                  key={arc.data.label}
                  startAngle={arc.startAngle}
                  endAngle={arc.endAngle}
                  innerRadius={INNER_RADIUS}
                  outerRadius={hoveredLabel === arc.data.label ? OUTER_RADIUS + 4 : OUTER_RADIUS}
                  padAngle={0.02}
                  cornerRadius={3}
                  fill={arc.data.color}
                  opacity={
                    hoveredLabel === null || hoveredLabel === arc.data.label
                      ? 0.85
                      : 0.4
                  }
                  className="transition-opacity"
                  onMouseMove={(e) => {
                    setHoveredLabel(arc.data.label);
                    tooltip.showTooltip({
                      tooltipData: {
                        title: arc.data.label,
                        items: [{
                          color: arc.data.color,
                          label: columns[valueColIdx]!,
                          value: `${formatValue(arc.data.value)} (${((arc.data.value / total) * 100).toFixed(1)}%)`,
                        }],
                      },
                      tooltipLeft: e.clientX,
                      tooltipTop: e.clientY,
                    });
                  }}
                  onMouseLeave={() => {
                    setHoveredLabel(null);
                    tooltip.hideTooltip();
                  }}
                />
              ))
            }
          </Pie>
          {/* Center total */}
          <text
            textAnchor="middle"
            dy="-0.2em"
            className={`text-[1.5em] font-mono font-semibold ${c("fill-text-bright", "fill-light-text-bright")}`}
          >
            {formatValue(total)}
          </text>
          <text
            textAnchor="middle"
            dy="1.2em"
            className={`text-[0.65em] font-mono ${c("fill-text-ghost", "fill-light-text-ghost")}`}
          >
            total
          </text>
        </Group>
      </svg>

      {/* Legend */}
      <div className="flex flex-wrap justify-center gap-x-4 gap-y-1">
        {data.map((d) => (
          <div
            key={d.label}
            className="flex items-center gap-1.5"
            onMouseEnter={() => setHoveredLabel(d.label)}
            onMouseLeave={() => setHoveredLabel(null)}
          >
            <span
              className="inline-block w-2 h-2 rounded-full shrink-0"
              style={{ backgroundColor: d.color }}
            />
            <span
              className={`text-[0.75em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
            >
              {d.label}
            </span>
            <span
              className={`text-[0.75em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              {formatValue(d.value)}
            </span>
          </div>
        ))}
      </div>

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
