import { useRef, useState, useMemo } from "react";
import type { ChunkMeta } from "../../api/gen/gastrolog/v1/store_pb";

interface ChunkTimelineProps {
  chunks: ChunkMeta[];
  dark: boolean;
  selectedChunkId?: string | null;
  onChunkClick?: (chunkId: string) => void;
}

export function ChunkTimeline({
  chunks,
  dark,
  selectedChunkId,
  onChunkClick,
}: ChunkTimelineProps) {
  const c = (d: string, l: string) => (dark ? d : l);
  const containerRef = useRef<HTMLDivElement>(null);
  const [hoveredChunk, setHoveredChunk] = useState<string | null>(null);

  const { bars, ticks, minTime, timeSpan } = useMemo(() => {
    if (chunks.length === 0) return { bars: [], ticks: [], minTime: 0, timeSpan: 0 };

    // Compute global time range across all chunks.
    let globalMin = Infinity;
    let globalMax = -Infinity;

    const parsed = chunks
      .map((chunk) => {
        const start = chunk.startTs?.toDate().getTime() ?? 0;
        const end = chunk.endTs?.toDate().getTime() ?? start;
        if (start === 0 && end === 0) return null;
        return { id: chunk.id, start, end, sealed: chunk.sealed, recordCount: chunk.recordCount, bytes: chunk.bytes };
      })
      .filter(Boolean) as {
        id: string;
        start: number;
        end: number;
        sealed: boolean;
        recordCount: bigint;
        bytes: bigint;
      }[];

    if (parsed.length === 0) return { bars: [], ticks: [], minTime: 0, timeSpan: 0 };

    for (const p of parsed) {
      if (p.start < globalMin) globalMin = p.start;
      if (p.end > globalMax) globalMax = p.end;
    }

    // Add 2% padding on each side.
    const rawSpan = globalMax - globalMin || 60_000; // at least 1 minute
    const padding = rawSpan * 0.02;
    const min = globalMin - padding;
    const span = rawSpan + padding * 2;

    // Sort oldest first (bottom to top visually, but we render top to bottom).
    parsed.sort((a, b) => a.start - b.start);

    const bars = parsed.map((p) => ({
      ...p,
      x: (p.start - min) / span,
      w: Math.max((p.end - p.start) / span, 0.003), // min 0.3% width so tiny chunks are visible
    }));

    // Generate time axis ticks.
    const ticks = generateTicks(min, min + span, 6);

    return { bars, ticks, minTime: min, timeSpan: span };
  }, [chunks]);

  if (bars.length === 0) return null;

  const barHeight = 20;
  const barGap = 3;
  const axisHeight = 22;
  const topPad = 4;
  const chartHeight = topPad + bars.length * (barHeight + barGap) - barGap + axisHeight;

  return (
    <div ref={containerRef} className="w-full px-4 pt-3 pb-1">
      <div
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Timeline
      </div>
      <svg
        width="100%"
        height={chartHeight}
        viewBox={`0 0 1000 ${chartHeight}`}
        preserveAspectRatio="none"
        className="block"
      >
        {/* Grid lines at tick positions */}
        {ticks.map((tick, i) => (
          <line
            key={i}
            x1={tick.x * 1000}
            y1={topPad}
            x2={tick.x * 1000}
            y2={chartHeight - axisHeight}
            stroke={dark ? "#222838" : "#e4ddd4"}
            strokeWidth="1"
            vectorEffect="non-scaling-stroke"
          />
        ))}

        {/* Chunk bars */}
        {bars.map((bar, i) => {
          const y = topPad + i * (barHeight + barGap);
          const isHovered = hoveredChunk === bar.id;
          const isSelected = selectedChunkId === bar.id;

          return (
            <g
              key={bar.id}
              className="cursor-pointer"
              onMouseEnter={() => setHoveredChunk(bar.id)}
              onMouseLeave={() => setHoveredChunk(null)}
              onClick={() => onChunkClick?.(bar.id)}
            >
              {/* Bar */}
              <rect
                x={bar.x * 1000}
                y={y}
                width={bar.w * 1000}
                height={barHeight}
                rx="3"
                ry="3"
                fill={
                  bar.sealed
                    ? isHovered || isSelected
                      ? dark ? "#d4a070" : "#c8875c"
                      : dark ? "#c8875c" : "#a06b44"
                    : isHovered || isSelected
                      ? "#6aaa7a"
                      : "#5a9a6a"
                }
                opacity={isHovered || isSelected ? 1 : 0.8}
                vectorEffect="non-scaling-stroke"
                stroke={isSelected ? (dark ? "#f0d0a0" : "#7a4a28") : "none"}
                strokeWidth={isSelected ? "2" : "0"}
              />

              {/* Active chunk pulse indicator */}
              {!bar.sealed && (
                <circle
                  cx={bar.x * 1000 + bar.w * 1000 - 2}
                  cy={y + barHeight / 2}
                  r="3"
                  fill="#5a9a6a"
                  vectorEffect="non-scaling-stroke"
                >
                  <animate
                    attributeName="opacity"
                    values="1;0.3;1"
                    dur="2s"
                    repeatCount="indefinite"
                  />
                </circle>
              )}

              {/* Chunk ID label (only if bar is wide enough) */}
              {bar.w * 1000 > 60 && (
                <text
                  x={bar.x * 1000 + 6}
                  y={y + barHeight / 2}
                  dominantBaseline="central"
                  fontSize="10"
                  fontFamily="var(--font-mono, monospace)"
                  fill={dark ? "#0d0f12" : "#faf8f4"}
                  opacity={0.9}
                  style={{ pointerEvents: "none" }}
                >
                  {bar.id.slice(0, 8)}
                </text>
              )}
            </g>
          );
        })}

        {/* Time axis */}
        <line
          x1="0"
          y1={chartHeight - axisHeight}
          x2="1000"
          y2={chartHeight - axisHeight}
          stroke={dark ? "#2a3040" : "#d8d0c4"}
          strokeWidth="1"
          vectorEffect="non-scaling-stroke"
        />
        {ticks.map((tick, i) => (
          <g key={i}>
            <line
              x1={tick.x * 1000}
              y1={chartHeight - axisHeight}
              x2={tick.x * 1000}
              y2={chartHeight - axisHeight + 4}
              stroke={dark ? "#2a3040" : "#d8d0c4"}
              strokeWidth="1"
              vectorEffect="non-scaling-stroke"
            />
            <text
              x={tick.x * 1000}
              y={chartHeight - 4}
              textAnchor="middle"
              fontSize="9"
              fontFamily="var(--font-mono, monospace)"
              fill={dark ? "#555d70" : "#958e84"}
              style={{ pointerEvents: "none" }}
            >
              {tick.label}
            </text>
          </g>
        ))}
      </svg>

      {/* Tooltip */}
      {hoveredChunk && (
        <ChunkTooltip
          chunk={bars.find((b) => b.id === hoveredChunk)!}
          dark={dark}
        />
      )}
    </div>
  );
}

function ChunkTooltip({
  chunk,
  dark,
}: {
  chunk: {
    id: string;
    start: number;
    end: number;
    sealed: boolean;
    recordCount: bigint;
    bytes: bigint;
  };
  dark: boolean;
}) {
  const c = (d: string, l: string) => (dark ? d : l);
  const start = new Date(chunk.start);
  const end = new Date(chunk.end);
  const duration = chunk.end - chunk.start;

  return (
    <div
      className={`mt-1 px-3 py-2 rounded text-[0.8em] border ${c(
        "bg-ink-raised border-ink-border-subtle",
        "bg-light-raised border-light-border-subtle",
      )}`}
    >
      <div className="flex items-center gap-2 mb-1">
        <span
          className={`font-mono font-medium ${c("text-text-bright", "text-light-text-bright")}`}
        >
          {chunk.id}
        </span>
        {chunk.sealed ? (
          <span className="px-1.5 py-0.5 text-[0.85em] rounded bg-copper/15 text-copper">
            sealed
          </span>
        ) : (
          <span className="px-1.5 py-0.5 text-[0.85em] rounded bg-severity-info/15 text-severity-info">
            active
          </span>
        )}
      </div>
      <div
        className={`flex gap-4 ${c("text-text-muted", "text-light-text-muted")}`}
      >
        <span>{formatTimeShort(start)} &rarr; {formatTimeShort(end)}</span>
        <span>{formatDuration(duration)}</span>
        <span className="font-mono">{Number(chunk.recordCount).toLocaleString()} records</span>
        <span className="font-mono">{formatBytes(Number(chunk.bytes))}</span>
      </div>
    </div>
  );
}

// --- Time utilities ---

function generateTicks(
  minMs: number,
  maxMs: number,
  targetCount: number,
): { x: number; label: string }[] {
  const span = maxMs - minMs;
  if (span <= 0) return [];

  // Pick a nice tick interval.
  const rawInterval = span / targetCount;
  const niceIntervals = [
    1_000, 2_000, 5_000, 10_000, 15_000, 30_000, // seconds
    60_000, 2 * 60_000, 5 * 60_000, 10 * 60_000, 15 * 60_000, 30 * 60_000, // minutes
    3600_000, 2 * 3600_000, 4 * 3600_000, 6 * 3600_000, 12 * 3600_000, // hours
    86400_000, 2 * 86400_000, 7 * 86400_000, // days
    30 * 86400_000, 90 * 86400_000, 365 * 86400_000, // months/years
  ];

  let interval = niceIntervals[0];
  for (const ni of niceIntervals) {
    if (ni >= rawInterval) {
      interval = ni;
      break;
    }
    interval = ni;
  }

  const ticks: { x: number; label: string }[] = [];
  const firstTick = Math.ceil(minMs / interval) * interval;

  for (let t = firstTick; t <= maxMs; t += interval) {
    ticks.push({
      x: (t - minMs) / span,
      label: formatTickLabel(t, interval),
    });
  }

  return ticks;
}

function formatTickLabel(ms: number, interval: number): string {
  const d = new Date(ms);
  if (interval >= 86400_000) {
    return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  }
  if (interval >= 3600_000) {
    return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
  }
  return d.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatTimeShort(d: Date): string {
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatDuration(ms: number): string {
  if (ms < 1_000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(0)}s`;
  if (ms < 3600_000) return `${(ms / 60_000).toFixed(0)}m`;
  if (ms < 86400_000) {
    const h = Math.floor(ms / 3600_000);
    const m = Math.floor((ms % 3600_000) / 60_000);
    return m > 0 ? `${h}h ${m}m` : `${h}h`;
  }
  const d = Math.floor(ms / 86400_000);
  const h = Math.floor((ms % 86400_000) / 3600_000);
  return h > 0 ? `${d}d ${h}h` : `${d}d`;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024)
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}
