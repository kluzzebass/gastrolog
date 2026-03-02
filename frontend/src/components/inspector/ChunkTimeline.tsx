import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import type { ChunkMeta } from "../../api/gen/gastrolog/v1/vault_pb";
import { formatBytes, formatDurationMs } from "../../utils/units";
import { Badge } from "../Badge";

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
}: Readonly<ChunkTimelineProps>) {
  const c = useThemeClass(dark);
  const [hoveredChunk, setHoveredChunk] = useState<string | null>(null);

  const { bars, ticks } = (() => {
    if (chunks.length === 0) return { bars: [], ticks: [] };

    let globalMin = Infinity;
    let globalMax = -Infinity;

    const parsed = chunks
      .map((chunk) => {
        const start = chunk.startTs?.toDate().getTime() ?? 0;
        const end = chunk.endTs?.toDate().getTime() ?? start;
        if (start === 0 && end === 0) return null;
        return {
          id: chunk.id,
          start,
          end,
          sealed: chunk.sealed,
          recordCount: chunk.recordCount,
          bytes: chunk.bytes,
          compressed: chunk.compressed,
          diskBytes: chunk.diskBytes,
        };
      })
      .filter(Boolean) as {
      id: string;
      start: number;
      end: number;
      sealed: boolean;
      recordCount: bigint;
      bytes: bigint;
      compressed: boolean;
      diskBytes: bigint;
    }[];

    if (parsed.length === 0) return { bars: [], ticks: [] };

    for (const p of parsed) {
      if (p.start < globalMin) globalMin = p.start;
      if (p.end > globalMax) globalMax = p.end;
    }

    const rawSpan = globalMax - globalMin || 60_000;
    const padding = rawSpan * 0.02;
    const min = globalMin - padding;
    const span = rawSpan + padding * 2;

    // Sort by start time.
    parsed.sort((a, b) => a.start - b.start);

    const minGap = 0.003; // minimum gap between bars so rounded corners don't overlap

    const bars = parsed.map((p, i) => {
      const x = (p.start - min) / span;
      let w = Math.max((p.end - p.start) / span, 0.005); // min 0.5% so tiny chunks stay visible

      // Ensure at least minGap before the next bar.
      if (i < parsed.length - 1) {
        const nextX = (parsed[i + 1]!.start - min) / span;
        const gap = nextX - (x + w);
        if (gap >= 0 && gap < minGap) {
          w = Math.max(nextX - minGap - x, 0.003);
        }
      }

      return { ...p, x, w };
    });

    const ticks = generateTicks(min, min + span, 6);

    return { bars, ticks };
  })();

  if (bars.length === 0) return null;

  const barHeight = 24;
  const tickMarkHeight = 4;
  const topPad = 2;
  const svgHeight = topPad + barHeight + tickMarkHeight;

  // Precompute theme-dependent colors outside the map callback.
  const sealedHi = dark ? "#d4a070" : "#c8875c";
  const sealedLo = dark ? "#c8875c" : "#a06b44";
  const selectedStroke = dark ? "#f0d0a0" : "#7a4a28";

  return (
    <div className="w-full px-4 pt-3 pb-1">
      <div
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Timeline
      </div>
      <div className="relative">
        <svg
          width="100%"
          height={svgHeight}
          viewBox={`0 0 1000 ${svgHeight}`}
          preserveAspectRatio="none"
          className="block"
        >
          {/* Grid lines */}
          {ticks.map((tick) => (
            <line
              key={`grid-${tick.x}`}
              x1={tick.x * 1000}
              y1={topPad}
              x2={tick.x * 1000}
              y2={topPad + barHeight}
              stroke={dark ? "#222838" : "#e4ddd4"}
              strokeWidth="1"
              vectorEffect="non-scaling-stroke"
            />
          ))}

          {/* Single row of chunk bars */}
          {bars.map((bar) => {
            const isHovered = hoveredChunk === bar.id;
            const isSelected = selectedChunkId === bar.id;
            const highlighted = isHovered || isSelected;

            const sealedFill = highlighted ? sealedHi : sealedLo;
            const activeFill = highlighted ? "#6aaa7a" : "#5a9a6a";
            const fill = bar.sealed ? sealedFill : activeFill;
            const strokeColor = isSelected ? selectedStroke : "none";

            return (
              <g
                key={bar.id}
                className="cursor-pointer"
                role="button"
                tabIndex={0}
                aria-label={`Chunk ${bar.id}`}
                onMouseEnter={() => setHoveredChunk(bar.id)}
                onMouseLeave={() => setHoveredChunk(null)}
                onClick={() => onChunkClick?.(bar.id)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    onChunkClick?.(bar.id);
                  }
                }}
              >
                <rect
                  x={bar.x * 1000}
                  y={topPad}
                  width={bar.w * 1000}
                  height={barHeight}
                  rx="2"
                  ry="2"
                  fill={fill}
                  opacity={highlighted ? 1 : 0.75}
                  vectorEffect="non-scaling-stroke"
                  stroke={strokeColor}
                  strokeWidth={isSelected ? "2" : "0"}
                />

                {/* Active chunk pulse */}
                {!bar.sealed && (
                  <circle
                    cx={bar.x * 1000 + bar.w * 1000 - 4}
                    cy={topPad + barHeight / 2}
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
              </g>
            );
          })}

          {/* Time axis line + tick marks */}
          <line
            x1="0"
            y1={topPad + barHeight}
            x2="1000"
            y2={topPad + barHeight}
            stroke={dark ? "#2a3040" : "#d8d0c4"}
            strokeWidth="1"
            vectorEffect="non-scaling-stroke"
          />
          {ticks.map((tick) => (
            <line
              key={`tick-${tick.x}`}
              x1={tick.x * 1000}
              y1={topPad + barHeight}
              x2={tick.x * 1000}
              y2={topPad + barHeight + tickMarkHeight}
              stroke={dark ? "#2a3040" : "#d8d0c4"}
              strokeWidth="1"
              vectorEffect="non-scaling-stroke"
            />
          ))}
        </svg>

        {/* Tick labels as HTML so they aren't distorted by preserveAspectRatio="none" */}
        <div className="relative h-4" aria-hidden>
          {ticks.map((tick) => (
            <span
              key={`label-${tick.x}`}
              className={`absolute top-0 font-mono text-[0.65em] ${c("text-text-ghost", "text-light-text-ghost")}`}
              style={{ left: `${tick.x * 100}%`, transform: "translateX(-50%)" }}
            >
              {tick.label}
            </span>
          ))}
        </div>
      </div>

      {/* Tooltip — always reserve space to prevent layout shift */}
      <div
        className={`transition-opacity duration-100 ${hoveredChunk ? "opacity-100" : "opacity-0"}`}
        aria-hidden={!hoveredChunk}
      >
        <ChunkTooltip
          chunk={
            hoveredChunk
              ? (bars.find((b) => b.id === hoveredChunk) ?? bars[0]!)
              : bars[0]!
          }
          dark={dark}
        />
      </div>
    </div>
  );
}

function ChunkTooltip({
  chunk,
  dark,
}: Readonly<{
  chunk: {
    id: string;
    start: number;
    end: number;
    sealed: boolean;
    recordCount: bigint;
    bytes: bigint;
    compressed: boolean;
    diskBytes: bigint;
  };
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const start = new Date(chunk.start);
  const end = new Date(chunk.end);
  const duration = chunk.end - chunk.start;

  const logicalBytes = Number(chunk.bytes);
  const diskBytes = Number(chunk.diskBytes);
  const showCompression = chunk.compressed && diskBytes > 0 && logicalBytes > 0;
  const reductionPct = showCompression
    ? Math.round((1 - diskBytes / logicalBytes) * 100)
    : 0;

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
          <Badge variant="copper" dark={dark}>sealed</Badge>
        ) : (
          <Badge variant="info" dark={dark}>active</Badge>
        )}
        {chunk.compressed && (
          <Badge variant="info" dark={dark}>compressed</Badge>
        )}
      </div>
      <div
        className={`flex gap-4 flex-wrap ${c("text-text-muted", "text-light-text-muted")}`}
      >
        <span>
          {formatTimeShort(start)} &rarr; {formatTimeShort(end)}
        </span>
        <span>{formatDurationMs(duration)}</span>
        <span className="font-mono">
          {Number(chunk.recordCount).toLocaleString()} records
        </span>
        <span className="font-mono">{formatBytes(logicalBytes)}</span>
        {showCompression && (
          <span className="font-mono">
            {formatBytes(logicalBytes)} &rarr; {formatBytes(diskBytes)} ({reductionPct}% reduction)
          </span>
        )}
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

  const rawInterval = span / targetCount;
  const niceIntervals = [
    1_000,
    2_000,
    5_000,
    10_000,
    15_000,
    30_000,
    60_000,
    2 * 60_000,
    5 * 60_000,
    10 * 60_000,
    15 * 60_000,
    30 * 60_000,
    3600_000,
    2 * 3600_000,
    4 * 3600_000,
    6 * 3600_000,
    12 * 3600_000,
    86400_000,
    2 * 86400_000,
    7 * 86400_000,
    30 * 86400_000,
    90 * 86400_000,
    365 * 86400_000,
  ];

  let interval = niceIntervals[0]!;
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
    return d.toLocaleTimeString(undefined, {
      hour: "2-digit",
      minute: "2-digit",
    });
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

