import { useState, useRef } from "react";
import type { HistogramData } from "../api/hooks/useHistogram";

const SEVERITY_COLORS = [
  ["error", "var(--color-severity-error)"],
  ["warn", "var(--color-severity-warn)"],
  ["info", "var(--color-severity-info)"],
  ["debug", "var(--color-severity-debug)"],
  ["trace", "var(--color-severity-trace)"],
] as const;

const SEVERITY_LEVELS = ["error", "warn", "info", "debug", "trace"] as const;

export function HistogramChart({
  data,
  dark,
  onBrushSelect,
  onPan,
}: {
  data: HistogramData;
  dark: boolean;
  onBrushSelect?: (start: Date, end: Date) => void;
  onPan?: (start: Date, end: Date) => void;
}) {
  const { buckets } = data;
  const barsRef = useRef<HTMLDivElement>(null);
  const [brushStart, setBrushStart] = useState<number | null>(null);
  const [brushEnd, setBrushEnd] = useState<number | null>(null);
  const brushingRef = useRef(false);

  if (buckets.length === 0) return null;

  const firstBucket = buckets[0]!;
  const lastBucket = buckets[buckets.length - 1]!;
  const maxCount = Math.max(...buckets.map((b) => b.count), 1);
  const totalCount = buckets.reduce((sum, b) => sum + b.count, 0);
  const barHeight = 48;
  const c = (d: string, l: string) => (dark ? d : l);

  const getBucketIndex = (clientX: number): number => {
    const el = barsRef.current;
    if (!el) return 0;
    const rect = el.getBoundingClientRect();
    const x = clientX - rect.left;
    const idx = Math.floor((x / rect.width) * buckets.length);
    return Math.max(0, Math.min(buckets.length - 1, idx));
  };

  const handleMouseDown = (e: React.MouseEvent) => {
    if (!onBrushSelect) return;
    e.preventDefault();
    const idx = getBucketIndex(e.clientX);
    setBrushStart(idx);
    setBrushEnd(idx);
    brushingRef.current = true;

    const onMouseMove = (e: MouseEvent) => {
      if (!brushingRef.current) return;
      setBrushEnd(getBucketIndex(e.clientX));
    };
    const onMouseUp = (e: MouseEvent) => {
      if (!brushingRef.current) return;
      brushingRef.current = false;
      const endIdx = getBucketIndex(e.clientX);
      const lo = Math.min(idx, endIdx);
      const hi = Math.max(idx, endIdx);
      if (lo !== hi) {
        onBrushSelect(buckets[lo]!.ts, buckets[hi]!.ts);
      }
      setBrushStart(null);
      setBrushEnd(null);
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
    };
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
  };

  const brushLo =
    brushStart !== null && brushEnd !== null
      ? Math.min(brushStart, brushEnd)
      : null;
  const brushHi =
    brushStart !== null && brushEnd !== null
      ? Math.max(brushStart, brushEnd)
      : null;

  // Pan handlers.
  const axisRef = useRef<HTMLDivElement>(null);
  const panStartX = useRef<number>(0);
  const panAxisWidth = useRef<number>(1);
  const panningRef = useRef(false);
  const [panOffset, setPanOffset] = useState(0);

  const handlePanStep = (direction: -1 | 1) => {
    if (!onPan || buckets.length < 2) return;
    const windowMs = lastBucket.ts.getTime() - firstBucket.ts.getTime();
    const stepMs = windowMs / 2;
    const first = firstBucket.ts.getTime();
    const last = lastBucket.ts.getTime();
    onPan(
      new Date(first + direction * stepMs),
      new Date(last + direction * stepMs),
    );
  };

  const handleAxisMouseDown = (e: React.MouseEvent) => {
    if (!onPan || buckets.length < 2) return;
    e.preventDefault();
    panStartX.current = e.clientX;
    panAxisWidth.current = axisRef.current?.getBoundingClientRect().width || 1;
    panningRef.current = true;
    document.body.style.cursor = "grabbing";
    document.body.style.userSelect = "none";

    const onMouseMove = (e: MouseEvent) => {
      setPanOffset(e.clientX - panStartX.current);
    };
    const onMouseUp = (e: MouseEvent) => {
      panningRef.current = false;
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      setPanOffset(0);
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);

      const el = axisRef.current;
      if (!el) return;
      const deltaX = panStartX.current - e.clientX; // drag left = positive = go back
      const axisWidth = el.getBoundingClientRect().width;
      if (Math.abs(deltaX) < 3) return; // ignore tiny movements
      const windowMs = lastBucket.ts.getTime() - firstBucket.ts.getTime();
      const deltaMs = (deltaX / axisWidth) * windowMs;
      const first = firstBucket.ts.getTime();
      const last = lastBucket.ts.getTime();
      onPan(new Date(first + deltaMs), new Date(last + deltaMs));
    };
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
  };

  // Format time label based on range span.
  const rangeMs =
    buckets.length > 1 ? lastBucket.ts.getTime() - firstBucket.ts.getTime() : 0;

  const formatTime = (d: Date) => {
    if (rangeMs > 24 * 60 * 60 * 1000) {
      return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    }
    if (rangeMs < 60 * 60 * 1000) {
      return d.toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      });
    }
    return d.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
  };

  // Show ~5 evenly spaced time labels.
  const labelCount = Math.min(5, buckets.length);
  const labelStep = Math.max(1, Math.floor(buckets.length / labelCount));

  // Compute human-readable pan delta during drag.
  const windowMs =
    buckets.length > 1 ? lastBucket.ts.getTime() - firstBucket.ts.getTime() : 0;
  const panDeltaMs =
    panOffset !== 0 ? -((panOffset / panAxisWidth.current) * windowMs) : 0;

  const formatDuration = (ms: number): string => {
    const abs = Math.abs(ms);
    const sign = ms < 0 ? "-" : "+";
    if (abs < 1000) return `${sign}${Math.round(abs)}ms`;
    if (abs < 60_000) return `${sign}${(abs / 1000).toFixed(1)}s`;
    if (abs < 3_600_000) {
      const m = Math.floor(abs / 60_000);
      const s = Math.round((abs % 60_000) / 1000);
      return s > 0 ? `${sign}${m}m ${s}s` : `${sign}${m}m`;
    }
    if (abs < 86_400_000) {
      const h = Math.floor(abs / 3_600_000);
      const m = Math.round((abs % 3_600_000) / 60_000);
      return m > 0 ? `${sign}${h}h ${m}m` : `${sign}${h}h`;
    }
    const d = Math.floor(abs / 86_400_000);
    const h = Math.round((abs % 86_400_000) / 3_600_000);
    return h > 0 ? `${sign}${d}d ${h}h` : `${sign}${d}d`;
  };

  return (
    <div className="relative">
      <div className="flex items-baseline justify-between mb-1.5">
        <span
          className={`text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Volume
        </span>
        <span
          className={`font-mono text-[0.75em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          {totalCount.toLocaleString()} records
        </span>
      </div>
      <div className="relative" style={{ height: barHeight }}>
        {/* Pan delta indicator — centered over bars */}
        {panOffset !== 0 && (
          <div
            className={`absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 px-2 py-0.5 text-[0.7em] font-mono rounded whitespace-nowrap pointer-events-none z-20 ${c(
              "bg-ink-surface text-copper border border-copper/30",
              "bg-light-surface text-copper border border-copper/30",
            )}`}
          >
            {formatDuration(panDeltaMs)}
          </div>
        )}
        <div
          ref={barsRef}
          onMouseDown={handleMouseDown}
          className={`relative flex items-end h-full gap-px ${onBrushSelect ? "cursor-crosshair" : ""}`}
        >
          {brushLo !== null && brushHi !== null && (
            <div
              className="absolute inset-y-0 bg-copper/20 pointer-events-none z-10 rounded-sm"
              style={{
                left: `${(brushLo / buckets.length) * 100}%`,
                width: `${((brushHi - brushLo + 1) / buckets.length) * 100}%`,
              }}
            />
          )}
          {buckets.map((bucket, i) => {
            const pct = maxCount > 0 ? bucket.count / maxCount : 0;
            const lc = bucket.levelCounts;
            const hasLevels = lc && Object.keys(lc).length > 0;
            const levelSum = hasLevels
              ? Object.values(lc).reduce((a, b) => a + b, 0)
              : 0;
            const other = bucket.count - levelSum;

            // Stack order bottom-to-top: error, warn, info, debug, trace, other
            const segments: { key: string; count: number; color: string }[] =
              [];
            if (hasLevels) {
              for (const [key, color] of SEVERITY_COLORS) {
                if (lc[key]! > 0)
                  segments.push({ key, count: lc[key]!, color });
              }
              if (other > 0)
                segments.push({
                  key: "other",
                  count: other,
                  color: "var(--color-copper)",
                });
            }

            return (
              <div
                key={i}
                className="flex-1 min-w-0 group relative"
                style={{ height: "100%" }}
              >
                {bucket.count > 0 && (
                  <div
                    className="absolute bottom-0 inset-x-0 rounded-t-sm overflow-hidden transition-colors"
                    style={{
                      height: `${Math.max(pct * 100, 4)}%`,
                    }}
                  >
                    {hasLevels && segments.length > 0 ? (
                      <div
                        className={`flex flex-col-reverse w-full h-full transition-opacity ${c("opacity-60 group-hover:opacity-100", "opacity-50 group-hover:opacity-80")}`}
                      >
                        {segments.map((seg) => (
                          <div
                            key={seg.key}
                            className="w-full shrink-0"
                            style={{
                              height: `${(seg.count / bucket.count) * 100}%`,
                              backgroundColor: seg.color,
                            }}
                          />
                        ))}
                      </div>
                    ) : (
                      <div
                        className={`w-full h-full transition-opacity ${c(
                          "bg-copper opacity-60 group-hover:opacity-100",
                          "bg-copper opacity-50 group-hover:opacity-80",
                        )}`}
                      />
                    )}
                  </div>
                )}
                {/* Tooltip */}
                <div
                  className={`absolute bottom-full left-1/2 -translate-x-1/2 mb-1 px-2 py-1 text-[0.7em] font-mono rounded whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity z-10 ${c("bg-ink-surface text-text-bright border border-ink-border-subtle", "bg-light-surface text-light-text-bright border border-light-border-subtle")}`}
                >
                  <div>
                    {bucket.count.toLocaleString()} &middot;{" "}
                    {formatTime(bucket.ts)}
                  </div>
                  {hasLevels && (
                    <div className="mt-0.5 space-y-px">
                      {SEVERITY_LEVELS.map(
                        (level) =>
                          lc[level]! > 0 && (
                            <div
                              key={level}
                              className="flex items-center gap-1.5"
                            >
                              <span
                                className="inline-block w-1.5 h-1.5 rounded-full"
                                style={{
                                  backgroundColor: `var(--color-severity-${level})`,
                                }}
                              />
                              <span className="opacity-70">{level}</span>
                              <span>{lc[level]!.toLocaleString()}</span>
                            </div>
                          ),
                      )}
                      {other > 0 && (
                        <div className="flex items-center gap-1.5">
                          <span className="inline-block w-1.5 h-1.5 rounded-full bg-copper/60" />
                          <span className="opacity-70">other</span>
                          <span>{other.toLocaleString()}</span>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>
      {/* Time axis with pan arrows + draggable labels */}
      <div className="flex items-center mt-1 gap-1">
        {onPan && (
          <button
            onClick={() => handlePanStep(-1)}
            className={`text-[0.7em] px-1 rounded transition-colors shrink-0 ${c(
              "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
              "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
            )}`}
            title="Pan backward"
          >
            {"\u25C2"}
          </button>
        )}
        <div
          ref={axisRef}
          onMouseDown={handleAxisMouseDown}
          className={`flex-1 flex justify-between overflow-hidden ${onPan ? "cursor-grab active:cursor-grabbing" : ""}`}
          style={
            panOffset ? { transform: `translateX(${panOffset}px)` } : undefined
          }
        >
          {Array.from({ length: labelCount }, (_, i) => {
            const idx = Math.min(i * labelStep, buckets.length - 1);
            return (
              <span
                key={i}
                className={`text-[0.65em] font-mono select-none ${c("text-text-ghost", "text-light-text-ghost")}`}
              >
                {formatTime(buckets[idx]!.ts)}
              </span>
            );
          })}
        </div>
        {onPan && (
          <button
            onClick={() => handlePanStep(1)}
            className={`text-[0.7em] px-1 rounded transition-colors shrink-0 ${c(
              "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
              "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
            )}`}
            title="Pan forward"
          >
            {"\u25B8"}
          </button>
        )}
      </div>
    </div>
  );
}
