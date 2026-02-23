import { useMemo } from "react";
import type { Record as ProtoRecord } from "../client";
import type { HistogramData } from "../../utils/histogramData";

const NUM_BUCKETS = 50;

/**
 * Builds a HistogramData from an array of records client-side.
 * Used in follow mode to show a live-updating histogram without a server call.
 */
export function useLiveHistogram(records: ProtoRecord[]): HistogramData | null {
  return useMemo(() => {
    if (records.length === 0) return null;

    // Extract timestamps. Records are newest-first (prepended in useFollow).
    // We need oldest-first for bucketing.
    const timestamps: { ms: number; level: string }[] = [];
    for (const r of records) {
      const ts = r.ingestTs?.toDate();
      if (!ts) continue;
      timestamps.push({
        ms: ts.getTime(),
        level: r.attrs["level"] ?? "",
      });
    }

    if (timestamps.length === 0) return null;

    let minMs = Infinity;
    let maxMs = -Infinity;
    for (const t of timestamps) {
      if (t.ms < minMs) minMs = t.ms;
      if (t.ms > maxMs) maxMs = t.ms;
    }

    // If all records have the same timestamp, create a 1-second window.
    if (maxMs === minMs) {
      minMs -= 500;
      maxMs += 500;
    }

    const rangeMs = maxMs - minMs;
    const bucketWidth = rangeMs / NUM_BUCKETS;

    // Initialize buckets.
    const buckets: {
      ts: Date;
      count: number;
      groupCounts: { [key: string]: number };
    }[] = [];
    for (let i = 0; i < NUM_BUCKETS; i++) {
      buckets.push({
        ts: new Date(minMs + i * bucketWidth),
        count: 0,
        groupCounts: {},
      });
    }

    // Distribute records into buckets.
    for (const t of timestamps) {
      let idx = Math.floor((t.ms - minMs) / bucketWidth);
      if (idx >= NUM_BUCKETS) idx = NUM_BUCKETS - 1;
      if (idx < 0) idx = 0;

      const bucket = buckets[idx]!;
      bucket.count++;
      if (t.level) {
        bucket.groupCounts[t.level] = (bucket.groupCounts[t.level] ?? 0) + 1;
      }
    }

    return {
      buckets,
      groupField: "level",
      start: new Date(minMs),
      end: new Date(maxMs),
    };
  }, [records]);
}
