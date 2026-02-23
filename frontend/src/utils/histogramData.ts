export interface HistogramData {
  buckets: {
    ts: Date;
    count: number;
    groupCounts: Record<string, number>;
  }[];
  /** The name of the group-by field (e.g. "level", "status", "host"). */
  groupField: string;
  start: Date | null;
  end: Date | null;
}

/**
 * Converts a table result (columns + rows) from a timechart query into HistogramData.
 * Expects columns to include "_time" and "count". Any additional column is treated
 * as the group-by field (e.g. "level", "status", etc.).
 */
export function tableResultToHistogramData(
  columns: string[],
  rows: { values: string[] }[],
): HistogramData | null {
  const timeIdx = columns.indexOf("_time");
  const countIdx = columns.indexOf("count");

  if (timeIdx < 0 || countIdx < 0) {
    return null;
  }

  // The group column is whichever column is neither _time nor count.
  const groupIdx = columns.findIndex(
    (c, i) => i !== timeIdx && i !== countIdx,
  );
  const groupField = groupIdx >= 0 ? columns[groupIdx]! : "";

  // Group rows by timestamp.
  const bucketMap = new Map<
    string,
    { count: number; groupCounts: Record<string, number> }
  >();

  for (const row of rows) {
    const tsStr = row.values[timeIdx]!;
    const group = groupIdx >= 0 ? row.values[groupIdx]! : "";
    const count = Number(row.values[countIdx]!);

    let bucket = bucketMap.get(tsStr);
    if (!bucket) {
      bucket = { count: 0, groupCounts: {} };
      bucketMap.set(tsStr, bucket);
    }
    bucket.count += count;
    if (group) {
      bucket.groupCounts[group] = (bucket.groupCounts[group] ?? 0) + count;
    }
  }

  // Convert to sorted array.
  const buckets = Array.from(bucketMap.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([tsStr, data]) => ({
      ts: new Date(tsStr),
      count: data.count,
      groupCounts: data.groupCounts,
    }));

  const start = buckets.length > 0 ? buckets[0]!.ts : null;
  const end = buckets.length > 0 ? buckets[buckets.length - 1]!.ts : null;

  return { buckets, groupField, start, end };
}
