export interface HistogramData {
  buckets: {
    ts: Date;
    count: number;
    groupCounts: Record<string, number>;
    hasCloudData: boolean;
    cloudCount: number;
  }[];
  /** The name of the group-by field (e.g. "level", "status", "host"). */
  groupField: string;
  start: Date | null;
  end: Date | null;
}

/**
 * Converts proto HistogramBucket[] (piggyback on SearchResponse) into HistogramData.
 * The histogram always groups by "level".
 */
export function histogramBucketsToData(
  buckets: { timestampMs: bigint; count: bigint; groupCounts: { [key: string]: bigint } }[],
): HistogramData | null {
  if (buckets.length === 0) return null;

  const converted = buckets
    .map((b) => {
      const gc: Record<string, number> = {};
      for (const [k, v] of Object.entries(b.groupCounts)) {
        gc[k] = Number(v);
      }
      return {
        ts: new Date(Number(b.timestampMs)),
        count: Number(b.count),
        groupCounts: gc,
        hasCloudData: Boolean((b as { hasCloudData?: boolean }).hasCloudData),
        cloudCount: Number((b as { cloudCount?: bigint }).cloudCount ?? 0),
      };
    })
    .sort((a, b) => a.ts.getTime() - b.ts.getTime());

  return {
    buckets: converted,
    groupField: "level",
    start: converted[0]!.ts,
    end: converted.at(-1)!.ts,
  };
}

// Sentinel columns the backend appends to every `| timechart` TableResult
// (see query.TimechartCloudFlagColumn / TimechartCloudCountColumn in
// backend/internal/query/histogram.go). They carry the same per-bucket
// cloud-estimate provenance as the sidebar histogram's HasCloudData/
// CloudCount fields, so the "table" chart view of a timechart pipeline
// result labels applyCloudSelectivity-derived buckets consistently with the
// sidebar rather than always reading as exact. See gastrolog-4of7c.
const CLOUD_FLAG_COLUMN = "_has_cloud_data";
const CLOUD_COUNT_COLUMN = "_cloud_count";

/**
 * Converts a table result (columns + rows) from a timechart query into HistogramData.
 * Expects columns to include "_time" and "count". Any additional column that
 * isn't one of the reserved cloud-estimate sentinel columns is treated as the
 * group-by field (e.g. "level", "status", etc.).
 */
export function tableResultToHistogramData(
  columns: string[],
  rows: { values: string[] }[],
): HistogramData | null {
  const timeIdx = columns.indexOf("_time");
  const countIdx = columns.indexOf("count");
  const cloudFlagIdx = columns.indexOf(CLOUD_FLAG_COLUMN);
  const cloudCountIdx = columns.indexOf(CLOUD_COUNT_COLUMN);

  if (timeIdx === -1 || countIdx === -1) {
    return null;
  }

  // The group column is whichever column is neither _time, count, nor one
  // of the reserved cloud-estimate sentinel columns.
  const groupIdx = columns.findIndex(
    (c, i) =>
      i !== timeIdx &&
      i !== countIdx &&
      c !== CLOUD_FLAG_COLUMN &&
      c !== CLOUD_COUNT_COLUMN,
  );
  const groupField = groupIdx !== -1 ? columns[groupIdx]! : "";

  // Group rows by timestamp.
  const bucketMap = new Map<
    string,
    { count: number; groupCounts: Record<string, number>; hasCloudData: boolean; cloudCount: number }
  >();

  for (const row of rows) {
    const tsStr = row.values[timeIdx]!;
    const group = groupIdx !== -1 ? row.values[groupIdx]! : "";
    const count = Number(row.values[countIdx]!);

    let bucket = bucketMap.get(tsStr);
    if (!bucket) {
      bucket = { count: 0, groupCounts: {}, hasCloudData: false, cloudCount: 0 };
      bucketMap.set(tsStr, bucket);
    }
    bucket.count += count;
    if (group) {
      bucket.groupCounts[group] = (bucket.groupCounts[group] ?? 0) + count;
    }
    // Every row for a given bucket carries the same sentinel values (the
    // backend sets them once per bucket, not per group) — set rather than
    // accumulate.
    if (cloudFlagIdx !== -1 && row.values[cloudFlagIdx] === "true") {
      bucket.hasCloudData = true;
    }
    if (cloudCountIdx !== -1) {
      bucket.cloudCount = Number(row.values[cloudCountIdx] ?? 0);
    }
  }

  // Convert to sorted array.
  const buckets = Array.from(bucketMap.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([tsStr, data]) => ({
      ts: new Date(tsStr),
      count: data.count,
      groupCounts: data.groupCounts,
      hasCloudData: data.hasCloudData,
      cloudCount: data.cloudCount,
    }));

  const start = buckets.length > 0 ? buckets[0]!.ts : null;
  const end = buckets.length > 0 ? buckets.at(-1)!.ts : null;

  return { buckets, groupField, start, end };
}
