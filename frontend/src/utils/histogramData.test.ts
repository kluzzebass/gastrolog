import { describe, expect, test } from "bun:test";
import { tableResultToHistogramData, histogramBucketsToData } from "./histogramData";

describe("tableResultToHistogramData", () => {
  test("returns null without _time/count columns", () => {
    expect(tableResultToHistogramData(["foo", "bar"], [])).toBeNull();
  });

  test("plain timechart (no group, no cloud data) reads exact, unflagged buckets", () => {
    const columns = ["_time", "count", "_has_cloud_data", "_cloud_count"];
    const rows = [
      { values: ["2026-03-01T12:00:00.000Z", "2", "false", "0"] },
      { values: ["2026-03-01T12:00:05.000Z", "3", "false", "0"] },
    ];
    const data = tableResultToHistogramData(columns, rows);
    expect(data).not.toBeNull();
    expect(data!.groupField).toBe("");
    expect(data!.buckets).toHaveLength(2);
    for (const b of data!.buckets) {
      expect(b.hasCloudData).toBe(false);
      expect(b.cloudCount).toBe(0);
    }
    expect(data!.buckets[0]!.count).toBe(2);
    expect(data!.buckets[1]!.count).toBe(3);
  });

  test("bucket derived via applyCloudSelectivity carries hasCloudData/cloudCount", () => {
    const columns = ["_time", "count", "_has_cloud_data", "_cloud_count"];
    const rows = [
      // Exact, local-only bucket — must NOT be flagged.
      { values: ["2026-03-01T12:00:00.000Z", "2", "false", "0"] },
      // Scaled cloud contribution — must be flagged with the estimated magnitude.
      { values: ["2026-03-01T12:00:05.000Z", "2", "true", "1"] },
    ];
    const data = tableResultToHistogramData(columns, rows);
    expect(data).not.toBeNull();
    const [exact, estimated] = data!.buckets;
    expect(exact!.hasCloudData).toBe(false);
    expect(exact!.cloudCount).toBe(0);
    expect(estimated!.hasCloudData).toBe(true);
    expect(estimated!.cloudCount).toBe(1);
  });

  test("sentinel columns are not mistaken for the group-by field", () => {
    const columns = ["_time", "level", "count", "_has_cloud_data", "_cloud_count"];
    const rows = [
      { values: ["2026-03-01T12:00:00.000Z", "error", "2", "true", "1"] },
      { values: ["2026-03-01T12:00:00.000Z", "info", "3", "true", "1"] },
    ];
    const data = tableResultToHistogramData(columns, rows);
    expect(data).not.toBeNull();
    expect(data!.groupField).toBe("level");
    expect(data!.buckets).toHaveLength(1);
    const bucket = data!.buckets[0]!;
    expect(bucket.groupCounts).toEqual({ error: 2, info: 3 });
    expect(bucket.count).toBe(5);
    expect(bucket.hasCloudData).toBe(true);
    expect(bucket.cloudCount).toBe(1);
  });

  test("missing sentinel columns (older/other table shapes) default to unflagged", () => {
    const columns = ["_time", "count"];
    const rows = [{ values: ["2026-03-01T12:00:00.000Z", "5"] }];
    const data = tableResultToHistogramData(columns, rows);
    expect(data).not.toBeNull();
    expect(data!.buckets[0]!.hasCloudData).toBe(false);
    expect(data!.buckets[0]!.cloudCount).toBe(0);
  });
});

describe("histogramBucketsToData cloud fields", () => {
  test("propagates hasCloudData/cloudCount from proto buckets", () => {
    // Not a fresh object literal at the call site (assigned to a variable
    // first) so TS doesn't excess-property-check it against the narrow
    // proto-subset param type below — the runtime function reads these
    // extra fields off the wire via an unchecked cast (see histogramData.ts).
    const rawBucket = {
      timestampMs: 0n,
      count: 4n,
      groupCounts: {},
      hasCloudData: true,
      cloudCount: 2n,
    };
    const data = histogramBucketsToData([rawBucket]);
    expect(data).not.toBeNull();
    expect(data!.buckets[0]!.hasCloudData).toBe(true);
    expect(data!.buckets[0]!.cloudCount).toBe(2);
  });

  test("defaults hasCloudData/cloudCount when absent", () => {
    const data = histogramBucketsToData([
      { timestampMs: 0n, count: 4n, groupCounts: {} },
    ]);
    expect(data).not.toBeNull();
    expect(data!.buckets[0]!.hasCloudData).toBe(false);
    expect(data!.buckets[0]!.cloudCount).toBe(0);
  });
});
