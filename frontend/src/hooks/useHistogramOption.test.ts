import { describe, test, expect } from "bun:test";
import { buildColorMap } from "./useHistogramOption";
import type { HistogramData } from "../utils/histogramData";

function makeData(groupCounts: Record<string, number>[]): HistogramData {
  return {
    buckets: groupCounts.map((gc, i) => ({
      ts: new Date(2026, 0, 1, 0, i),
      count: Object.values(gc).reduce((a, b) => a + b, 0),
      cloudCount: 0,
      hasCloudData: false,
      groupCounts: gc,
    })),
    groupField: "level",
    start: null,
    end: null,
  };
}

describe("buildColorMap", () => {
  test("assigns severity colors to known levels", () => {
    const map = buildColorMap(makeData([{ error: 5, warn: 3 }]));
    expect(map.has("error")).toBe(true);
    expect(map.has("warn")).toBe(true);
    // Severity colors come from SEVERITY_COLOR_MAP, not the palette.
    expect(map.get("error")).not.toBe(map.get("warn"));
  });

  test("assigns 'other' the copper theme color", () => {
    const map = buildColorMap(makeData([{ other: 2 }]));
    expect(map.get("other")).toBe("var(--color-copper)");
  });

  test("assigns palette colors to non-severity keys", () => {
    const map = buildColorMap(makeData([{ custom_field: 1, another: 2 }]));
    expect(map.has("custom_field")).toBe(true);
    expect(map.has("another")).toBe(true);
  });

  test("severity keys are ordered before non-severity keys", () => {
    const map = buildColorMap(makeData([{ zzz: 1, error: 5, aaa: 3 }]));
    const keys = [...map.keys()];
    expect(keys.indexOf("error")).toBeLessThan(keys.indexOf("aaa"));
    expect(keys.indexOf("error")).toBeLessThan(keys.indexOf("zzz"));
  });

  test("empty buckets return empty map", () => {
    const map = buildColorMap({ buckets: [], groupField: "", start: null, end: null });
    expect(map.size).toBe(0);
  });

  test("deduplicates keys across buckets", () => {
    const map = buildColorMap(makeData([
      { error: 1, info: 2 },
      { error: 3, debug: 1 },
    ]));
    // error, info, debug — each appears once in the map.
    expect(map.size).toBe(3);
  });
});
