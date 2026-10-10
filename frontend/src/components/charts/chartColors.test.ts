import { describe, expect, test } from "bun:test";
import { formatChartValue } from "./chartColors";

function referenceChartValue(v: number): string {
  return Number.isInteger(v) ? String(v) : v.toFixed(1);
}

describe("formatChartValue", () => {
  test("below 1K in magnitude matches the reference rendering", () => {
    for (let i = -999; i < 1_000; i++) {
      expect(formatChartValue(i)).toBe(referenceChartValue(i));
    }
    for (const v of [0, 0.25, -0.25, 999.9, -999.9, 12.345]) {
      expect(formatChartValue(v)).toBe(referenceChartValue(v));
    }
  });

  test("a magnitude that rounds to 1000 renders as 1.0K, with sign", () => {
    expect(formatChartValue(999.95)).toBe("1.0K");
    expect(formatChartValue(-999.95)).toBe("-1.0K");
    expect(formatChartValue(-999_950)).toBe("-1.0M");
    expect(formatChartValue(1_150)).toBe("1.2K");
  });

  test("large magnitudes use the full suffix ladder with sign", () => {
    expect(formatChartValue(25_561_234_567)).toBe("25.6B");
    expect(formatChartValue(-25_561_234_567)).toBe("-25.6B");
    expect(formatChartValue(4.2e12)).toBe("4.2T");
  });
});
