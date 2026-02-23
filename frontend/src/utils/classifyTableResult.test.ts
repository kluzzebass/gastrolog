import { describe, expect, test } from "bun:test";
import { classifyTableResult } from "./classifyTableResult";

describe("classifyTableResult", () => {
  test("single value: 1 col, 1 row", () => {
    expect(classifyTableResult(["count"], [["42"]])).toBe("single-value");
  });

  test("table: 1 col, multiple rows", () => {
    expect(classifyTableResult(["level"], [["error"], ["warn"]])).toBe("table");
  });

  test("table: 0 rows", () => {
    expect(classifyTableResult(["level", "count"], [])).toBe("table");
  });

  test("table: 1 row only", () => {
    expect(classifyTableResult(["level", "count"], [["error", "5"]])).toBe("table");
  });

  test("donut: 2 cols, 2-6 rows, numeric last col", () => {
    const rows = [
      ["error", "10"],
      ["warn", "20"],
      ["info", "50"],
    ];
    expect(classifyTableResult(["level", "count"], rows)).toBe("donut-chart");
  });

  test("donut: exactly 2 rows", () => {
    expect(
      classifyTableResult(["level", "count"], [["error", "10"], ["warn", "20"]]),
    ).toBe("donut-chart");
  });

  test("donut: exactly 6 rows", () => {
    const rows = Array.from({ length: 6 }, (_, i) => [`g${i}`, String(i + 1)]);
    expect(classifyTableResult(["group", "count"], rows)).toBe("donut-chart");
  });

  test("bar: 2 cols, 7 rows (exceeds donut threshold)", () => {
    const rows = Array.from({ length: 7 }, (_, i) => [`g${i}`, String(i + 1)]);
    expect(classifyTableResult(["group", "count"], rows)).toBe("bar-chart");
  });

  test("bar: 3 cols, 5 rows, numeric last col", () => {
    const rows = [
      ["us-east", "web", "100"],
      ["us-west", "web", "80"],
      ["eu-west", "api", "60"],
      ["ap-south", "api", "40"],
      ["us-east", "api", "90"],
    ];
    expect(classifyTableResult(["region", "service", "count"], rows)).toBe("bar-chart");
  });

  test("bar: 20 rows (at limit)", () => {
    const rows = Array.from({ length: 20 }, (_, i) => [`g${i}`, String(i + 1)]);
    expect(classifyTableResult(["group", "count"], rows)).toBe("bar-chart");
  });

  test("table: 21 rows (exceeds limit)", () => {
    const rows = Array.from({ length: 21 }, (_, i) => [`g${i}`, String(i + 1)]);
    expect(classifyTableResult(["group", "count"], rows)).toBe("table");
  });

  test("table: last col not numeric", () => {
    const rows = [
      ["10", "error"],
      ["20", "warn"],
      ["50", "info"],
    ];
    expect(classifyTableResult(["count", "level"], rows)).toBe("table");
  });

  test("table: last col has empty values", () => {
    const rows = [
      ["error", "10"],
      ["warn", ""],
      ["info", "50"],
    ];
    expect(classifyTableResult(["level", "count"], rows)).toBe("table");
  });

  test("table: 1 col only", () => {
    expect(classifyTableResult(["count"], [["42"], ["53"]])).toBe("table");
  });
});
