import { describe, expect, test } from "bun:test";
import {
  escapeRegex,
  findRanges,
  findBareWordRanges,
  stepToRanges,
  buildSegments,
  formatTs,
  actionColor,
} from "./explainHighlighter";
import type { PipelineStep } from "../api/client";

// ── escapeRegex ──

describe("escapeRegex", () => {
  test("escapes special regex characters", () => {
    expect(escapeRegex("hello.world")).toBe("hello\\.world");
    expect(escapeRegex("$100")).toBe("\\$100");
    expect(escapeRegex("(a|b)")).toBe("\\(a\\|b\\)");
    expect(escapeRegex("[test]")).toBe("\\[test\\]");
  });

  test("leaves plain text unchanged", () => {
    expect(escapeRegex("hello")).toBe("hello");
    expect(escapeRegex("abc123")).toBe("abc123");
  });

  test("escapes all special chars", () => {
    const specials = ".*+?^${}()|[]\\";
    const escaped = escapeRegex(specials);
    // Every char should be preceded by a backslash
    expect(escaped).toBe("\\.\\*\\+\\?\\^\\$\\{\\}\\(\\)\\|\\[\\]\\\\");
  });
});

// ── findRanges ──

describe("findRanges", () => {
  test("finds all matches", () => {
    const result = findRanges("abcabc", /abc/g);
    expect(result).toEqual([
      [0, 3],
      [3, 6],
    ]);
  });

  test("returns empty for no matches", () => {
    expect(findRanges("hello", /xyz/g)).toEqual([]);
  });

  test("finds single match", () => {
    expect(findRanges("hello world", /world/g)).toEqual([[6, 11]]);
  });
});

// ── findBareWordRanges ──

describe("findBareWordRanges", () => {
  test("finds standalone word", () => {
    const result = findBareWordRanges("error occurred here", "error");
    expect(result).toEqual([[0, 5]]);
  });

  test("does not match word in key=value", () => {
    const result = findBareWordRanges("level=error", "error");
    expect(result).toEqual([]);
  });

  test("case-insensitive matching", () => {
    const result = findBareWordRanges("ERROR occurred", "error");
    expect(result).toEqual([[0, 5]]);
  });

  test("finds multiple occurrences", () => {
    const result = findBareWordRanges("error and error again", "error");
    expect(result).toEqual([
      [0, 5],
      [10, 15],
    ]);
  });

  test("does not match partial words", () => {
    const result = findBareWordRanges("errorhandler", "error");
    expect(result).toEqual([]);
  });
});

// ── stepToRanges ──

describe("stepToRanges", () => {
  test("time step finds start= and end= ranges", () => {
    const step = { name: "time", predicate: "", action: "" } as PipelineStep;
    const expr = "start=2024-01-01 end=2024-12-31 error";
    const result = stepToRanges(step, expr);
    expect(result.length).toBe(2);
    expect(expr.slice(result[0]![0], result[0]![1])).toBe("start=2024-01-01");
    expect(expr.slice(result[1]![0], result[1]![1])).toBe("end=2024-12-31");
  });

  test("token step finds bare words from predicate", () => {
    const step = {
      name: "token",
      predicate: "token(error,timeout)",
      action: "",
    } as PipelineStep;
    const expr = "error occurred timeout";
    const result = stepToRanges(step, expr);
    expect(result.length).toBe(2);
  });

  test("token step returns empty for invalid predicate", () => {
    const step = {
      name: "token",
      predicate: "invalid",
      action: "",
    } as PipelineStep;
    expect(stepToRanges(step, "error")).toEqual([]);
  });

  test("kv step finds literal predicate", () => {
    const step = {
      name: "kv",
      predicate: "level=error",
      action: "",
    } as PipelineStep;
    const expr = "last=1h level=error";
    const result = stepToRanges(step, expr);
    expect(result.length).toBe(1);
    expect(expr.slice(result[0]![0], result[0]![1])).toBe("level=error");
  });

  test("unknown step returns empty", () => {
    const step = {
      name: "unknown",
      predicate: "foo",
      action: "",
    } as PipelineStep;
    expect(stepToRanges(step, "foo bar")).toEqual([]);
  });
});

// ── buildSegments ──

describe("buildSegments", () => {
  test("returns single unhighlighted segment with no ranges", () => {
    expect(buildSegments("hello", [])).toEqual([
      { text: "hello", highlighted: false },
    ]);
  });

  test("highlights a range in the middle", () => {
    const result = buildSegments("hello world", [[6, 11]]);
    expect(result).toEqual([
      { text: "hello ", highlighted: false },
      { text: "world", highlighted: true },
    ]);
  });

  test("highlights at the start", () => {
    const result = buildSegments("hello world", [[0, 5]]);
    expect(result).toEqual([
      { text: "hello", highlighted: true },
      { text: " world", highlighted: false },
    ]);
  });

  test("merges overlapping ranges", () => {
    const result = buildSegments("abcdefgh", [
      [1, 4],
      [3, 6],
    ]);
    expect(result).toEqual([
      { text: "a", highlighted: false },
      { text: "bcdef", highlighted: true },
      { text: "gh", highlighted: false },
    ]);
  });

  test("handles multiple non-overlapping ranges", () => {
    const result = buildSegments("hello beautiful world", [
      [0, 5],
      [16, 21],
    ]);
    expect(result).toEqual([
      { text: "hello", highlighted: true },
      { text: " beautiful ", highlighted: false },
      { text: "world", highlighted: true },
    ]);
  });

  test("highlights entire expression", () => {
    const result = buildSegments("abc", [[0, 3]]);
    expect(result).toEqual([{ text: "abc", highlighted: true }]);
  });
});

// ── formatTs ──

describe("formatTs", () => {
  test("returns empty string for undefined", () => {
    expect(formatTs(undefined)).toBe("");
  });

  test("formats a timestamp", () => {
    const ts = {
      toDate: () => new Date("2024-03-15T14:30:45Z"),
    };
    const result = formatTs(ts);
    // Should be in HH:MM:SS 24-hour format
    expect(result).toMatch(/^\d{2}:\d{2}:\d{2}$/);
  });
});

// ── actionColor ──

describe("actionColor", () => {
  test("seek returns info colors", () => {
    expect(actionColor("seek")).toContain("severity-info");
  });

  test("indexed returns info colors", () => {
    expect(actionColor("indexed")).toContain("severity-info");
  });

  test("skipped returns error colors", () => {
    expect(actionColor("skipped")).toContain("severity-error");
  });

  test("default returns warn colors", () => {
    expect(actionColor("scan")).toContain("severity-warn");
    expect(actionColor("filter")).toContain("severity-warn");
  });
});
