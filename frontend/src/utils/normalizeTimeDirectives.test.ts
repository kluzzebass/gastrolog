import { describe, expect, test } from "bun:test";
import { normalizeTimeDirectives } from "./normalizeTimeDirectives";

const NOW = new Date(2024, 5, 15, 14, 30, 0, 0); // June 15, 2024 14:30 local
const opts = { now: NOW, dateOrder: "mdy" as const };

// Helper: get ISO string for a local date.
function localISO(
  y: number,
  m: number,
  d: number,
  h = 0,
  min = 0,
  s = 0,
): string {
  return new Date(y, m - 1, d, h, min, s).toISOString();
}

describe("normalizeTimeDirectives", () => {
  test("passthrough RFC3339", () => {
    const q = "start=2024-01-15T08:00:00Z level=error";
    expect(normalizeTimeDirectives(q, opts)).toBe(q);
  });

  test("passthrough Unix timestamp", () => {
    const q = "start=1705312800";
    expect(normalizeTimeDirectives(q, opts)).toBe(q);
  });

  test("normalize relaxed ISO date", () => {
    const result = normalizeTimeDirectives("start=2024-01-15 level=error", opts);
    expect(result).toBe(`start=${localISO(2024, 1, 15)} level=error`);
  });

  test("normalize yesterday keyword", () => {
    const result = normalizeTimeDirectives("start=yesterday", opts);
    expect(result).toBe(`start=${localISO(2024, 6, 14)}`);
  });

  test("normalize today keyword", () => {
    const result = normalizeTimeDirectives("start=today", opts);
    expect(result).toBe(`start=${localISO(2024, 6, 15)}`);
  });

  test("normalize time-only", () => {
    const result = normalizeTimeDirectives("start=08:00", opts);
    expect(result).toBe(`start=${localISO(2024, 6, 15, 8, 0)}`);
  });

  test("normalize quoted multi-word value", () => {
    const result = normalizeTimeDirectives('start="3 hours ago"', opts);
    const expected = new Date(NOW);
    expected.setHours(expected.getHours() - 3);
    expect(result).toBe(`start=${expected.toISOString()}`);
  });

  test("normalize multiple time directives", () => {
    const result = normalizeTimeDirectives(
      "start=yesterday end=today level=error",
      opts,
    );
    expect(result).toBe(
      `start=${localISO(2024, 6, 14)} end=${localISO(2024, 6, 15)} level=error`,
    );
  });

  test("does not touch last= directive", () => {
    const q = "last=1h level=error";
    expect(normalizeTimeDirectives(q, opts)).toBe(q);
  });

  test("does not touch non-time directives", () => {
    const q = "level=error reverse=true limit=50";
    expect(normalizeTimeDirectives(q, opts)).toBe(q);
  });

  test("handles source_start and ingest_end", () => {
    const result = normalizeTimeDirectives(
      "source_start=yesterday ingest_end=now",
      opts,
    );
    expect(result).toBe(
      `source_start=${localISO(2024, 6, 14)} ingest_end=${NOW.toISOString()}`,
    );
  });

  test("preserves surrounding expression intact", () => {
    const result = normalizeTimeDirectives(
      '(error OR warn) start="1 hour ago" NOT debug',
      opts,
    );
    const expected = new Date(NOW);
    expected.setHours(expected.getHours() - 1);
    expect(result).toBe(
      `(error OR warn) start=${expected.toISOString()} NOT debug`,
    );
  });

  test("quoted value with keyword", () => {
    const result = normalizeTimeDirectives('start="this morning"', opts);
    const morning = new Date(2024, 5, 15, 6, 0, 0);
    expect(result).toBe(`start=${morning.toISOString()}`);
  });

  test("slash date (must be quoted due to / regex syntax)", () => {
    const result = normalizeTimeDirectives('start="1/15/2024"', opts);
    expect(result).toBe(`start=${localISO(2024, 1, 15)}`);
  });

  test("empty expression", () => {
    expect(normalizeTimeDirectives("", opts)).toBe("");
  });

  test("expression with no time directives", () => {
    const q = "error timeout level=warn";
    expect(normalizeTimeDirectives(q, opts)).toBe(q);
  });
});
