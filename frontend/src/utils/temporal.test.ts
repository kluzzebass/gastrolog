import { describe, expect, test } from "bun:test";
import { Temporal } from "temporal-polyfill";
import { COUNTDOWN_MAX_CHARS, ELAPSED_MAX_CHARS, countdown, elapsed } from "./temporal";

const NOW = 1_800_000_000_000;
const DAY = 86_400;

function ago(secs: number): string {
  return elapsed(Temporal.Instant.fromEpochMilliseconds(NOW - secs * 1000), NOW);
}

function ahead(secs: number): string {
  return countdown(Temporal.Instant.fromEpochMilliseconds(NOW + secs * 1000), NOW);
}

// Every second of the first day, then every minute out to 100 days.
function spansUnder100Days(): number[] {
  const out: number[] = [];
  for (let s = 0; s < DAY; s++) out.push(s);
  for (let s = DAY; s < 100 * DAY; s += 60) out.push(s);
  return out;
}

function longestLength(spans: number[], fmt: (s: number) => string): number {
  let max = 0;
  for (const s of spans) max = Math.max(max, fmt(s).length);
  return max;
}

function lengthsBy(spans: number[], fmt: (s: number) => string, key: (s: number) => string): Map<string, Set<number>> {
  const out = new Map<string, Set<number>>();
  for (const s of spans) {
    const k = key(s);
    if (!out.has(k)) out.set(k, new Set());
    out.get(k)!.add(fmt(s).length);
  }
  return out;
}

// Leading unit ("s", "m", "h", "d") and its digit count: within such a
// group the trailing unit is zero-padded, so the length is fixed.
function leadingGroup(s: number): string {
  if (s < 60) return `s${String(s).length}`;
  if (s < 3600) return `m${String(Math.floor(s / 60)).length}`;
  if (s < DAY) return `h${String(Math.floor(s / 3600)).length}`;
  return `d${String(Math.floor(s / DAY)).length}`;
}

describe("elapsed", () => {
  test("zero-pads the trailing unit", () => {
    expect(ago(65)).toBe("1m 05s ago");
    expect(ago(2 * 3600 + 7 * 60)).toBe("2h 07m ago");
    expect(ago(3 * DAY + 4 * 3600)).toBe("3d 04h ago");
  });

  test("seconds alone below a minute", () => {
    expect(ago(9)).toBe("9s ago");
    expect(ago(59)).toBe("59s ago");
  });

  test("future instants read as just now", () => {
    expect(elapsed(Temporal.Instant.fromEpochMilliseconds(NOW + 5000), NOW)).toBe("just now");
  });

  test("length is fixed while only the trailing unit ticks", () => {
    for (const [group, lengths] of lengthsBy(spansUnder100Days(), ago, leadingGroup)) {
      expect({ group, lengths: [...lengths] }).toEqual({ group, lengths: [[...lengths][0]!] });
    }
  });

  test("stays within ELAPSED_MAX_CHARS under 100 days", () => {
    expect(longestLength(spansUnder100Days(), ago)).toBe(ELAPSED_MAX_CHARS);
  });
});

describe("countdown", () => {
  test("zero-pads the trailing unit", () => {
    expect(ahead(65)).toBe("in 1m 05s");
    expect(ahead(2 * 3600 + 7 * 60)).toBe("in 2h 07m");
    expect(ahead(3 * DAY + 4 * 3600)).toBe("in 3d 04h");
  });

  test("past or present instants read as now", () => {
    expect(ahead(0)).toBe("now");
  });

  test("length is fixed while only the trailing unit ticks", () => {
    const spans = spansUnder100Days().filter((s) => s > 0);
    for (const [group, lengths] of lengthsBy(spans, ahead, leadingGroup)) {
      expect({ group, lengths: [...lengths] }).toEqual({ group, lengths: [[...lengths][0]!] });
    }
  });

  test("stays within COUNTDOWN_MAX_CHARS under 100 days", () => {
    expect(longestLength(spansUnder100Days(), ahead)).toBe(COUNTDOWN_MAX_CHARS);
  });
});
