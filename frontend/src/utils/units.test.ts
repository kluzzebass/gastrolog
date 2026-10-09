import { describe, expect, test } from "bun:test";
import {
  BYTES_MAX_CHARS,
  BYTES_PER_SEC_MAX_CHARS,
  COUNT_MAX_CHARS,
  PERCENT_MAX_CHARS,
  RATE_MAX_CHARS,
  RATE_PER_SEC_MAX_CHARS,
  formatBytes,
  formatBytesPerSec,
  formatCount,
  formatPercent,
  formatRate,
  formatRatePerSec,
  formatBytesBigint,
  parseBytes,
  formatDuration,
  parseDuration,
  parseDurationNanos,
  formatDurationNanos,
  formatDurationMs,
} from "./units";

// Binary math, honest IEC labels: GB means 10^9, GiB means 2^30, and the
// display computes /1024 so it says GiB. Shared semantics with the backend
// (system.ParseSize, units.FormatBytesDisplay).
describe("formatBytes", () => {
  test("zero", () => expect(formatBytes(0)).toBe("0 B"));
  test("bytes", () => expect(formatBytes(512)).toBe("512 B"));
  test("KiB", () => expect(formatBytes(1024)).toBe("1.0 KiB"));
  test("KiB fractional", () => expect(formatBytes(1536)).toBe("1.5 KiB"));
  test("MiB", () => expect(formatBytes(1048576)).toBe("1.0 MiB"));
  test("MiB fractional", () => expect(formatBytes(1572864)).toBe("1.5 MiB"));
  test("GiB", () => expect(formatBytes(1073741824)).toBe("1.0 GiB"));
  test("GiB fractional", () => expect(formatBytes(1610612736)).toBe("1.5 GiB"));
  test("just under KiB", () => expect(formatBytes(1023)).toBe("1023 B"));
  test("just under MiB", () =>
    expect(formatBytes(1048575)).toBe("1024.0 KiB"));
  test("decimal 2GB shows exact binary size", () =>
    expect(formatBytes(2_000_000_000)).toBe("1.9 GiB"));
});

// Compact exact echo: largest evenly-dividing unit, decimal preferred so a
// value entered as "2GB" round-trips verbatim, binary as exact fallback.
describe("formatBytesBigint", () => {
  test("zero returns empty", () => expect(formatBytesBigint(0n)).toBe(""));
  test("exact GiB", () => expect(formatBytesBigint(1073741824n)).toBe("1GiB"));
  test("exact decimal GB", () =>
    expect(formatBytesBigint(2_000_000_000n)).toBe("2GB"));
  test("exact MiB", () => expect(formatBytesBigint(67108864n)).toBe("64MiB"));
  test("exact decimal MB", () =>
    expect(formatBytesBigint(64_000_000n)).toBe("64MB"));
  test("exact KiB", () => expect(formatBytesBigint(1024n)).toBe("1KiB"));
  test("exact decimal KB", () => expect(formatBytesBigint(1000n)).toBe("1KB"));
  test("raw bytes", () => expect(formatBytesBigint(500n)).toBe("500B"));
  test("odd value stays raw bytes", () =>
    expect(formatBytesBigint(999n)).toBe("999B"));
  test("2GiB", () => expect(formatBytesBigint(2147483648n)).toBe("2GiB"));
});

// Strict SI/IEC, same table as backend system.ParseSize: KB/MB/GB/TB are
// decimal (x1000), KiB/MiB/GiB/TiB binary (x1024).
describe("parseBytes", () => {
  test("empty string", () => expect(parseBytes("")).toBe(0n));
  test("whitespace only", () => expect(parseBytes("  ")).toBe(0n));
  test("raw number (no unit)", () => expect(parseBytes("1024")).toBe(1024n));
  test("B suffix", () => expect(parseBytes("512B")).toBe(512n));
  test("KB is decimal", () => expect(parseBytes("1KB")).toBe(1000n));
  test("KiB is binary", () => expect(parseBytes("1KiB")).toBe(1024n));
  test("MB is decimal", () => expect(parseBytes("64MB")).toBe(64_000_000n));
  test("MiB is binary", () => expect(parseBytes("64MiB")).toBe(67108864n));
  test("GB is decimal", () => expect(parseBytes("1GB")).toBe(1_000_000_000n));
  test("GiB is binary", () => expect(parseBytes("1GiB")).toBe(1073741824n));
  test("TB is decimal", () =>
    expect(parseBytes("2TB")).toBe(2_000_000_000_000n));
  test("decimals accepted", () =>
    expect(parseBytes("1.5GB")).toBe(1_500_000_000n));
  test("case insensitive", () => expect(parseBytes("64mb")).toBe(64_000_000n));
  test("case insensitive IEC", () =>
    expect(parseBytes("1gib")).toBe(1073741824n));
  test("with whitespace", () => expect(parseBytes(" 64MB ")).toBe(64_000_000n));
  test("invalid returns 0", () => expect(parseBytes("abc")).toBe(0n));
  test("negative-like returns 0", () => expect(parseBytes("-1MB")).toBe(0n));
});

describe("formatDuration", () => {
  test("zero returns empty", () => expect(formatDuration(0n)).toBe(""));
  test("seconds only", () => expect(formatDuration(30n)).toBe("30s"));
  test("minutes only", () => expect(formatDuration(300n)).toBe("5m"));
  test("hours only", () => expect(formatDuration(3600n)).toBe("1h"));
  test("hours and minutes", () => expect(formatDuration(5400n)).toBe("1h30m"));
  test("24h (1 day)", () => expect(formatDuration(86400n)).toBe("24h"));
  test("48h (2 days)", () => expect(formatDuration(172800n)).toBe("48h"));
  test("days + hours", () => expect(formatDuration(90000n)).toBe("25h"));
  test("complex: h+m+s", () => expect(formatDuration(3661n)).toBe("1h1m1s"));
  test("720h (30 days)", () => expect(formatDuration(2592000n)).toBe("720h"));
});

describe("parseDuration", () => {
  test("empty string", () => expect(parseDuration("")).toBe(0n));
  test("whitespace only", () => expect(parseDuration("  ")).toBe(0n));
  test("seconds", () => expect(parseDuration("30s")).toBe(30n));
  test("minutes", () => expect(parseDuration("5m")).toBe(300n));
  test("hours", () => expect(parseDuration("1h")).toBe(3600n));
  test("days", () => expect(parseDuration("1d")).toBe(86400n));
  test("combined h+m", () => expect(parseDuration("1h30m")).toBe(5400n));
  test("combined d+h", () => expect(parseDuration("1d12h")).toBe(129600n));
  test("combined d+h+m+s", () =>
    expect(parseDuration("1d1h1m1s")).toBe(90061n));
  test("bare number treated as seconds", () =>
    expect(parseDuration("300")).toBe(300n));
  test("case insensitive", () => expect(parseDuration("1H30M")).toBe(5400n));
  test("with whitespace", () => expect(parseDuration(" 5m ")).toBe(300n));
});

describe("formatDurationMs", () => {
  test("milliseconds", () => expect(formatDurationMs(500)).toBe("500ms"));
  test("seconds", () => expect(formatDurationMs(5000)).toBe("5s"));
  test("minutes", () => expect(formatDurationMs(120_000)).toBe("2m"));
  test("hours only", () => expect(formatDurationMs(7_200_000)).toBe("2h"));
  test("hours and minutes", () =>
    expect(formatDurationMs(8_100_000)).toBe("2h 15m"));
  test("days only", () => expect(formatDurationMs(172_800_000)).toBe("2d"));
  test("days and hours", () =>
    expect(formatDurationMs(180_000_000)).toBe("2d 2h"));
  test("just under 1s", () => expect(formatDurationMs(999)).toBe("999ms"));
  test("exactly 1s", () => expect(formatDurationMs(1000)).toBe("1s"));
  test("exactly 1m", () => expect(formatDurationMs(60_000)).toBe("1m"));
  test("exactly 1h", () => expect(formatDurationMs(3_600_000)).toBe("1h"));
  test("exactly 1d", () => expect(formatDurationMs(86_400_000)).toBe("1d"));
});

describe("roundtrip: parseBytes <-> formatBytesBigint", () => {
  for (const s of ["1KB", "64MB", "1GB", "2GB", "1KiB", "64MiB", "2GiB"]) {
    test(s, () => expect(formatBytesBigint(parseBytes(s))).toBe(s));
  }
});

// Full-precision duration helpers for stored config (nanoseconds at rest):
// value-faithful canonical output, not spelling-faithful — "2h3m10s1004ms"
// is exactly "2h3m11.004s".
describe("parseDurationNanos", () => {
  test("empty", () => expect(parseDurationNanos("")).toBe(0n));
  test("seconds", () => expect(parseDurationNanos("30s")).toBe(30_000_000_000n));
  test("bare integer = seconds", () =>
    expect(parseDurationNanos("300")).toBe(300_000_000_000n));
  test("milliseconds", () => expect(parseDurationNanos("1004ms")).toBe(1_004_000_000n));
  test("mixed full precision", () =>
    expect(parseDurationNanos("2h3m10s1004ms")).toBe(7_391_004_000_000n));
  test("days convenience", () =>
    expect(parseDurationNanos("1d")).toBe(86_400_000_000_000n));
  test("decimals", () => expect(parseDurationNanos("1.5h")).toBe(5_400_000_000_000n));
  test("junk returns 0", () => expect(parseDurationNanos("abc")).toBe(0n));
});

describe("formatDurationNanos", () => {
  test("zero returns empty", () => expect(formatDurationNanos(0n)).toBe(""));
  test("whole seconds delegate to canonical h/m/s", () =>
    expect(formatDurationNanos(5_400_000_000_000n)).toBe("1h30m"));
  test("720h stays 720h", () =>
    expect(formatDurationNanos(2_592_000_000_000_000n)).toBe("720h"));
  test("sub-second precision exact", () =>
    expect(formatDurationNanos(7_391_004_000_000n)).toBe("2h3m11.004s"));
  test("bare millis", () => expect(formatDurationNanos(1_004_000_000n)).toBe("1.004s"));
});

describe("roundtrip: parseDurationNanos <-> formatDurationNanos (value-exact)", () => {
  for (const s of ["30s", "5m", "1h30m", "720h"]) {
    test(s, () => expect(formatDurationNanos(parseDurationNanos(s))).toBe(s));
  }
  test("2h3m10s1004ms normalizes to canonical equivalent", () => {
    const n = parseDurationNanos("2h3m10s1004ms");
    expect(formatDurationNanos(n)).toBe("2h3m11.004s");
    expect(parseDurationNanos(formatDurationNanos(n))).toBe(n);
  });
});

describe("roundtrip: parseDuration <-> formatDuration", () => {
  for (const s of ["30s", "5m", "1h", "1h30m"]) {
    test(s, () => expect(formatDuration(parseDuration(s))).toBe(s));
  }
});

// Every power of `base` up to `limit`, each nudged by the fractions that
// sit on a one-decimal rounding edge, plus the integers either side.
function boundarySweep(base: number, limit: number): number[] {
  const nudges = [0.94, 0.9499, 0.95, 0.9501, 0.999, 0.99949, 0.9995, 0.99951, 1, 1.0001];
  const out = [0, 0.04, 0.05, 0.94, 0.95, 0.96, 1, 9.94, 9.95, 9.96, 99.4, 99.5, 999.4, 999.5];
  for (let p = 1; p <= limit; p *= base) {
    for (const scale of [1, 10, 100, 1000, base]) {
      for (const f of nudges) out.push(p * scale * f);
    }
    out.push(p - 1, p, p + 1);
  }
  return out.filter((v) => v < limit);
}

function longest(values: number[], format: (v: number) => string): string {
  let out = "";
  for (const v of values) {
    const s = format(v);
    if (s.length > out.length) out = s;
  }
  return out;
}

describe("width contracts", () => {
  test("formatBytes stays within BYTES_MAX_CHARS for every 64-bit byte count", () => {
    const values = [...boundarySweep(1024, 2 ** 64), ...boundarySweep(10, 2 ** 64), 2 ** 64 - 1];
    expect(longest(values, formatBytes).length).toBe(BYTES_MAX_CHARS);
  });

  test("formatBytes covers the PiB and EiB units like the backend", () => {
    expect(formatBytes(1024 ** 5)).toBe("1.0 PiB");
    expect(formatBytes(1024 ** 6)).toBe("1.0 EiB");
    expect(formatBytes(2 ** 64 - 1)).toBe("16.0 EiB");
  });

  test("formatBytesPerSec stays within BYTES_PER_SEC_MAX_CHARS", () => {
    expect(longest(boundarySweep(1024, 2 ** 64), formatBytesPerSec).length).toBe(BYTES_PER_SEC_MAX_CHARS);
    expect(formatBytesPerSec(1536)).toBe("1.5 KiB/s");
  });

  test("a fractional byte rate below 1 KiB prints whole bytes", () => {
    expect(formatBytesPerSec(123.456789)).toBe("123 B/s");
  });

  test("formatRate stays within RATE_MAX_CHARS below 999.95T", () => {
    const values = boundarySweep(10, 999.94e12);
    expect(longest(values, formatRate).length).toBe(RATE_MAX_CHARS);
  });

  test("formatRatePerSec stays within RATE_PER_SEC_MAX_CHARS", () => {
    expect(longest(boundarySweep(10, 999.94e12), formatRatePerSec).length).toBe(RATE_PER_SEC_MAX_CHARS);
    expect(formatRatePerSec(1500)).toBe("1.5K/s");
  });

  test("formatCount stays within COUNT_MAX_CHARS below 999.95T", () => {
    expect(longest(boundarySweep(10, 999.94e12), formatCount).length).toBe(COUNT_MAX_CHARS);
  });

  test("formatPercent stays within PERCENT_MAX_CHARS below 999.95%", () => {
    expect(longest(boundarySweep(10, 999.94), formatPercent).length).toBe(PERCENT_MAX_CHARS);
  });
});

describe("formatRate", () => {
  test("below ten keeps one decimal", () => expect(formatRate(5)).toBe("5.0"));
  test("rounds into whole numbers at ten", () => expect(formatRate(9.96)).toBe("10"));
  test("whole numbers below a thousand", () => expect(formatRate(999.4)).toBe("999"));
  test("rolls into K where rounding reaches a thousand", () => expect(formatRate(999.6)).toBe("1.0K"));
  test("K", () => expect(formatRate(1500)).toBe("1.5K"));
  test("rolls into M rather than printing 1000.0K", () => expect(formatRate(999_950)).toBe("1.0M"));
  test("stays in K just below the rollover", () => expect(formatRate(999_949)).toBe("999.9K"));
  test("G", () => expect(formatRate(2.5e9)).toBe("2.5G"));
});

describe("formatCount", () => {
  test("small counts print whole", () => expect(formatCount(5)).toBe("5"));
  test("below a thousand", () => expect(formatCount(999)).toBe("999"));
  test("K", () => expect(formatCount(1500)).toBe("1.5K"));
  test("rolls into M rather than printing 1000.0K", () => expect(formatCount(999_999)).toBe("1.0M"));
  test("accepts bigint", () => expect(formatCount(2_000_000n)).toBe("2.0M"));
});

describe("formatPercent", () => {
  test("one decimal", () => expect(formatPercent(12.345)).toBe("12.3%"));
});
