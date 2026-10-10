import { describe, expect, test } from "bun:test";
import {
  formatBytes,
  formatBytesBigint,
  parseBytes,
  formatDuration,
  parseDuration,
  parseDurationNanos,
  formatDurationNanos,
  formatDurationMs,
  formatCount,
  formatApproxCount,
  formatRate,
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

// Reference renderings the compact count formatters must keep reproducing
// below one thousand, so unscaled inspector output stays byte-for-byte.
function referenceCount(n: bigint | number | string): string {
  return Number(n).toLocaleString();
}

function referenceRate(n: number): string {
  if (n >= 10) return Math.round(n).toString();
  return n.toFixed(1);
}

function referenceApproxCount(n: number): string {
  if (n < 100) return n.toString();
  const magnitude = 10 ** (Math.floor(Math.log10(n)) - 1);
  const rounded = Math.round(n / magnitude) * magnitude;
  if (rounded < 1000) return rounded.toString();
  if (rounded < 1_000_000) {
    const k = rounded / 1000;
    return k < 10 ? `${k.toFixed(1)}K` : `${Math.round(k)}K`;
  }
  if (rounded < 1_000_000_000) {
    const m = rounded / 1_000_000;
    return m < 10 ? `${m.toFixed(1)}M` : `${Math.round(m)}M`;
  }
  const b = rounded / 1_000_000_000;
  return b < 10 ? `${b.toFixed(1)}B` : `${Math.round(b)}B`;
}

function subMillionSamples(): number[] {
  const out: number[] = [];
  for (let i = 0; i <= 2_000; i++) out.push(i);
  for (let i = 2_000; i < 1_000_000; i += 37) out.push(i);
  out.push(
    999_949, 999_950, 999_999, 1_050, 1_150, 1_250, 99_950, 999.95, 1_000.05,
    1_049.95, 0.5, 12.345, 9.95, 9.99, 999_949.9,
  );
  return out;
}

function subThousandSamples(): number[] {
  const out: number[] = [];
  for (let i = 0; i < 1_000; i++) out.push(i);
  out.push(999.4, 999.5, 999.95, 0.5, 12.345, 9.95, 9.99);
  return out;
}

const SCALE_DIVISORS = [10n ** 3n, 10n ** 6n, 10n ** 9n, 10n ** 12n, 10n ** 15n];

describe("formatCount", () => {
  test("below 1K matches the reference rendering for number, bigint and string input", () => {
    for (const n of subThousandSamples()) {
      expect(formatCount(n)).toBe(referenceCount(n));
      if (Number.isInteger(n)) {
        expect(formatCount(BigInt(n))).toBe(referenceCount(n));
        expect(formatCount(String(n))).toBe(referenceCount(n));
      }
    }
  });

  test("suffix boundaries", () => {
    expect(formatCount(0)).toBe("0");
    expect(formatCount(999)).toBe("999");
    expect(formatCount(1_000)).toBe("1.0K");
    expect(formatCount(999_949)).toBe("999.9K");
    expect(formatCount(999_950)).toBe("1.0M");
    expect(formatCount(999_999)).toBe("1.0M");
    expect(formatCount(1_000_000)).toBe("1.0M");
    expect(formatCount(999_949_999)).toBe("999.9M");
    expect(formatCount(999_950_000)).toBe("1.0B");
    expect(formatCount(1_000_000_000)).toBe("1.0B");
    expect(formatCount(1_000_000_000_000)).toBe("1.0T");
    expect(formatCount(1_000_000_000_000_000n)).toBe("1.0P");
    expect(formatCount(1_000_000_000_000_000_000n)).toBe("1.0E");
  });

  test("tens of billions render as billions, not thousands of millions", () => {
    expect(formatCount(25_561_234_567n)).toBe("25.6B");
    expect(formatCount(25_561_200_000)).toBe("25.6B");
  });

  test("max uint64 formats without overflow", () => {
    expect(formatCount(18_446_744_073_709_551_615n)).toBe("18.4E");
    expect(formatCount("18446744073709551615")).toBe("18.4E");
  });

  test("past the largest suffix stays in E", () => {
    expect(formatCount(10n ** 21n)).toBe("1000.0E");
  });

  test("no scale below E ever renders 1000.0", () => {
    for (const div of SCALE_DIVISORS) {
      const top = div * 1_000n;
      for (const v of [top - div / 20n - 1n, top - div / 20n, top - 1n, top]) {
        expect(formatCount(v)).not.toMatch(/^1000\.0/);
      }
    }
  });

  test("thousands round half-up exactly like every larger scale", () => {
    expect(formatCount(1_049)).toBe("1.0K");
    expect(formatCount(1_050)).toBe("1.1K");
    expect(formatCount(1_150)).toBe("1.2K");
    expect(formatCount(1_150_000)).toBe("1.2M");
    expect(formatCount(1_049.95)).toBe("1.0K");
  });

  test("rounds half-up in exact integer math from 1M up", () => {
    expect(formatCount(1_049_999)).toBe("1.0M");
    expect(formatCount(1_050_000)).toBe("1.1M");
    expect(formatCount(1_150_000)).toBe("1.2M");
    expect(formatCount(1_049_999_999_999_999_999n)).toBe("1.0E");
    expect(formatCount(1_050_000_000_000_000_000n)).toBe("1.1E");
  });

  test("fractional numbers from 1M up never round across a boundary twice", () => {
    expect(formatCount(1_049_999.9)).toBe("1.0M");
    expect(formatCount(2_500_000.5)).toBe("2.5M");
    expect(formatCount("2500000.7")).toBe("2.5M");
  });

  test("negative, invalid and non-finite input render like the reference", () => {
    for (const n of [-1, -999, -1_000, -1_500, -999_999, -5_000_000, Number.NaN, Number.NEGATIVE_INFINITY]) {
      expect(formatCount(n)).toBe(referenceCount(n));
    }
    expect(formatCount(-25_561_234_567n)).toBe(referenceCount(-25_561_234_567n));
    expect(formatCount("abc")).toBe("NaN");
    expect(formatCount("")).toBe("0");
  });

  test("positive infinity renders as the locale infinity sign", () => {
    expect(formatCount(Number.POSITIVE_INFINITY)).toBe(Number.POSITIVE_INFINITY.toLocaleString());
  });
});

describe("formatRate", () => {
  test("below 1K matches the reference rendering", () => {
    for (const n of subThousandSamples().filter((s) => Math.round(s) < 1_000)) {
      expect(formatRate(n)).toBe(referenceRate(n));
    }
    for (const n of [-1, -12.5, Number.NaN]) expect(formatRate(n)).toBe(referenceRate(n));
  });

  test("small rates keep one decimal, mid rates round to integers", () => {
    expect(formatRate(0)).toBe("0.0");
    expect(formatRate(9.4)).toBe("9.4");
    expect(formatRate(10)).toBe("10");
    expect(formatRate(999.4)).toBe("999");
    expect(formatRate(1_000)).toBe("1.0K");
    expect(formatRate(999.5)).toBe("1.0K");
    expect(formatRate(999.95)).toBe("1.0K");
    expect(formatRate(999_950)).toBe("1.0M");
  });

  test("large rates use the full suffix ladder", () => {
    expect(formatRate(1_500_000)).toBe("1.5M");
    expect(formatRate(25_561_234_567)).toBe("25.6B");
    expect(formatRate(3.2e12)).toBe("3.2T");
  });
});

describe("formatApproxCount", () => {
  test("below 1T matches the reference rendering", () => {
    const samples = subMillionSamples();
    for (let n = 1_000_000; n < 1e12; n = Math.floor(n * 1.37) + 11) samples.push(n);
    for (const n of samples) expect(formatApproxCount(n)).toBe(referenceApproxCount(n));
  });

  test("two significant figures per suffix", () => {
    expect(formatApproxCount(99)).toBe("99");
    expect(formatApproxCount(188_093)).toBe("190K");
    expect(formatApproxCount(2_540_000)).toBe("2.5M");
    expect(formatApproxCount(999_500)).toBe("1.0M");
  });

  test("suffixes continue past billions", () => {
    expect(formatApproxCount(25_000_000_000_000)).toBe("25T");
    expect(formatApproxCount(1.94e15)).toBe("1.9P");
    expect(formatApproxCount(1.8e19)).toBe("18E");
  });
});
