import { describe, expect, test } from "bun:test";
import {
  formatBytes,
  formatBytesBigint,
  parseBytes,
  formatDuration,
  parseDuration,
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

describe("roundtrip: parseDuration <-> formatDuration", () => {
  for (const s of ["30s", "5m", "1h", "1h30m"]) {
    test(s, () => expect(formatDuration(parseDuration(s))).toBe(s));
  }
});
