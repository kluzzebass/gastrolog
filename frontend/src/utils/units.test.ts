import { describe, expect, test } from "bun:test";
import {
  formatBytes,
  formatBytesBigint,
  parseBytes,
  formatDuration,
  parseDuration,
  formatDurationMs,
} from "./units";

describe("formatBytes", () => {
  test("zero", () => expect(formatBytes(0)).toBe("0 B"));
  test("bytes", () => expect(formatBytes(512)).toBe("512 B"));
  test("KB", () => expect(formatBytes(1024)).toBe("1.0 KB"));
  test("KB fractional", () => expect(formatBytes(1536)).toBe("1.5 KB"));
  test("MB", () => expect(formatBytes(1048576)).toBe("1.0 MB"));
  test("MB fractional", () => expect(formatBytes(1572864)).toBe("1.5 MB"));
  test("GB", () => expect(formatBytes(1073741824)).toBe("1.0 GB"));
  test("GB fractional", () => expect(formatBytes(1610612736)).toBe("1.5 GB"));
  test("just under KB", () => expect(formatBytes(1023)).toBe("1023 B"));
  test("just under MB", () =>
    expect(formatBytes(1048575)).toBe("1024.0 KB"));
});

describe("formatBytesBigint", () => {
  test("zero returns empty", () => expect(formatBytesBigint(0n)).toBe(""));
  test("exact GB", () => expect(formatBytesBigint(1073741824n)).toBe("1GB"));
  test("exact MB", () => expect(formatBytesBigint(67108864n)).toBe("64MB"));
  test("exact KB", () => expect(formatBytesBigint(1024n)).toBe("1KB"));
  test("raw bytes", () => expect(formatBytesBigint(500n)).toBe("500B"));
  test("non-even MB falls to KB", () =>
    expect(formatBytesBigint(1049600n)).toBe("1025KB"));
  test("2GB", () => expect(formatBytesBigint(2147483648n)).toBe("2GB"));
});

describe("parseBytes", () => {
  test("empty string", () => expect(parseBytes("")).toBe(0n));
  test("whitespace only", () => expect(parseBytes("  ")).toBe(0n));
  test("raw number (no unit)", () => expect(parseBytes("1024")).toBe(1024n));
  test("B suffix", () => expect(parseBytes("512B")).toBe(512n));
  test("KB", () => expect(parseBytes("1KB")).toBe(1024n));
  test("MB", () => expect(parseBytes("64MB")).toBe(67108864n));
  test("GB", () => expect(parseBytes("1GB")).toBe(1073741824n));
  test("case insensitive", () => expect(parseBytes("64mb")).toBe(67108864n));
  test("with whitespace", () => expect(parseBytes(" 64MB ")).toBe(67108864n));
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
  for (const s of ["1KB", "64MB", "1GB", "2GB"]) {
    test(s, () => expect(formatBytesBigint(parseBytes(s))).toBe(s));
  }
});

describe("roundtrip: parseDuration <-> formatDuration", () => {
  for (const s of ["30s", "5m", "1h", "1h30m"]) {
    test(s, () => expect(formatDuration(parseDuration(s))).toBe(s));
  }
});
