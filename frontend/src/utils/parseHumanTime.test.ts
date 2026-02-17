import { describe, expect, test } from "bun:test";
import { parseHumanTime } from "./parseHumanTime";

// Fixed "now" for deterministic tests: 2024-06-15 14:30:00 local time
const NOW = new Date(2024, 5, 15, 14, 30, 0, 0); // June 15, 2024 (Saturday)

describe("parseHumanTime", () => {
  describe("passthrough (returns null)", () => {
    test("RFC3339", () => {
      expect(parseHumanTime("2024-01-15T08:00:00Z", { now: NOW })).toBeNull();
    });
    test("RFC3339 with offset", () => {
      expect(
        parseHumanTime("2024-01-15T08:00:00+05:00", { now: NOW }),
      ).toBeNull();
    });
    test("Unix timestamp", () => {
      expect(parseHumanTime("1705312800", { now: NOW })).toBeNull();
    });
    test("empty string", () => {
      expect(parseHumanTime("", { now: NOW })).toBeNull();
    });
    test("whitespace only", () => {
      expect(parseHumanTime("  ", { now: NOW })).toBeNull();
    });
  });

  describe("relaxed ISO", () => {
    test("date only", () => {
      const d = parseHumanTime("2024-01-15", { now: NOW })!;
      expect(d.getFullYear()).toBe(2024);
      expect(d.getMonth()).toBe(0);
      expect(d.getDate()).toBe(15);
      expect(d.getHours()).toBe(0);
      expect(d.getMinutes()).toBe(0);
    });
    test("date with space time", () => {
      const d = parseHumanTime("2024-01-15 08:30", { now: NOW })!;
      expect(d.getFullYear()).toBe(2024);
      expect(d.getMonth()).toBe(0);
      expect(d.getDate()).toBe(15);
      expect(d.getHours()).toBe(8);
      expect(d.getMinutes()).toBe(30);
    });
    test("date with T time", () => {
      const d = parseHumanTime("2024-01-15T08:30", { now: NOW })!;
      expect(d.getHours()).toBe(8);
      expect(d.getMinutes()).toBe(30);
    });
    test("date with time and seconds", () => {
      const d = parseHumanTime("2024-01-15 08:30:45", { now: NOW })!;
      expect(d.getSeconds()).toBe(45);
    });
    test("invalid month", () => {
      expect(parseHumanTime("2024-13-01", { now: NOW })).toBeNull();
    });
    test("invalid day", () => {
      expect(parseHumanTime("2024-01-32", { now: NOW })).toBeNull();
    });
    test("invalid hour", () => {
      expect(parseHumanTime("2024-01-15 25:00", { now: NOW })).toBeNull();
    });
  });

  describe("slash/dot dates", () => {
    test("MDY format", () => {
      const d = parseHumanTime("1/15/2024", {
        now: NOW,
        dateOrder: "mdy",
      })!;
      expect(d.getFullYear()).toBe(2024);
      expect(d.getMonth()).toBe(0); // Jan
      expect(d.getDate()).toBe(15);
    });
    test("DMY format", () => {
      const d = parseHumanTime("15/1/2024", {
        now: NOW,
        dateOrder: "dmy",
      })!;
      expect(d.getFullYear()).toBe(2024);
      expect(d.getMonth()).toBe(0); // Jan
      expect(d.getDate()).toBe(15);
    });
    test("YMD with 4-digit first", () => {
      // 4-digit first → always YMD regardless of locale
      const d = parseHumanTime("2024/01/15", {
        now: NOW,
        dateOrder: "dmy",
      })!;
      expect(d.getFullYear()).toBe(2024);
      expect(d.getMonth()).toBe(0);
      expect(d.getDate()).toBe(15);
    });
    test("dot separator", () => {
      const d = parseHumanTime("15.1.2024", {
        now: NOW,
        dateOrder: "dmy",
      })!;
      expect(d.getFullYear()).toBe(2024);
      expect(d.getMonth()).toBe(0);
      expect(d.getDate()).toBe(15);
    });
    test("invalid month in slash date", () => {
      expect(
        parseHumanTime("13/15/2024", { now: NOW, dateOrder: "mdy" }),
      ).toBeNull();
    });
  });

  describe("time-only", () => {
    test("HH:MM", () => {
      const d = parseHumanTime("08:00", { now: NOW })!;
      expect(d.getFullYear()).toBe(2024);
      expect(d.getMonth()).toBe(5); // June (same day as NOW)
      expect(d.getDate()).toBe(15);
      expect(d.getHours()).toBe(8);
      expect(d.getMinutes()).toBe(0);
    });
    test("H:MM", () => {
      const d = parseHumanTime("8:00", { now: NOW })!;
      expect(d.getHours()).toBe(8);
    });
    test("HH:MM:SS", () => {
      const d = parseHumanTime("14:30:45", { now: NOW })!;
      expect(d.getHours()).toBe(14);
      expect(d.getMinutes()).toBe(30);
      expect(d.getSeconds()).toBe(45);
    });
    test("invalid time", () => {
      expect(parseHumanTime("25:00", { now: NOW })).toBeNull();
    });
  });

  describe("named keywords", () => {
    test("now", () => {
      const d = parseHumanTime("now", { now: NOW })!;
      expect(d.getTime()).toBe(NOW.getTime());
    });
    test("NOW (case insensitive)", () => {
      const d = parseHumanTime("NOW", { now: NOW })!;
      expect(d.getTime()).toBe(NOW.getTime());
    });
    test("today", () => {
      const d = parseHumanTime("today", { now: NOW })!;
      expect(d.getFullYear()).toBe(2024);
      expect(d.getMonth()).toBe(5);
      expect(d.getDate()).toBe(15);
      expect(d.getHours()).toBe(0);
      expect(d.getMinutes()).toBe(0);
    });
    test("yesterday", () => {
      const d = parseHumanTime("yesterday", { now: NOW })!;
      expect(d.getDate()).toBe(14);
      expect(d.getHours()).toBe(0);
    });
    test("tomorrow", () => {
      const d = parseHumanTime("tomorrow", { now: NOW })!;
      expect(d.getDate()).toBe(16);
      expect(d.getHours()).toBe(0);
    });
  });

  describe("period keywords", () => {
    test("this morning", () => {
      const d = parseHumanTime("this morning", { now: NOW })!;
      expect(d.getDate()).toBe(15);
      expect(d.getHours()).toBe(6);
    });
    test("this afternoon", () => {
      const d = parseHumanTime("this afternoon", { now: NOW })!;
      expect(d.getHours()).toBe(12);
    });
    test("this evening", () => {
      const d = parseHumanTime("this evening", { now: NOW })!;
      expect(d.getHours()).toBe(18);
    });
    test("tonight", () => {
      const d = parseHumanTime("tonight", { now: NOW })!;
      expect(d.getHours()).toBe(18);
    });
  });

  describe("last day-of-week", () => {
    // NOW is Saturday (day 6), June 15, 2024
    test("last friday", () => {
      const d = parseHumanTime("last friday", { now: NOW })!;
      expect(d.getDay()).toBe(5);
      expect(d.getDate()).toBe(14); // June 14
      expect(d.getHours()).toBe(0);
    });
    test("last saturday (7 days ago, not today)", () => {
      const d = parseHumanTime("last saturday", { now: NOW })!;
      expect(d.getDay()).toBe(6);
      expect(d.getDate()).toBe(8); // June 8, not today
    });
    test("last monday", () => {
      const d = parseHumanTime("last monday", { now: NOW })!;
      expect(d.getDay()).toBe(1);
      expect(d.getDate()).toBe(10); // June 10
    });
    test("last sunday", () => {
      const d = parseHumanTime("last sunday", { now: NOW })!;
      expect(d.getDay()).toBe(0);
      expect(d.getDate()).toBe(9); // June 9
    });
    test("invalid day name", () => {
      expect(parseHumanTime("last foobar", { now: NOW })).toBeNull();
    });
  });

  describe("relative phrases", () => {
    test("3 hours ago", () => {
      const d = parseHumanTime("3 hours ago", { now: NOW })!;
      expect(d.getHours()).toBe(11);
      expect(d.getMinutes()).toBe(30);
    });
    test("1 hour ago", () => {
      const d = parseHumanTime("1 hour ago", { now: NOW })!;
      expect(d.getHours()).toBe(13);
    });
    test("30 minutes ago", () => {
      const d = parseHumanTime("30 minutes ago", { now: NOW })!;
      expect(d.getHours()).toBe(14);
      expect(d.getMinutes()).toBe(0);
    });
    test("2 days ago", () => {
      const d = parseHumanTime("2 days ago", { now: NOW })!;
      expect(d.getDate()).toBe(13);
    });
    test("1 week ago", () => {
      const d = parseHumanTime("1 week ago", { now: NOW })!;
      expect(d.getDate()).toBe(8);
    });
    test("2 months ago", () => {
      const d = parseHumanTime("2 months ago", { now: NOW })!;
      expect(d.getMonth()).toBe(3); // April
    });
    test("1 year ago", () => {
      const d = parseHumanTime("1 year ago", { now: NOW })!;
      expect(d.getFullYear()).toBe(2023);
    });
    test("abbreviated units: 5 min ago", () => {
      const d = parseHumanTime("5 min ago", { now: NOW })!;
      expect(d.getMinutes()).toBe(25);
    });
    test("abbreviated units: 2h ago", () => {
      const d = parseHumanTime("2h ago", { now: NOW })!;
      expect(d.getHours()).toBe(12);
    });
    test("abbreviated units: 10s ago", () => {
      const d = parseHumanTime("10s ago", { now: NOW })!;
      expect(d.getSeconds()).toBe(50);
    });
    test("invalid unit", () => {
      expect(parseHumanTime("3 foos ago", { now: NOW })).toBeNull();
    });
  });

  describe("unparseable", () => {
    test("random text", () => {
      expect(parseHumanTime("hello world", { now: NOW })).toBeNull();
    });
    test("partial format", () => {
      expect(parseHumanTime("2024-01", { now: NOW })).toBeNull();
    });
  });
});
