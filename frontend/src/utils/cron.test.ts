import { describe, expect, test } from "bun:test";
import { validateCron, describeCron } from "./cron";

describe("validateCron", () => {
  describe("valid expressions", () => {
    const valid = [
      "* * * * *",           // every minute
      "0 * * * *",           // every hour
      "*/5 * * * *",         // every 5 minutes
      "0 0 * * *",           // daily at midnight
      "0 12 * * *",          // daily at noon
      "0 0 * * 0",           // weekly on Sunday
      "0 0 1 * *",           // monthly on 1st
      "0 0 1 1 *",           // yearly on Jan 1
      "30 4 * * *",          // daily at 4:30
      "0 0 * * 1-5",         // weekdays at midnight
      "0 */2 * * *",         // every 2 hours
      "0,30 * * * *",        // at 0 and 30 minutes
      "0 0 * * mon",         // weekly on Monday (name)
      "0 0 1 jan *",         // Jan 1st (month name)
      // 6-field (with seconds)
      "0 * * * * *",         // every minute at second 0
      "*/10 * * * * *",      // every 10 seconds
      "0 0 0 * * *",        // daily at midnight
      "0 0 12 * * *",       // daily at noon
    ];

    for (const expr of valid) {
      test(expr, () => expect(validateCron(expr).valid).toBe(true));
    }
  });

  describe("invalid expressions", () => {
    test("empty", () => {
      const r = validateCron("");
      expect(r.valid).toBe(false);
      expect(r.error).toContain("required");
    });

    test("too few fields", () => {
      const r = validateCron("* * *");
      expect(r.valid).toBe(false);
      expect(r.error).toContain("expected 5 or 6");
    });

    test("too many fields", () => {
      const r = validateCron("* * * * * * *");
      expect(r.valid).toBe(false);
    });

    test("minute out of range", () => {
      const r = validateCron("60 * * * *");
      expect(r.valid).toBe(false);
      expect(r.error).toContain("minute");
    });

    test("hour out of range", () => {
      const r = validateCron("0 24 * * *");
      expect(r.valid).toBe(false);
      expect(r.error).toContain("hour");
    });

    test("day out of range (0)", () => {
      const r = validateCron("0 0 0 * *");
      expect(r.valid).toBe(false);
      expect(r.error).toContain("day");
    });

    test("day out of range (32)", () => {
      const r = validateCron("0 0 32 * *");
      expect(r.valid).toBe(false);
    });

    test("month out of range", () => {
      const r = validateCron("0 0 1 13 *");
      expect(r.valid).toBe(false);
    });

    test("dow out of range (7)", () => {
      const r = validateCron("0 0 * * 7");
      expect(r.valid).toBe(false);
    });

    test("invalid range (start > end)", () => {
      const r = validateCron("0 0 * * 5-1");
      expect(r.valid).toBe(false);
      expect(r.error).toContain("range");
    });

    test("invalid step", () => {
      const r = validateCron("*/0 * * * *");
      expect(r.valid).toBe(false);
      expect(r.error).toContain("step");
    });

    test("invalid name", () => {
      const r = validateCron("0 0 * foo *");
      expect(r.valid).toBe(false);
    });
  });

  describe("named values", () => {
    test("month names", () => {
      expect(validateCron("0 0 1 jan *").valid).toBe(true);
      expect(validateCron("0 0 1 dec *").valid).toBe(true);
      expect(validateCron("0 0 1 JAN *").valid).toBe(true);
    });

    test("day-of-week names", () => {
      expect(validateCron("0 0 * * sun").valid).toBe(true);
      expect(validateCron("0 0 * * sat").valid).toBe(true);
      expect(validateCron("0 0 * * MON").valid).toBe(true);
    });

    test("named ranges", () => {
      expect(validateCron("0 0 * * mon-fri").valid).toBe(true);
      expect(validateCron("0 0 1 jan-jun *").valid).toBe(true);
    });
  });
});

describe("describeCron", () => {
  // 5-field expressions
  test("every minute", () =>
    expect(describeCron("* * * * *")).toBe("Every minute"));
  test("every 5 minutes", () =>
    expect(describeCron("*/5 * * * *")).toBe("Every 5 minutes"));
  test("every 1 minute", () =>
    expect(describeCron("*/1 * * * *")).toBe("Every 1 minute"));
  test("every hour on the hour", () =>
    expect(describeCron("0 * * * *")).toBe("Every hour, on the hour"));
  test("every hour at minute 30", () =>
    expect(describeCron("30 * * * *")).toBe("Every hour at minute 30"));
  test("every 2 hours", () =>
    expect(describeCron("0 */2 * * *")).toBe("Every 2 hours"));
  test("daily at midnight", () =>
    expect(describeCron("0 0 * * *")).toBe("Daily at midnight"));
  test("daily at noon", () =>
    expect(describeCron("0 12 * * *")).toBe("Daily at noon"));
  test("daily at 04:30", () =>
    expect(describeCron("30 4 * * *")).toBe("Daily at 04:30"));
  test("weekly on Sunday", () =>
    expect(describeCron("0 0 * * 0")).toBe("Every Sunday at 00:00"));
  test("weekly on Monday at 9am", () =>
    expect(describeCron("0 9 * * 1")).toBe("Every Monday at 09:00"));
  test("monthly on 1st at midnight", () =>
    expect(describeCron("0 0 1 * *")).toBe("Monthly on the 1st at 00:00"));
  test("monthly on 2nd", () =>
    expect(describeCron("0 0 2 * *")).toBe("Monthly on the 2nd at 00:00"));
  test("monthly on 3rd", () =>
    expect(describeCron("0 0 3 * *")).toBe("Monthly on the 3rd at 00:00"));
  test("monthly on 15th", () =>
    expect(describeCron("0 0 15 * *")).toBe("Monthly on the 15th at 00:00"));

  // 6-field expressions (with seconds)
  test("every second", () =>
    expect(describeCron("* * * * * *")).toBe("Every second"));
  test("every 10 seconds", () =>
    expect(describeCron("*/10 * * * * *")).toBe("Every 10 seconds"));
  test("every minute (6-field)", () =>
    expect(describeCron("0 * * * * *")).toBe("Every minute"));
  test("daily at midnight (6-field)", () =>
    expect(describeCron("0 0 0 * * *")).toBe("Daily at midnight"));
  test("daily at specific time with seconds", () =>
    expect(describeCron("30 15 4 * * *")).toBe("Daily at 04:15:30"));

  // Fallback
  test("complex expression falls back to field summary", () => {
    const desc = describeCron("0,30 9-17 * * 1-5");
    expect(desc).toContain("minute:");
    expect(desc).toContain("hour:");
    expect(desc).toContain("weekday:");
  });

  test("invalid field count returns empty", () =>
    expect(describeCron("* * *")).toBe(""));
});
