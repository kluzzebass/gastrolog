import { describe, expect, test } from "bun:test";
import { formatJobSchedule } from "./jobSchedule";

describe("formatJobSchedule", () => {
  test("converts @every to 6-field cron", () => {
    expect(formatJobSchedule("@every 1s")).toBe("* * * * * *");
    expect(formatJobSchedule("@every 5s")).toBe("*/5 * * * * *");
  });

  test("upgrades 5-field cron", () => {
    expect(formatJobSchedule("* * * * *")).toBe("0 * * * * *");
    expect(formatJobSchedule("0 * * * *")).toBe("0 0 * * * *");
  });

  test("passes through canonical cron", () => {
    expect(formatJobSchedule("*/30 * * * * *")).toBe("*/30 * * * * *");
    expect(formatJobSchedule("once")).toBe("once");
  });
});
