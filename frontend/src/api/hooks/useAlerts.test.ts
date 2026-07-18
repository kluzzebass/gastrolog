import { describe, expect, test } from "bun:test";
import { alarmRank, sortAlerts, AlarmPriority } from "./useAlerts";
import type { NodeAlert } from "./useAlerts";

/** Minimal NodeAlert stand-in: rank and sort only read priority,
 *  softwareFault and firstSeen. */
function alert(
  id: string,
  priority: AlarmPriority,
  opts: { softwareFault?: boolean; firstSeenSec?: number } = {},
): NodeAlert {
  return {
    nodeId: "node-a",
    nodeName: "node-a",
    id: new TextEncoder().encode(id),
    priority,
    softwareFault: opts.softwareFault ?? false,
    firstSeen: { seconds: BigInt(opts.firstSeenSec ?? 0) },
  } as unknown as NodeAlert;
}

describe("alarmRank", () => {
  test("software faults outrank every priority", () => {
    expect(
      alarmRank({ priority: AlarmPriority.CRITICAL, softwareFault: true }),
    ).toBeGreaterThan(alarmRank({ priority: AlarmPriority.CRITICAL, softwareFault: false }));
  });

  test("priorities rank critical > high > low > unspecified", () => {
    const ranks = [
      AlarmPriority.CRITICAL,
      AlarmPriority.HIGH,
      AlarmPriority.LOW,
      AlarmPriority.UNSPECIFIED,
    ].map((p) => alarmRank({ priority: p, softwareFault: false }));
    expect(ranks).toEqual([...ranks].sort((a, b) => b - a));
  });
});

describe("sortAlerts", () => {
  test("highest rank first, oldest first within a rank", () => {
    const alerts = [
      alert("low", AlarmPriority.LOW, { firstSeenSec: 10 }),
      alert("critical-new", AlarmPriority.CRITICAL, { firstSeenSec: 30 }),
      alert("fault", AlarmPriority.UNSPECIFIED, { softwareFault: true, firstSeenSec: 40 }),
      alert("critical-old", AlarmPriority.CRITICAL, { firstSeenSec: 20 }),
      alert("high", AlarmPriority.HIGH, { firstSeenSec: 5 }),
    ];
    sortAlerts(alerts);
    expect(alerts.map((a) => new TextDecoder().decode(a.id))).toEqual([
      "fault",
      "critical-old",
      "critical-new",
      "high",
      "low",
    ]);
  });
});
