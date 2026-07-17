import { describe, expect, test } from "bun:test";
import { collapseFloodAlerts, FLOOD_TYPE_ID } from "./useAlerts";
import type { NodeAlert } from "./useAlerts";

/** Minimal NodeAlert stand-in: the collapse helper only reads nodeId,
 *  typeId and id. */
function alert(nodeId: string, typeId: string, instance: string): NodeAlert {
  const id = instance ? `${typeId}:${instance}` : typeId;
  return {
    nodeId,
    nodeName: nodeId,
    typeId,
    id: new TextEncoder().encode(id),
  } as unknown as NodeAlert;
}

describe("collapseFloodAlerts", () => {
  test("collapses same-type alarms of a flooding node with an accurate count", () => {
    const alerts = [
      alert("node-a", FLOOD_TYPE_ID, ""),
      alert("node-a", "node-unreachable", "p1"),
      alert("node-a", "node-unreachable", "p2"),
      alert("node-a", "node-unreachable", "p3"),
      alert("node-a", "vault-leaderless", "v1"),
    ];
    const groups = collapseFloodAlerts(alerts, new Set(["node-a"]));

    expect(groups).toHaveLength(3);
    // The flood alarm itself never collapses into a type group.
    expect(groups[0]!.alerts).toHaveLength(1);
    expect(groups[0]!.alerts[0]!.typeId).toBe(FLOOD_TYPE_ID);
    // Three node-unreachable instances fold into one group of 3.
    expect(groups[1]!.alerts).toHaveLength(3);
    expect(groups[1]!.alerts.map((a) => a.nodeId)).toEqual([
      "node-a",
      "node-a",
      "node-a",
    ]);
    // A lone instance of another type stays a singleton.
    expect(groups[2]!.alerts).toHaveLength(1);
  });

  test("non-flooding nodes keep per-instance rows", () => {
    const alerts = [
      alert("node-b", "node-unreachable", "p1"),
      alert("node-b", "node-unreachable", "p2"),
    ];
    const groups = collapseFloodAlerts(alerts, new Set());
    expect(groups).toHaveLength(2);
  });

  test("collapse is per node: one node flooding never folds another node's alarms", () => {
    const alerts = [
      alert("node-a", "node-unreachable", "p1"),
      alert("node-a", "node-unreachable", "p2"),
      alert("node-b", "node-unreachable", "p1"),
      alert("node-b", "node-unreachable", "p2"),
    ];
    const groups = collapseFloodAlerts(alerts, new Set(["node-a"]));
    expect(groups).toHaveLength(3);
    expect(groups[0]!.alerts).toHaveLength(2); // node-a collapsed
    expect(groups[1]!.alerts).toHaveLength(1); // node-b itemized
    expect(groups[2]!.alerts).toHaveLength(1);
  });

  test("a group sits where its first member sorted", () => {
    const alerts = [
      alert("node-a", "vault-leaderless", "v1"),
      alert("node-a", "node-unreachable", "p1"),
      alert("node-a", "vault-leaderless", "v2"),
    ];
    const groups = collapseFloodAlerts(alerts, new Set(["node-a"]));
    expect(groups).toHaveLength(2);
    expect(groups[0]!.alerts[0]!.typeId).toBe("vault-leaderless");
    expect(groups[0]!.alerts).toHaveLength(2);
    expect(groups[1]!.alerts[0]!.typeId).toBe("node-unreachable");
  });
});

describe("alarmSection", () => {
  test("maps lifecycle states to panel sections, defaulting to active", async () => {
    const { alarmSection, AlarmState } = await import("./useAlerts");
    expect(alarmSection({ state: AlarmState.ACTIVE_UNACKED })).toBe("active");
    expect(alarmSection({ state: AlarmState.ACTIVE_ACKED })).toBe("acked");
    expect(alarmSection({ state: AlarmState.CLEARED_UNACKED })).toBe("cleared");
    expect(alarmSection({ state: AlarmState.SHELVED })).toBe("shelved");
    // Pre-lifecycle broadcasts (UNSPECIFIED) read as active-unacked.
    expect(alarmSection({ state: AlarmState.UNSPECIFIED })).toBe("active");
  });
});
