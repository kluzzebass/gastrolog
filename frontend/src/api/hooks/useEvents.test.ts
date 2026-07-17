import { describe, test, expect } from "bun:test";
import { filterEvents, eventFilterOptions, eventNodeLabel } from "./useEvents";
import type { SystemEvent } from "../gen/gastrolog/v1/lifecycle_pb";

function ev(partial: Partial<SystemEvent>): SystemEvent {
  return {
    nodeId: new Uint8Array(),
    nodeName: "",
    seq: BigInt(0),
    type: "",
    source: "",
    alarmId: new Uint8Array(),
    detail: "",
    by: "",
    ...partial,
  } as unknown as SystemEvent;
}

const events = [
  ev({ type: "node-started", source: "node", nodeName: "alpha" }),
  ev({ type: "alarm-raised", source: "storage", nodeName: "alpha" }),
  ev({ type: "alarm-acked", source: "storage", nodeName: "bravo", by: "op" }),
  ev({ type: "election-storm", source: "raft", nodeName: "bravo" }),
];

describe("filterEvents", () => {
  test("empty filters pass everything through", () => {
    expect(filterEvents(events, { type: "", source: "" })).toHaveLength(4);
  });

  test("type filter is exact", () => {
    const got = filterEvents(events, { type: "alarm-raised", source: "" });
    expect(got).toHaveLength(1);
    expect(got[0]?.source).toBe("storage");
  });

  test("source filter is exact and composes with type", () => {
    expect(filterEvents(events, { type: "", source: "storage" })).toHaveLength(2);
    expect(filterEvents(events, { type: "alarm-acked", source: "storage" })).toHaveLength(1);
    expect(filterEvents(events, { type: "alarm-acked", source: "raft" })).toHaveLength(0);
  });
});

describe("eventFilterOptions", () => {
  test("derives sorted, deduplicated option lists", () => {
    const { types, sources } = eventFilterOptions(events);
    expect(types).toEqual(["alarm-acked", "alarm-raised", "election-storm", "node-started"]);
    expect(sources).toEqual(["node", "raft", "storage"]);
  });

  test("empty input yields empty options", () => {
    expect(eventFilterOptions([])).toEqual({ types: [], sources: [] });
  });
});

describe("eventNodeLabel", () => {
  test("prefers the resolved node name", () => {
    expect(eventNodeLabel(ev({ nodeName: "alpha", nodeId: new TextEncoder().encode("n1") }))).toBe("alpha");
  });

  test("falls back to the node ID — attribution is never blank", () => {
    expect(eventNodeLabel(ev({ nodeId: new TextEncoder().encode("n1") }))).toBe("n1");
  });
});
