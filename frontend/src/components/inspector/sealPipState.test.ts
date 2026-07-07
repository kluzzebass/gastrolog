import { describe, expect, test } from "bun:test";
import { computePips, pipOrder, type PipInputs } from "./sealPipState";

const base: PipInputs = {
  chunkState: "sealed",
  placementNodes: ["node-1", "node-2", "node-3"],
  residentNodes: ["node-1", "node-2", "node-3"],
  pendingAckNodes: [],
  deleteInFlight: false,
  liveNodes: new Set(["node-1", "node-2", "node-3", "node-4"]),
};

describe("pipOrder", () => {
  test("natural sort keeps node-2 before node-10", () => {
    expect(pipOrder(["node-10", "node-2", "node-1"])).toEqual(["node-1", "node-2", "node-10"]);
  });
});

describe("computePips — birth fills green", () => {
  test("fully sealed healthy row is uniformly calm", () => {
    const { pips, ghosts } = computePips(base);
    expect(pips.map((p) => p.state)).toEqual(["sealed", "sealed", "sealed"]);
    expect(ghosts).toEqual([]);
  });

  test("active chunk shows hollow rings", () => {
    const { pips } = computePips({ ...base, chunkState: "active", residentNodes: [] });
    expect(pips.map((p) => p.state)).toEqual(["active", "active", "active"]);
  });

  test("rejoin catch-up: the LAGGING node carries the emphasis, neighbors stay calm", () => {
    const { pips } = computePips({ ...base, residentNodes: ["node-1", "node-3"] });
    expect(pips.map((p) => p.state)).toEqual(["sealed", "lagging", "sealed"]);
    expect(pips[1]?.title).toContain("copy lagging");
  });

  test("sealing chunk: residents sealed, others building (routine, no lag emphasis)", () => {
    const { pips } = computePips({
      ...base,
      chunkState: "sealing",
      residentNodes: ["node-2"],
    });
    expect(pips.map((p) => p.state)).toEqual(["sealing", "sealed", "sealing"]);
  });

  test("unreachable placement node renders missing, neighbors stay calm", () => {
    const { pips } = computePips({
      ...base,
      liveNodes: new Set(["node-1", "node-2"]),
    });
    expect(pips.map((p) => p.state)).toEqual(["sealed", "sealed", "missing"]);
  });
});

describe("computePips — death drains red", () => {
  test("delete in flight: laggard holds, others acked", () => {
    const { pips } = computePips({
      ...base,
      deleteInFlight: true,
      pendingAckNodes: ["node-2"],
      residentNodes: ["node-2"],
    });
    expect(pips.map((p) => p.state)).toEqual(["acked", "holds", "acked"]);
  });

  test("delete overrides calm even when everything is resident", () => {
    const { pips } = computePips({
      ...base,
      deleteInFlight: true,
      pendingAckNodes: ["node-1", "node-2", "node-3"],
    });
    expect(pips.every((p) => p.state === "holds")).toBe(true);
  });
});

describe("computePips — ghosts", () => {
  test("resident outside placement renders as ghost after the gap", () => {
    const { pips, ghosts } = computePips({
      ...base,
      residentNodes: [...base.residentNodes, "node-4"],
    });
    expect(ghosts.map((g) => g.node)).toEqual(["node-4"]);
    expect(ghosts[0]?.state).toBe("ghost");
    // The ghost itself carries the anomaly emphasis; placement pips stay calm.
    expect(pips.every((p) => p.state === "sealed")).toBe(true);
  });

  test("ghosts natural-sort by name", () => {
    const { ghosts } = computePips({
      ...base,
      residentNodes: [...base.residentNodes, "node-10", "node-4"],
    });
    expect(ghosts.map((g) => g.node)).toEqual(["node-4", "node-10"]);
  });
});
