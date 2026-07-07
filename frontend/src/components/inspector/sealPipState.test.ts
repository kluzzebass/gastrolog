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
  test("fully sealed healthy row dims to calm", () => {
    const { pips, ghosts } = computePips(base);
    expect(pips.map((p) => p.state)).toEqual(["sealedCalm", "sealedCalm", "sealedCalm"]);
    expect(ghosts).toEqual([]);
  });

  test("active chunk shows hollow rings", () => {
    const { pips } = computePips({ ...base, chunkState: "active", residentNodes: [] });
    expect(pips.map((p) => p.state)).toEqual(["active", "active", "active"]);
  });

  test("rejoin catch-up: sealed row with one copy pending stays bright", () => {
    const { pips } = computePips({ ...base, residentNodes: ["node-1", "node-3"] });
    expect(pips.map((p) => p.state)).toEqual(["sealed", "sealing", "sealed"]);
  });

  test("tooltips distinguish calm from divergent-row sealed", () => {
    const calm = computePips(base).pips[0];
    const divergent = computePips({ ...base, residentNodes: ["node-1", "node-3"] }).pips[0];
    expect(calm?.title).toContain("all copies healthy");
    expect(divergent?.title).toContain("highlighted because");
    expect(calm?.title).not.toEqual(divergent?.title);
  });

  test("sealing chunk: residents sealed, others building", () => {
    const { pips } = computePips({
      ...base,
      chunkState: "sealing",
      residentNodes: ["node-2"],
    });
    expect(pips.map((p) => p.state)).toEqual(["sealing", "sealed", "sealing"]);
  });

  test("unreachable placement node renders missing, breaks calm", () => {
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
    // A ghost is an anomaly: the placement row must not read as calm.
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
