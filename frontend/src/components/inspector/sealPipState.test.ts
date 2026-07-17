import { describe, expect, test } from "bun:test";
import { ChunkMeta, ChunkState } from "../../api/gen/gastrolog/v1/vault_pb";
import {
  chunkLifecycleState,
  computeCloudPips,
  computePips,
  pipOrder,
  type PipInputs,
} from "./sealPipState";

const base: PipInputs = {
  chunkState: "sealed",
  placementNodes: ["node-1", "node-2", "node-3"],
  residentNodes: ["node-1", "node-2", "node-3"],
  pendingAckNodes: [],
  deleteInFlight: false,
  liveNodes: new Set(["node-1", "node-2", "node-3", "node-4"]),
};

describe("chunkLifecycleState", () => {
  // gastrolog-5wh571: the enum is the sole authority — the legacy Sealed
  // bool used to paint SEALING chunks as active in the CLI, and the UI
  // shares this derivation for parity. Same states, same words.
  test("maps each lifecycle state to its word", () => {
    expect(chunkLifecycleState(new ChunkMeta({ state: ChunkState.ACTIVE }))).toBe("active");
    expect(chunkLifecycleState(new ChunkMeta({ state: ChunkState.SEALING }))).toBe("sealing");
    expect(chunkLifecycleState(new ChunkMeta({ state: ChunkState.SEALED }))).toBe("sealed");
  });

  test("unspecified state is unknown, never a guess", () => {
    expect(chunkLifecycleState(new ChunkMeta({}))).toBe("unknown");
  });

  test("legacy sealed bool never overrides the enum", () => {
    expect(chunkLifecycleState(new ChunkMeta({ sealed: true }))).toBe("unknown");
    expect(chunkLifecycleState(new ChunkMeta({ sealed: true, state: ChunkState.SEALING }))).toBe(
      "sealing",
    );
  });
});

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

  test("active chunk ignores residency (placement fallback is not a copy seal)", () => {
    const { pips } = computePips({ ...base, chunkState: "active" });
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

  test("unknown lifecycle claims nothing — even for resident nodes", () => {
    const { pips } = computePips({ ...base, chunkState: "unknown" });
    expect(pips.map((p) => p.state)).toEqual(["unknown", "unknown", "unknown"]);
    expect(pips[0]?.title).toContain("lifecycle unknown");
  });

  test("unknown lifecycle still reports unreachable nodes as missing", () => {
    const { pips } = computePips({
      ...base,
      chunkState: "unknown",
      liveNodes: new Set(["node-1", "node-2"]),
    });
    expect(pips.map((p) => p.state)).toEqual(["unknown", "unknown", "missing"]);
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

describe("computeCloudPips — local cache per node", () => {
  test("full placement row: cached, uncached, and unreachable", () => {
    const { pips, ghosts } = computeCloudPips(
      {
        placementNodes: ["node-1", "node-2", "node-3"],
        residentNodes: ["node-1"],
        liveNodes: new Set(["node-1", "node-2"]),
      },
      "S3",
    );
    expect(pips.map((p) => p.state)).toEqual(["sealed", "uncached", "missing"]);
    expect(pips[0]?.title).toContain("cached");
    expect(pips[1]?.title).toContain("served from S3");
    expect(ghosts).toEqual([]);
  });

  test("stale cache copy outside placement renders as ghost", () => {
    const { ghosts } = computeCloudPips(
      {
        placementNodes: ["node-1"],
        residentNodes: ["node-1", "node-9"],
        liveNodes: new Set(["node-1", "node-9"]),
      },
      "S3",
    );
    expect(ghosts.map((g) => g.node)).toEqual(["node-9"]);
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

const rowNodes = (input: PipInputs): string[] => {
  const { pips, ghosts } = computePips(input);
  return [...pips, ...ghosts].map((p) => p.node);
};

// gastrolog-68wsli: placement rides the config snapshot while residency /
// pending-ack ride the chunk stream — two data paths with different
// staleness. The row's node universe is the union of all three sets, so a
// node referenced by ANY source renders (in the grammar-correct state)
// instead of vanishing, and the pip count cannot flap while sources
// converge during a placement transition.
describe("computePips — union row universe (gastrolog-68wsli)", () => {

  test("row is exactly the union of placement, residency, and pending-ack sets", () => {
    const input: PipInputs = {
      ...base,
      placementNodes: ["node-1", "node-2", "node-3"],
      residentNodes: ["node-2", "node-4"],
      pendingAckNodes: ["node-5"],
      deleteInFlight: true,
      liveNodes: new Set(["node-1", "node-2", "node-3", "node-4", "node-5"]),
    };
    expect(rowNodes(input).toSorted()).toEqual(
      ["node-1", "node-2", "node-3", "node-4", "node-5"],
    );
    // Each node appears exactly once.
    expect(new Set(rowNodes(input)).size).toBe(5);
  });

  test("pending-ack node outside placement stays in the row as holds", () => {
    // Placement moved off node-4 while it still owes a delete ack. The
    // laggard blocking the receipt protocol must stay visible (red,
    // pulsing), not vanish with the placement change.
    const { ghosts } = computePips({
      ...base,
      placementNodes: ["node-1", "node-2", "node-3"],
      residentNodes: ["node-4"],
      pendingAckNodes: ["node-4"],
      deleteInFlight: true,
    });
    expect(ghosts.map((g) => [g.node, g.state])).toEqual([["node-4", "holds"]]);
    expect(ghosts[0]?.title).toContain("not in placement");
  });

  test("placement transition: node count is stable whichever source knows the node", () => {
    // Node rejoin, placements growing 3→4. Before the config snapshot
    // catches up, the chunk stream already references node-4 (residency);
    // after, placement references it. Either way the row has 4 pips —
    // node-4 changes STATE (ghost → sealed/lagging), never disappears.
    const staleConfig: PipInputs = {
      ...base,
      placementNodes: ["node-1", "node-2", "node-3"],
      residentNodes: ["node-1", "node-2", "node-3", "node-4"],
    };
    const freshConfig: PipInputs = {
      ...base,
      placementNodes: ["node-1", "node-2", "node-3", "node-4"],
      residentNodes: ["node-1", "node-2", "node-3", "node-4"],
    };
    expect(rowNodes(staleConfig)).toEqual(["node-1", "node-2", "node-3", "node-4"]);
    expect(rowNodes(freshConfig)).toEqual(["node-1", "node-2", "node-3", "node-4"]);
  });

  test("out-of-placement pips keep natural sort after the gap", () => {
    const { ghosts } = computePips({
      ...base,
      residentNodes: [...base.residentNodes, "node-10"],
      pendingAckNodes: ["node-4"],
      deleteInFlight: true,
    });
    expect(ghosts.map((g) => g.node)).toEqual(["node-4", "node-10"]);
  });
});
