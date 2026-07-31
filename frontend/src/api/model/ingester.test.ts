import { describe, test, expect } from "bun:test";
import { IngesterConfig } from "../gen/gastrolog/v1/system_pb";
import { Ingester, type NodeStatusMap } from "./ingester";
import { type EntityID, idFromBytes, EMPTY_ID } from "./id";

function bytesOf(b: number): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(new ArrayBuffer(16));
  out[0] = b;
  return out;
}

// Wire format for node_ids is "ASCII bytes of the GLID string" (legacy
// []byte(string) encoding documented in glid.ts), not 16 raw bytes.
function nodeIDBytes(id: EntityID): Uint8Array<ArrayBuffer> {
  const ascii = new TextEncoder().encode(id);
  const buf = new ArrayBuffer(ascii.length);
  const out = new Uint8Array(buf);
  out.set(ascii);
  return out;
}

const NODE_A = idFromBytes(bytesOf(1));
const NODE_B = idFromBytes(bytesOf(2));
const NODE_C = idFromBytes(bytesOf(3));
const NODE_DEAD = idFromBytes(bytesOf(99));
const liveNodes: ReadonlySet<EntityID> = new Set([NODE_A, NODE_B, NODE_C]);

function makeIngester(cfg: Partial<IngesterConfig>): Ingester {
  return new Ingester(new IngesterConfig({ id: bytesOf(42), name: "x", type: "syslog", enabled: true, ...cfg }));
}

describe("isEligibleOn", () => {
  test("allNodes=true accepts every node", () => {
    const i = makeIngester({ allNodes: true, nodeIds: [] });
    expect(i.isEligibleOn(NODE_A)).toBe(true);
    expect(i.isEligibleOn(NODE_DEAD)).toBe(true);
  });

  test("allNodes=true ignores stale pinnedNodeIds (matches backend cold-start fix)", () => {
    const i = makeIngester({ allNodes: true, nodeIds: [nodeIDBytes(NODE_A)] });
    expect(i.isEligibleOn(NODE_A)).toBe(true);
    expect(i.isEligibleOn(NODE_B)).toBe(true);
  });

  test("empty pinnedNodeIds + allNodes=false → legacy match-all", () => {
    const i = makeIngester({ allNodes: false, nodeIds: [] });
    expect(i.isEligibleOn(NODE_A)).toBe(true);
    expect(i.isEligibleOn(NODE_B)).toBe(true);
  });

  test("pinned: only listed nodes match", () => {
    const i = makeIngester({ allNodes: false, nodeIds: [nodeIDBytes(NODE_A)] });
    expect(i.isEligibleOn(NODE_A)).toBe(true);
    expect(i.isEligibleOn(NODE_B)).toBe(false);
  });
});

describe("selectedCount", () => {
  test("allNodes counts every live node", () => {
    const i = makeIngester({ allNodes: true });
    expect(i.selectedCount(liveNodes)).toBe(3);
  });

  test("pinned counts the pin list length regardless of liveness", () => {
    const i = makeIngester({ allNodes: false, nodeIds: [nodeIDBytes(NODE_A), nodeIDBytes(NODE_DEAD)] });
    expect(i.selectedCount(liveNodes)).toBe(2);
  });

  test("empty pin + allNodes=false → cluster size (legacy match-all)", () => {
    const i = makeIngester({ allNodes: false, nodeIds: [] });
    expect(i.selectedCount(liveNodes)).toBe(3);
  });
});

describe("runningCount", () => {
  test("counts only true entries in the alive map", () => {
    const i = makeIngester({});
    const alive = new Map<EntityID, NodeStatusMap>([[i.id, { [NODE_A]: true, [NODE_B]: false }]]);
    expect(i.runningCount(alive, liveNodes)).toBe(1);
  });

  test("absent id → 0", () => {
    const i = makeIngester({});
    expect(i.runningCount(new Map(), liveNodes)).toBe(0);
  });

  // Defense-in-depth filter — alive flags for nodes that aren't in the
  // cluster's current live-node set don't count. Stale FSM IngesterAlive
  // entries left by a cluster scale-down inflated the badge to "10/3".
  test("ignores alive flags for nodes not in liveNodes", () => {
    const i = makeIngester({});
    const alive = new Map<EntityID, NodeStatusMap>([
      [i.id, { [NODE_A]: true, [NODE_B]: true, [NODE_DEAD]: true }],
    ]);
    expect(i.runningCount(alive, liveNodes)).toBe(2);
  });
});

describe("statusVariant", () => {
  test("disabled → muted", () => {
    const i = makeIngester({ enabled: false, allNodes: true });
    expect(i.statusVariant(new Map(), liveNodes)).toBe("muted");
  });

  test("no selected nodes → muted", () => {
    const i = makeIngester({ enabled: true, allNodes: false, nodeIds: [] });
    expect(i.statusVariant(new Map(), new Set())).toBe("muted");
  });

  test("fully running → info", () => {
    const i = makeIngester({ allNodes: true });
    const alive = new Map<EntityID, NodeStatusMap>([[i.id, { [NODE_A]: true, [NODE_B]: true, [NODE_C]: true }]]);
    expect(i.statusVariant(alive, liveNodes)).toBe("info");
  });

  test("partial running with live pins → warn", () => {
    const i = makeIngester({ allNodes: false, nodeIds: [nodeIDBytes(NODE_A), nodeIDBytes(NODE_B)] });
    const alive = new Map<EntityID, NodeStatusMap>([[i.id, { [NODE_A]: true }]]);
    expect(i.statusVariant(alive, liveNodes)).toBe("warn");
  });

  test("pinned node has died → error", () => {
    const i = makeIngester({ allNodes: false, nodeIds: [nodeIDBytes(NODE_A), nodeIDBytes(NODE_DEAD)] });
    const alive = new Map<EntityID, NodeStatusMap>([[i.id, { [NODE_A]: true }]]);
    expect(i.statusVariant(alive, liveNodes)).toBe("error");
  });
});

describe("displayLabel", () => {
  test("uses name when set", () => {
    expect(makeIngester({ name: "syslog" }).displayLabel).toBe("syslog");
  });

  test("falls back to id when name is empty", () => {
    const i = makeIngester({ name: "" });
    expect(i.displayLabel).toBe(i.id);
    expect(i.id).not.toBe(EMPTY_ID);
  });
});
