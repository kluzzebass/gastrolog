import { describe, expect, test } from "bun:test";
import { StorageState } from "../../api/gen/gastrolog/v1/storage_pb";
import { Storage } from "../../api/model/storage";
import { asEntityID, EMPTY_ID } from "../../api/model/id";
import { groupStoragesByNode } from "./groupStoragesByNode";

const NODE_A = asEntityID("node-a");
const NODE_B = asEntityID("node-b");

// 16-byte GLID stand-in — the model's `id` getter isn't under test here,
// only `nodeName` (the grouping key), but a real-shaped id keeps
// Storage's constructor honest. Same pattern as storage.test.ts.
function storageId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

function storage(n: number, name: string, nodeName: string): Storage {
  return new Storage(new StorageState({ id: storageId(n), name, nodeName }));
}

const nodeNames = new Map([
  [NODE_A, "node-a"],
  [NODE_B, "node-b"],
]);

describe("groupStoragesByNode", () => {
  test("groups storages under their resolved owning node", () => {
    const nodeIdByName = new Map([
      ["node-a", NODE_A],
      ["node-b", NODE_B],
    ]);
    const storages = [storage(1, "fast-a", "node-a"), storage(2, "fast-b", "node-b")];

    const groups = groupStoragesByNode(storages, nodeIdByName, nodeNames, NODE_A);

    expect(groups).toHaveLength(2);
    expect(groups[0]?.nodeId).toBe(NODE_A);
    expect(groups[0]?.storages.map((s) => s.name)).toEqual(["fast-a"]);
    expect(groups[1]?.nodeId).toBe(NODE_B);
    expect(groups[1]?.storages.map((s) => s.name)).toEqual(["fast-b"]);
  });

  test("local node's group sorts first regardless of name", () => {
    const nodeIdByName = new Map([
      ["node-a", NODE_A],
      ["node-b", NODE_B],
    ]);
    const storages = [storage(1, "z-storage", "node-b"), storage(2, "a-storage", "node-a")];

    // Local node is node-b, alphabetically after node-a — groupByNode's
    // local-first ordering must still put it first.
    const groups = groupStoragesByNode(storages, nodeIdByName, nodeNames, NODE_B);

    expect(groups[0]?.nodeId).toBe(NODE_B);
    expect(groups[1]?.nodeId).toBe(NODE_A);
  });

  test("a node_name the registry can't resolve falls back to the local node, not lost", () => {
    const nodeIdByName = new Map([["node-a", NODE_A]]);
    const storages = [storage(1, "orphaned", "node-unknown")];

    const groups = groupStoragesByNode(storages, nodeIdByName, nodeNames, NODE_A);

    expect(groups).toHaveLength(1);
    expect(groups[0]?.nodeId).toBe(NODE_A);
    expect(groups[0]?.storages.map((s) => s.name)).toEqual(["orphaned"]);
  });

  test("multiple storages on the same node land in one group", () => {
    const nodeIdByName = new Map([["node-a", NODE_A]]);
    const storages = [
      storage(1, "fast-1", "node-a"),
      storage(2, "fast-2", "node-a"),
      storage(3, "fast-3", "node-a"),
    ];

    const groups = groupStoragesByNode(storages, nodeIdByName, nodeNames, NODE_A);

    expect(groups).toHaveLength(1);
    expect(groups[0]?.storages).toHaveLength(3);
  });

  test("empty input yields no groups", () => {
    expect(groupStoragesByNode([], new Map(), nodeNames, EMPTY_ID)).toEqual([]);
  });
});
