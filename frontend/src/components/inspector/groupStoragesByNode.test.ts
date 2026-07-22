import { describe, expect, test } from "bun:test";
import { StorageState } from "../../api/gen/gastrolog/v1/storage_pb";
import { Storage } from "../../api/model/storage";
import { asEntityID, EMPTY_ID, idFromBytes } from "../../api/model/id";
import { groupStoragesByNode } from "./groupStoragesByNode";

// Real node IDs are always 26-char GLID strings (system.NodeConfig.ID) —
// idFromBytes/isZero treat any byte array shorter than the 16-byte GLID
// size as zero, so a short opaque tag like "node-a" would NOT round-trip
// through the wire's actual bytes representation. These fixtures use
// GLID-shaped strings to match production faithfully.
const NODE_A_ID = "06f1cjsv7tqth15mftnml5cs4a";
const NODE_B_ID = "06f1cjsv7tqth15mftnml5cs4b";
const NODE_A = asEntityID(NODE_A_ID);
const NODE_B = asEntityID(NODE_B_ID);

// 16-byte GLID stand-in — the model's `id` getter isn't under test here,
// only `nodeId` (the grouping key), but a real-shaped id keeps Storage's
// constructor honest. Same pattern as storage.test.ts.
function storageId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

// StorageState.node_id is wire-encoded as raw UTF-8 string bytes (matching
// NodeStorageConfig.node_id's existing convention, not raw GLID bytes —
// gastrolog-3cobq4 review), so fixtures build it the same way and let
// idFromBytes/Storage.nodeId round-trip it back, exactly like production.
function nodeIdBytes(nodeId: string): Uint8Array<ArrayBuffer> {
  return new Uint8Array(new TextEncoder().encode(nodeId));
}

function storage(n: number, name: string, nodeId: string): Storage {
  return new Storage(new StorageState({ id: storageId(n), name, nodeId: nodeIdBytes(nodeId) }));
}

const nodeNames = new Map([
  [NODE_A, "node-a"],
  [NODE_B, "node-b"],
]);

describe("groupStoragesByNode", () => {
  test("groups storages under their owning node", () => {
    const storages = [storage(1, "fast-a", NODE_A_ID), storage(2, "fast-b", NODE_B_ID)];

    const groups = groupStoragesByNode(storages, nodeNames, NODE_A);

    expect(groups).toHaveLength(2);
    expect(groups[0]?.nodeId).toBe(NODE_A);
    expect(groups[0]?.storages.map((s) => s.name)).toEqual(["fast-a"]);
    expect(groups[1]?.nodeId).toBe(NODE_B);
    expect(groups[1]?.storages.map((s) => s.name)).toEqual(["fast-b"]);
  });

  test("local node's group sorts first regardless of name", () => {
    const storages = [storage(1, "z-storage", NODE_B_ID), storage(2, "a-storage", NODE_A_ID)];

    // Local node is node-b, alphabetically after node-a — groupByNode's
    // local-first ordering must still put it first.
    const groups = groupStoragesByNode(storages, nodeNames, NODE_B);

    expect(groups[0]?.nodeId).toBe(NODE_B);
    expect(groups[1]?.nodeId).toBe(NODE_A);
  });

  test("an empty node_id (e.g. a stale cached entry) falls back to the local node, not lost", () => {
    const storages = [new Storage(new StorageState({ id: storageId(1), name: "orphaned", nodeId: new Uint8Array(0) }))];

    const groups = groupStoragesByNode(storages, nodeNames, NODE_A);

    expect(groups).toHaveLength(1);
    expect(groups[0]?.nodeId).toBe(NODE_A);
    expect(groups[0]?.storages.map((s) => s.name)).toEqual(["orphaned"]);
  });

  test("two nodes sharing a display name never collide onto one group", () => {
    // Reproduces the review's core concern about a name-based join: two
    // distinct node_id values must stay two groups even when their
    // resolved display names happen to collide — the join key is id.
    const storages = [storage(1, "fast-a", NODE_A_ID), storage(2, "fast-b", NODE_B_ID)];
    const collidingNames = new Map([
      [NODE_A, "shared-name"],
      [NODE_B, "shared-name"],
    ]);

    const groups = groupStoragesByNode(storages, collidingNames, NODE_A);

    expect(groups).toHaveLength(2);
    expect(new Set(groups.map((g) => g.nodeId))).toEqual(new Set([NODE_A, NODE_B]));
  });

  test("multiple storages on the same node land in one group", () => {
    const storages = [
      storage(1, "fast-1", NODE_A_ID),
      storage(2, "fast-2", NODE_A_ID),
      storage(3, "fast-3", NODE_A_ID),
    ];

    const groups = groupStoragesByNode(storages, nodeNames, NODE_A);

    expect(groups).toHaveLength(1);
    expect(groups[0]?.storages).toHaveLength(3);
  });

  test("empty input yields no groups", () => {
    expect(groupStoragesByNode([], nodeNames, EMPTY_ID)).toEqual([]);
  });

  test("nodeIdBytes round-trips through idFromBytes exactly like the wire does", () => {
    expect(idFromBytes(nodeIdBytes(NODE_A_ID))).toBe(NODE_A);
  });
});
