import { describe, test, expect } from "bun:test";
import { NodeState, VaultType } from "../api/gen/gastrolog/v1/system_pb";
import { encode } from "../api/glid";
import { removalEffect, removalRefusalReason } from "./nodeRemoval";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

// Three nodes; node 1 and 2 each have a class-1 file storage, node 3 has
// a class-2 one. Node IDs encode to the same strings the UI uses.
const n1 = encode(testId(1));
const n2 = encode(testId(2));
const n3 = encode(testId(3));
const fs1 = testId(11);
const fs2 = testId(12);
const fs3 = testId(13);
const nscs = [
  { nodeId: testId(1), fileStorages: [{ id: fs1, storageClass: 1 }] },
  { nodeId: testId(2), fileStorages: [{ id: fs2, storageClass: 1 }] },
  { nodeId: testId(3), fileStorages: [{ id: fs3, storageClass: 2 }] },
];
const live = (id: string) => ({ id, state: NodeState.LIVE });
const members = [live(n1), live(n2), live(n3)];

function fileVault(name: string, rf: number, storages: Uint8Array[], storageClass = 1) {
  return {
    name,
    type: VaultType.FILE,
    storageClass,
    replicationFactor: rf,
    placements: storages.map((s) => ({ storageId: s })),
  };
}

describe("removalEffect", () => {
  test("a vault whose only copy is on the node is orphaned", () => {
    const effect = removalEffect(n1, [fileVault("solo", 1, [fs1])], nscs, members);
    expect(effect.orphaned).toEqual(["solo"]);
    expect(removalRefusalReason(effect)).toContain("only copy of solo");
  });

  test("a vault with a surviving copy elsewhere is re-placed, not refused", () => {
    const effect = removalEffect(n1, [fileVault("pair", 1, [fs1, fs2])], nscs, members);
    expect(effect.orphaned).toEqual([]);
    expect(effect.belowRF).toEqual([]);
    expect(effect.rePlaced).toEqual(["pair"]);
    expect(removalRefusalReason(effect)).toBeUndefined();
  });

  test("a vault left below RF with no eligible node is refused with the arithmetic", () => {
    // RF=2 across the two class-1 storages; node 3 has only class 2.
    const effect = removalEffect(n1, [fileVault("pair", 2, [fs1, fs2])], nscs, members);
    expect(effect.belowRF).toEqual([{ name: "pair", rf: 2, surviving: 1 }]);
    expect(removalRefusalReason(effect)).toContain("pair (1 of 2 copies would remain)");
  });

  test("an eligible live node to re-place onto lifts the RF refusal", () => {
    const spare = [...nscs, { nodeId: testId(4), fileStorages: [{ id: testId(14), storageClass: 1 }] }];
    const effect = removalEffect(n1, [fileVault("pair", 2, [fs1, fs2])], spare, [...members, live(encode(testId(4)))]);
    expect(effect.belowRF).toEqual([]);
    expect(effect.rePlaced).toEqual(["pair"]);
  });

  test("a node that is not live cannot be the re-placement target", () => {
    const spare = [...nscs, { nodeId: testId(4), fileStorages: [{ id: testId(14), storageClass: 1 }] }];
    const down = { id: encode(testId(4)), state: NodeState.UNREACHABLE };
    const effect = removalEffect(n1, [fileVault("pair", 2, [fs1, fs2])], spare, [...members, down]);
    expect(effect.belowRF).toHaveLength(1);
  });

  test("a memory vault on a synthetic storage counts the node it names, and any node is eligible", () => {
    const memory = {
      name: "mem",
      type: VaultType.MEMORY,
      storageClass: 0,
      replicationFactor: 2,
      placements: [{ storageId: new TextEncoder().encode(`node:${n1}`) }, { storageId: new TextEncoder().encode(`node:${n2}`) }],
    };
    const effect = removalEffect(n1, [memory], nscs, members);
    expect(effect.belowRF).toEqual([]);
    expect(effect.rePlaced).toEqual(["mem"]);
  });

  test("vaults with no copy on the node are untouched", () => {
    const effect = removalEffect(n3, [fileVault("pair", 1, [fs1, fs2])], nscs, members);
    expect(effect).toEqual({ orphaned: [], belowRF: [], rePlaced: [] });
  });

  test("a placement on a node that already left the cluster is not a surviving copy", () => {
    const effect = removalEffect(n1, [fileVault("pair", 1, [fs1, fs2])], nscs, [live(n1), live(n3)]);
    expect(effect.orphaned).toEqual(["pair"]);
  });

  test("the orphan refusal comes first when both gates fire", () => {
    const effect = removalEffect(n1, [fileVault("solo", 1, [fs1]), fileVault("pair", 2, [fs1, fs2])], nscs, members);
    expect(removalRefusalReason(effect)).toContain("only copy of solo");
  });
});
