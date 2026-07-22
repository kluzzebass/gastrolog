import { describe, expect, test } from "bun:test";
import { StorageState } from "../gen/gastrolog/v1/storage_pb";
import { Storage } from "./storage";
import { encode } from "../glid";
import { asEntityID } from "./id";

// 16-byte GLID stand-ins for tests — decode() only accepts real 26-char
// base32hex strings, so short literal ids don't round-trip through it.
// Same pattern as FileStorageCard.test.tsx / VaultsSettings.test.tsx.
function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

describe("Storage", () => {
  test("exposes identity and threshold fields verbatim from the wire", () => {
    const state = new StorageState({
      id: testId(1),
      name: "nvme-fast",
      path: "storage/nvme-fast",
      nodeName: "node-1",
      storageClass: 1,
      warnExpr: "10%",
      floorExpr: "",
      warnInherited: false,
      floorInherited: true,
      warnBytes: BigInt(5_000_000_000),
      floorBytes: BigInt(1_000_000_000),
      freeBytes: BigInt(20_000_000_000),
      totalBytes: BigInt(100_000_000_000),
      warnVerdict: false,
      protectVerdict: false,
    });
    const storage = new Storage(state);

    expect(storage.id).toBe(asEntityID(encode(testId(1))));
    expect(storage.name).toBe("nvme-fast");
    expect(storage.displayLabel).toBe("nvme-fast");
    expect(storage.path).toBe("storage/nvme-fast");
    expect(storage.nodeName).toBe("node-1");
    expect(storage.storageClass).toBe(1);
    expect(storage.warnExpr).toBe("10%");
    expect(storage.floorExpr).toBe("");
    expect(storage.warnInherited).toBe(false);
    expect(storage.floorInherited).toBe(true);
    expect(storage.warnBytes).toBe(BigInt(5_000_000_000));
    expect(storage.floorBytes).toBe(BigInt(1_000_000_000));
    expect(storage.freeBytes).toBe(BigInt(20_000_000_000));
    expect(storage.totalBytes).toBe(BigInt(100_000_000_000));
    expect(storage.warnVerdict).toBe(false);
    expect(storage.protectVerdict).toBe(false);
  });

  test("falls back to the id when unnamed", () => {
    const storage = new Storage(new StorageState({ id: testId(1), name: "" }));
    expect(storage.displayLabel).toBe(storage.id);
    expect(storage.displayLabel).not.toBe("");
  });

  test("placedVaultIds converts raw proto bytes to EntityIDs", () => {
    const storage = new Storage(
      new StorageState({ id: testId(1), placedVaultIds: [testId(20), testId(21)] }),
    );
    expect(storage.placedVaultIds).toEqual([asEntityID(encode(testId(20))), asEntityID(encode(testId(21)))]);
  });

  test("empty placedVaultIds yields an empty list, not undefined", () => {
    const storage = new Storage(new StorageState({ id: testId(1) }));
    expect(storage.placedVaultIds).toEqual([]);
  });

  describe("hasSample", () => {
    test("false before any statfs sample has landed (totalBytes zero)", () => {
      const storage = new Storage(new StorageState({ id: testId(1), totalBytes: BigInt(0) }));
      expect(storage.hasSample).toBe(false);
    });

    test("true once a real sample has a nonzero volume total", () => {
      const storage = new Storage(
        new StorageState({ id: testId(1), totalBytes: BigInt(100_000_000_000) }),
      );
      expect(storage.hasSample).toBe(true);
    });
  });
});
