import { describe, test, expect } from "bun:test";
import { chunkDiskClaimBytes } from "./VaultCard";
import { ChunkMeta } from "../../api/gen/gastrolog/v1/vault_pb";

// Pins the gastrolog-33ul6h fix: the vault size badge and per-row size cell
// both sum this LOCAL disk claim, never the cloud object size and never a
// logical-bytes fallback for an evicted cloud-backed chunk.
describe("chunkDiskClaimBytes", () => {
  test("local sealed chunk reports diskBytes", () => {
    const chunk = new ChunkMeta({ bytes: BigInt(4000), diskBytes: BigInt(900) });
    expect(chunkDiskClaimBytes(chunk)).toBe(900);
  });

  test("pipeline GLCB chunk with no diskBytes falls back to logical bytes", () => {
    const chunk = new ChunkMeta({ bytes: BigInt(4000), diskBytes: BigInt(0) });
    expect(chunkDiskClaimBytes(chunk)).toBe(4000);
  });

  test("cached cloud-backed chunk reports its cache size, not the cloud object size", () => {
    const chunk = new ChunkMeta({
      bytes: BigInt(999999),
      cloudBacked: true,
      diskBytes: BigInt(1200),
      cloudBytes: BigInt(300),
    });
    expect(chunkDiskClaimBytes(chunk)).toBe(1200);
  });

  test("evicted cloud-backed chunk reports 0, never logical bytes or cloud bytes", () => {
    const chunk = new ChunkMeta({
      bytes: BigInt(999999),
      cloudBacked: true,
      diskBytes: BigInt(0),
      cloudBytes: BigInt(300),
    });
    expect(chunkDiskClaimBytes(chunk)).toBe(0);
  });

  test("mixed vault: sum excludes evicted cloud chunks, includes cached at local size", () => {
    const plain = new ChunkMeta({ bytes: BigInt(4000), diskBytes: BigInt(900) });
    const cachedCloud = new ChunkMeta({
      bytes: BigInt(999999),
      cloudBacked: true,
      diskBytes: BigInt(1200),
      cloudBytes: BigInt(300),
    });
    const evictedCloud = new ChunkMeta({
      bytes: BigInt(999999),
      cloudBacked: true,
      diskBytes: BigInt(0),
      cloudBytes: BigInt(300),
    });
    const sum = [plain, cachedCloud, evictedCloud].reduce(
      (acc, c) => acc + chunkDiskClaimBytes(c),
      0,
    );
    expect(sum).toBe(900 + 1200 + 0);
  });
});
