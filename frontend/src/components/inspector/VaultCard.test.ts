import { describe, test, expect } from "bun:test";
import { chunkDiskClaimBytes, vaultRefusingCauseLabels } from "./VaultCard";
import { ChunkMeta, VaultAdmissionCause } from "../../api/gen/gastrolog/v1/vault_pb";

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

// Pins gastrolog-33ul6h: the vault card's "refusing" badge reads a
// first-class backend field (VaultInfo.admissionRefused, populated by the
// responding node's own admission-causes collector) and maps its enum
// values to terse labels — never a UI-side derivation from alarm state.
// vaultRefusingCauseLabels' empty-vs-non-empty result is exactly what gates
// the badge's visibility in VaultCard (`refusingCauses.length > 0`).
describe("vaultRefusingCauseLabels", () => {
  test("empty causes yields no labels (badge hidden)", () => {
    expect(vaultRefusingCauseLabels([])).toEqual([]);
  });

  test("MAX_SIZE_BOUND maps to its label", () => {
    expect(vaultRefusingCauseLabels([VaultAdmissionCause.MAX_SIZE_BOUND])).toEqual([
      "at max-size bound",
    ]);
  });

  test("VAULT_DISK_PROTECT maps to its label", () => {
    expect(vaultRefusingCauseLabels([VaultAdmissionCause.VAULT_DISK_PROTECT])).toEqual([
      "volume below floor",
    ]);
  });

  test("BACKLOG_BUDGET maps to its label", () => {
    expect(vaultRefusingCauseLabels([VaultAdmissionCause.BACKLOG_BUDGET])).toEqual([
      "backlog at budget",
    ]);
  });

  test("all three causes at once yields all three labels, in backend order", () => {
    expect(
      vaultRefusingCauseLabels([
        VaultAdmissionCause.VAULT_DISK_PROTECT,
        VaultAdmissionCause.MAX_SIZE_BOUND,
        VaultAdmissionCause.BACKLOG_BUDGET,
      ]),
    ).toEqual(["volume below floor", "at max-size bound", "backlog at budget"]);
  });

  test("UNSPECIFIED is dropped, never rendered as a blank badge cause", () => {
    expect(
      vaultRefusingCauseLabels([
        VaultAdmissionCause.UNSPECIFIED,
        VaultAdmissionCause.MAX_SIZE_BOUND,
      ]),
    ).toEqual(["at max-size bound"]);
  });

  test("a vault with only UNSPECIFIED yields no labels (badge hidden)", () => {
    expect(vaultRefusingCauseLabels([VaultAdmissionCause.UNSPECIFIED])).toEqual([]);
  });
});
