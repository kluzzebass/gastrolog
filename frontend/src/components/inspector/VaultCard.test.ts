import { describe, test, expect } from "bun:test";
import { chunkDiskClaimBytes, vaultRefusingCauses } from "./VaultCard";
import { ChunkMeta } from "../../api/gen/gastrolog/v1/vault_pb";
import type { NodeAlert } from "../../api/hooks/useAlerts";

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

// Pins gastrolog-33ul6h: the vault card's "refusing" badge derives entirely
// from the cluster's own standing alarms — the three per-vault admission
// gates (max-size bound, disk floor, backlog budget) — never a UI heuristic.
describe("vaultRefusingCauses", () => {
  // gateAlert stands in for a NodeAlert carrying a gate alarm's full id
  // ("type:instanceKey"), the shape SystemAlert.id is decoded into via
  // glid.encode's UTF-8 fallback for non-16-byte ids.
  function gateAlert(id: string, nodeId = "node-a"): NodeAlert {
    return {
      nodeId,
      nodeName: nodeId,
      id: new TextEncoder().encode(id),
    } as unknown as NodeAlert;
  }

  const vaultA = "vault-a-id";
  const vaultB = "vault-b-id";

  test("empty alerts yields no causes", () => {
    expect(vaultRefusingCauses([], vaultA)).toEqual([]);
  });

  test("vault-max-size-capped keyed to this vault yields its cause", () => {
    const alerts = [gateAlert(`vault-max-size-capped:${vaultA}`)];
    expect(vaultRefusingCauses(alerts, vaultA)).toEqual(["at max-size bound"]);
  });

  test("disk-space-exhausted keyed to this vault yields its cause", () => {
    const alerts = [gateAlert(`disk-space-exhausted:${vaultA}`)];
    expect(vaultRefusingCauses(alerts, vaultA)).toEqual(["volume below floor"]);
  });

  test("pipeline-backlog-capped keyed to this vault yields its cause", () => {
    const alerts = [gateAlert(`pipeline-backlog-capped:${vaultA}`)];
    expect(vaultRefusingCauses(alerts, vaultA)).toEqual(["backlog at budget"]);
  });

  test("all three gates standing at once yields all three causes", () => {
    const alerts = [
      gateAlert(`vault-max-size-capped:${vaultA}`),
      gateAlert(`disk-space-exhausted:${vaultA}`),
      gateAlert(`pipeline-backlog-capped:${vaultA}`),
    ];
    expect(vaultRefusingCauses(alerts, vaultA)).toEqual([
      "at max-size bound",
      "volume below floor",
      "backlog at budget",
    ]);
  });

  test("hidden for a different vault's alarm of the same type", () => {
    const alerts = [gateAlert(`vault-max-size-capped:${vaultB}`)];
    expect(vaultRefusingCauses(alerts, vaultA)).toEqual([]);
  });

  test("hidden for an unrelated alarm type on this vault", () => {
    const alerts = [gateAlert(`chunking-build-blocked:${vaultA}`)];
    expect(vaultRefusingCauses(alerts, vaultA)).toEqual([]);
  });

  test("hidden for a node-scoped alarm with no instance key", () => {
    const alerts = [gateAlert("node-disk-space-exhausted")];
    expect(vaultRefusingCauses(alerts, vaultA)).toEqual([]);
  });

  test("dedupes the same standing alarm reported by several nodes", () => {
    const alerts = [
      gateAlert(`vault-max-size-capped:${vaultA}`, "node-a"),
      gateAlert(`vault-max-size-capped:${vaultA}`, "node-b"),
      gateAlert(`vault-max-size-capped:${vaultA}`, "node-c"),
    ];
    expect(vaultRefusingCauses(alerts, vaultA)).toEqual(["at max-size bound"]);
  });

  test("mixed: one vault's alarms don't leak into another's badge", () => {
    const alerts = [
      gateAlert(`vault-max-size-capped:${vaultA}`),
      gateAlert(`disk-space-exhausted:${vaultB}`),
    ];
    expect(vaultRefusingCauses(alerts, vaultA)).toEqual(["at max-size bound"]);
    expect(vaultRefusingCauses(alerts, vaultB)).toEqual(["volume below floor"]);
  });
});
