import { describe, test, expect } from "bun:test";
import { encode } from "../api/glid";
import { ClusterNode } from "../api/gen/gastrolog/v1/lifecycle_pb";
import {
  GetSequencedVaultDiagnosticsResponse,
  VaultStats,
} from "../api/gen/gastrolog/v1/vault_pb";
import {
  extractPeerSequencedWatermarks,
  formatSeq,
  hasSequencedWatermarkEvidence,
  peerSpoolDivergence,
  sequencedLagWarnings,
  watermarksFromDiagnostics,
} from "./sequencedDiagnostics";

describe("sequencedDiagnostics utils", () => {
  test("formatSeq renders zero and non-zero values", () => {
    expect(formatSeq(0)).toBe("0");
    expect(formatSeq(42n)).toBe("42");
  });

  test("sequencedLagWarnings detects pipeline lag", () => {
    const warnings = sequencedLagWarnings({
      h: 100n,
      sR: 90n,
      fN: 80n,
      mR: 70n,
      cR: 60n,
    });
    expect(warnings).toContain("Ingest ahead of spool (H > S_r)");
    expect(warnings).toContain("Spool ahead of fence (S_r > F_n)");
    expect(warnings).toContain("Fence ahead of materialization (F_n > M_r)");
    expect(warnings).toContain("Materialization ahead of convergence (M_r > C_r)");
  });

  test("sequencedLagWarnings is empty when watermarks are aligned", () => {
    expect(
      sequencedLagWarnings({ h: 50n, sR: 50n, fN: 50n, mR: 50n, cR: 50n }),
    ).toEqual([]);
  });

  test("hasSequencedWatermarkEvidence requires a non-zero marker", () => {
    expect(hasSequencedWatermarkEvidence({ h: 0n, sR: 0n, fN: 0n, mR: 0n, cR: 0n })).toBe(false);
    expect(hasSequencedWatermarkEvidence({ h: 1n, sR: 0n, fN: 0n, mR: 0n, cR: 0n })).toBe(true);
  });

  test("watermarksFromDiagnostics maps response fields", () => {
    const diag = new GetSequencedVaultDiagnosticsResponse({
      ingestHighWatermark: 10n,
      spoolWatermark: 9n,
      fenceHighWatermark: 8n,
      materializationWatermark: 7n,
      convergenceWatermark: 6n,
    });
    expect(watermarksFromDiagnostics(diag)).toEqual({
      h: 10n,
      sR: 9n,
      fN: 8n,
      mR: 7n,
      cR: 6n,
    });
  });

  test("extractPeerSequencedWatermarks filters by vault and skips empty stats", () => {
    const vaultBytes = new Uint8Array(16);
    vaultBytes[15] = 1;
    const vaultId = encode(vaultBytes);

    const nodeA = new Uint8Array(16);
    nodeA[15] = 2;
    const nodeB = new Uint8Array(16);
    nodeB[15] = 3;

    const nodes = [
      new ClusterNode({
        id: nodeA,
        name: "node-a",
        stats: {
          vaults: [
            new VaultStats({
              id: vaultBytes,
              ingestHighWatermark: 5n,
              spoolWatermark: 4n,
            }),
          ],
        },
      }),
      new ClusterNode({
        id: nodeB,
        name: "node-b",
        stats: {
          vaults: [new VaultStats({ id: vaultBytes })],
        },
      }),
    ];

    const peers = extractPeerSequencedWatermarks(
      vaultId,
      nodes,
      new Map([[encode(nodes[0]!.id), "node-a"]]),
    );
    expect(peers).toHaveLength(1);
    expect(peers[0]!.nodeName).toBe("node-a");
    expect(peers[0]!.watermarks.h).toBe(5n);
  });

  test("peerSpoolDivergence reports spread across replicas", () => {
    const msg = peerSpoolDivergence([
      { nodeId: "a", nodeName: "node-a", watermarks: { h: 10n, sR: 10n, fN: 0n, mR: 0n, cR: 0n } },
      { nodeId: "b", nodeName: "node-b", watermarks: { h: 10n, sR: 8n, fN: 0n, mR: 0n, cR: 0n } },
    ]);
    expect(msg).toContain("Replica spool spread");
  });
});
