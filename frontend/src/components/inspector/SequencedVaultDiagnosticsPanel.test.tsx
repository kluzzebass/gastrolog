import { describe, test, expect, beforeEach } from "bun:test";
import { render, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { encode } from "../../api/glid";
import {
  GetSequencedVaultDiagnosticsResponse,
  SeqActiveLeaseDiagnostics,
  SeqAllocatorDiagnostics,
  FenceRecordDiagnostics,
  VaultStats,
} from "../../api/gen/gastrolog/v1/vault_pb";
import { ClusterNode } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import { VaultConfig, VaultType } from "../../api/gen/gastrolog/v1/system_pb";
import { Timestamp } from "@bufbuild/protobuf";

const mocks = installMockClients();

import { SequencedVaultDiagnosticsPanel } from "./SequencedVaultDiagnosticsPanel";

const vaultBytes = new Uint8Array(16);
vaultBytes[15] = 1;
const vaultId = encode(vaultBytes);
const nodeBytes = new Uint8Array(16);
nodeBytes[15] = 2;

function seedQueries(qc: ReturnType<typeof createTestQueryClient>) {
  qc.setQueryData(["system"], {
    vaults: [
      new VaultConfig({
        id: vaultBytes,
        name: "seq-vault",
        type: VaultType.FILE,
        writeModel: "sequenced",
      }),
    ],
    nodeConfigs: [{ id: nodeBytes, name: "node-a" }],
  });
  qc.setQueryData(["clusterStatus"], {
    nodes: [
      new ClusterNode({
        id: nodeBytes,
        name: "node-a",
        stats: {
          vaults: [
            new VaultStats({
              id: vaultBytes,
              ingestHighWatermark: 100n,
              spoolWatermark: 90n,
            }),
          ],
        },
      }),
    ],
  });
}

beforeEach(() => {
  m(mocks.vaultClient, "getSequencedVaultDiagnostics").mockClear();
});

describe("SequencedVaultDiagnosticsPanel", () => {
  test("renders local watermarks allocator fences and cluster peers", async () => {
    m(mocks.vaultClient, "getSequencedVaultDiagnostics").mockResolvedValue(
      new GetSequencedVaultDiagnosticsResponse({
        writeModel: "sequenced",
        nodeId: "node-local",
        spoolWatermark: 90n,
        ingestHighWatermark: 100n,
        fenceHighWatermark: 80n,
        materializationWatermark: 70n,
        convergenceWatermark: 60n,
        allocator: new SeqAllocatorDiagnostics({
          nextSeq: 101n,
          epoch: 2n,
          activeSwaths: [
            new SeqActiveLeaseDiagnostics({
              holderId: "ingest-1",
              epoch: 2n,
              rangeStart: 95n,
              rangeEnd: 110n,
            }),
          ],
        }),
        fences: [
          new FenceRecordDiagnostics({
            id: 1n,
            upperBoundSeq: 80n,
            prevBoundSeq: 60n,
            createdAt: Timestamp.fromDate(new Date("2026-05-26T12:00:00Z")),
          }),
        ],
      }),
    );

    const qc = createTestQueryClient();
    seedQueries(qc);
    const { getByText } = render(
      <SequencedVaultDiagnosticsPanel vaultId={vaultId} dark />,
      { wrapper: settingsWrapper(qc) },
    );

    await waitFor(() => {
      expect(getByText("Sequenced diagnostics")).toBeTruthy();
      expect(getByText("Ingest ahead of spool (H > S_r)")).toBeTruthy();
      expect(getByText(/holder=ingest-1/)).toBeTruthy();
      expect(getByText(/F_1 upper=80/)).toBeTruthy();
      expect(getByText(/node-a: H=100 S_r=90/)).toBeTruthy();
    });
  });

  test("shows unavailable message when local diagnostics RPC fails", async () => {
    m(mocks.vaultClient, "getSequencedVaultDiagnostics").mockRejectedValue(
      new Error("vault write model is not sequenced"),
    );

    const qc = createTestQueryClient();
    seedQueries(qc);
    const { getByText } = render(
      <SequencedVaultDiagnosticsPanel vaultId={vaultId} dark />,
      { wrapper: settingsWrapper(qc) },
    );

    await waitFor(() => {
      expect(getByText("vault write model is not sequenced")).toBeTruthy();
    });
  });
});
