import { describe, test, expect } from "bun:test";
import { render } from "@testing-library/react";
import { createTestQueryClient, wrapper } from "../../test/render";
import { encode } from "../api/glid";
import { ContributionReport, DegradedPeer } from "../api/gen/gastrolog/v1/vault_pb";
import { DegradedPeersBadge } from "./DegradedPeersBadge";

function testId(n: number): Uint8Array {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const NODE_A = testId(40);
const NODE_B = testId(41);

function seedCluster(qc: ReturnType<typeof createTestQueryClient>) {
  qc.setQueryData(["settings"], { nodeId: NODE_A });
  qc.setQueryData(["system"], {
    nodeConfigs: [
      { id: NODE_A, name: "node-1" },
      { id: NODE_B, name: "node-2" },
    ],
    vaults: [],
    ingesters: [],
    routes: [],
    nodeStorageConfigs: [],
  });
  qc.setQueryData(["clusterStatus"], {
    clusterEnabled: true,
    nodes: [
      { id: NODE_A, name: "node-1" },
      { id: NODE_B, name: "node-2" },
    ],
  });
}

describe("DegradedPeersBadge", () => {
  test("renders nothing when report is null (quiet-until-needed)", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const { container } = render(<DegradedPeersBadge report={null} dark />, {
      wrapper: wrapper(qc),
    });
    expect(container.firstChild).toBeNull();
  });

  test("renders nothing when the report has no degraded peers", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const report = new ContributionReport({ degraded: [] });
    const { container } = render(<DegradedPeersBadge report={report} dark />, {
      wrapper: wrapper(qc),
    });
    expect(container.firstChild).toBeNull();
  });

  test("renders a partial pill counting the degraded peers", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const report = new ContributionReport({
      degraded: [new DegradedPeer({ nodeId: encode(NODE_B), reason: "timeout" })],
    });
    const { getByText } = render(<DegradedPeersBadge report={report} dark />, {
      wrapper: wrapper(qc),
    });
    expect(getByText("partial (1)")).toBeTruthy();
  });

  test("resolves node IDs to names and includes the reason in the tooltip", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const report = new ContributionReport({
      degraded: [new DegradedPeer({ nodeId: encode(NODE_B), reason: "timeout" })],
    });
    const { getByText } = render(<DegradedPeersBadge report={report} dark />, {
      wrapper: wrapper(qc),
    });
    const badge = getByText("partial (1)");
    const title = badge.getAttribute("title") ?? "";
    expect(title).toContain("node-2");
    expect(title).toContain("timeout");
  });

  test("falls back to the raw node ID when it does not resolve", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const unknown = encode(testId(99));
    const report = new ContributionReport({
      degraded: [new DegradedPeer({ nodeId: unknown, reason: "connection refused" })],
    });
    const { getByText } = render(<DegradedPeersBadge report={report} dark />, {
      wrapper: wrapper(qc),
    });
    const title = getByText("partial (1)").getAttribute("title") ?? "";
    expect(title).toContain(unknown);
    expect(title).toContain("connection refused");
  });
});
