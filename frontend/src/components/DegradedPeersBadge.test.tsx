import { describe, test, expect } from "bun:test";
import { render } from "@testing-library/react";
import { createTestQueryClient, settingsWrapper } from "../../test/render";
import { encode } from "../api/glid";
import { ContributionReport, DegradedPeer } from "../api/gen/gastrolog/v1/vault_pb";
import { DegradedPeersBadge, PARTIAL_HELP_REF, partialBadgeText } from "./DegradedPeersBadge";

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

describe("partialBadgeText", () => {
  test("names the nodes the merge missed instead of a bare count", () => {
    expect(partialBadgeText(1)).toBe("partial — 1 node unreached");
    expect(partialBadgeText(3)).toBe("partial — 3 nodes unreached");
  });

  test("links to the partial-results explanation", () => {
    expect(PARTIAL_HELP_REF).toBe("inspector#partial-results");
  });
});

describe("DegradedPeersBadge", () => {
  test("renders nothing when report is null (quiet-until-needed)", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const { container } = render(<DegradedPeersBadge report={null} dark />, {
      wrapper: settingsWrapper(qc),
    });
    expect(container.firstChild).toBeNull();
  });

  test("renders nothing when the report has no degraded peers", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const report = new ContributionReport({ degraded: [] });
    const { container } = render(<DegradedPeersBadge report={report} dark />, {
      wrapper: settingsWrapper(qc),
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
      wrapper: settingsWrapper(qc),
    });
    expect(getByText("partial — 1 node unreached")).toBeTruthy();
  });

  test("resolves node IDs to names and includes the reason in the tooltip", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const report = new ContributionReport({
      degraded: [new DegradedPeer({ nodeId: encode(NODE_B), reason: "timeout" })],
    });
    const { getByText } = render(<DegradedPeersBadge report={report} dark />, {
      wrapper: settingsWrapper(qc),
    });
    const badge = getByText("partial — 1 node unreached");
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
      wrapper: settingsWrapper(qc),
    });
    const title = getByText("partial — 1 node unreached").getAttribute("title") ?? "";
    expect(title).toContain(unknown);
    expect(title).toContain("connection refused");
  });
});
