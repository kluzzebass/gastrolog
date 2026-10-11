import { describe, test, expect } from "bun:test";
import { render } from "@testing-library/react";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { IngesterNodeBadge } from "./IngestersSettings";

// The node badge reads the ingester's saved eligibility config exactly as
// the dispatcher does: all nodes when allNodes is set or nothing is pinned,
// otherwise the pin list. Nothing about the ingester's type or mode makes
// it run everywhere.

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

function seed(qc: ReturnType<typeof createTestQueryClient>) {
  qc.setQueryData(["settings"], { nodeId: testId(1) });
  qc.setQueryData(["system"], { nodeConfigs: [{ id: testId(1), name: "node-1" }, { id: testId(2), name: "node-2" }] });
  qc.setQueryData(["clusterStatus"], { clusterEnabled: true, localNodeId: testId(1), nodes: [{ id: testId(1), name: "node-1" }, { id: testId(2), name: "node-2" }] });
}

describe("IngesterNodeBadge", () => {
  test("allNodes set: all nodes", () => {
    const qc = createTestQueryClient();
    seed(qc);
    const { getByText } = render(<IngesterNodeBadge nodeIds={[testId(2)]} allNodes dark />, { wrapper: settingsWrapper(qc) });
    expect(getByText("all nodes")).toBeTruthy();
  });

  test("nothing pinned and allNodes unset: eligible everywhere, so all nodes", () => {
    const qc = createTestQueryClient();
    seed(qc);
    const { getByText } = render(<IngesterNodeBadge nodeIds={[]} allNodes={false} dark />, { wrapper: settingsWrapper(qc) });
    expect(getByText("all nodes")).toBeTruthy();
  });

  test("two pins: the pin count, whatever the ingester is", () => {
    const qc = createTestQueryClient();
    seed(qc);
    const { getByText, queryByText } = render(
      <IngesterNodeBadge nodeIds={[testId(1), testId(2)]} allNodes={false} dark />,
      { wrapper: settingsWrapper(qc) },
    );
    expect(getByText("2 nodes")).toBeTruthy();
    expect(queryByText("all nodes")).toBeNull();
  });

  test("one pin: that node", () => {
    const qc = createTestQueryClient();
    seed(qc);
    const { getByText, queryByText } = render(
      <IngesterNodeBadge nodeIds={[testId(2)]} allNodes={false} dark />,
      { wrapper: settingsWrapper(qc) },
    );
    expect(getByText("node-2")).toBeTruthy();
    expect(queryByText("all nodes")).toBeNull();
  });
});
