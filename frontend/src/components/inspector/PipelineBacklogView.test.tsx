import { describe, test, expect } from "bun:test";
import { render } from "@testing-library/react";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { VaultPipelineBacklog } from "../../api/gen/gastrolog/v1/vault_pb";
import { idFromBytes } from "../../api/model/id";
import { PipelineBacklogView } from "./PipelineBacklogView";

// The backlog's warn tone is the backend's verdict: the responding node's
// admission gate reports the backlog at the configured budget. The browser
// holds no segment-count threshold of its own, so a large registry with the
// budget not reached is calm, and a refused vault warns at any count.

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const VAULT = testId(1);
const vaultId = idFromBytes(VAULT);

function renderBacklog(eligible: number, backlogAtBudget: boolean) {
  const qc = createTestQueryClient();
  qc.setQueryData(["settings"], { nodeId: testId(40) });
  qc.setQueryData(["system"], { nodeConfigs: [], vaults: [], ingesters: [], routes: [], nodeStorageConfigs: [] });
  qc.setQueryData(["clusterStatus"], { clusterEnabled: false, nodes: [] });
  qc.setQueryData(["pipeline-backlog-contribution", vaultId], null);
  qc.setQueryData(
    ["pipeline-backlog", vaultId],
    new VaultPipelineBacklog({ vaultId: VAULT, eligibleSegments: eligible, registrySegments: eligible }),
  );
  return render(<PipelineBacklogView vaultId={vaultId} backlogAtBudget={backlogAtBudget} dark />, {
    wrapper: settingsWrapper(qc),
  });
}

describe("PipelineBacklogView warn tone", () => {
  test("many eligible segments without the backend's verdict: no warn tone", () => {
    const { container } = renderBacklog(5000, false);
    expect(container.textContent).toContain("Pipeline backlog");
    expect(container.querySelector(".text-severity-warn")).toBeNull();
  });

  test("the backend reports the backlog at budget: warn tone at any count", () => {
    const { container, getByText } = renderBacklog(3, true);
    expect(getByText("3 / 3")).toBeTruthy();
    expect(container.querySelector(".text-severity-warn")).not.toBeNull();
  });
});
