import { describe, test, expect } from "bun:test";
import { render, waitFor } from "@testing-library/react";
import { QueryClientProvider } from "@tanstack/react-query";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient } from "../../../test/render";

const mocks = installMockClients();

import { HelpProvider } from "../../hooks/useHelp";
import { ToastProvider } from "../Toast";
import { NodeStats } from "../../api/gen/gastrolog/v1/cluster_pb";
import { idFromBytes } from "../../api/model/id";
import { NodeDetailPane } from "./NodeDetailPane";

// Until the job stream delivers its first snapshot, an empty job list means
// "not yet known": the node pane's Scheduled and Tasks sections show a loading
// cue, never a claim that the node has no jobs.

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const NODE_A = testId(40);

function renderPane() {
  const qc = createTestQueryClient();
  qc.setQueryData(["settings"], { nodeId: NODE_A });
  qc.setQueryData(["system"], {
    nodeConfigs: [{ id: NODE_A, name: "node-1" }],
    vaults: [],
    ingesters: [],
    routes: [],
    nodeStorageConfigs: [],
    cloudServices: [],
  });
  qc.setQueryData(["clusterStatus"], {
    clusterEnabled: true,
    nodes: [{ id: NODE_A, name: "node-1", isLeader: true, stats: new NodeStats() }],
  });
  qc.setQueryData(["vaults"], []);
  qc.setQueryData(["storages"], []);
  return render(
    <QueryClientProvider client={qc}>
      <HelpProvider onOpen={() => {}}>
        <ToastProvider dark={true}>
          <NodeDetailPane nodeId={idFromBytes(NODE_A)} dark />
        </ToastProvider>
      </HelpProvider>
    </QueryClientProvider>,
  );
}

describe("NodeDetailPane job sections", () => {
  test("before the job stream's first snapshot, neither section claims the node has no jobs", () => {
    m(mocks.jobClient, "watchJobs").mockImplementation(async function* () {
      await new Promise<never>(() => {});
      yield { jobs: [] };
    });
    const { queryByText } = renderPane();
    expect(queryByText("No scheduled jobs on this node.")).toBeNull();
    expect(queryByText("No tasks on this node.")).toBeNull();
  });

  test("an empty snapshot from the stream reads as no jobs", async () => {
    m(mocks.jobClient, "watchJobs").mockImplementation(async function* () {
      yield { jobs: [] };
      await new Promise(() => {});
    });
    const { findByText } = renderPane();
    expect(await findByText("No scheduled jobs on this node.")).toBeTruthy();
    await waitFor(() => expect(findByText("No tasks on this node.")).resolves.toBeTruthy());
  });
});
