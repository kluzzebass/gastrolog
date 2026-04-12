import { describe, test, expect, beforeEach } from "bun:test";
import { render, fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { NodesSettings } from "./NodesSettings";
import { ClusterNodeRole, ClusterNodeSuffrage } from "../../api/gen/gastrolog/v1/lifecycle_pb";

const localNodeId = "node-local";

const sampleSettings = {
  nodeId: localNodeId,
  nodeName: "my-node",
};

const sampleConfig = {
  nodeConfigs: [
    { id: localNodeId, name: "my-node" },
    { id: "node-remote", name: "remote-node" },
  ],
  routes: [],
  filters: [],
  vaults: [],
  ingesters: [],
};

const sampleCluster = {
  clusterEnabled: true,
  nodes: [
    {
      id: localNodeId,
      name: "my-node",
      role: ClusterNodeRole.LEADER,
      suffrage: ClusterNodeSuffrage.VOTER,
      isLeader: true,
      stats: { vaultCount: 2 },
    },
    {
      id: "node-remote",
      name: "remote-node",
      role: ClusterNodeRole.FOLLOWER,
      suffrage: ClusterNodeSuffrage.VOTER,
      isLeader: false,
      stats: { vaultCount: 1 },
    },
  ],
  joinToken: "",
  clusterAddress: "",
};

beforeEach(() => {
  m(mocks.systemClient, "getConfig").mockClear();
  m(mocks.systemClient, "getSettings").mockClear();
  m(mocks.systemClient, "putNodeConfig").mockClear();
  m(mocks.lifecycleClient, "getClusterStatus").mockClear();
  m(mocks.lifecycleClient, "setNodeSuffrage").mockClear();
  m(mocks.lifecycleClient, "removeNode").mockClear();
});

function seedQueries(qc: ReturnType<typeof createTestQueryClient>, overrides?: { cluster?: object }) {
  qc.setQueryData(["settings"], sampleSettings);
  qc.setQueryData(["system"], sampleConfig);
  qc.setQueryData(["clusterStatus"], overrides?.cluster ?? sampleCluster);
}

describe("NodesSettings", () => {
  test("renders single node in non-cluster mode", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["system"], sampleConfig);
    qc.setQueryData(["clusterStatus"], { clusterEnabled: false, nodes: [] });

    const { getByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("my-node")).toBeTruthy();
    expect(getByText("this node")).toBeTruthy();
  });

  test("renders cluster nodes with role badges", () => {
    const qc = createTestQueryClient();
    seedQueries(qc);

    const { getByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("my-node")).toBeTruthy();
    expect(getByText("remote-node")).toBeTruthy();
    expect(getByText("leader")).toBeTruthy();
    expect(getByText("follower")).toBeTruthy();
  });

  test("local node shows this node badge", () => {
    const qc = createTestQueryClient();
    seedQueries(qc);

    const { getByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("this node")).toBeTruthy();
  });

  test("expanded node shows name edit field", () => {
    const qc = createTestQueryClient();
    seedQueries(qc);

    const { getByText, getByDisplayValue } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // Local node is auto-expanded
    expect(getByText("Node Name")).toBeTruthy();
    expect(getByDisplayValue("my-node")).toBeTruthy();
  });

  test("demote button disabled when only one voter", () => {
    const qc = createTestQueryClient();
    const singleVoter = {
      ...sampleCluster,
      nodes: [sampleCluster.nodes[0]!],
    };
    seedQueries(qc, { cluster: singleVoter });

    const { getByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    const demoteBtn = getByText("Demote").closest("button")!;
    expect(demoteBtn.disabled).toBe(true);
  });

  test("demote button enabled when multiple voters", () => {
    const qc = createTestQueryClient();
    seedQueries(qc);

    const { getAllByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // Both nodes are voters, so demote should be enabled
    const demoteButtons = getAllByText("Demote");
    expect(demoteButtons.length).toBeGreaterThanOrEqual(1);
    expect(demoteButtons[0]!.closest("button")!.disabled).toBe(false);
  });

  test("remove button only on remote nodes", () => {
    const qc = createTestQueryClient();
    seedQueries(qc);

    const { getByText, getAllByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // Expand remote node
    fireEvent.click(getByText("remote-node"));
    // Remote node should have Remove button
    const removeButtons = getAllByText("Remove");
    expect(removeButtons.length).toBeGreaterThanOrEqual(1);
  });

  test("remove shows confirmation warning", () => {
    const qc = createTestQueryClient();
    seedQueries(qc);

    const { getByText, getAllByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("remote-node"));
    const removeButtons = getAllByText("Remove");
    fireEvent.click(removeButtons[0]!);

    expect(getByText(/evict the node/)).toBeTruthy();
    expect(getByText("Cancel")).toBeTruthy();
  });

  test("confirmed remove calls API", async () => {
    m(mocks.lifecycleClient, "removeNode").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    seedQueries(qc);

    const { getByText, getAllByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("remote-node"));
    fireEvent.click(getAllByText("Remove")[0]!);
    // Now in confirm state — click Remove again
    fireEvent.click(getAllByText("Remove")[0]!);

    await waitFor(() => {
      expect(m(mocks.lifecycleClient, "removeNode")).toHaveBeenCalledTimes(1);
    });
  });

  test("shows join info card when join token exists", () => {
    const qc = createTestQueryClient();
    seedQueries(qc, {
      cluster: {
        ...sampleCluster,
        joinToken: "abcd1234secrettoken9876",
        clusterAddress: "10.0.0.1:4575",
      },
    });

    const { getByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("Cluster Join Info")).toBeTruthy();
    expect(getByText("Cluster Address")).toBeTruthy();
    expect(getByText("10.0.0.1:4575")).toBeTruthy();
  });

  test("join info card masks token by default", () => {
    const qc = createTestQueryClient();
    seedQueries(qc, {
      cluster: {
        ...sampleCluster,
        joinToken: "abcd1234secrettoken9876",
        clusterAddress: "10.0.0.1:4575",
      },
    });

    const { getByText, queryByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("abcd1234...9876")).toBeTruthy();
    expect(queryByText("abcd1234secrettoken9876")).toBeNull();
  });

  test("empty state when no nodes", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], { nodeId: "", nodeName: "" });
    qc.setQueryData(["system"], { ...sampleConfig, nodeConfigs: [] });
    qc.setQueryData(["clusterStatus"], { clusterEnabled: false, nodes: [] });

    const { getByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("No nodes found.")).toBeTruthy();
  });
});
