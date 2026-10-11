import { describe, test, expect, beforeEach, mock } from "bun:test";
import { render, fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { HelpProvider } from "../../hooks/useHelp";

const mocks = installMockClients();

import { NodesSettings } from "./NodesSettings";
import { ClusterNodeRole, ClusterNodeSuffrage } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import { renderWritable } from "../../testing/renderWritable";

/** Create a distinct 16-byte Uint8Array test ID from a small number. */
function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const localNodeId = testId(1);
const remoteNodeId = testId(2);

const sampleSettings = {
  nodeId: localNodeId,
  nodeName: "my-node",
};

const sampleConfig = {
  nodeConfigs: [
    { id: localNodeId, name: "my-node" },
    { id: remoteNodeId, name: "remote-node" },
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
      id: remoteNodeId,
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

    const { getByText } = renderWritable(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("my-node")).toBeTruthy();
    expect(getByText("this node")).toBeTruthy();
  });

  test("renders cluster nodes with role badges", () => {
    const qc = createTestQueryClient();
    seedQueries(qc);

    const { getByText } = renderWritable(<NodesSettings dark />, {
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

    const { getByText } = renderWritable(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("this node")).toBeTruthy();
  });

  test("expanded node shows name edit field", () => {
    const qc = createTestQueryClient();
    seedQueries(qc);

    const { getByText, getByDisplayValue } = renderWritable(<NodesSettings dark />, {
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

    const { getByText } = renderWritable(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    const demoteBtn = getByText("Demote").closest("button")!;
    expect(demoteBtn.disabled).toBe(true);
  });

  test("demote button enabled when multiple voters", () => {
    const qc = createTestQueryClient();
    seedQueries(qc);

    const { getAllByText } = renderWritable(<NodesSettings dark />, {
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

    const { getByText, getAllByText } = renderWritable(<NodesSettings dark />, {
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

    const { getByText, getAllByText } = renderWritable(<NodesSettings dark />, {
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

    const { getByText, getAllByText } = renderWritable(<NodesSettings dark />, {
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

    const { getByText } = renderWritable(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("Cluster Join Info")).toBeTruthy();
    expect(getByText("Cluster Address")).toBeTruthy();
    expect(getByText("10.0.0.1:4575")).toBeTruthy();
  });

  // Join tokens expire, so there is no standing one to show. Rendering the
  // page must not put a credential on screen for a session that never asks
  // for one; it offers to mint one instead.
  test("join info card shows no token until one is minted", () => {
    const qc = createTestQueryClient();
    seedQueries(qc, {
      cluster: { ...sampleCluster, clusterAddress: "10.0.0.1:4575" },
    });

    const { getByText, queryByText } = renderWritable(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("Mint a join token")).toBeTruthy();
    expect(queryByText("Join Command")).toBeNull();
    expect(getByText("10.0.0.1:4575")).toBeTruthy();
  });

  // The join command sends the operator to the advertised cluster address
  // with the minted token and nothing else: the joining node's own cluster
  // listen address is its own choice, and the server default stands unless
  // the operator says otherwise.
  test("join command names the cluster address and token and dictates no listen address", async () => {
    const qc = createTestQueryClient();
    seedQueries(qc, {
      cluster: { ...sampleCluster, clusterAddress: "10.0.0.1:4566" },
    });
    m(mocks.lifecycleClient, "createJoinToken").mockResolvedValueOnce({
      joinToken: "tok-abcdef0123456789",
      expiresAtUnix: 1_900_000_000n,
    });

    const { getByText, container } = renderWritable(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });
    fireEvent.click(getByText("Mint a join token"));
    await waitFor(() => expect(getByText("Join Command")).toBeTruthy());

    const cmd = [...container.querySelectorAll("code")]
      .map((el) => el.textContent)
      .find((t) => t.startsWith("gastrolog server"));
    expect(cmd).toBe("gastrolog server --join-addr 10.0.0.1:4566 --join-token tok-abcdef0123456789");
    expect(cmd).not.toContain("--cluster-addr");
  });

  // "this node" names the node that answered. When settings and cluster
  // status were answered by different nodes, the answering node varies per
  // request and no node is labeled; when they agree, the label is shown.
  test("this node is withheld when the answering node varies between responses", () => {
    const varying = createTestQueryClient();
    seedQueries(varying, { cluster: { ...sampleCluster, localNodeId: remoteNodeId } });
    const hidden = renderWritable(<NodesSettings dark />, { wrapper: settingsWrapper(varying) });
    expect(hidden.queryByText("this node")).toBeNull();
    hidden.unmount();

    const stable = createTestQueryClient();
    seedQueries(stable, { cluster: { ...sampleCluster, localNodeId } });
    const shown = renderWritable(<NodesSettings dark />, { wrapper: settingsWrapper(stable) });
    expect(shown.queryByText("this node")).not.toBeNull();
  });

  // A caller who cannot change anything cannot mint a credential either.
  test("join info card offers no minting without write access", () => {
    const qc = createTestQueryClient();
    seedQueries(qc, {
      cluster: { ...sampleCluster, clusterAddress: "10.0.0.1:4575" },
    });

    const { queryByText } = render(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(queryByText("Mint a join token")).toBeNull();
  });

  test("empty state when no nodes", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], { nodeId: new Uint8Array(0), nodeName: "" });
    qc.setQueryData(["system"], { ...sampleConfig, nodeConfigs: [] });
    qc.setQueryData(["clusterStatus"], { clusterEnabled: false, nodes: [] });

    const { getByText } = renderWritable(<NodesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("No nodes found.")).toBeTruthy();
  });
});

describe("NodesSettings help", () => {
  function renderWithHelp() {
    const qc = createTestQueryClient();
    seedQueries(qc);
    const openHelp = mock((_helpRef?: string) => {});
    const result = renderWritable(
      <HelpProvider onOpen={openHelp}>
        <NodesSettings dark />
      </HelpProvider>,
      { wrapper: settingsWrapper(qc) },
    );
    return { ...result, openHelp };
  }

  test("a role badge explains itself on hover and opens the node roles section", () => {
    const { getByText, getByDisplayValue, openHelp } = renderWithHelp();
    const leader = getByText("leader");
    expect(leader.getAttribute("title")).toContain("Raft leader");
    expect(getByText("follower").getAttribute("title")).toContain("Raft follower");

    fireEvent.click(leader);

    expect(openHelp).toHaveBeenCalledWith("clustering#node-roles");
    // The badge sits in the card header; opening help must not collapse the card.
    expect(getByDisplayValue("my-node")).toBeTruthy();
  });
});
