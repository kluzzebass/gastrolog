import { describe, test, expect } from "bun:test";
import { render } from "@testing-library/react";
import { QueryClientProvider } from "@tanstack/react-query";
import { createTestQueryClient } from "../../../test/render";
import { NodeStats, PeerConnStat } from "../../api/gen/gastrolog/v1/cluster_pb";
import { Node } from "../../api/model/node";
import { asEntityID, type EntityID } from "../../api/model/id";
import type { NodeRegistry } from "../../api/hooks";
import { laneLabel, raftLaneDetail, PeerBytesSection } from "./PeerBytesSection";
import { PurposeWindowText } from "./PurposeWindowText";
import { orderPeerPurposes } from "./peerPurposeDisplay";
import type { MergedPeerLane } from "./peerTrafficMerge";

// Pins the Network section's lane wording: a raft lane names its Raft group
// (cluster-ctl, or the vault), a pooled RPC lane names its pool slot, and a
// pooled lane's detail lists the purposes it carried as text, dimming the
// ones idle this interval.

const NODE_A = asEntityID("node-a");
const NODE_B = asEntityID("node-b");

function registry(ids: EntityID[]): NodeRegistry {
  const byId = new Map<EntityID, Node>();
  for (const id of ids) byId.set(id, new Node(id, null, null));
  return {
    all: [...byId.values()],
    byId,
    localNodeId: NODE_A,
    leader: null,
    multiNode: true,
    nameOf: (id) => byId.get(id)?.name ?? id,
    isLocal: (id) => id === NODE_A,
    respondingNodeStable: true,
    statsTtlMs: null,
  };
}

function lane(overrides: Partial<MergedPeerLane>): MergedPeerLane {
  return {
    lane: "service",
    poolIndex: 0,
    groupId: "",
    purposes: [],
    purposesWindow: [],
    bytesSent: 0,
    bytesReceived: 0,
    txBytesPerSec: 0,
    rxBytesPerSec: 0,
    txSpark: [],
    rxSpark: [],
    ...overrides,
  };
}

function renderSection(nodeStats: NodeStats) {
  const qc = createTestQueryClient();
  qc.setQueryData(["vaults"], []);
  qc.setQueryData(["system"], {
    nodeConfigs: [],
    vaults: [],
    ingesters: [],
    routes: [],
    nodeStorageConfigs: [],
    cloudServices: [],
  });
  return render(
    <QueryClientProvider client={qc}>
      <PeerBytesSection viewNodeId={NODE_A} nodeStats={nodeStats} nodes={registry([NODE_A, NODE_B])} dark />
    </QueryClientProvider>,
  );
}

describe("laneLabel", () => {
  test("a raft lane is labelled raft; a pooled RPC lane names its pool slot", () => {
    expect(laneLabel("raft", 0)).toBe("raft");
    expect(laneLabel("service", 0)).toBe("rpc pool 0");
    expect(laneLabel("service", 3)).toBe("rpc pool 3");
  });
});

describe("raftLaneDetail", () => {
  test("the cluster-ctl group reads cluster-ctl, with the group ID on hover", () => {
    expect(raftLaneDetail(lane({ lane: "raft", groupId: "cluster-ctl" }))).toEqual({
      label: "cluster-ctl",
      title: "cluster-ctl",
    });
  });

  test("a vault-ctl group reads the vault's name when known, vault-ctl when not", () => {
    const vaultLane = lane({ lane: "raft", groupId: "vault/abc123/ctl" });
    expect(raftLaneDetail(vaultLane, (id) => (id === asEntityID("abc123") ? "logs" : undefined))).toEqual({
      label: "logs",
      title: "vault/abc123/ctl",
    });
    expect(raftLaneDetail(vaultLane)).toEqual({ label: "vault-ctl", title: "vault/abc123/ctl" });
  });

  test("a raft lane with no group reads a dash", () => {
    expect(raftLaneDetail(lane({ lane: "raft" }))).toEqual({ label: "—" });
  });
});

describe("orderPeerPurposes", () => {
  test("orders purposes canonically, drops blanks and unknown, appends unlisted ones sorted", () => {
    expect(orderPeerPurposes(["raft", "search", "", "unknown", "zzz-new", "chunk-apply", "aaa-new"])).toEqual([
      "search",
      "chunk-apply",
      "raft",
      "aaa-new",
      "zzz-new",
    ]);
  });
});

describe("PurposeWindowText", () => {
  test("lists carried purposes by label, lit when active this interval and dimmed otherwise", () => {
    const { container, getByText } = render(
      <PurposeWindowText carried={["chunk-apply", "search", "segment-pull"]} active={new Set(["search"])} />,
    );
    expect(container.textContent).toBe("search, chunk apply, segment pull");
    const search = getByText("search");
    expect(search.dataset.active).toBe("true");
    expect(search.className).not.toContain("opacity-40");
    expect(search.title).toBe("search — active this interval");
    const chunkApply = getByText("chunk apply");
    expect(chunkApply.dataset.active).toBe("false");
    expect(chunkApply.className).toContain("opacity-40");
    expect(chunkApply.title).toBe("chunk apply — idle this interval");
    expect(getByText("segment pull").dataset.active).toBe("false");
    expect(container.querySelector("img, svg")).toBeNull();
  });

  test("a lane that has carried nothing reads a dash", () => {
    const { container } = render(<PurposeWindowText carried={[]} active={new Set()} />);
    expect(container.textContent).toBe("—");
  });
});

describe("PeerBytesSection lanes", () => {
  test("renders cluster-ctl, vault-ctl and rpc pool lanes with their purposes from the stats", () => {
    const stats = new NodeStats({
      peerConnections: [
        new PeerConnStat({ peer: "node-b", lane: "raft", groupId: "cluster-ctl", bytesSent: BigInt(10), bytesReceived: BigInt(5) }),
        new PeerConnStat({ peer: "node-b", lane: "raft", groupId: "vault/abc123/ctl", bytesSent: BigInt(10), bytesReceived: BigInt(5) }),
        new PeerConnStat({
          peer: "node-b",
          lane: "service",
          poolIndex: 2,
          purposes: ["search", "chunk-apply"],
          purposesWindow: ["chunk-apply"],
          bytesSent: BigInt(10),
          bytesReceived: BigInt(5),
        }),
      ],
    });
    const { getByText } = renderSection(stats);
    expect(getByText("cluster-ctl")).toBeDefined();
    expect(getByText("vault-ctl")).toBeDefined();
    expect(getByText("rpc pool 2")).toBeDefined();
    expect(getByText("chunk apply").dataset.active).toBe("true");
    expect(getByText("search").dataset.active).toBe("false");
  });

  test("a node with no recorded connections says so", () => {
    const { getByText } = renderSection(new NodeStats());
    expect(getByText("No inter-node connections recorded for this node.")).toBeDefined();
  });
});
