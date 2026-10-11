import { describe, test, expect, beforeEach } from "bun:test";
import { render, renderHook } from "@testing-library/react";
import { createTestQueryClient, wrapper } from "../../../test/render";
import { useNodeRegistry } from "./useNodes";
import { recordRespondingNode, resetRespondingNodeObservation } from "./respondingNode";
import { idFromBytes } from "../model/id";
import { NodeBadge } from "../../components/settings/NodeBadge";

// "this node" names the node answering this browser's RPCs. The API has no
// session affinity, so behind a load balancer each request may land on a
// different node and the label would name a different node per surface.
// The UI cannot pin a node; it can only notice the answering node varying,
// and then it withholds the label instead of guessing.

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const NODE_A = testId(1);
const NODE_B = testId(2);
const A = idFromBytes(NODE_A);
const B = idFromBytes(NODE_B);

function seed(qc: ReturnType<typeof createTestQueryClient>, settingsNode: Uint8Array, clusterLocalNode: Uint8Array | undefined, statsTtl = "15s") {
  qc.setQueryData(["settings"], { nodeId: settingsNode });
  qc.setQueryData(["system"], { nodeConfigs: [{ id: NODE_A, name: "node-1" }, { id: NODE_B, name: "node-2" }] });
  qc.setQueryData(["clusterStatus"], {
    clusterEnabled: true,
    localNodeId: clusterLocalNode,
    statsTtl,
    nodes: [{ id: NODE_A, name: "node-1" }, { id: NODE_B, name: "node-2" }],
  });
}

beforeEach(() => resetRespondingNodeObservation());

describe("responding node identity", () => {
  test("one answering node: the settings node is this node", () => {
    const qc = createTestQueryClient();
    seed(qc, NODE_A, NODE_A);
    const { result } = renderHook(() => useNodeRegistry(), { wrapper: wrapper(qc) });
    expect(result.current.respondingNodeStable).toBe(true);
    expect(result.current.isLocal(A)).toBe(true);
    expect(result.current.isLocal(B)).toBe(false);
  });

  test("settings and cluster status answered by different nodes: no node is this node", () => {
    const qc = createTestQueryClient();
    seed(qc, NODE_A, NODE_B);
    const { result } = renderHook(() => useNodeRegistry(), { wrapper: wrapper(qc) });
    expect(result.current.respondingNodeStable).toBe(false);
    expect(result.current.isLocal(A)).toBe(false);
    expect(result.current.isLocal(B)).toBe(false);
    // The default-node role of localNodeId is unaffected: something still
    // has to be selected first.
    expect(result.current.localNodeId).toBe(A);
  });

  test("two distinct nodes observed over the session: the label is withdrawn even when the cache agrees", () => {
    const qc = createTestQueryClient();
    seed(qc, NODE_A, NODE_A);
    recordRespondingNode(A);
    recordRespondingNode(B);
    const { result } = renderHook(() => useNodeRegistry(), { wrapper: wrapper(qc) });
    expect(result.current.respondingNodeStable).toBe(false);
    expect(result.current.isLocal(A)).toBe(false);
  });

  test("the same node observed repeatedly stays stable", () => {
    const qc = createTestQueryClient();
    seed(qc, NODE_A, NODE_A);
    recordRespondingNode(A);
    recordRespondingNode(A);
    const { result } = renderHook(() => useNodeRegistry(), { wrapper: wrapper(qc) });
    expect(result.current.isLocal(A)).toBe(true);
  });

  test("stats TTL comes from the cluster status, in milliseconds", () => {
    const qc = createTestQueryClient();
    seed(qc, NODE_A, NODE_A, "15s");
    const { result } = renderHook(() => useNodeRegistry(), { wrapper: wrapper(qc) });
    expect(result.current.statsTtlMs).toBe(15_000);
  });

  test("stats TTL is unknown until the cluster reports it", () => {
    const qc = createTestQueryClient();
    seed(qc, NODE_A, NODE_A, "");
    const { result } = renderHook(() => useNodeRegistry(), { wrapper: wrapper(qc) });
    expect(result.current.statsTtlMs).toBeNull();
  });

  test("NodeBadge shows this node only while the answering node is stable", () => {
    const stable = createTestQueryClient();
    seed(stable, NODE_A, NODE_A);
    const shown = render(<NodeBadge nodeId={A} dark />, { wrapper: wrapper(stable) });
    expect(shown.queryByText("this node")).not.toBeNull();
    shown.unmount();

    const varying = createTestQueryClient();
    seed(varying, NODE_A, NODE_B);
    const hidden = render(<NodeBadge nodeId={A} dark />, { wrapper: wrapper(varying) });
    expect(hidden.queryByText("this node")).toBeNull();
    expect(hidden.queryByText("node-1")).not.toBeNull();
  });
});
