import { describe, test, expect } from "bun:test";
import { ClusterNode, ClusterNodeRole, ClusterNodeSuffrage } from "../gen/gastrolog/v1/lifecycle_pb";
import { NodeConfig } from "../gen/gastrolog/v1/system_pb";
import { Node } from "./node";
import { idFromBytes } from "./id";

function idBytes(b: number): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(new ArrayBuffer(16));
  out[0] = b;
  return out;
}

describe("Node", () => {
  test("name prefers live cluster name", () => {
    const id = idFromBytes(idBytes(1));
    const cluster = new ClusterNode({ id: idBytes(1), name: "live-name" });
    const config = new NodeConfig({ id: idBytes(1), name: "config-name" });
    const n = new Node(id, cluster, config);
    expect(n.name).toBe("live-name");
  });

  test("name falls back to config name when cluster name is empty", () => {
    const id = idFromBytes(idBytes(2));
    const cluster = new ClusterNode({ id: idBytes(2), name: "" });
    const config = new NodeConfig({ id: idBytes(2), name: "config-name" });
    const n = new Node(id, cluster, config);
    expect(n.name).toBe("config-name");
  });

  test("name falls back to id when both names are empty", () => {
    const id = idFromBytes(idBytes(3));
    const n = new Node(id, null, null);
    expect(n.name).toBe(id);
  });

  test("isLive reflects cluster membership", () => {
    const id = idFromBytes(idBytes(4));
    expect(new Node(id, new ClusterNode({}), null).isLive).toBe(true);
    expect(new Node(id, null, new NodeConfig({})).isLive).toBe(false);
  });

  test("role/suffrage/stats are undefined when not live", () => {
    const id = idFromBytes(idBytes(5));
    const n = new Node(id, null, new NodeConfig({}));
    expect(n.role).toBeUndefined();
    expect(n.suffrage).toBeUndefined();
    expect(n.stats).toBeNull();
  });

  test("role/suffrage/isLeader come from the live cluster snapshot", () => {
    const id = idFromBytes(idBytes(6));
    const cluster = new ClusterNode({
      id: idBytes(6),
      role: ClusterNodeRole.LEADER,
      suffrage: ClusterNodeSuffrage.VOTER,
      isLeader: true,
    });
    const n = new Node(id, cluster, null);
    expect(n.role).toBe(ClusterNodeRole.LEADER);
    expect(n.suffrage).toBe(ClusterNodeSuffrage.VOTER);
    expect(n.isLeader).toBe(true);
  });
});
