import { describe, expect, test } from "bun:test";
import { PeerConnStat } from "../../api/gen/gastrolog/v1/cluster_pb";
import { asEntityID } from "../../api/model/id";
import { mergePeerTraffic, laneDetailText, mergedPurposesWindow } from "./peerTrafficMerge";

const NODE_A = asEntityID("node-a");
const NODE_B = asEntityID("node-b");

function conn(
  peer: string,
  lane: string,
  sent: bigint,
  recv: bigint,
  groupId = "",
): PeerConnStat {
  return new PeerConnStat({
    peer,
    lane,
    groupId,
    bytesSent: sent,
    bytesReceived: recv,
    txBytesPerSec: Number(sent) / 100,
    rxBytesPerSec: Number(recv) / 100,
    txSpark: [1, 2],
    rxSpark: [3, 4],
  });
}

describe("mergePeerTraffic", () => {
  test("merges raft lanes from both nodes on the same group", () => {
    const merged = mergePeerTraffic(
      NODE_A,
      {
        peerConnections: [
          conn("node-b", "raft", 100n, 50n, "vault/x/ctl"),
        ],
      },
      NODE_B,
      {
        peerConnections: [
          conn("node-a", "raft", 200n, 80n, "vault/x/ctl"),
        ],
      },
    );
    expect(merged).not.toBeNull();
    expect(merged!.lanes).toHaveLength(1);
    expect(merged!.lanes[0]!.bytesSent).toBe(300);
    expect(merged!.lanes[0]!.bytesReceived).toBe(130);
    expect(merged!.total.bytesSent).toBe(300);
  });

  test("merges service lanes per pool index from both nodes", () => {
    const merged = mergePeerTraffic(
      NODE_A,
      {
        peerConnections: [
          conn("node-b", "service", 10n, 5n),
          new PeerConnStat({
            peer: "node-b",
            lane: "service",
            poolIndex: 1,
            bytesSent: 3n,
            bytesReceived: 1n,
          }),
        ],
      },
      NODE_B,
      {
        peerConnections: [
          conn("node-a", "service", 20n, 8n),
          new PeerConnStat({
            peer: "node-a",
            lane: "service",
            poolIndex: 1,
            bytesSent: 7n,
            bytesReceived: 2n,
          }),
        ],
      },
    );
    expect(merged!.lanes).toHaveLength(2);
    expect(merged!.lanes[0]!.poolIndex).toBe(0);
    expect(merged!.lanes[0]!.bytesSent).toBe(30);
    expect(merged!.lanes[1]!.poolIndex).toBe(1);
    expect(merged!.lanes[1]!.bytesSent).toBe(10);
  });

  test("merges service pool rows from both nodes into one service lane", () => {
    const merged = mergePeerTraffic(
      NODE_A,
      {
        peerConnections: [
          conn("node-b", "service", 10n, 5n),
        ],
      },
      NODE_B,
      {
        peerConnections: [
          conn("node-a", "service", 20n, 8n),
        ],
      },
    );
    expect(merged!.lanes).toHaveLength(1);
    expect(merged!.lanes[0]!.lane).toBe("service");
    expect(merged!.lanes[0]!.poolIndex).toBe(0);
    expect(merged!.lanes[0]!.bytesSent).toBe(30);
  });

  test("works with only local side when peer is offline", () => {
    const merged = mergePeerTraffic(
      NODE_A,
      { peerConnections: [conn("node-b", "raft", 50n, 25n, "g1")] },
      NODE_B,
      null,
    );
    expect(merged!.lanes[0]!.bytesSent).toBe(50);
  });

  test("laneDetailText shows vault name for raft and purposes for service", () => {
    const raftLane = {
      lane: "raft",
      poolIndex: 0,
      groupId: "vault/abc123/ctl",
      purposes: ["raft"],
      purposesWindow: [],
      bytesSent: 0,
      bytesReceived: 0,
      txBytesPerSec: 0,
      rxBytesPerSec: 0,
      txSpark: [],
      rxSpark: [],
    };
    expect(
      laneDetailText(raftLane, {
        vaultNameOf: (id) => (id === asEntityID("abc123") ? "logs" : undefined),
      }).label,
    ).toBe("logs");
    expect(laneDetailText(raftLane).label).toBe("—");
    const serviceDetail = laneDetailText({
      lane: "service",
      poolIndex: 0,
      groupId: "",
      purposes: ["search"],
      purposesWindow: ["search", "chunk-apply"],
      bytesSent: 0,
      bytesReceived: 0,
      txBytesPerSec: 0,
      rxBytesPerSec: 0,
      txSpark: [],
      rxSpark: [],
    });
    expect(serviceDetail.label).toBe("search");
    expect(
      [...mergedPurposesWindow({
        lane: "service",
        poolIndex: 0,
        groupId: "",
        purposes: [],
        purposesWindow: ["search", "chunk-apply"],
        bytesSent: 0,
        bytesReceived: 0,
        txBytesPerSec: 0,
        rxBytesPerSec: 0,
        txSpark: [],
        rxSpark: [],
      })].sort(),
    ).toEqual(["chunk-apply", "search"]);
  });

  test("merges purposes_window across both peers on the same pool slot", () => {
    const merged = mergePeerTraffic(
      NODE_A,
      {
        peerConnections: [
          new PeerConnStat({
            peer: "node-b",
            lane: "service",
            poolIndex: 0,
            purposesWindow: ["search"],
            bytesSent: 1n,
            bytesReceived: 1n,
          }),
        ],
      },
      NODE_B,
      {
        peerConnections: [
          new PeerConnStat({
            peer: "node-a",
            lane: "service",
            poolIndex: 0,
            purposesWindow: ["chunk-apply"],
            bytesSent: 1n,
            bytesReceived: 1n,
          }),
        ],
      },
    );
    expect(merged!.lanes[0]!.purposesWindow).toEqual(["chunk-apply", "search"]);
  });
});
