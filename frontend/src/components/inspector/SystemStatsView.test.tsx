import { describe, expect, test } from "bun:test";
import { render } from "@testing-library/react";
import { Timestamp } from "@bufbuild/protobuf";
import { NodeStats } from "../../api/gen/gastrolog/v1/cluster_pb";
import { HelpProvider } from "../../hooks/useHelp";
import { SystemStatsView } from "./SystemStatsView";
import { formatRaftContact, leaderLabel, nodeStatsEmptyText } from "./nodeStatsText";

const NOW_MS = 1_800_000_000_000;

describe("nodeStatsEmptyText", () => {
  test("a node the cluster has never heard from is waiting for its first broadcast", () => {
    expect(nodeStatsEmptyText(undefined, NOW_MS)).toBe("No stats yet — waiting for this node's first broadcast.");
    expect(nodeStatsEmptyText(new Timestamp({ seconds: 0n }), NOW_MS)).toBe(
      "No stats yet — waiting for this node's first broadcast.",
    );
  });

  test("a node that went silent says how long ago the cluster last heard from it", () => {
    const seen = new Timestamp({ seconds: BigInt(NOW_MS / 1000 - 3 * 3600 - 120) });
    expect(nodeStatsEmptyText(seen, NOW_MS)).toBe("No stats — the cluster last heard from this node 3h2m ago.");
  });

  test("a node heard from this second is pending, not silent", () => {
    const seen = new Timestamp({ seconds: BigInt(NOW_MS / 1000) });
    expect(nodeStatsEmptyText(seen, NOW_MS)).toContain("just now");
  });
});

describe("formatRaftContact", () => {
  test("rounds a Go duration to one decimal in its own unit", () => {
    expect(formatRaftContact("8.556285ms")).toBe("8.6ms");
    expect(formatRaftContact("1.04s")).toBe("1.0s");
    expect(formatRaftContact("12ms")).toBe("12ms");
    expect(formatRaftContact("250µs")).toBe("250µs");
  });

  test("an empty value reads never; a compound duration passes through", () => {
    expect(formatRaftContact("")).toBe("never");
    expect(formatRaftContact("1m2.5s")).toBe("1m2.5s");
  });
});

describe("leaderLabel", () => {
  test("no leader is spelled out as no quorum", () => {
    expect(leaderLabel("")).toBe("none — no quorum");
    expect(leaderLabel("node-2")).toBe("node-2");
  });
});

// The Raft liveness rows appear once the node has appended to its WAL; their
// labels are spelled out, no abbreviations an operator has to decode.
describe("SystemStatsView raft liveness", () => {
  test("failed heartbeats are labelled in full", () => {
    const stats = new NodeStats({
      raftState: "Follower",
      raftWalAppendsTotal: BigInt(12),
      raftWalAppendAvgMs: 1.5,
      raftWalAppendMaxMs: 9,
      raftElectionsTotal: BigInt(1),
      raftElectionsPerMin: 0,
      raftLeaderLossesTotal: BigInt(0),
      raftFailedHeartbeatsTotal: BigInt(3),
    });
    const { getByText, queryByText } = render(
      <HelpProvider onOpen={() => {}}>
        <SystemStatsView nodeStats={stats} dark />
      </HelpProvider>,
    );
    expect(getByText("Failed heartbeats")).toBeTruthy();
    expect(getByText("3")).toBeTruthy();
    expect(queryByText("Failed HBs")).toBeNull();
  });
});
