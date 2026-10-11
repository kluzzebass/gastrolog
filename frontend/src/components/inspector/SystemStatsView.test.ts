import { describe, expect, test } from "bun:test";
import { Timestamp } from "@bufbuild/protobuf";
import { formatRaftContact, leaderLabel, nodeStatsEmptyText } from "./SystemStatsView";

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
