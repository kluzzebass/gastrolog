import { describe, expect, test } from "bun:test";
import { NodeStats } from "../../api/gen/gastrolog/v1/cluster_pb";
import { GetStatsResponse, ThroughputRate, VaultStats } from "../../api/gen/gastrolog/v1/vault_pb";
import { type ClusterSummaryNode, summarizeCluster } from "./clusterSummary";

const VAULT_A = new Uint8Array(16).fill(1);
const VAULT_B = new Uint8Array(16).fill(2);

// What a node broadcasts for a vault: its OWN copy. Every node reports a row
// for every vault, with zeros where it holds no copy.
function copy(id: Uint8Array<ArrayBuffer>, records: number, chunks: number, bytes: number): VaultStats {
  return new VaultStats({
    id,
    recordCount: BigInt(records),
    chunkCount: BigInt(chunks),
    dataBytes: BigInt(bytes),
  });
}

function node(name: string, vaults: VaultStats[], extra: Partial<NodeStats> = {}): ClusterSummaryNode {
  return {
    id: new TextEncoder().encode(name),
    name,
    isLeader: false,
    stats: new NodeStats({ vaults, ...extra }),
  };
}

function clusterStats(vaults: number, records: number, chunks: number, bytes: number): GetStatsResponse {
  return new GetStatsResponse({
    totalVaults: BigInt(vaults),
    totalRecords: BigInt(records),
    totalChunks: BigInt(chunks),
    totalBytes: BigInt(bytes),
  });
}

describe("summarizeCluster vault holdings", () => {
  test("a vault at RF=4 across 4 nodes counts once", () => {
    const nodes = ["n1", "n2", "n3", "n4"].map((n) =>
      node(n, [copy(VAULT_A, 607_073, 61, 161_000_000), copy(VAULT_B, 0, 0, 0)]),
    );
    const s = summarizeCluster(nodes, clusterStats(2, 607_073, 61, 150_000_000), 2);
    expect(s.holdings).toEqual({
      vaults: 2,
      reporting: 2,
      records: 607_073,
      chunks: 61,
      bytes: 150_000_000,
    });
  });

  test("a vault at RF=1 counts once and the empty rows on other nodes add nothing", () => {
    const nodes = [
      node("n1", [copy(VAULT_A, 500, 5, 5000)]),
      node("n2", [copy(VAULT_A, 0, 0, 0)]),
      node("n3", [copy(VAULT_A, 0, 0, 0)]),
      node("n4", [copy(VAULT_A, 0, 0, 0)]),
    ];
    const s = summarizeCluster(nodes, clusterStats(1, 500, 5, 4800), 1);
    expect(s.holdings).toEqual({ vaults: 1, reporting: 1, records: 500, chunks: 5, bytes: 4800 });
  });

  test("mixed replication factors count each vault once", () => {
    const nodes = [
      node("n1", [copy(VAULT_A, 1000, 10, 9000), copy(VAULT_B, 300, 3, 2000)]),
      node("n2", [copy(VAULT_A, 1000, 10, 9000), copy(VAULT_B, 0, 0, 0)]),
      node("n3", [copy(VAULT_A, 1000, 10, 9000), copy(VAULT_B, 0, 0, 0)]),
      node("n4", [copy(VAULT_A, 1000, 10, 9000), copy(VAULT_B, 0, 0, 0)]),
    ];
    const s = summarizeCluster(nodes, clusterStats(2, 1300, 13, 11_000), 2);
    expect(s.holdings).toEqual({ vaults: 2, reporting: 2, records: 1300, chunks: 13, bytes: 11_000 });
  });

  test("a node missing stats changes neither the holdings nor the leader", () => {
    const silent: ClusterSummaryNode = {
      id: new TextEncoder().encode("n4"),
      name: "n4",
      isLeader: true,
    };
    const nodes = [
      node("n1", [copy(VAULT_A, 100, 1, 900)], { cpuPercent: 10, goroutines: 50 }),
      node("n2", [copy(VAULT_A, 100, 1, 900)], { cpuPercent: 20, goroutines: 60 }),
      node("n3", [copy(VAULT_A, 100, 1, 900)], { cpuPercent: 30, goroutines: 70 }),
      silent,
    ];
    const s = summarizeCluster(nodes, clusterStats(1, 100, 1, 900), 1);
    expect(s.holdings).toEqual({ vaults: 1, reporting: 1, records: 100, chunks: 1, bytes: 900 });
    expect(s.leaderName).toBe("n4");
    expect(s.cpuPercent).toBe(60);
    expect(s.goroutines).toBe(180);
  });

  test("a vault without figures is reported as uncovered, never filled in from node copies", () => {
    // Vault B's figures are unavailable to the serving node (no manifest
    // readable for it yet), while three nodes still broadcast copies of it.
    const nodes = ["n1", "n2", "n3", "n4"].map((n, i) =>
      node(n, [copy(VAULT_A, 200, 2, 1000), copy(VAULT_B, i < 3 ? 70 : 0, i < 3 ? 1 : 0, i < 3 ? 700 : 0)]),
    );
    const s = summarizeCluster(nodes, clusterStats(1, 200, 2, 1000), 2);
    expect(s.holdings).toEqual({ vaults: 2, reporting: 1, records: 200, chunks: 2, bytes: 1000 });
  });

  test("no holdings before the cluster stats arrive, whatever the nodes broadcast", () => {
    const nodes = ["n1", "n2"].map((n) => node(n, [copy(VAULT_A, 10, 1, 100)]));
    expect(summarizeCluster(nodes, undefined, 1).holdings).toBeNull();
    expect(summarizeCluster(nodes, null, 1).holdings).toBeNull();
  });

  test("an empty cluster reports zero holdings, not unknown ones", () => {
    const s = summarizeCluster([node("n1", [])], clusterStats(0, 0, 0, 0), 0);
    expect(s.holdings).toEqual({ vaults: 0, reporting: 0, records: 0, chunks: 0, bytes: 0 });
  });
});

function rate(perSec: number): ThroughputRate {
  return new ThroughputRate({ instantPerSec: perSec });
}

describe("summarizeCluster per-node quantities", () => {
  test("append rates add up across origins", () => {
    const nodes = [
      node("n1", [new VaultStats({ id: VAULT_A, appendRecords: rate(100), appendBytes: rate(10_000) })]),
      node("n2", [new VaultStats({ id: VAULT_A, appendRecords: rate(50), appendBytes: rate(5000) })]),
      node("n3", [new VaultStats({ id: VAULT_A })]),
    ];
    const s = summarizeCluster(nodes, clusterStats(1, 0, 0, 0), 1);
    expect(s.appendedPerSec).toBe(150);
    expect(s.appendedBytesPerSec).toBe(15_000);
  });
});
