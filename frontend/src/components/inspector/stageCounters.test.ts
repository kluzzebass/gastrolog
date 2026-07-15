import { describe, expect, test } from "bun:test";
import { encode } from "../../api/glid";
import { ThroughputRate, VaultStats } from "../../api/gen/gastrolog/v1/vault_pb";
import { aggregateStageCounters, perNodeTitle, type StageCountersNode } from "./stageCounters";

const VAULT = new Uint8Array(16).fill(7);
const VAULT_ID = encode(VAULT);
const OTHER = new Uint8Array(16).fill(9);

function node(name: string, vs: Partial<VaultStats>[]): StageCountersNode {
  return {
    id: new TextEncoder().encode(name),
    name,
    stats: { vaults: vs.map((v) => new VaultStats(v)) },
  };
}

describe("aggregateStageCounters", () => {
  test("returns empty when no node has the vault or all counters are zero", () => {
    expect(aggregateStageCounters(undefined, VAULT_ID)).toEqual([]);
    const n = node("a", [{ id: OTHER, chunksBuiltTotal: 5n }]);
    expect(aggregateStageCounters([n], VAULT_ID)).toEqual([]);
    const zero = node("a", [{ id: VAULT }]);
    expect(aggregateStageCounters([zero], VAULT_ID)).toEqual([]);
  });

  test("sums a milestone across nodes and keeps the per-node breakdown", () => {
    const nodes = [
      node("home-1", [{ id: VAULT, chunksBuiltTotal: 4n }]),
      node("home-2", [{ id: VAULT, chunksBuiltTotal: 2n }]),
      node("home-3", [{ id: VAULT, chunksBuiltTotal: 0n }]),
    ];
    const out = aggregateStageCounters(nodes, VAULT_ID);
    const built = out.find((m) => m.key === "chunksBuilt");
    expect(built?.total).toBe(6);
    // Sorted by count desc; zero-count nodes omitted from the breakdown.
    expect(built?.perNode).toEqual([
      { node: "home-1", count: 4 },
      { node: "home-2", count: 2 },
    ]);
    expect(perNodeTitle(built!)).toBe("home-1: 4\nhome-2: 2");
  });

  test("carries cluster instant rate (sum) for rate-bearing milestones", () => {
    const nodes = [
      node("home-1", [
        {
          id: VAULT,
          chunksBuiltTotal: 3n,
          chunksBuiltRate: new ThroughputRate({ instantPerSec: 2, spark: [1, 2] }),
        },
      ]),
      node("home-2", [
        {
          id: VAULT,
          chunksBuiltTotal: 1n,
          chunksBuiltRate: new ThroughputRate({ instantPerSec: 1.5, spark: [0, 3, 1] }),
        },
      ]),
    ];
    const built = aggregateStageCounters(nodes, VAULT_ID).find((m) => m.key === "chunksBuilt");
    expect(built?.clusterInstantPerSec).toBeCloseTo(3.5);
    expect(built?.perNodeRate?.length).toBe(2);
  });

  test("orders milestones by pipeline stage: segments then chunks then recovery", () => {
    const nodes = [
      node("n", [
        {
          id: VAULT,
          segmentsCompletedTotal: 1n,
          chunksSealedTotal: 1n,
          retentionDeletesTotal: 1n,
        },
      ]),
    ];
    const keys = aggregateStageCounters(nodes, VAULT_ID).map((m) => m.key);
    expect(keys).toEqual(["segmentsCompleted", "chunksSealed", "retentionDeletes"]);
  });

  test("includes a milestone with only a rate (no total yet)", () => {
    const nodes = [
      node("n", [
        {
          id: VAULT,
          segmentsCompletedRate: new ThroughputRate({ instantPerSec: 5, spark: [5] }),
        },
      ]),
    ];
    const completed = aggregateStageCounters(nodes, VAULT_ID).find(
      (m) => m.key === "segmentsCompleted",
    );
    expect(completed?.total).toBe(0);
    expect(completed?.clusterInstantPerSec).toBeCloseTo(5);
  });
});
