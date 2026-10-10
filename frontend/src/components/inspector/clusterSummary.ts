import { encode } from "../../api/glid";
// eslint-disable-next-line no-restricted-imports -- NodeStats is a passthrough type from Node.stats; no model wrap planned
import type { NodeStats } from "../../api/gen/gastrolog/v1/cluster_pb";
// eslint-disable-next-line no-restricted-imports -- GetStatsResponse is a passthrough stats type; no model wrap planned
import type { GetStatsResponse } from "../../api/gen/gastrolog/v1/vault_pb";

export interface ClusterSummaryNode {
  id: Uint8Array;
  name: string;
  isLeader: boolean;
  stats?: NodeStats;
}

export type ClusterVaultStats = Pick<
  GetStatsResponse,
  "totalVaults" | "totalRecords" | "totalChunks" | "totalBytes"
>;

// Holdings come from the cluster-wide vault stats, never from summing node
// broadcasts: every home reports its own copy of a vault, so a per-node sum
// counts each record once per replica. Null until the stats arrive.
export interface VaultHoldings {
  vaults: number;
  // Vaults whose figures the totals include. Below `vaults` when the serving
  // node could read no figures for some vault: the totals then cover only
  // the reporting vaults.
  reporting: number;
  records: number;
  chunks: number;
  bytes: number;
}

export interface ClusterSummary {
  leaderName: string;
  holdings: VaultHoldings | null;
  cpuPercent: number;
  rssBytes: number;
  heapAllocBytes: number;
  goroutines: number;
  ingestQueueDepth: number;
  ingestQueueCapacity: number;
  routedPerSec: number;
  matchedPerSec: number;
  // Appends happen once per record, on the origin node that segments it,
  // so the per-node rates add up to the cluster rate.
  appendedPerSec: number;
  appendedBytesPerSec: number;
}

export function summarizeCluster(
  nodes: readonly ClusterSummaryNode[],
  vaultStats: ClusterVaultStats | null | undefined,
  configuredVaults: number,
): ClusterSummary {
  const summary: ClusterSummary = {
    leaderName: "",
    holdings: vaultHoldings(vaultStats, configuredVaults),
    cpuPercent: 0,
    rssBytes: 0,
    heapAllocBytes: 0,
    goroutines: 0,
    ingestQueueDepth: 0,
    ingestQueueCapacity: 0,
    routedPerSec: 0,
    matchedPerSec: 0,
    appendedPerSec: 0,
    appendedBytesPerSec: 0,
  };
  for (const node of nodes) {
    if (node.isLeader) summary.leaderName = node.name || encode(node.id);
    const s = node.stats;
    if (!s) continue;
    summary.cpuPercent += s.cpuPercent;
    summary.rssBytes += Number(s.memoryRss);
    summary.heapAllocBytes += Number(s.memoryHeapAlloc);
    summary.goroutines += s.goroutines;
    summary.ingestQueueDepth += s.ingestQueueDepth;
    summary.ingestQueueCapacity += s.ingestQueueCapacity;
    summary.routedPerSec += s.routeRouted?.instantPerSec ?? 0;
    summary.matchedPerSec += s.routeMatched?.instantPerSec ?? 0;
    for (const v of s.vaults) {
      summary.appendedPerSec += v.appendRecords?.instantPerSec ?? 0;
      summary.appendedBytesPerSec += v.appendBytes?.instantPerSec ?? 0;
    }
  }
  return summary;
}

function vaultHoldings(
  vaultStats: ClusterVaultStats | null | undefined,
  configuredVaults: number,
): VaultHoldings | null {
  if (!vaultStats) return null;
  const reporting = Number(vaultStats.totalVaults);
  return {
    vaults: Math.max(configuredVaults, reporting),
    reporting,
    records: Number(vaultStats.totalRecords),
    chunks: Number(vaultStats.totalChunks),
    bytes: Number(vaultStats.totalBytes),
  };
}
