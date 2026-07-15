import { encode } from "../../api/glid";
// eslint-disable-next-line no-restricted-imports -- passthrough proto stats types; no model wrap planned
import type { ThroughputRate, VaultStats } from "../../api/gen/gastrolog/v1/vault_pb";
// eslint-disable-next-line no-restricted-imports -- passthrough proto stats types; no model wrap planned
import type { NodeStats } from "../../api/gen/gastrolog/v1/cluster_pb";

// A minimal shape of a cluster status carrying per-node NodeStats — kept loose
// so the aggregator is unit-testable without constructing a full RPC response
// (gastrolog-4r784a).
export interface StageCountersNode {
  id: Uint8Array;
  name: string;
  stats?: { vaults: VaultStats[] } | Pick<NodeStats, "vaults"> | null;
}

// perNode is one node's contribution to one milestone; total is the cluster
// sum. Cluster totals are a plain sum: every milestone is counted exactly once
// by its owner (origin/home/leader) on the node where it happened, so summing
// across nodes never double-counts.
export interface StageMilestone {
  key: string;
  label: string;
  group: "segments" | "chunks" | "recovery";
  total: number;
  perNode: { node: string; count: number }[];
  // rate is present only for the milestones the server windows into a
  // per-second rate (segments completed/published, chunks built/sealed). The
  // cluster rate is the sum of per-node instant rates; sparks stay per-node
  // (phase-skewed sums fabricate a series no node observed).
  clusterInstantPerSec?: number;
  perNodeRate?: { node: string; rate?: ThroughputRate }[];
}

interface MilestoneSpec {
  key: string;
  label: string;
  group: StageMilestone["group"];
  total: (vs: VaultStats) => bigint;
  rate?: (vs: VaultStats) => ThroughputRate | undefined;
}

const specs: MilestoneSpec[] = [
  {
    key: "segmentsCompleted",
    label: "Segments completed",
    group: "segments",
    total: (vs) => vs.segmentsCompletedTotal,
    rate: (vs) => vs.segmentsCompletedRate,
  },
  {
    key: "segmentsPublished",
    label: "Segments published",
    group: "segments",
    total: (vs) => vs.segmentsPublishedTotal,
    rate: (vs) => vs.segmentsPublishedRate,
  },
  {
    key: "segmentsReleased",
    label: "Segments released",
    group: "segments",
    total: (vs) => vs.segmentsReleasedTotal,
  },
  {
    key: "chunksPlanned",
    label: "Chunks planned",
    group: "chunks",
    total: (vs) => vs.chunksPlannedTotal,
  },
  {
    key: "chunksBuilt",
    label: "Chunks built",
    group: "chunks",
    total: (vs) => vs.chunksBuiltTotal,
    rate: (vs) => vs.chunksBuiltRate,
  },
  {
    key: "chunksSealed",
    label: "Chunks sealed",
    group: "chunks",
    total: (vs) => vs.chunksSealedTotal,
    rate: (vs) => vs.chunksSealedRate,
  },
  {
    key: "headPurges",
    label: "Head purges",
    group: "recovery",
    total: (vs) => vs.headPurgesTotal,
  },
  {
    key: "glcbPullsAttempted",
    label: "GLCB pulls attempted",
    group: "recovery",
    total: (vs) => vs.glcbPullsAttemptedTotal,
  },
  {
    key: "glcbPullsFailed",
    label: "GLCB pulls failed",
    group: "recovery",
    total: (vs) => vs.glcbPullsFailedTotal,
  },
  {
    key: "retentionDeletes",
    label: "Retention deletes",
    group: "recovery",
    total: (vs) => vs.retentionDeletesTotal,
  },
];

function nodeLabel(node: StageCountersNode): string {
  if (node.name) return node.name;
  return encode(node.id).slice(0, 8);
}

// milestoneAcc accumulates one spec's counts and rates across nodes.
interface MilestoneAcc {
  total: number;
  perNode: { node: string; count: number }[];
  perNodeRate: { node: string; rate?: ThroughputRate }[];
  clusterInstantPerSec: number;
  anyRate: boolean;
}

// accumulateVault folds one node's VaultStats for a vault into the accumulator
// for one spec. Extracted so aggregateStageCounters stays flat.
function accumulateVault(
  acc: MilestoneAcc,
  spec: MilestoneSpec,
  vs: VaultStats,
  label: string,
): void {
  const count = Number(spec.total(vs));
  if (count > 0) {
    acc.total += count;
    acc.perNode.push({ node: label, count });
  }
  if (!spec.rate) return;
  const rate = spec.rate(vs);
  const instant = rate?.instantPerSec ?? 0;
  if (instant > 0 || (rate?.spark.length ?? 0) > 0) {
    acc.anyRate = true;
    acc.clusterInstantPerSec += instant;
    acc.perNodeRate.push({ node: label, rate });
  }
}

/**
 * aggregateStageCounters collapses every cluster node's per-vault
 * stage-milestone counters into one array of milestones for the vault, each
 * carrying the cluster total and the per-node breakdown. Returns only
 * milestones that reached a non-zero total anywhere (a calm default: quiet
 * until an event happens). Order follows the pipeline: segments → chunks →
 * recovery (gastrolog-4r784a).
 */
export function aggregateStageCounters(
  nodes: readonly StageCountersNode[] | undefined,
  vaultId: string,
): StageMilestone[] {
  const out: StageMilestone[] = [];
  for (const spec of specs) {
    const milestone = computeMilestone(spec, nodes, vaultId);
    if (milestone) out.push(milestone);
  }
  return out;
}

// computeMilestone folds one spec across every node, returning the milestone or
// null when it has neither a non-zero total nor a live rate anywhere.
function computeMilestone(
  spec: MilestoneSpec,
  nodes: readonly StageCountersNode[] | undefined,
  vaultId: string,
): StageMilestone | null {
  const acc: MilestoneAcc = {
    total: 0,
    perNode: [],
    perNodeRate: [],
    clusterInstantPerSec: 0,
    anyRate: false,
  };
  for (const n of nodes ?? []) {
    if (!n.stats) continue;
    const label = nodeLabel(n);
    for (const vs of n.stats.vaults) {
      if (encode(vs.id) === vaultId) accumulateVault(acc, spec, vs, label);
    }
  }
  if (acc.total === 0 && !acc.anyRate) return null;
  acc.perNode.sort((a, b) => b.count - a.count || a.node.localeCompare(b.node));
  return {
    key: spec.key,
    label: spec.label,
    group: spec.group,
    total: acc.total,
    perNode: acc.perNode,
    ...(spec.rate
      ? { clusterInstantPerSec: acc.clusterInstantPerSec, perNodeRate: acc.perNodeRate }
      : {}),
  };
}

/** Human-readable per-node breakdown for a milestone's hover title. */
export function perNodeTitle(m: StageMilestone): string {
  if (m.perNode.length === 0) return "";
  return m.perNode.map((p) => `${p.node}: ${p.count.toLocaleString()}`).join("\n");
}
