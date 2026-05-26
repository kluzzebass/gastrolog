import { encode } from "../api/glid";
import type { ClusterNode } from "../api/gen/gastrolog/v1/lifecycle_pb";
import type {
  FenceRecordDiagnostics,
  GetSequencedVaultDiagnosticsResponse,
  SeqAllocatorDiagnostics,
  VaultStats,
} from "../api/gen/gastrolog/v1/vault_pb";
import { protoToInstant, formatTimestamp } from "./temporal";

export interface SequencedWatermarkSet {
  h: bigint;
  sR: bigint;
  fN: bigint;
  mR: bigint;
  cR: bigint;
}

export interface PeerSequencedWatermark {
  nodeId: string;
  nodeName: string;
  watermarks: SequencedWatermarkSet;
}

/** Formats a vault_seq for display (matches CLI inspect output). */
export function formatSeq(value: bigint | number | undefined): string {
  const v = typeof value === "bigint" ? value : BigInt(value ?? 0);
  if (v === 0n) return "0";
  return v.toString();
}

export function watermarksFromStats(stats: VaultStats): SequencedWatermarkSet {
  return {
    h: stats.ingestHighWatermark,
    sR: stats.spoolWatermark,
    fN: stats.fenceHighWatermark,
    mR: stats.materializationWatermark,
    cR: stats.convergenceWatermark,
  };
}

export function watermarksFromDiagnostics(
  diag: GetSequencedVaultDiagnosticsResponse,
): SequencedWatermarkSet {
  return {
    h: diag.ingestHighWatermark,
    sR: diag.spoolWatermark,
    fN: diag.fenceHighWatermark,
    mR: diag.materializationWatermark,
    cR: diag.convergenceWatermark,
  };
}

/** True when gossip stats carry any sequenced watermark evidence. */
export function hasSequencedWatermarkEvidence(w: SequencedWatermarkSet): boolean {
  return w.h > 0n || w.sR > 0n || w.fN > 0n || w.mR > 0n || w.cR > 0n;
}

export function hasVaultStatsEvidence(stats: VaultStats): boolean {
  return hasSequencedWatermarkEvidence(watermarksFromStats(stats));
}

/** Detect local lag between sequenced pipeline stages (matches CLI triage semantics). */
export function sequencedLagWarnings(w: SequencedWatermarkSet): string[] {
  const warnings: string[] = [];
  if (w.h > w.sR) warnings.push("Ingest ahead of spool (H > S_r)");
  if (w.sR > w.fN && w.fN > 0n) warnings.push("Spool ahead of fence (S_r > F_n)");
  if (w.fN > w.mR && w.fN > 0n) warnings.push("Fence ahead of materialization (F_n > M_r)");
  if (w.mR > w.cR && w.mR > 0n) warnings.push("Materialization ahead of convergence (M_r > C_r)");
  return warnings;
}

/** Compare peer S_r values — divergence when spread exceeds zero with multiple reporters. */
export function peerSpoolDivergence(peers: PeerSequencedWatermark[]): string | null {
  if (peers.length < 2) return null;
  let min = peers[0]!.watermarks.sR;
  let max = min;
  for (let i = 1; i < peers.length; i++) {
    const sR = peers[i]!.watermarks.sR;
    if (sR < min) min = sR;
    if (sR > max) max = sR;
  }
  if (max > min) {
    return `Replica spool spread: S_r ${formatSeq(min)}–${formatSeq(max)} across ${peers.length} nodes`;
  }
  return null;
}

export function extractPeerSequencedWatermarks(
  vaultId: string,
  nodes: readonly ClusterNode[],
  nodeNames: ReadonlyMap<string, string>,
): PeerSequencedWatermark[] {
  const peers: PeerSequencedWatermark[] = [];
  for (const node of nodes) {
    const nodeId = encode(node.id);
    if (!node.stats) continue;
    for (const vs of node.stats.vaults) {
      if (encode(vs.id) !== vaultId) continue;
      const watermarks = watermarksFromStats(vs);
      if (!hasSequencedWatermarkEvidence(watermarks)) continue;
      peers.push({
        nodeId,
        nodeName: nodeNames.get(nodeId) || node.name || nodeId,
        watermarks,
      });
    }
  }
  return peers.toSorted((a, b) => a.nodeName.localeCompare(b.nodeName));
}

export function formatFenceCreatedAt(fence: FenceRecordDiagnostics): string {
  if (!fence.createdAt) return "—";
  return formatTimestamp(protoToInstant(fence.createdAt));
}

export function allocatorSummary(alloc: SeqAllocatorDiagnostics | undefined): {
  nextSeq: string;
  epoch: string;
  activeSwaths: SeqAllocatorDiagnostics["activeSwaths"];
  burnedTails: SeqAllocatorDiagnostics["burnedTails"];
} {
  return {
    nextSeq: formatSeq(alloc?.nextSeq),
    epoch: formatSeq(alloc?.epoch),
    activeSwaths: alloc?.activeSwaths ?? [],
    burnedTails: alloc?.burnedTails ?? [],
  };
}
