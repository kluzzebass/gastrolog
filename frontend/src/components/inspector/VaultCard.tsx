import { encode } from "../../api/glid";
import { Fragment, useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { clickableProps } from "../../utils";
import { useChunks, useIndexes, useValidateVault, useConfig, useArchiveChunk, useRestoreChunk } from "../../api/hooks";
import { useToast } from "../Toast";
import { buildNodeNameMap, resolveNodeName } from "../../utils/nodeNames";
import type { VaultInfo, ChunkMeta } from "../../api/gen/gastrolog/v1/vault_pb";
import { protoToInstant, instantToMs, instantToDate, formatDateTimeShort } from "../../utils/temporal";
import { formatBytes } from "../../utils/units";
import { leaderNodeId, followerNodeIds } from "../../utils/tierPlacement";
import { Badge } from "../Badge";
import { CogIcon } from "../icons";
import { ExpandableCard } from "../settings/ExpandableCard";
import { CrossLinkBadge } from "./CrossLinkBadge";

interface VaultCardProps {
  vault: VaultInfo;
  cloudProvider?: string;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  onOpenSettings?: () => void;
}

export function VaultCard({
  vault,
  cloudProvider,
  dark,
  expanded,
  onToggle,
  onOpenSettings,
}: Readonly<VaultCardProps>) {
  // Use ListChunks data (fans out to leader nodes, authoritative per chunk).
  // ListVaults stats rely on periodic peer broadcasts and flicker.
  const { data: chunks } = useChunks(encode(vault.id));
  const chunkCount = chunks?.length ?? 0;
  const recordCount = (chunks ?? []).reduce((sum, c) => sum + Number(c.recordCount), 0);

  return (
    <ExpandableCard
      key={encode(vault.id)}
      id={vault.name || encode(vault.id)}
      typeBadge={vault.type}
      secondaryBadge={cloudProvider}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      headerRight={
        <span className="flex items-center gap-1.5">
          {!vault.enabled && (
            <Badge variant="warn" dark={dark}>disabled</Badge>
          )}
          <Badge variant="muted" dark={dark}>
            {chunkCount.toLocaleString()} chunks
          </Badge>
          <Badge variant="muted" dark={dark}>
            {recordCount.toLocaleString()} records
          </Badge>
          {onOpenSettings && (
            <CrossLinkBadge dark={dark} title="Open in Settings" onClick={onOpenSettings}>
              <CogIcon className="w-3 h-3" />
            </CrossLinkBadge>
          )}
        </span>
      }
    >
      <VaultActions vaultId={encode(vault.id)} dark={dark} />
      <ChunkList vaultId={encode(vault.id)} dark={dark} />
    </ExpandableCard>
  );
}

// Chunk IDs are 26-char base32 strings. Render as <8>…<5> so adjacent
// rows stay distinguishable while fitting in a stable column width
// without any layout-measurement library or flex tricks. Full ID is in
// the title attribute.
function middleTruncateChunkID(id: string): string {
  if (id.length <= 16) return id;
  return id.slice(0, 8) + "…" + id.slice(-5);
}

function VaultActions({
  vaultId,
  dark,
}: Readonly<{
  vaultId: string;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const validate = useValidateVault();
  const { addToast } = useToast();

  return (
    <div
      className={`flex items-center gap-2 px-4 py-2 border-b ${c(
        "border-ink-border-subtle",
        "border-light-border-subtle",
      )}`}
    >
      <button
        type="button"
        className={`px-2.5 py-1 text-[0.8em] rounded border transition-colors ${c(
          "border-ink-border-subtle text-text-muted hover:bg-ink-hover",
          "border-light-border-subtle text-light-text-muted hover:bg-light-hover",
        )}`}
        disabled={validate.isPending}
        onClick={async () => {
          try {
            const result = await validate.mutateAsync(vaultId);
            if (result.valid) {
              addToast(
                `Vault valid (${result.chunks.length} chunk(s) checked)`,
                "info",
              );
            } else {
              const issues = result.chunks
                .filter((ch) => !ch.valid)
                .map((ch) => `${encode(ch.chunkId)}: ${ch.issues.join(", ")}`)
                .join("; ");
              addToast(`Validation failed: ${issues}`, "error");
            }
          } catch (err: unknown) {
            addToast(err instanceof Error ? err.message : "Validation failed", "error");
          }
        }}
      >
        {validate.isPending ? "Validating..." : "Validate"}
      </button>
    </div>
  );
}

function ChunkList({ vaultId, dark }: Readonly<{ vaultId: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data: chunks, isLoading } = useChunks(vaultId);
  const { data: config } = useConfig();
  const [expandedChunk, setExpandedChunk] = useState<string | null>(null);

  // Build tier position map from vault config for labeling.
  // Tiers own their vault association via vaultId + position fields.
  const vaultTiers = (config?.tiers ?? [])
    .filter((t) => encode(t.vaultId) === vaultId)
    .toSorted((a, b) => a.position - b.position);
  const tierPositions = new Map<string, number>(
    vaultTiers.map((t) => [encode(t.id), t.position + 1]),
  );

  if (isLoading) {
    return (
      <div
        className={`px-4 py-3 text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
      >
        Loading chunks...
      </div>
    );
  }

  // Backend already deduplicates chunks and populates replica_count. Each
  // chunk appears exactly once with authoritative metadata from the leader.
  const dedupedChunks = chunks ?? [];

  // Group chunks by tier, then sort within each tier by time (newest first).
  const tierGroups = new Map<string, { tierType: string; chunks: ChunkMeta[] }>();
  for (const chunk of dedupedChunks) {
    const key = encode(chunk.tierId) || "unknown";
    const existing = tierGroups.get(key);
    if (existing) {
      existing.chunks.push(chunk);
    } else {
      tierGroups.set(key, { tierType: chunk.tierType, chunks: [chunk] });
    }
  }

  // Node name resolution — used by both local and remote tier headers.
  const nodeNameMap = buildNodeNameMap(config?.nodeConfigs ?? []);

  // Identify remote tiers (in vault config but no local chunks).
  const remoteTierInfo = (() => {
    if (vaultTiers.length === 0) return [];
    const localTierIds = new Set(tierGroups.keys());
    const tierTypeMap: Record<number, string> = { 1: "memory", 2: "file", 3: "cloud", 4: "jsonl" };
    const nscs = config?.nodeStorageConfigs ?? [];
    return vaultTiers
      .filter((tc) => !localTierIds.has(encode(tc.id)))
      .map((tc) => {
        const pnId = leaderNodeId(tc, nscs);
        return {
          id: encode(tc.id),
          pos: tierPositions.get(encode(tc.id)) ?? 0,
          type: tierTypeMap[tc.type] ?? "unknown",
          nodeName: pnId ? resolveNodeName(nodeNameMap, pnId) : "",
          rf: tc.replicationFactor || 1,
          followerNodeIds: followerNodeIds(tc, nscs),
          storageClass: tc.storageClass,
        };
      });
  })();

  const sortChunks = (arr: ChunkMeta[]) =>
    arr.toSorted((a, b) => {
      const aTs = a.ingestStart ?? a.writeStart;
      const bTs = b.ingestStart ?? b.writeStart;
      const aTime = aTs ? instantToMs(protoToInstant(aTs)) : 0;
      const bTime = bTs ? instantToMs(protoToInstant(bTs)) : 0;
      return bTime - aTime;
    });

  // One <table> for ALL tiers so column widths align across the vault.
  // Tier headers are colSpan rows interleaved between chunk groups; remote
  // (placement-only, no local chunks) tiers render as a single placeholder
  // header row with no chunk rows underneath. See gastrolog-28yi3.
  const nscs = config?.nodeStorageConfigs ?? [];
  return (
    <div>
      <table className="w-full border-collapse">
        <thead>
          <tr
            className={`text-left text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c(
              "text-text-muted border-ink-border-subtle",
              "text-light-text-muted border-light-border-subtle",
            )}`}
          >
            <th className="px-4 py-2 font-medium">Chunk ID</th>
            <th className="px-2 py-2 font-medium">Time Range</th>
            <th className="px-2 py-2 font-medium">Status</th>
            <th className="px-2 py-2 font-medium text-right">Records</th>
            <th className="px-4 py-2 font-medium text-right">Size</th>
          </tr>
        </thead>
        <tbody>
          {vaultTiers.map((vt) => encode(vt.id)).map((tierId) => {
            const pos = tierPositions.get(tierId) ?? 0;
            const group = tierGroups.get(tierId);
            const remote = remoteTierInfo.find((rt) => rt.id === tierId);

            // Remote tier with no local chunks — single placeholder row.
            if (!group && remote) {
              return (
                <tr
                  key={tierId}
                  className={`text-[0.75em] font-medium uppercase tracking-[0.12em] border-b ${c(
                    "text-text-muted border-ink-border-subtle bg-ink-base/30",
                    "text-light-text-muted border-light-border-subtle bg-light-base/30",
                  )}`}
                >
                  <td colSpan={5} className="px-4 py-1.5">
                    <span className="inline-flex flex-wrap items-center gap-2">
              <Badge variant="muted" dark={dark}>{`Tier ${String(pos)}: ${remote.type}`}</Badge>
              <span>{remote.nodeName ? `on ${remote.nodeName}` : "unplaced"}</span>
              {remote.rf > 1 && <Badge variant="info" dark={dark}>{`RF=${String(remote.rf)}`}</Badge>}
              {remote.followerNodeIds.length > 0 && (
                <span>
                  {"\u2192 "}
                  {remote.followerNodeIds.map((id, si) => {
                    const name = resolveNodeName(nodeNameMap, id);
                    let fallbackClass = 0;
                    if (remote.storageClass > 0) {
                      const nsc = (config?.nodeStorageConfigs ?? []).find((n) => encode(n.nodeId) === id);
                      const hasExact = nsc?.fileStorages.some((a) => a.storageClass === remote.storageClass);
                      if (!hasExact && nsc && nsc.fileStorages.length > 0) {
                        fallbackClass = nsc.fileStorages[0]!.storageClass;
                      }
                    }
                    return (
                      <span key={id}>
                        {si > 0 && ", "}
                        {name}
                        {fallbackClass > 0 && (
                          <span className="text-severity-warn">{` (class ${String(fallbackClass)})`}</span>
                        )}
                      </span>
                    );
                  })}
                </span>
              )}
                    </span>
                  </td>
                </tr>
              );
            }

            if (!group) return null;

            const label = `Tier ${String(pos)}: ${group.tierType}`;
            const tierCfg = config?.tiers.find((t) => encode(t.id) === tierId);
            const rf = tierCfg?.replicationFactor || 1;
            const secondaries = tierCfg ? followerNodeIds(tierCfg, nscs) : [];
            const pnId = tierCfg ? leaderNodeId(tierCfg, nscs) : "";
            const nodeName = pnId ? resolveNodeName(nodeNameMap, pnId) : "";
            return (
              <Fragment key={tierId}>
                <tr
                  className={`text-[0.75em] font-medium uppercase tracking-[0.12em] border-b ${c(
                    "text-text-muted border-ink-border-subtle bg-ink-base/30",
                    "text-light-text-muted border-light-border-subtle bg-light-base/30",
                  )}`}
                >
                  <td colSpan={5} className="px-4 py-1.5">
                    <span className="inline-flex flex-wrap items-center gap-2">
                      <Badge variant="copper" dark={dark}>{label}</Badge>
                      {nodeName && <span>{`on ${nodeName}`}</span>}
                      {group.tierType === "jsonl" && tierCfg?.path && (
                        <span className="font-mono">{tierCfg.path}</span>
                      )}
                      <span>{`${String(group.chunks.length)} ${group.chunks.length === 1 ? "chunk" : "chunks"}`}</span>
                      <span>{`${group.chunks.reduce((sum, ch) => sum + Number(ch.recordCount), 0).toLocaleString()} records`}</span>
                      {rf > 1 && <Badge variant="info" dark={dark}>{`RF=${String(rf)}`}</Badge>}
                      {secondaries.length > 0 && (
                        <span>
                          {"\u2192 "}
                          {secondaries.map((id, si) => {
                            const name = resolveNodeName(nodeNameMap, id);
                            const requiredClass = tierCfg?.storageClass ?? 0;
                            let fallbackClass = 0;
                            if (requiredClass > 0) {
                              const nsc = nscs.find((n) => encode(n.nodeId) === id);
                              const hasExact = nsc?.fileStorages.some((a) => a.storageClass === requiredClass);
                              if (!hasExact && nsc && nsc.fileStorages.length > 0) {
                                fallbackClass = nsc.fileStorages[0]!.storageClass;
                              }
                            }
                            return (
                              <span key={id}>
                                {si > 0 && ", "}
                                {name}
                                {fallbackClass > 0 && (
                                  <span className="text-severity-warn">{` (class ${String(fallbackClass)})`}</span>
                                )}
                              </span>
                            );
                          })}
                        </span>
                      )}
                    </span>
                  </td>
                </tr>
                {sortChunks(group.chunks).map((chunk) => {
                const startTs = chunk.ingestStart ?? chunk.writeStart;
                const endTs = chunk.ingestEnd ?? chunk.writeEnd;
                const start = startTs ? instantToDate(protoToInstant(startTs)) : undefined;
                const end = endTs ? instantToDate(protoToInstant(endTs)) : undefined;
                const isExpanded = expandedChunk === encode(chunk.id);

                const replicas = chunk.replicaCount || 1;
                // Actual residency from the cluster fan-out: which nodes
                // physically hold this chunk right now. Distinct from
                // placement (leader + secondaries from tier config), which
                // says where the chunk SHOULD live, not where it IS.
                const residentNodes = chunk.replicaNodeIds.map((id) =>
                  resolveNodeName(nodeNameMap, id),
                );
                const placementNodes = tierCfg
                  ? [pnId, ...secondaries].filter(Boolean).map((id) => resolveNodeName(nodeNameMap, id))
                  : [];
                // Per-node ack laggards for chunks stuck in the receipt
                // protocol's pendingDeletes — tells operators which node
                // is holding up a delete.
                const pendingAckNodes = chunk.pendingAckNodeIds.map((id) =>
                  resolveNodeName(nodeNameMap, id),
                );
                return (
                  <ChunkRow
                    key={encode(chunk.id)}
                    chunk={chunk}
                    vaultId={vaultId}
                    start={start}
                    end={end}
                    isExpanded={isExpanded}
                    onToggle={() => setExpandedChunk(isExpanded ? null : encode(chunk.id))}
                    dark={dark}
                    c={c}
                    replicas={replicas}
                    rf={rf}
                    residentNodes={residentNodes}
                    placementNodes={placementNodes}
                    pendingAckNodes={pendingAckNodes}
                  />
                );
                })}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ChunkReplicaBadges renders one Badge per node in the chunk's
// placement set, colored to encode that node's relationship to this
// chunk. Mirrors the per-node-status row used by IngesterCard so
// operators get a single visual language across the inspector.
//
// Variant mapping per node:
//   info        node holds the replica (healthy)
//   warn        node is in placement but missing the replica
//               (replication lag, transition mid-flight, or lost)
//   error       node owes a receipt-protocol delete ack (laggard
//               blocking the delete) — overrides info/warn so the
//               ack-blocker stands out even on a held replica
//   muted       node is NOT in placement but reports having the
//               replica anyway (rare: stale follower copy after a
//               placement change). Surfaces something an operator
//               would want to clean up.
function ChunkReplicaBadges({
  placementNodes,
  residentNodes,
  pendingAckNodes,
  dark,
}: Readonly<{
  placementNodes: string[];
  residentNodes: string[];
  pendingAckNodes: string[];
  dark: boolean;
}>) {
  const placementSet = new Set(placementNodes);
  const residentSet = new Set(residentNodes);
  const ackSet = new Set(pendingAckNodes);

  // Union of (placement ∪ residency ∪ pending-ack) so unexpected
  // residencies and pending-ack laggards both surface even when they
  // fall outside placement. Sorted for deterministic display.
  const seen = new Set<string>();
  const order: string[] = [];
  for (const n of placementNodes) {
    if (!seen.has(n)) {
      seen.add(n);
      order.push(n);
    }
  }
  for (const n of residentNodes) {
    if (!seen.has(n)) {
      seen.add(n);
      order.push(n);
    }
  }
  for (const n of pendingAckNodes) {
    if (!seen.has(n)) {
      seen.add(n);
      order.push(n);
    }
  }
  order.sort();

  if (order.length === 0) return null;

  return (
    <span className="flex items-center gap-1 flex-wrap">
      {order.map((n) => {
        const inPlacement = placementSet.has(n);
        const hasReplica = residentSet.has(n);
        const owesAck = ackSet.has(n);

        let variant: "info" | "warn" | "error" | "muted";
        let title: string;
        if (owesAck) {
          variant = "error";
          title = `${n}: pending delete-ack — this node hasn't applied CmdAckDelete yet`;
        } else if (!inPlacement && hasReplica) {
          variant = "muted";
          title = `${n}: stale residency (chunk found here but node is not in placement)`;
        } else if (inPlacement && !hasReplica) {
          variant = "warn";
          title = `${n}: missing replica (placement says yes, no node-local report)`;
        } else {
          variant = "info";
          title = `${n}: replica present`;
        }
        return (
          <Badge key={n} variant={variant} dark={dark} title={title}>
            {n}
          </Badge>
        );
      })}
    </span>
  );
}

function ChunkRow({
  chunk,
  vaultId,
  start,
  end,
  isExpanded,
  onToggle,
  dark,
  c,
  replicas,
  rf,
  residentNodes,
  placementNodes,
  pendingAckNodes,
}: Readonly<{
  chunk: ChunkMeta;
  vaultId: string;
  start: Date | undefined;
  end: Date | undefined;
  isExpanded: boolean;
  onToggle: () => void;
  dark: boolean;
  c: (darkCls: string, lightCls: string) => string;
  replicas: number;
  rf: number;
  residentNodes: string[];
  placementNodes: string[];
  pendingAckNodes: string[];
}>) {
  return (
    <>
      <tr
        className={`border-b text-[0.85em] cursor-pointer transition-colors ${c(
          "border-ink-border-subtle hover:bg-ink-hover",
          "border-light-border-subtle hover:bg-light-hover",
        )} ${isExpanded ? c("bg-ink-hover", "bg-light-hover") : ""}`}
        onClick={onToggle}
        {...clickableProps(onToggle)}
        aria-expanded={isExpanded}
      >
        <td className="px-4 py-2 whitespace-nowrap">
          <span
            className={`text-[0.6em] transition-transform inline-block mr-1.5 ${isExpanded ? "rotate-90" : ""} ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {"\u25B6"}
          </span>
          <span
            className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}
            title={encode(chunk.id)}
          >
            {middleTruncateChunkID(encode(chunk.id))}
          </span>
        </td>
        <td className="px-2 py-2">
          <span
            className={`text-[0.95em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {start ? formatDateTimeShort(start) : "\u2014"}
            <span className={`mx-1.5 ${c("text-text-muted", "text-light-text-muted")}`}>
              {"\u2192"}
            </span>
            {end ? formatDateTimeShort(end) : "\u2014"}
          </span>
        </td>
        <td className="px-2 py-2">
          <span className="flex items-center gap-1 whitespace-nowrap">
            {chunk.sealed ? (
              <Badge variant="copper" dark={dark}>sealed</Badge>
            ) : (
              <Badge variant="info" dark={dark}>active</Badge>
            )}
            {chunk.compressed && (
              <Badge variant="info" dark={dark}>compr</Badge>
            )}
            {chunk.cloudBacked && (
              <Badge variant="muted" dark={dark}>cloud</Badge>
            )}
            {chunk.archived && (
              <Badge variant="warn" dark={dark}>{chunk.storageClass || "archived"}</Badge>
            )}
            {chunk.retentionPending && (
              <Badge
                variant="warn"
                dark={dark}
                title="Retention pending — chunk is queued for expire/eject/transition, or mid-stream to the next tier (not the same as TTL elapsed)"
              >
                ret
              </Badge>
            )}
            {chunk.transitionStreamed && (
              <Badge
                variant="warn"
                dark={dark}
                title="Transition complete on this tier — records were streamed to the next tier; this copy is awaiting replicated confirmation before deletion"
              >
                del
              </Badge>
            )}
            {rf > 1 && (() => {
              // Compact summary in the row: a single replica-count badge.
              // Per-node detail lives in the expanded pane (see ChunkDetail).
              let badgeVariant: "info" | "error" | "warn";
              if (replicas >= rf) {
                badgeVariant = "info";
              } else if (placementNodes.length < rf) {
                badgeVariant = "error";
              } else {
                badgeVariant = "warn";
              }
              return (
                <Badge
                  variant={badgeVariant}
                  dark={dark}
                  title="Expand the chunk row for per-node replica status"
                >
                  {String(replicas)}
                </Badge>
              );
            })()}
            {pendingAckNodes.length > 0 && (
              <Badge
                variant="error"
                dark={dark}
                title={`Pending delete-ack from: ${pendingAckNodes.join(", ")}`}
              >
                pending-ack
              </Badge>
            )}
          </span>
        </td>
        <td className={`px-2 py-2 text-right font-mono whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}>
          {Number(chunk.recordCount).toLocaleString()}
        </td>
        <td
          className={`px-4 py-2 text-right font-mono whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
          title={chunk.compressed && Number(chunk.diskBytes) > 0
            ? `${formatBytes(Number(chunk.bytes))} \u2192 ${formatBytes(Number(chunk.diskBytes))} on disk`
            : undefined}
        >
          {chunk.compressed && Number(chunk.diskBytes) > 0
            ? formatBytes(Number(chunk.diskBytes))
            : formatBytes(Number(chunk.bytes))}
        </td>
      </tr>
      {isExpanded && (
        <tr>
          <td colSpan={5} className="p-0">
            <ChunkDetail
              vaultId={vaultId}
              chunk={chunk}
              dark={dark}
              rf={rf}
              residentNodes={residentNodes}
              placementNodes={placementNodes}
              pendingAckNodes={pendingAckNodes}
            />
          </td>
        </tr>
      )}
    </>
  );
}

function ChunkDetail({
  vaultId,
  chunk,
  dark,
  rf,
  residentNodes,
  placementNodes,
  pendingAckNodes,
}: Readonly<{
  vaultId: string;
  chunk: ChunkMeta;
  dark: boolean;
  rf: number;
  residentNodes: string[];
  placementNodes: string[];
  pendingAckNodes: string[];
}>) {
  const c = useThemeClass(dark);
  // Skip index fetch for cloud-backed chunks — they don't have local indexes.
  const { data, isLoading } = useIndexes(vaultId, chunk.cloudBacked ? "" : encode(chunk.id));

  const logicalBytes = Number(chunk.bytes);
  const diskBytes = Number(chunk.diskBytes);
  const showCompression = chunk.compressed && diskBytes > 0 && logicalBytes > 0;
  const reductionPct = showCompression
    ? Math.round((1 - diskBytes / logicalBytes) * 100)
    : 0;

  return (
    <div className={`px-4 py-3 ${c("bg-ink-raised", "bg-light-bg")}`}>
      {/* Full chunk ID — selectable for copy/paste */}
      <div className="mb-3">
        <div
          className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Chunk ID
        </div>
        <div
          className={`font-mono text-[0.85em] select-all ${c("text-text-muted", "text-light-text-muted")}`}
        >
          {encode(chunk.id)}
        </div>
      </div>

      {/* Replication info — only shown when RF > 1 */}
      {rf > 1 && (
        <div className="mb-3">
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Replicas
          </div>
          <ChunkReplicaBadges
            placementNodes={placementNodes}
            residentNodes={residentNodes}
            pendingAckNodes={pendingAckNodes}
            dark={dark}
          />
          {placementNodes.length < rf && (
            <div className="mt-1 text-[0.8em] text-severity-error">
              Not enough nodes with the required storage class to satisfy RF={String(rf)}
            </div>
          )}
        </div>
      )}

      {/* Compression / storage info */}
      {showCompression && (
        <div className="mb-3">
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Compression
          </div>
          <div className={`flex items-center gap-3 text-[0.85em]`}>
            <span
              className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}
            >
              {formatBytes(logicalBytes)} &rarr; {formatBytes(diskBytes)}
            </span>
            <span
              className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}
            >
              {reductionPct}% reduction
            </span>
          </div>
        </div>
      )}

      {/* Active chunks: no indexes yet */}
      {!chunk.sealed && (
        <div
          className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Indexes are built when the chunk is sealed.
        </div>
      )}
      {chunk.sealed && chunk.cloudBacked && (
        <>
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Cloud Storage
          </div>
          <div className="flex flex-col gap-1.5">
            <div className={`flex items-center gap-3 text-[0.85em]`}>
              <span className={`font-mono w-20 ${c("text-text-bright", "text-light-text-bright")}`}>blob</span>
              <span className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
                {formatBytes(Number(chunk.diskBytes))}
              </span>
              <span className={c("text-text-muted", "text-light-text-muted")}>
                GLCB{chunk.numFrames > 0 ? `, ${chunk.numFrames} seekable zstd frames` : ", seekable zstd"}
              </span>
            </div>
            <div className={`flex items-center gap-3 text-[0.85em]`}>
              <span className={`font-mono w-20 ${c("text-text-bright", "text-light-text-bright")}`}>ts-index</span>
              <Badge variant="info" dark={dark}>embedded</Badge>
              <span className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
                {Number(chunk.recordCount).toLocaleString()} entries
              </span>
            </div>
            <div className={`flex items-center gap-3 text-[0.85em]`}>
              <span className={`font-mono w-20 ${c("text-text-bright", "text-light-text-bright")}`}>class</span>
              <span className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
                {chunk.storageClass || "standard"}
              </span>
              {!chunk.archived && (
                <ArchiveButton vaultId={vaultId} chunkId={encode(chunk.id)} dark={dark} />
              )}
              {chunk.archived && (
                <RestoreButton vaultId={vaultId} chunkId={encode(chunk.id)} dark={dark} />
              )}
            </div>
          </div>
        </>
      )}
      {chunk.sealed && !chunk.cloudBacked && (
        <>
          {/* Local indexes */}
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Indexes
          </div>

          {isLoading && (
            <div
              className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Loading indexes...
            </div>
          )}
          {!isLoading && (!data?.indexes || data.indexes.length === 0) && (
            <div
              className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
            >
              No indexes.
            </div>
          )}
          {!isLoading && data?.indexes && data.indexes.length > 0 && (
            <div className="flex flex-col gap-1.5">
              {data.indexes.map((idx) => (
                <div
                  key={idx.name}
                  className={`flex items-center gap-3 text-[0.85em]`}
                >
                  <span
                    className={`font-mono w-20 ${c("text-text-bright", "text-light-text-bright")}`}
                  >
                    {idx.name}
                  </span>
                  {idx.exists ? (
                    <>
                      <Badge variant="info" dark={dark}>ok</Badge>
                      <span
                        className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}
                      >
                        {Number(idx.entryCount).toLocaleString()} entries
                      </span>
                      <span
                        className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}
                      >
                        {formatBytes(Number(idx.sizeBytes))}
                      </span>
                    </>
                  ) : (
                    <Badge variant="muted" dark={dark}>missing</Badge>
                  )}
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function ArchiveButton({ vaultId, chunkId, dark }: Readonly<{ vaultId: string; chunkId: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  const archive = useArchiveChunk();
  const { addToast } = useToast();
  return (
    <button
      onClick={(e) => {
        e.stopPropagation();
        archive.mutate(
          { vaultId, chunkId },
          {
            onSuccess: () => addToast("Chunk archived to Glacier", "info"),
            onError: (err) => addToast(err instanceof Error ? err.message : "Archive failed", "error"),
          },
        );
      }}
      disabled={archive.isPending}
      title="Archive chunk to offline storage"
      className={`px-2 py-0.5 text-[0.8em] rounded border transition-colors ${c(
        "border-ink-border text-text-muted hover:text-copper hover:border-copper/40 hover:bg-ink-hover",
        "border-light-border text-light-text-muted hover:text-copper hover:border-copper/40 hover:bg-light-hover",
      )}`}
    >
      {archive.isPending ? "Archiving..." : "Archive"}
    </button>
  );
}

function RestoreButton({ vaultId, chunkId, dark }: Readonly<{ vaultId: string; chunkId: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  const restore = useRestoreChunk();
  const { addToast } = useToast();
  return (
    <button
      onClick={(e) => {
        e.stopPropagation();
        restore.mutate(
          { vaultId, chunkId },
          {
            onSuccess: () => addToast("Chunk restore initiated", "info"),
            onError: (err) => addToast(err instanceof Error ? err.message : "Restore failed", "error"),
          },
        );
      }}
      disabled={restore.isPending}
      title="Restore chunk from offline storage"
      className={`px-2 py-0.5 text-[0.8em] rounded border transition-colors ${c(
        "border-ink-border text-severity-warn hover:text-copper hover:border-copper/40 hover:bg-ink-hover",
        "border-light-border text-severity-warn hover:text-copper hover:border-copper/40 hover:bg-light-hover",
      )}`}
    >
      {restore.isPending ? "Restoring..." : "Restore"}
    </button>
  );
}

