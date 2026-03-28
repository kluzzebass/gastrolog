import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { clickableProps } from "../../utils";
import { useChunks, useIndexes, useValidateVault, useConfig } from "../../api/hooks";
import { useToast } from "../Toast";
import type { VaultInfo, ChunkMeta } from "../../api/gen/gastrolog/v1/vault_pb";
import { protoToInstant, instantToMs, instantToDate, formatDateTimeShort } from "../../utils/temporal";
import { formatBytes } from "../../utils/units";
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
  // Use ListChunks data (fans out to all nodes) for accurate counts.
  // ListVaults stats rely on periodic peer broadcasts and flicker.
  const { data: chunks } = useChunks(vault.id);
  const dedupedChunks = (() => {
    if (!chunks) return [];
    const seen = new Set<string>();
    return chunks.filter((c) => {
      if (seen.has(c.id)) return false;
      seen.add(c.id);
      return true;
    });
  })();
  const chunkCount = dedupedChunks.length;
  const recordCount = dedupedChunks.reduce((sum, c) => sum + Number(c.recordCount), 0);

  return (
    <ExpandableCard
      key={vault.id}
      id={vault.name || vault.id}
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
      <VaultActions vaultId={vault.id} dark={dark} />
      <ChunkList vaultId={vault.id} dark={dark} />
    </ExpandableCard>
  );
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
                .map((ch) => `${ch.chunkId}: ${ch.issues.join(", ")}`)
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
  const vaultCfg = config?.vaults?.find((v) => v.id === vaultId);
  const tierPositions = new Map<string, number>();
  if (vaultCfg) {
    for (const [i, tid] of vaultCfg.tierIds.entries()) {
      tierPositions.set(tid, i + 1);
    }
  }

  if (isLoading) {
    return (
      <div
        className={`px-4 py-3 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Loading chunks...
      </div>
    );
  }

  // Count replicas before dedup: how many nodes returned each chunk ID.
  const replicaCount = new Map<string, number>();
  for (const ch of chunks ?? []) {
    replicaCount.set(ch.id, (replicaCount.get(ch.id) ?? 0) + 1);
  }

  // Deduplicate replicas: when RF > 1, the same chunk ID appears from
  // multiple nodes. Keep the primary's version — it's compressed and
  // indexed. Secondaries may briefly have uncompressed forwarded copies
  // before ImportToTier replaces them with the canonical version.
  const bestChunk = new Map<string, ChunkMeta>();
  for (const ch of chunks ?? []) {
    const existing = bestChunk.get(ch.id);
    if (!existing || (!existing.compressed && ch.compressed) || (!existing.sealed && ch.sealed)) {
      bestChunk.set(ch.id, ch);
    }
  }
  const dedupedChunks = [...bestChunk.values()];

  // Group chunks by tier, then sort within each tier by time (newest first).
  const tierGroups = new Map<string, { tierType: string; chunks: ChunkMeta[] }>();
  for (const chunk of dedupedChunks) {
    const key = chunk.tierId || "unknown";
    const existing = tierGroups.get(key);
    if (existing) {
      existing.chunks.push(chunk);
    } else {
      tierGroups.set(key, { tierType: chunk.tierType || "unknown", chunks: [chunk] });
    }
  }

  // Node name resolution — used by both local and remote tier headers.
  const nodeNameMap = new Map((config?.nodeConfigs ?? []).map((n) => [n.id, n.name || n.id]));

  // Identify remote tiers (in vault config but no local chunks).
  const remoteTierInfo = (() => {
    if (!vaultCfg || !config?.tiers) return [];
    const localTierIds = new Set(tierGroups.keys());
    const tierTypeMap: Record<number, string> = { 1: "memory", 2: "file", 3: "cloud", 4: "jsonl" };
    return vaultCfg.tierIds
      .filter((tid) => !localTierIds.has(tid))
      .map((tid) => {
        const tc = config.tiers.find((t) => t.id === tid);
        if (!tc) return null;
        return {
          id: tid,
          pos: tierPositions.get(tid) ?? 0,
          type: tierTypeMap[tc.type] ?? "unknown",
          nodeName: tc.nodeId ? (nodeNameMap.get(tc.nodeId) ?? tc.nodeId) : "",
          rf: tc.replicationFactor || 1,
          secondaryNodeIds: [...tc.secondaryNodeIds],
          storageClass: tc.storageClass,
        };
      })
      .filter((t): t is NonNullable<typeof t> => t !== null);
  })();

  const sortChunks = (arr: ChunkMeta[]) =>
    arr.toSorted((a, b) => {
      const aTs = a.ingestStart ?? a.writeStart;
      const bTs = b.ingestStart ?? b.writeStart;
      const aTime = aTs ? instantToMs(protoToInstant(aTs)) : 0;
      const bTime = bTs ? instantToMs(protoToInstant(bTs)) : 0;
      return bTime - aTime;
    });

  return (
    <div>
      {/* Build a unified tier list: local tiers with chunks + remote tiers without */}
      {(vaultCfg?.tierIds ?? []).map((tierId) => {
        const pos = tierPositions.get(tierId) ?? 0;
        const group = tierGroups.get(tierId);
        const remote = remoteTierInfo.find((rt) => rt.id === tierId);

        // Remote tier with no local chunks — show as a placeholder.
        if (!group && remote) {
          return (
            <div
              key={tierId}
              className={`flex items-center gap-2 px-4 py-1.5 text-[0.75em] font-medium uppercase tracking-[0.12em] border-b ${c(
                "text-text-ghost border-ink-border-subtle bg-ink-base/30",
                "text-light-text-ghost border-light-border-subtle bg-light-base/30",
              )}`}
            >
              <Badge variant="muted" dark={dark}>{`Tier ${String(pos)}: ${remote.type}`}</Badge>
              <span>{remote.nodeName ? `on ${remote.nodeName}` : "unplaced"}</span>
              {remote.rf > 1 && <Badge variant="info" dark={dark}>{`RF=${String(remote.rf)}`}</Badge>}
              {remote.secondaryNodeIds.length > 0 && (
                <span>
                  {"\u2192 "}
                  {remote.secondaryNodeIds.map((id, si) => {
                    const name = nodeNameMap.get(id) ?? id;
                    let fallbackClass = 0;
                    if (remote.storageClass > 0) {
                      const nsc = (config?.nodeStorageConfigs ?? []).find((n) => n.nodeId === id);
                      const hasExact = nsc?.areas.some((a) => a.storageClass === remote.storageClass);
                      if (!hasExact && nsc && nsc.areas.length > 0) {
                        fallbackClass = nsc.areas[0]!.storageClass;
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
            </div>
          );
        }

        if (!group) return null;

        const label = `Tier ${String(pos)}: ${group.tierType}`;
        const tierCfg = config?.tiers?.find((t) => t.id === tierId);
        const rf = tierCfg?.replicationFactor || 1;
        const secondaries = tierCfg?.secondaryNodeIds ?? [];
        const nodeName = tierCfg?.nodeId ? (nodeNameMap.get(tierCfg.nodeId) ?? tierCfg.nodeId) : "";
        return (
        <div key={tierId}>
          <div
            className={`flex items-center gap-2 px-4 py-1.5 text-[0.75em] font-medium uppercase tracking-[0.12em] border-b ${c(
              "text-text-ghost border-ink-border-subtle bg-ink-base/30",
              "text-light-text-ghost border-light-border-subtle bg-light-base/30",
            )}`}
          >
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
                  const name = nodeNameMap.get(id) ?? id;
                  const requiredClass = tierCfg?.storageClass ?? 0;
                  let fallbackClass = 0;
                  if (requiredClass > 0) {
                    const nsc = (config?.nodeStorageConfigs ?? []).find((n) => n.nodeId === id);
                    const hasExact = nsc?.areas.some((a) => a.storageClass === requiredClass);
                    if (!hasExact && nsc && nsc.areas.length > 0) {
                      fallbackClass = nsc.areas[0]!.storageClass;
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
          </div>
          <table className="w-full border-collapse">
            <thead>
              <tr
                className={`text-left text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c(
                  "text-text-ghost border-ink-border-subtle",
                  "text-light-text-ghost border-light-border-subtle",
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
              {sortChunks(group.chunks).map((chunk) => {
                const startTs = chunk.ingestStart ?? chunk.writeStart;
                const endTs = chunk.ingestEnd ?? chunk.writeEnd;
                const start = startTs ? instantToDate(protoToInstant(startTs)) : undefined;
                const end = endTs ? instantToDate(protoToInstant(endTs)) : undefined;
                const isExpanded = expandedChunk === chunk.id;

                const replicas = replicaCount.get(chunk.id) ?? 1;
                return (
                  <ChunkRow
                    key={chunk.id}
                    chunk={chunk}
                    vaultId={vaultId}
                    start={start}
                    end={end}
                    isExpanded={isExpanded}
                    onToggle={() => setExpandedChunk(isExpanded ? null : chunk.id)}
                    dark={dark}
                    c={c}
                    replicas={replicas}
                    rf={rf}
                    replicaNodes={tierCfg ? [tierCfg.nodeId, ...tierCfg.secondaryNodeIds].filter(Boolean).map((id) => nodeNameMap.get(id) ?? id) : []}
                  />
                );
              })}
            </tbody>
          </table>
        </div>
        ); })}
    </div>
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
  replicaNodes,
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
  replicaNodes: string[];
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
        <td className="px-4 py-2">
          <span className="flex items-center gap-1.5">
            <span
              className={`text-[0.6em] transition-transform inline-block ${isExpanded ? "rotate-90" : ""} ${c("text-text-muted", "text-light-text-muted")}`}
            >
              {"\u25B6"}
            </span>
            <span
              className={`font-mono truncate block max-w-36 ${c("text-text-muted", "text-light-text-muted")}`}
              title={chunk.id}
            >
              {chunk.id}
            </span>
          </span>
        </td>
        <td className="px-2 py-2">
          <span
            className={`text-[0.95em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {start ? formatDateTimeShort(start) : "\u2014"}
            <span className={`mx-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}>
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
              <Badge variant="warn" dark={dark}>archived</Badge>
            )}
            {rf > 1 && (
              <Badge
                variant={replicas >= rf ? "info" : replicaNodes.length < rf ? "error" : "warn"}
                dark={dark}
                title={replicas >= rf
                  ? `${String(replicas)} replicas (fully replicated)`
                  : replicaNodes.length < rf
                    ? `${String(replicas)}/${String(rf)} replicas — insufficient nodes with required storage`
                    : `${String(replicas)}/${String(rf)} replicas — replication in progress`}
              >
                {String(replicas)}
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
            <ChunkDetail vaultId={vaultId} chunk={chunk} dark={dark} replicas={replicas} rf={rf} replicaNodes={replicaNodes} />
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
  replicas,
  rf,
  replicaNodes,
}: Readonly<{
  vaultId: string;
  chunk: ChunkMeta;
  dark: boolean;
  replicas: number;
  rf: number;
  replicaNodes: string[];
}>) {
  const c = useThemeClass(dark);
  // Skip index fetch for cloud-backed chunks — they don't have local indexes.
  const { data, isLoading } = useIndexes(vaultId, chunk.cloudBacked ? "" : chunk.id);

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
          className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Chunk ID
        </div>
        <div
          className={`font-mono text-[0.85em] select-all ${c("text-text-muted", "text-light-text-muted")}`}
        >
          {chunk.id}
        </div>
      </div>

      {/* Replication info — only shown when RF > 1 */}
      {rf > 1 && (
        <div className="mb-3">
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Replicas
          </div>
          <div className={`flex flex-col gap-1`}>
            <div className={`flex items-center gap-3 text-[0.85em]`}>
              <span className={`font-mono ${replicas >= rf
                ? c("text-text-muted", "text-light-text-muted")
                : replicaNodes.length < rf ? "text-severity-error" : "text-severity-warn"
              }`}>
                {`${String(replicas)}/${String(rf)}`}
              </span>
              {replicaNodes.length > 0 && (
                <span className={c("text-text-ghost", "text-light-text-ghost")}>
                  {replicaNodes.join(", ")}
                </span>
              )}
            </div>
            {replicaNodes.length < rf && (
              <span className="text-[0.8em] text-severity-error">
                Not enough nodes with the required storage class to satisfy RF={String(rf)}
              </span>
            )}
          </div>
        </div>
      )}

      {/* Compression / storage info */}
      {showCompression && (
        <div className="mb-3">
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
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
              className={`font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              {reductionPct}% reduction
            </span>
          </div>
        </div>
      )}

      {/* Active chunks: no indexes yet */}
      {!chunk.sealed ? (
        <div
          className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Indexes are built when the chunk is sealed.
        </div>
      ) : chunk.cloudBacked ? (
        <>
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Cloud Storage
          </div>
          <div className="flex flex-col gap-1.5">
            <div className={`flex items-center gap-3 text-[0.85em]`}>
              <span className={`font-mono w-20 ${c("text-text-bright", "text-light-text-bright")}`}>blob</span>
              <span className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
                {formatBytes(Number(chunk.diskBytes))}
              </span>
              <span className={c("text-text-ghost", "text-light-text-ghost")}>
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
          </div>
        </>
      ) : (
        <>
          {/* Local indexes */}
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Indexes
          </div>

          {isLoading && (
            <div
              className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              Loading indexes...
            </div>
          )}
          {!isLoading && (!data?.indexes || data.indexes.length === 0) && (
            <div
              className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
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
                        className={`font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
                      >
                        {formatBytes(Number(idx.sizeBytes))}
                      </span>
                    </>
                  ) : (
                    <Badge variant="ghost" dark={dark}>missing</Badge>
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

