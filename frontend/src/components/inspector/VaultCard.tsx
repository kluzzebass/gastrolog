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
            {Number(vault.chunkCount).toLocaleString()} chunks
          </Badge>
          <Badge variant="muted" dark={dark}>
            {vault.recordCount.toLocaleString()} records
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

  if (!chunks || chunks.length === 0) {
    return (
      <div
        className={`px-4 py-3 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        No chunks.
      </div>
    );
  }

  // Group chunks by tier, then sort within each tier by time (newest first).
  const tierGroups = new Map<string, { tierType: string; chunks: ChunkMeta[] }>();
  for (const chunk of chunks) {
    const key = chunk.tierId || "unknown";
    const existing = tierGroups.get(key);
    if (existing) {
      existing.chunks.push(chunk);
    } else {
      tierGroups.set(key, { tierType: chunk.tierType || "unknown", chunks: [chunk] });
    }
  }

  // Identify remote tiers (in vault config but no local chunks).
  const remoteTierInfo = (() => {
    if (!vaultCfg || !config?.tiers) return [];
    const localTierIds = new Set(tierGroups.keys());
    const nodeNameMap = new Map((config.nodeConfigs ?? []).map((n) => [n.id, n.name || n.id]));
    const tierTypeMap: Record<number, string> = { 1: "memory", 2: "local", 3: "cloud" };
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
            </div>
          );
        }

        if (!group) return null;

        const label = `Tier ${String(pos)}: ${group.tierType}`;
        return (
        <div key={tierId}>
          <div
            className={`flex items-center gap-2 px-4 py-1.5 text-[0.75em] font-medium uppercase tracking-[0.12em] border-b ${c(
              "text-text-ghost border-ink-border-subtle bg-ink-base/30",
              "text-light-text-ghost border-light-border-subtle bg-light-base/30",
            )}`}
          >
            <Badge variant="copper" dark={dark}>{label}</Badge>
            <span>{`${String(group.chunks.length)} ${group.chunks.length === 1 ? "chunk" : "chunks"}`}</span>
            <span>{`${group.chunks.reduce((sum, ch) => sum + Number(ch.recordCount), 0).toLocaleString()} records`}</span>
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
}: Readonly<{
  chunk: ChunkMeta;
  vaultId: string;
  start: Date | undefined;
  end: Date | undefined;
  isExpanded: boolean;
  onToggle: () => void;
  dark: boolean;
  c: (darkCls: string, lightCls: string) => string;
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
            <ChunkDetail vaultId={vaultId} chunk={chunk} dark={dark} />
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
}: Readonly<{
  vaultId: string;
  chunk: ChunkMeta;
  dark: boolean;
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

