import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { clickableProps } from "../../utils";
import { useChunks, useIndexes, useValidateVault } from "../../api/hooks";
import { useToast } from "../Toast";
import type { VaultInfo, ChunkMeta } from "../../api/gen/gastrolog/v1/vault_pb";
import { formatBytes } from "../../utils/units";
import { Badge } from "../Badge";
import { ExpandableCard } from "../settings/ExpandableCard";
import { NodeBadge } from "../settings/NodeBadge";
import { ChunkTimeline } from "./ChunkTimeline";

interface VaultCardProps {
  vault: VaultInfo;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  showNodeBadge?: boolean;
}

export function VaultCard({
  vault,
  dark,
  expanded,
  onToggle,
  showNodeBadge = true,
}: Readonly<VaultCardProps>) {
  return (
    <ExpandableCard
      key={vault.id}
      id={vault.name || vault.id}
      typeBadge={vault.type}
      typeBadgeAccent
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      headerRight={
        <span className="flex items-center gap-1.5">
          {showNodeBadge && <NodeBadge nodeId={vault.nodeId} dark={dark} />}
          {!vault.enabled && (
            <Badge variant="warn" dark={dark}>disabled</Badge>
          )}
          <Badge variant="muted" dark={dark}>
            {Number(vault.chunkCount).toLocaleString()} chunks
          </Badge>
          <Badge variant="muted" dark={dark}>
            {vault.recordCount.toLocaleString()} records
          </Badge>
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
          } catch (err: any) {
            addToast(err.message ?? "Validation failed", "error");
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
  const [expandedChunk, setExpandedChunk] = useState<string | null>(null);

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

  // Sort by start time, newest first.
  const sorted = [...chunks].sort((a, b) => {
    const aTime = a.startTs?.toDate().getTime() ?? 0;
    const bTime = b.startTs?.toDate().getTime() ?? 0;
    return bTime - aTime;
  });

  return (
    <div>
      {/* Timeline visualization */}
      {chunks.length > 0 && (
        <ChunkTimeline
          chunks={chunks}
          dark={dark}
          selectedChunkId={expandedChunk}
          onChunkClick={(id) =>
            setExpandedChunk(expandedChunk === id ? null : id)
          }
        />
      )}

      {/* Column headers */}
      <div
        className={`grid grid-cols-[1fr_2fr_6rem_5rem_5rem] gap-3 px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c(
          "text-text-ghost border-ink-border-subtle",
          "text-light-text-ghost border-light-border-subtle",
        )}`}
      >
        <span>Chunk ID</span>
        <span>Time Range</span>
        <span>Status</span>
        <span className="text-right">Records</span>
        <span className="text-right">Size</span>
      </div>

      {/* Rows */}
      {sorted.map((chunk) => {
        const start = chunk.startTs?.toDate();
        const end = chunk.endTs?.toDate();
        const isExpanded = expandedChunk === chunk.id;

        return (
          <div
            key={chunk.id}
            className={`border-b last:border-b-0 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
          >
            <div
              className={`grid grid-cols-[1fr_2fr_6rem_5rem_5rem] gap-3 px-4 py-2 text-[0.85em] cursor-pointer transition-colors ${c(
                "hover:bg-ink-hover",
                "hover:bg-light-hover",
              )} ${isExpanded ? c("bg-ink-hover", "bg-light-hover") : ""}`}
              onClick={() => setExpandedChunk(isExpanded ? null : chunk.id)}
              {...clickableProps(() => setExpandedChunk(isExpanded ? null : chunk.id))}
              aria-expanded={isExpanded}
            >
              <span
                className={`font-mono truncate ${c("text-text-muted", "text-light-text-muted")}`}
                title={chunk.id}
              >
                {chunk.id}
              </span>
              <span
                className={`text-[0.95em] ${c("text-text-muted", "text-light-text-muted")}`}
              >
                {start ? formatTime(start) : "\u2014"}
                <span
                  className={`mx-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  {"\u2192"}
                </span>
                {end ? formatTime(end) : "\u2014"}
              </span>
              <span className="flex items-center gap-1 flex-wrap">
                {chunk.sealed ? (
                  <Badge variant="copper" dark={dark}>sealed</Badge>
                ) : (
                  <Badge variant="info" dark={dark}>active</Badge>
                )}
                {chunk.compressed && (
                  <Badge variant="info" dark={dark}>zstd</Badge>
                )}
              </span>
              <span
                className={`text-right font-mono ${c("text-text-muted", "text-light-text-muted")}`}
              >
                {Number(chunk.recordCount).toLocaleString()}
              </span>
              <span
                className={`text-right font-mono ${c("text-text-muted", "text-light-text-muted")}`}
                title={chunk.compressed && Number(chunk.diskBytes) > 0
                  ? `${formatBytes(Number(chunk.bytes))} \u2192 ${formatBytes(Number(chunk.diskBytes))} on disk`
                  : undefined}
              >
                {chunk.compressed && Number(chunk.diskBytes) > 0
                  ? formatBytes(Number(chunk.diskBytes))
                  : formatBytes(Number(chunk.bytes))}
              </span>
            </div>

            {/* Expanded: index info */}
            {isExpanded && (
              <ChunkDetail vaultId={vaultId} chunk={chunk} dark={dark} />
            )}
          </div>
        );
      })}
    </div>
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
  const { data, isLoading } = useIndexes(vaultId, chunk.id);

  const logicalBytes = Number(chunk.bytes);
  const diskBytes = Number(chunk.diskBytes);
  const showCompression = chunk.compressed && diskBytes > 0 && logicalBytes > 0;
  const reductionPct = showCompression
    ? Math.round((1 - diskBytes / logicalBytes) * 100)
    : 0;

  return (
    <div className={`px-4 py-3 ${c("bg-ink-raised", "bg-light-bg")}`}>
      {/* Compression info */}
      {showCompression && (
        <div className="mb-3">
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Compression
          </div>
          <div className={`flex items-center gap-3 text-[0.85em]`}>
            <Badge variant="info" dark={dark}>zstd</Badge>
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

      {/* Indexes */}
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
    </div>
  );
}

function formatTime(date: Date): string {
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}
