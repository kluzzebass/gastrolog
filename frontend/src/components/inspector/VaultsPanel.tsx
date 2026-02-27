import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { clickableProps } from "../../utils";
import {
  useVaults,
  useChunks,
  useIndexes,
  useValidateVault,
} from "../../api/hooks";
import { useToast } from "../Toast";
import type { ChunkMeta } from "../../api/gen/gastrolog/v1/vault_pb";
import { formatBytes } from "../../utils/units";
import { ExpandableCard } from "../settings/ExpandableCard";
import { NodeBadge } from "../settings/NodeBadge";
import { ChunkTimeline } from "./ChunkTimeline";
import { HelpButton } from "../HelpButton";

export function VaultsPanel({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data: vaults, isLoading } = useVaults();
  const [expanded, setExpanded] = useState<string | null>(null);

  if (isLoading) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Loading...
      </div>
    );
  }

  if (!vaults || vaults.length === 0) {
    return (
      <div
        className={`flex items-center justify-center h-full text-[0.9em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        No vaults configured.
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center gap-2 mb-2">
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Vaults
        </h2>
        <HelpButton topicId="inspector-vaults" />
      </div>
      {[...vaults].sort((a, b) => (a.name || a.id).localeCompare(b.name || b.id)).map((vault) => (
        <ExpandableCard
          key={vault.id}
          id={vault.name || vault.id}
          typeBadge={vault.type}
          typeBadgeAccent
          dark={dark}
          expanded={expanded === vault.id}
          onToggle={() => setExpanded(expanded === vault.id ? null : vault.id)}
          headerRight={
            <span
              className={`text-[0.8em] flex items-center gap-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              <NodeBadge nodeId={vault.nodeId} dark={dark} />
              {!vault.enabled && (
                <span className="px-1.5 py-0.5 text-[0.75em] font-medium uppercase tracking-wider rounded bg-severity-warn/15 text-severity-warn">
                  Disabled
                </span>
              )}
              {Number(vault.chunkCount).toLocaleString()} chunks
              {" \u00B7 "}
              {vault.recordCount.toLocaleString()} records
            </span>
          }
        >
          <VaultActions vaultId={vault.id} dark={dark} />
          <ChunkList vaultId={vault.id} dark={dark} />
        </ExpandableCard>
      ))}
    </div>
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
                  <span className="px-1.5 py-0.5 text-[0.8em] rounded bg-copper/15 text-copper">
                    sealed
                  </span>
                ) : (
                  <span className="px-1.5 py-0.5 text-[0.8em] rounded bg-severity-info/15 text-severity-info">
                    active
                  </span>
                )}
                {chunk.compressed && (
                  <span className="px-1.5 py-0.5 text-[0.8em] rounded bg-severity-info/15 text-severity-info">
                    zstd
                  </span>
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
                  ? `${formatBytes(Number(chunk.bytes))} → ${formatBytes(Number(chunk.diskBytes))} on disk`
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
            <span className="px-1.5 py-0.5 text-[0.8em] rounded bg-severity-info/15 text-severity-info">
              zstd
            </span>
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
                  <span className="px-1.5 py-0.5 text-[0.8em] rounded bg-severity-info/15 text-severity-info">
                    ok
                  </span>
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
                <span
                  className={`px-1.5 py-0.5 text-[0.8em] rounded ${c("bg-ink-hover text-text-ghost", "bg-light-hover text-light-text-ghost")}`}
                >
                  missing
                </span>
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
