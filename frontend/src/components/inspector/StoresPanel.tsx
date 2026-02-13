import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { clickableProps } from "../../utils";
import { useStores, useChunks, useIndexes } from "../../api/hooks";
import { formatBytes } from "../../utils/units";
import { ChunkTimeline } from "./ChunkTimeline";

export function StoresPanel({ dark }: { dark: boolean }) {
  const c = useThemeClass(dark);
  const { data: stores, isLoading } = useStores();
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

  if (!stores || stores.length === 0) {
    return (
      <div
        className={`flex items-center justify-center h-full text-[0.9em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        No stores configured.
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      {stores.map((store) => (
        <StoreCard
          key={store.id}
          storeId={store.id}
          storeType={store.type}
          recordCount={store.recordCount}
          chunkCount={store.chunkCount}
          dark={dark}
          expanded={expanded === store.id}
          onToggle={() => setExpanded(expanded === store.id ? null : store.id)}
        />
      ))}
    </div>
  );
}

function StoreCard({
  storeId,
  storeType,
  recordCount,
  chunkCount,
  dark,
  expanded,
  onToggle,
}: {
  storeId: string;
  storeType: string;
  recordCount: bigint;
  chunkCount: bigint;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
}) {
  const c = useThemeClass(dark);

  return (
    <div
      className={`border rounded-lg overflow-hidden transition-colors ${c(
        "border-ink-border-subtle bg-ink-surface",
        "border-light-border-subtle bg-light-surface",
      )}`}
    >
      {/* Header */}
      <div
        className={`flex items-center justify-between px-4 py-3 cursor-pointer select-none transition-colors ${c(
          "hover:bg-ink-hover",
          "hover:bg-light-hover",
        )}`}
        onClick={onToggle}
        {...clickableProps(onToggle)}
        aria-expanded={expanded}
      >
        <div className="flex items-center gap-2.5">
          <span
            className={`text-[0.7em] transition-transform ${expanded ? "rotate-90" : ""}`}
          >
            {"\u25B6"}
          </span>
          <span
            className={`font-mono text-[0.9em] font-medium ${c("text-text-bright", "text-light-text-bright")}`}
          >
            {storeId}
          </span>
          {storeType && (
            <span className="px-1.5 py-0.5 text-[0.75em] rounded bg-copper/15 text-copper">
              {storeType}
            </span>
          )}
          <span
            className={`text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            {Number(chunkCount).toLocaleString()} chunks
            {" \u00B7 "}
            {recordCount.toLocaleString()} records
          </span>
        </div>
      </div>

      {/* Chunk list */}
      {expanded && (
        <div
          className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
        >
          <ChunkList storeId={storeId} dark={dark} />
        </div>
      )}
    </div>
  );
}

function ChunkList({ storeId, dark }: { storeId: string; dark: boolean }) {
  const c = useThemeClass(dark);
  const { data: chunks, isLoading } = useChunks(storeId);
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
        className={`grid grid-cols-[1fr_2fr_4rem_5rem_5rem] gap-3 px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c(
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
              className={`grid grid-cols-[1fr_2fr_4rem_5rem_5rem] gap-3 px-4 py-2 text-[0.85em] cursor-pointer transition-colors ${c(
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
              <span>
                {chunk.sealed ? (
                  <span className="px-1.5 py-0.5 text-[0.8em] rounded bg-copper/15 text-copper">
                    sealed
                  </span>
                ) : (
                  <span className="px-1.5 py-0.5 text-[0.8em] rounded bg-severity-info/15 text-severity-info">
                    active
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
              >
                {formatBytes(Number(chunk.bytes))}
              </span>
            </div>

            {/* Expanded: index info */}
            {isExpanded && (
              <ChunkDetail storeId={storeId} chunkId={chunk.id} dark={dark} />
            )}
          </div>
        );
      })}
    </div>
  );
}

function ChunkDetail({
  storeId,
  chunkId,
  dark,
}: {
  storeId: string;
  chunkId: string;
  dark: boolean;
}) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useIndexes(storeId, chunkId);

  return (
    <div className={`px-4 py-3 ${c("bg-ink-raised", "bg-light-bg")}`}>
      <div
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Indexes
      </div>

      {isLoading ? (
        <div
          className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Loading indexes...
        </div>
      ) : !data?.indexes || data.indexes.length === 0 ? (
        <div
          className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          No indexes.
        </div>
      ) : (
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

