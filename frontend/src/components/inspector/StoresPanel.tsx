import { useState } from "react";
import { useStores, useChunks } from "../../api/hooks";

export function StoresPanel({ dark }: { dark: boolean }) {
  const c = (d: string, l: string) => (dark ? d : l);
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
          recordCount={store.recordCount}
          dark={dark}
          expanded={expanded === store.id}
          onToggle={() =>
            setExpanded(expanded === store.id ? null : store.id)
          }
        />
      ))}
    </div>
  );
}

function StoreCard({
  storeId,
  recordCount,
  dark,
  expanded,
  onToggle,
}: {
  storeId: string;
  recordCount: bigint;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
}) {
  const c = (d: string, l: string) => (dark ? d : l);

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
          <span
            className={`text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
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
  const c = (d: string, l: string) => (dark ? d : l);
  const { data: chunks, isLoading } = useChunks(storeId);

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
      {/* Column headers */}
      <div
        className={`grid grid-cols-[1fr_2fr_auto_auto] gap-3 px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c(
          "text-text-ghost border-ink-border-subtle",
          "text-light-text-ghost border-light-border-subtle",
        )}`}
      >
        <span>Chunk ID</span>
        <span>Time Range</span>
        <span>Status</span>
        <span className="text-right">Records</span>
      </div>

      {/* Rows */}
      {sorted.map((chunk) => {
        const start = chunk.startTs?.toDate();
        const end = chunk.endTs?.toDate();

        return (
          <div
            key={chunk.id}
            className={`grid grid-cols-[1fr_2fr_auto_auto] gap-3 px-4 py-2 text-[0.85em] border-b last:border-b-0 transition-colors ${c(
              "border-ink-border-subtle hover:bg-ink-hover",
              "border-light-border-subtle hover:bg-light-hover",
            )}`}
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
              {start ? formatTime(start) : "—"}
              <span
                className={`mx-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
              >
                →
              </span>
              {end ? formatTime(end) : "—"}
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
          </div>
        );
      })}
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
