import { Record as ProtoRecord } from "../api/client";
import { AutoRefreshControls } from "./AutoRefreshControls";
import { ExportButton } from "./ExportButton";
import { useThemeClass } from "../hooks/useThemeClass";

export const FOLLOW_BUFFER_SIZES = [100, 500, 1000, 2500, 5000, 10_000, 25_000] as const;

interface ResultsToolbarProps {
  dark: boolean;
  isFollowMode: boolean;
  isReversed: boolean;
  followReversed: boolean;
  toggleReverse: () => void;
  selectedRecord: ProtoRecord | null;
  records: ProtoRecord[];
  followRecords: ProtoRecord[];
  hasMore: boolean;
  reconnecting: boolean;
  reconnectAttempt: number;
  displayRecords: ProtoRecord[];
  followBufferSize: number;
  onFollowBufferSizeChange: (size: number) => void;
  pollInterval: number | null;
  onPollIntervalChange: (ms: number | null) => void;
  onZoomOut: () => void;
}

export function ResultsToolbar({
  dark,
  isFollowMode,
  isReversed,
  followReversed,
  toggleReverse,
  selectedRecord,
  records,
  followRecords,
  hasMore,
  reconnecting,
  reconnectAttempt,
  displayRecords,
  followBufferSize,
  onFollowBufferSizeChange,
  pollInterval,
  onPollIntervalChange,
  onZoomOut,
}: Readonly<ResultsToolbarProps>) {
  const c = useThemeClass(dark);
  const effectiveReversed = isFollowMode ? followReversed : isReversed;

  return (
    <div
      className={`flex justify-between items-center px-5 py-2.5 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
    >
      <div className="flex items-center gap-3">
        {!isFollowMode && selectedRecord && (
          <button
            onClick={onZoomOut}
            aria-label="Zoom out"
            title="Zoom out — double time span around selected record"
            className={`w-8 h-8 flex items-center justify-center rounded transition-colors ${c(
              "text-text-muted hover:text-copper hover:bg-ink-hover",
              "text-light-text-muted hover:text-copper hover:bg-light-hover",
            )}`}
          >
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="w-4 h-4"
            >
              <circle cx="11" cy="11" r="8" />
              <line x1="21" y1="21" x2="16.65" y2="16.65" />
              <line x1="8" y1="11" x2="14" y2="11" />
            </svg>
          </button>
        )}
        <button
          onClick={toggleReverse}
          aria-label={
            effectiveReversed
              ? "Sort oldest first"
              : "Sort newest first"
          }
          title={
            effectiveReversed
              ? "Newest first (click for oldest first)"
              : "Oldest first (click for newest first)"
          }
          className={`w-8 h-8 flex items-center justify-center rounded transition-colors ${c(
            "text-text-muted hover:text-copper hover:bg-ink-hover",
            "text-light-text-muted hover:text-copper hover:bg-light-hover",
          )}`}
        >
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="w-4 h-4"
          >
            {effectiveReversed ? (
              <>
                <path d="M12 5v14" />
                <path d="M6 13l6 6 6-6" />
              </>
            ) : (
              <>
                <path d="M12 5v14" />
                <path d="M6 11l6-6 6 6" />
              </>
            )}
          </svg>
        </button>
        <h3
          className={`font-display text-[1.15em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          {isFollowMode ? "Following" : "Results"}
        </h3>
        {isFollowMode &&
          (reconnecting ? (
            <span
              className="relative flex h-2 w-2"
              title={`Reconnecting (attempt ${reconnectAttempt})...`}
            >
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-severity-warn opacity-75" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-severity-warn" />
            </span>
          ) : (
            <span className="relative flex h-2 w-2" title="Connected">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-severity-info opacity-75" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-severity-info" />
            </span>
          ))}
        <span
          role="status"
          aria-live="polite"
          aria-label={`${isFollowMode ? followRecords.length : records.length}${!isFollowMode && hasMore ? "+" : ""} results`}
          className={`font-mono text-[0.8em] px-2 py-0.5 rounded ${c("bg-ink-surface text-text-muted", "bg-light-hover text-light-text-muted")}`}
        >
          {isFollowMode ? followRecords.length : records.length}
          {!isFollowMode && hasMore ? "+" : ""}
        </span>
        {isFollowMode && (
          <span className={`flex items-center gap-1 font-mono text-[0.75em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
            <span>/</span>
            <select
              value={followBufferSize}
              onChange={(e) => onFollowBufferSizeChange(Number(e.target.value))}
              className={`bg-transparent border rounded px-1 py-0.5 cursor-pointer ${c(
                "border-ink-border-subtle text-text-muted hover:text-text-bright",
                "border-light-border-subtle text-light-text-muted hover:text-light-text-bright",
              )}`}
              title="Follow buffer size — max records kept in memory"
            >
              {FOLLOW_BUFFER_SIZES.map((size) => (
                <option key={size} value={size}>
                  {size.toLocaleString()}
                </option>
              ))}
            </select>
          </span>
        )}
      </div>
      <div className="flex items-center gap-3">
        {!isFollowMode && (
          <AutoRefreshControls
            pollInterval={pollInterval}
            onPollIntervalChange={onPollIntervalChange}
            dark={dark}
          />
        )}
        <ExportButton records={displayRecords} dark={dark} />
      </div>
    </div>
  );
}
