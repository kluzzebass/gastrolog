import { Record as ProtoRecord } from "../api/client";
import { ExportButton } from "./ExportButton";
import { useThemeClass } from "../hooks/useThemeClass";

interface ResultsToolbarProps {
  dark: boolean;
  isFollowMode: boolean;
  isReversed: boolean;
  followReversed: boolean;
  toggleReverse: () => void;
  selectedRecord: ProtoRecord | null;
  rangeStart: Date | null;
  rangeEnd: Date | null;
  records: ProtoRecord[];
  followRecords: ProtoRecord[];
  hasMore: boolean;
  reconnecting: boolean;
  reconnectAttempt: number;
  displayRecords: ProtoRecord[];
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
  onZoomOut,
}: ResultsToolbarProps) {
  const c = useThemeClass(dark);

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
            className={`w-6 h-6 flex items-center justify-center rounded transition-colors ${c(
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
            (isFollowMode ? followReversed : isReversed)
              ? "Sort oldest first"
              : "Sort newest first"
          }
          title={
            (isFollowMode ? followReversed : isReversed)
              ? "Newest first (click for oldest first)"
              : "Oldest first (click for newest first)"
          }
          className={`w-6 h-6 flex items-center justify-center rounded transition-colors ${c(
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
            {(isFollowMode ? followReversed : isReversed) ? (
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
      </div>
      <div className="flex items-center gap-3">
        <ExportButton records={displayRecords} dark={dark} />
        {(isFollowMode ? followRecords : records).length > 0 && (
          <span
            className={`font-mono text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            {new Date().toLocaleTimeString("en-US", { hour12: false })}
          </span>
        )}
      </div>
    </div>
  );
}
