import type { RefObject } from "react";
import { Record as ProtoRecord } from "../api/client";
import { TableResult } from "../api/gen/gastrolog/v1/query_pb";
import { sameRecord } from "../utils";
import { EmptyState } from "./EmptyState";
import { LogEntry } from "./LogEntry";
import { PipelineResults } from "./PipelineResults";
import { ResultsToolbar } from "./ResultsToolbar";
import type { HighlightMode } from "../hooks/useThemeSync";

interface SearchResultsProps {
  dark: boolean;
  c: (dark: string, light: string) => string;
  isFollowMode: boolean;
  isReversed: boolean;
  followReversed: boolean;
  // Data
  records: ProtoRecord[];
  followRecords: ProtoRecord[];
  displayRecords: ProtoRecord[];
  selectedRecord: ProtoRecord | null;
  effectiveTableResult: TableResult | null;
  // State
  isSearching: boolean;
  hasMore: boolean;
  isPipelineResult: boolean;
  isRawQuery: boolean;
  queryIsPipeline: boolean;
  // Follow
  isScrolledDown: boolean;
  followNewCount: number;
  reconnecting: boolean;
  reconnectAttempt: number;
  followBufferSize: number;
  // Handlers
  onSelectRecord: (rec: ProtoRecord | null) => void;
  toggleReverse: () => void;
  onTokenToggle: (token: string) => void;
  onSpanClick: (value: string) => void;
  onZoomOut: () => void;
  onFollowBufferSizeChange: (size: number) => void;
  resetFollowNewCount: () => void;
  onPollIntervalChange: (ms: number | null) => void;
  // Display
  tokens: string[];
  highlightMode: HighlightMode;
  pollInterval: number | null;
  // Refs
  logScrollRef: RefObject<HTMLDivElement | null>;
  sentinelRef: RefObject<HTMLDivElement | null>;
  selectedRowRef: RefObject<HTMLElement | null>;
}

export function SearchResults({
  dark,
  c,
  isFollowMode,
  isReversed,
  followReversed,
  records,
  followRecords,
  displayRecords,
  selectedRecord,
  effectiveTableResult,
  isSearching,
  hasMore,
  isPipelineResult,
  isRawQuery,
  queryIsPipeline,
  isScrolledDown,
  followNewCount,
  reconnecting,
  reconnectAttempt,
  followBufferSize,
  onSelectRecord,
  toggleReverse,
  onTokenToggle,
  onSpanClick,
  onZoomOut,
  onFollowBufferSizeChange,
  resetFollowNewCount,
  onPollIntervalChange,
  tokens,
  highlightMode,
  pollInterval,
  logScrollRef,
  sentinelRef,
  selectedRowRef,
}: Readonly<SearchResultsProps>) {
  // Pipeline loading spinner
  if (isSearching && !effectiveTableResult && queryIsPipeline && records.length === 0) {
    return (
      <div className="flex-1 flex flex-col overflow-hidden">
        <div className="flex-1 flex items-center justify-center">
          <div className={`text-center font-mono text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
            <div className="inline-block w-5 h-5 border-2 border-current border-t-transparent rounded-full animate-spin mb-3" />
            <div>Running pipeline...</div>
          </div>
        </div>
      </div>
    );
  }

  // Pipeline / table results
  if (isPipelineResult) {
    return (
      <div className="flex-1 flex flex-col overflow-hidden">
        <PipelineResults
          tableResult={effectiveTableResult!}
          dark={dark}
          pollInterval={pollInterval}
          onPollIntervalChange={onPollIntervalChange}
          scrollRef={isRawQuery ? logScrollRef : undefined}
          footer={isRawQuery ? <div ref={sentinelRef} className="h-1" /> : undefined}
        />
      </div>
    );
  }

  // Filter results (log entries)
  const effectiveRecords = isFollowMode ? followRecords : records;
  const isEmpty = effectiveRecords.length === 0;
  const orderedFollowRecords = followReversed ? followRecords : followRecords.toReversed();
  const displayList = isFollowMode ? orderedFollowRecords : records;

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <ResultsToolbar
        dark={dark}
        isFollowMode={isFollowMode}
        isReversed={isReversed}
        followReversed={followReversed}
        toggleReverse={toggleReverse}
        selectedRecord={selectedRecord}
        records={records}
        followRecords={followRecords}
        hasMore={hasMore}
        reconnecting={reconnecting}
        reconnectAttempt={reconnectAttempt}
        displayRecords={displayRecords}
        followBufferSize={followBufferSize}
        onFollowBufferSizeChange={onFollowBufferSizeChange}
        pollInterval={pollInterval}
        onPollIntervalChange={onPollIntervalChange}
        onZoomOut={onZoomOut}
      />

      <div className="relative flex-1 overflow-hidden">
        {/* "N new logs" floating badge */}
        {isFollowMode && isScrolledDown && followNewCount > 0 && (
          <button
            onClick={() => {
              logScrollRef.current?.scrollTo({ top: 0, behavior: "smooth" });
              resetFollowNewCount();
            }}
            className={`absolute top-3 left-1/2 -translate-x-1/2 z-10 px-3 py-1.5 rounded-full font-mono text-[0.8em] shadow-lg backdrop-blur cursor-pointer transition-all hover:brightness-110 animate-[fadeSlideDown_200ms_ease-out] ${c(
              "bg-copper/90 text-ink",
              "bg-copper/90 text-white",
            )}`}
          >
            {followNewCount} new log{followNewCount !== 1 ? "s" : ""}
          </button>
        )}
        <div
          ref={logScrollRef}
          className="h-full overflow-y-auto app-scroll"
        >
          {isEmpty && !isSearching && !isFollowMode && <EmptyState dark={dark} />}
          {isEmpty && isFollowMode && (
            <div
              className={`py-8 text-center text-[0.85em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              Waiting for new records...
            </div>
          )}
          {!isEmpty && (
            <div>
              {displayList.map((record, i) => {
                const selected = sameRecord(selectedRecord, record);
                return (
                  <LogEntry
                    key={record.ref ? `${record.ref.vaultId}:${record.ref.chunkId}:${record.ref.pos}` : `follow-${i}`}
                    ref={selected ? selectedRowRef : undefined}
                    record={record}
                    tokens={tokens}
                    isSelected={selected}
                    onSelect={() => onSelectRecord(selected ? null : record)}
                    onFilterToggle={onTokenToggle}
                    onSpanClick={onSpanClick}
                    dark={dark}
                    highlightMode={highlightMode}
                  />
                );
              })}
              {/* Infinite scroll sentinel (search only) */}
              {!isFollowMode && <div ref={sentinelRef} className="h-1" />}
              {(isSearching && records.length > 0) && (
                <div
                  className={`py-3 text-center text-[0.85em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Loading more...
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
