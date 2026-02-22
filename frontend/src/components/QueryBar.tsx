import { QueryInput } from "./QueryInput";
import { QueryHistory } from "./QueryHistory";
import { SavedQueries } from "./SavedQueries";
import { QueryAutocomplete } from "./QueryAutocomplete";
import type { HistoryEntry } from "../hooks/useQueryHistory";
import { useHelp } from "../hooks/useHelp";
import type { SavedQuery } from "../api/gen/gastrolog/v1/config_pb";
import { useThemeClass } from "../hooks/useThemeClass";
import type { SyntaxSets } from "../queryTokenizer";

interface QueryBarProps {
  dark: boolean;
  draft: string;
  setDraft: (v: string) => void;
  setCursorPos: (pos: number) => void;
  queryInputRef: React.RefObject<HTMLTextAreaElement | null>;
  autocomplete: {
    suggestions: string[];
    selectedIndex: number;
    isOpen: boolean;
    selectNext: () => void;
    selectPrev: () => void;
    accept: (index?: number) => { newDraft: string; newCursor: number } | null;
    dismiss: () => void;
  };
  showHistory: boolean;
  setShowHistory: (v: boolean | ((prev: boolean) => boolean)) => void;
  showSavedQueries: boolean;
  setShowSavedQueries: (v: boolean | ((prev: boolean) => boolean)) => void;
  historyEntries: HistoryEntry[];
  onHistoryRemove: (query: string) => void;
  onHistoryClear: () => void;
  savedQueries: SavedQuery[];
  onSaveQuery: (name: string, query: string) => void;
  onDeleteSavedQuery: (name: string) => void;
  executeQuery: () => void;
  isSearching: boolean;
  isFollowMode: boolean;
  startFollow: () => void;
  stopFollowMode: () => void;
  draftHasErrors: boolean;
  draftIsPipeline: boolean;
  showPlan: boolean;
  handleShowPlan: () => void;
  syntax?: SyntaxSets;
}

export function QueryBar({
  dark,
  draft,
  setDraft,
  setCursorPos,
  queryInputRef,
  autocomplete,
  showHistory,
  setShowHistory,
  showSavedQueries,
  setShowSavedQueries,
  historyEntries,
  onHistoryRemove,
  onHistoryClear,
  savedQueries,
  onSaveQuery,
  onDeleteSavedQuery,
  executeQuery,
  isSearching,
  isFollowMode,
  startFollow,
  stopFollowMode,
  draftHasErrors,
  draftIsPipeline,
  showPlan,
  handleShowPlan,
  syntax,
}: Readonly<QueryBarProps>) {
  const c = useThemeClass(dark);
  const { openHelp } = useHelp();

  return (
    <div
      className={`px-5 py-4 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
    >
      <div className="flex gap-3 items-start">
        <div className="flex-1 relative">
          <QueryInput
            ref={queryInputRef}
            value={draft}
            syntax={syntax}
            onChange={(e) => {
              setDraft(e.target.value);
              setCursorPos(e.target.selectionStart ?? 0);
            }}
            onKeyDown={(e) => {
              if (autocomplete.isOpen) {
                if (e.key === "ArrowDown") {
                  e.preventDefault();
                  autocomplete.selectNext();
                  return;
                }
                if (e.key === "ArrowUp") {
                  e.preventDefault();
                  autocomplete.selectPrev();
                  return;
                }
                if (
                  e.key === "Tab" ||
                  (e.key === "Enter" && !e.shiftKey)
                ) {
                  e.preventDefault();
                  const result = autocomplete.accept();
                  if (result) {
                    setDraft(result.newDraft);
                    setCursorPos(result.newCursor);
                    requestAnimationFrame(() => {
                      const ta = queryInputRef.current;
                      if (ta) {
                        ta.selectionStart = result.newCursor;
                        ta.selectionEnd = result.newCursor;
                      }
                    });
                  }
                  return;
                }
                if (e.key === "Escape") {
                  e.preventDefault();
                  autocomplete.dismiss();
                  return;
                }
              }
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                if (!draftHasErrors) executeQuery();
              }
            }}
            onKeyUp={(e) => {
              const ta = e.target as HTMLTextAreaElement;
              setCursorPos(ta.selectionStart ?? 0);
            }}
            onClick={(e) => {
              const ta = e.target as HTMLTextAreaElement;
              setCursorPos(ta.selectionStart ?? 0);
            }}
            placeholder="Search logs... tokens for full-text, key=value for attributes"
            dark={dark}
          >
            <button
              onMouseDown={(e) => {
                e.stopPropagation();
                e.preventDefault();
                setShowHistory((h: boolean) => !h);
                setShowSavedQueries(false);
              }}
              className={`transition-colors ${c(
                "text-text-ghost hover:text-copper",
                "text-light-text-ghost hover:text-copper",
              )}`}
              aria-label="Query history"
              title="Query history"
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
                <circle cx="12" cy="12" r="10" />
                <polyline points="12 6 12 12 16 14" />
              </svg>
            </button>
            <button
              onMouseDown={(e) => {
                e.stopPropagation();
                e.preventDefault();
                setShowSavedQueries((s: boolean) => !s);
                setShowHistory(false);
              }}
              className={`transition-colors ${c(
                "text-text-ghost hover:text-copper",
                "text-light-text-ghost hover:text-copper",
              )}`}
              aria-label="Saved queries"
              title="Saved queries"
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
                <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z" />
              </svg>
            </button>
            <button
              onClick={() => openHelp("query-language")}
              className={`transition-colors ${c(
                "text-text-ghost hover:text-copper",
                "text-light-text-ghost hover:text-copper",
              )}`}
              aria-label="Query language help"
              title="Query language help"
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
                <circle cx="12" cy="12" r="10" />
                <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
                <line x1="12" y1="17" x2="12.01" y2="17" />
              </svg>
            </button>
          </QueryInput>
          {showHistory && (
            <QueryHistory
              entries={historyEntries}
              dark={dark}
              onSelect={(query) => {
                setDraft(query);
                setShowHistory(false);
                queryInputRef.current?.focus();
              }}
              onRemove={onHistoryRemove}
              onClear={onHistoryClear}
              onClose={() => setShowHistory(false)}
            />
          )}
          {showSavedQueries && (
            <SavedQueries
              queries={savedQueries}
              dark={dark}
              currentQuery={draft}
              onSelect={(query) => {
                setDraft(query);
                setShowSavedQueries(false);
                queryInputRef.current?.focus();
              }}
              onSave={onSaveQuery}
              onDelete={onDeleteSavedQuery}
              onClose={() => setShowSavedQueries(false)}
            />
          )}
          {autocomplete.isOpen && !showHistory && !showSavedQueries && (
            <QueryAutocomplete
              suggestions={autocomplete.suggestions}
              selectedIndex={autocomplete.selectedIndex}
              dark={dark}
              onSelect={(i) => {
                const result = autocomplete.accept(i);
                if (result) {
                  setDraft(result.newDraft);
                  setCursorPos(result.newCursor);
                  requestAnimationFrame(() => {
                    const ta = queryInputRef.current;
                    if (ta) {
                      ta.selectionStart = result.newCursor;
                      ta.selectionEnd = result.newCursor;
                      ta.focus();
                    }
                  });
                }
              }}
              onClose={autocomplete.dismiss}
            />
          )}
        </div>
        <QueryActionButtons
          dark={dark}
          executeQuery={executeQuery}
          isSearching={isSearching}
          isFollowMode={isFollowMode}
          startFollow={startFollow}
          stopFollowMode={stopFollowMode}
          draftHasErrors={draftHasErrors}
          draftIsPipeline={draftIsPipeline}
          showPlan={showPlan}
          handleShowPlan={handleShowPlan}
        />
      </div>

    </div>
  );
}

function QueryActionButtons({
  dark,
  executeQuery,
  isSearching,
  isFollowMode,
  startFollow,
  stopFollowMode,
  draftHasErrors,
  draftIsPipeline,
  showPlan,
  handleShowPlan,
}: Readonly<{
  dark: boolean;
  executeQuery: () => void;
  isSearching: boolean;
  isFollowMode: boolean;
  startFollow: () => void;
  stopFollowMode: () => void;
  draftHasErrors: boolean;
  draftIsPipeline: boolean;
  showPlan: boolean;
  handleShowPlan: () => void;
}>) {
  const c = useThemeClass(dark);
  return (
    <>
      <button
        onClick={executeQuery}
        disabled={isSearching || draftHasErrors}
        aria-label="Search"
        title="Search"
        className="px-2 py-2.5 rounded border border-transparent bg-copper text-white hover:bg-copper-glow transition-all duration-200 disabled:opacity-40 disabled:cursor-not-allowed"
      >
        <svg
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="w-4.5 h-4.5"
        >
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
      </button>
      <button
        onClick={isFollowMode ? stopFollowMode : startFollow}
        disabled={!isFollowMode && (draftHasErrors || draftIsPipeline)}
        aria-label={isFollowMode ? "Stop following" : "Follow"}
        title={isFollowMode ? "Stop following" : draftIsPipeline ? "Pipeline queries cannot be followed" : "Follow"}
        className={`px-2 py-2.5 rounded border transition-all duration-200 disabled:opacity-40 disabled:cursor-not-allowed ${
          isFollowMode
            ? "bg-severity-error/15 border-severity-error text-severity-error hover:bg-severity-error/25"
            : c(
                "border-ink-border text-text-muted hover:border-copper-dim hover:text-copper-dim",
                "border-light-border text-light-text-muted hover:border-copper hover:text-copper",
              )
        }`}
      >
        {isFollowMode ? (
          <svg
            viewBox="0 0 24 24"
            fill="currentColor"
            stroke="none"
            className="w-4.5 h-4.5"
          >
            <rect x="6" y="6" width="12" height="12" rx="1" />
          </svg>
        ) : (
          <svg
            viewBox="0 0 24 24"
            fill="currentColor"
            stroke="none"
            className="w-4.5 h-4.5"
          >
            <polygon points="6,4 20,12 6,20" />
          </svg>
        )}
      </button>
      <button
        onClick={handleShowPlan}
        disabled={!showPlan && draftHasErrors}
        aria-label="Explain query plan"
        title="Explain query plan"
        className={`px-2 py-2.5 border rounded transition-all duration-200 disabled:opacity-40 disabled:cursor-not-allowed ${
          showPlan
            ? c(
                "border-copper text-copper",
                "border-copper text-copper",
              )
            : c(
                "border-ink-border text-text-muted hover:border-copper-dim hover:text-copper-dim",
                "border-light-border text-light-text-muted hover:border-copper hover:text-copper",
              )
        }`}
      >
        <svg
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="w-4.5 h-4.5"
        >
          <rect x="3" y="3" width="7" height="5" rx="1" />
          <rect x="14" y="8" width="7" height="5" rx="1" />
          <rect x="3" y="16" width="7" height="5" rx="1" />
          <path d="M10 5.5h2.5a1 1 0 0 1 1 1v4" />
          <path d="M14 11.5h-2.5a1 1 0 0 0-1 1v4" />
        </svg>
      </button>
    </>
  );
}
