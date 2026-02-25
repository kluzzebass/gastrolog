import { QueryInput } from "./QueryInput";
import { QueryHistory } from "./QueryHistory";
import { SavedQueries } from "./SavedQueries";
import { QueryAutocomplete } from "./QueryAutocomplete";
import {
  HistoryIcon,
  BookmarkIcon,
  FormatIcon,
  HelpCircleIcon,
  SearchIcon,
  XIcon,
  PlayIcon,
  StopIcon,
  PlanIcon,
} from "./icons";
import type { HistoryEntry } from "../hooks/useQueryHistory";
import { useHelp } from "../hooks/useHelp";
import type { SavedQuery } from "../api/gen/gastrolog/v1/config_pb";
import { useThemeClass } from "../hooks/useThemeClass";
import type { SyntaxSets } from "../queryTokenizer";

// Format a query by placing each pipe segment on its own line.
// Respects quoted strings — only splits on unquoted |.
function formatPipeQuery(query: string): string {
  const segments: string[] = [];
  let current = "";
  let inQuote: string | null = null;

  for (let i = 0; i < query.length; i++) {
    const ch = query[i]!;
    if (inQuote) {
      current += ch;
      if (ch === "\\" && i + 1 < query.length) {
        current += query[++i];
      } else if (ch === inQuote) {
        inQuote = null;
      }
    } else if (ch === '"' || ch === "'") {
      inQuote = ch;
      current += ch;
    } else if (ch === "|") {
      segments.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  segments.push(current);

  // First segment is the filter, rest are pipe operators.
  return segments
    .map((s, i) => (i === 0 ? s.trim() : "| " + s.trim()))
    .join("\n");
}

interface QueryBarProps {
  dark: boolean;
  draft: string;
  setDraft: (v: string) => void;
  cursorRef: React.RefObject<number>;
  queryInputRef: React.RefObject<HTMLTextAreaElement | null>;
  autocomplete: {
    suggestions: string[];
    selectedIndex: number;
    isOpen: boolean;
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
  cancelSearch: () => void;
  isSearching: boolean;
  isFollowMode: boolean;
  startFollow: () => void;
  stopFollowMode: () => void;
  draftHasErrors: boolean;
  draftIsPipeline: boolean;
  showPlan: boolean;
  handleShowPlan: () => void;
  syntax?: SyntaxSets;
  errorOffset?: number;
  errorMessage?: string | null;
}

export function QueryBar({
  dark,
  draft,
  setDraft,
  cursorRef,
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
  cancelSearch,
  isSearching,
  isFollowMode,
  startFollow,
  stopFollowMode,
  draftHasErrors,
  draftIsPipeline,
  showPlan,
  handleShowPlan,
  syntax,
  errorOffset,
  errorMessage,
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
            errorOffset={errorOffset}
            errorMessage={errorMessage}
            onChange={(e) => {
              setDraft(e.target.value);
              cursorRef.current = e.target.selectionStart ?? 0;
            }}
            onKeyDown={(e) => {
              if (autocomplete.isOpen) {
                if (e.key === "Tab") {
                  e.preventDefault();
                  const result = autocomplete.accept();
                  if (result) {
                    setDraft(result.newDraft);
                    cursorRef.current = result.newCursor;
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
              // Auto-format pipes: typing "|" inserts "\n| " instead.
              if (e.key === "|") {
                e.preventDefault();
                const ta = e.target as HTMLTextAreaElement;
                const start = ta.selectionStart;
                const end = ta.selectionEnd;
                const before = draft.slice(0, start);
                const after = draft.slice(end);
                // Trim trailing whitespace on current line before inserting.
                const trimmed = before.replace(/[ \t]+$/, "");
                const newDraft = trimmed + "\n| " + after;
                const newCursor = trimmed.length + 3; // after "| "
                setDraft(newDraft);
                cursorRef.current = newCursor;
                requestAnimationFrame(() => {
                  ta.selectionStart = newCursor;
                  ta.selectionEnd = newCursor;
                });
                return;
              }
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                if (!draftHasErrors) executeQuery();
              }
            }}
            onClick={(e) => {
              const ta = e.target as HTMLTextAreaElement;
              cursorRef.current = ta.selectionStart ?? 0;
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
              <HistoryIcon className="w-4 h-4" />
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
              <BookmarkIcon className="w-4 h-4" />
            </button>
            <button
              onMouseDown={(e) => {
                e.stopPropagation();
                e.preventDefault();
                setDraft(formatPipeQuery(draft));
                queryInputRef.current?.focus();
              }}
              className={`transition-colors ${c(
                "text-text-ghost hover:text-copper",
                "text-light-text-ghost hover:text-copper",
              )}`}
              aria-label="Format query"
              title="Format query"
            >
              <FormatIcon className="w-4 h-4" />
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
              <HelpCircleIcon className="w-4 h-4" />
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
                  cursorRef.current = result.newCursor;
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
          cancelSearch={cancelSearch}
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
  cancelSearch,
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
  cancelSearch: () => void;
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
      {isSearching ? (
        <button
          onClick={cancelSearch}
          aria-label="Cancel search"
          title="Cancel search"
          className="px-2 py-2.5 rounded border border-transparent bg-severity-error text-white hover:bg-severity-error/80 transition-all duration-200"
        >
          <XIcon className="w-4.5 h-4.5" />
        </button>
      ) : (
        <button
          onClick={executeQuery}
          disabled={draftHasErrors}
          aria-label="Search"
          title="Search"
          className="px-2 py-2.5 rounded border border-transparent bg-copper text-white hover:bg-copper-glow transition-all duration-200 disabled:opacity-40 disabled:cursor-not-allowed"
        >
          <SearchIcon className="w-4.5 h-4.5" />
        </button>
      )}
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
          <StopIcon className="w-4.5 h-4.5" />
        ) : (
          <PlayIcon className="w-4.5 h-4.5" />
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
        <PlanIcon className="w-4.5 h-4.5" />
      </button>
    </>
  );
}
