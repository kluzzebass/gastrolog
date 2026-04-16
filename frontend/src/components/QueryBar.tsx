import { useState, useRef, useEffect } from "react";
import { QueryInput, resolveSpans, roleStyle } from "./QueryInput";
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
import type { SavedQuery } from "../api/gen/gastrolog/v1/system_pb";
import { useThemeClass } from "../hooks/useThemeClass";

/**
 * Insert text into a textarea using execCommand so the browser's undo
 * stack is preserved.  Optionally replaces a range (start..end).
 * Returns the new cursor position after the inserted text.
 */
function insertText(
  ta: HTMLTextAreaElement,
  text: string,
  start?: number,
  end?: number,
): number {
  if (start !== undefined) ta.selectionStart = start;
  if (end !== undefined) ta.selectionEnd = end;
  ta.focus();
  document.execCommand("insertText", false, text);
  return ta.selectionStart;
}

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
        current += query[++i]!;
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
  draftCanFollow: boolean;
  showPlan: boolean;
  handleShowPlan: () => void;
  highlightSpans?: Array<{ text: string; role: string }>;
  highlightExpression?: string;
  errorMessage?: string | null;
}

function caretOffsetFromClick(e: React.MouseEvent, fallback: number): number {
  const range = document.caretRangeFromPoint(e.clientX, e.clientY);
  if (!range) return fallback;
  const container = (e.currentTarget as HTMLElement).querySelector(".truncate");
  if (!container) return fallback;
  let pos = 0;
  const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT);
  let node: Text | null;
  while ((node = walker.nextNode() as Text | null)) {
    if (node === range.startContainer) {
      return pos + range.startOffset;
    }
    pos += node.length;
  }
  return fallback;
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
  draftCanFollow,
  showPlan,
  handleShowPlan,
  highlightSpans,
  highlightExpression,
  errorMessage,
}: Readonly<QueryBarProps>) {
  const c = useThemeClass(dark);
  const { openHelp } = useHelp();
  const [focused, setFocused] = useState(false);
  const barRef = useRef<HTMLDivElement>(null);
  // Tracks whether a mousedown happened, so the blur handler can tell
  // keyboard blur (Tab) apart from mouse blur (click outside).
  const mouseDownRef = useRef(false);

  // Collapse on outside click — uses the `click` event (not mousedown/blur)
  // so the target's own click handler fires first. This prevents the
  // "two clicks needed" problem where blur-driven collapse ate the first click.
  useEffect(() => {
    if (!focused) return;
    const onDown = () => { mouseDownRef.current = true; };
    const onUp = () => { mouseDownRef.current = false; };
    const onClick = (e: MouseEvent) => {
      if (barRef.current?.contains(e.target as Node)) return;
      setFocused(false);
      setShowHistory(false);
      setShowSavedQueries(false);
    };
    document.addEventListener("mousedown", onDown, { passive: true });
    document.addEventListener("mouseup", onUp, { passive: true });
    document.addEventListener("click", onClick, { passive: true });
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("mouseup", onUp);
      document.removeEventListener("click", onClick);
    };
  }, [focused, setShowHistory, setShowSavedQueries]);

  const collapsed = !focused;
  const lines = draft.split("\n");
  const firstLine = lines[0] || "";
  const hiddenCount = lines.length - 1;
  const collapsedSpans = collapsed
    ? resolveSpans(firstLine, highlightSpans, highlightExpression)
    : [];

  const expand = (e?: React.MouseEvent) => {
    const offset = e ? caretOffsetFromClick(e, draft.length) : draft.length;
    setFocused(true);
    requestAnimationFrame(() => {
      const ta = queryInputRef.current;
      if (ta) {
        ta.focus();
        ta.selectionStart = offset;
        ta.selectionEnd = offset;
        cursorRef.current = offset;
      }
    });
  };

  return (
    <div
      ref={barRef}
      className={`px-5 py-4 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
    >
      <div className="flex gap-3 items-start">
        {collapsed ? (
          /* ── Collapsed: single-line preview ── */
          <div
            className={`flex-1 flex items-center gap-2 min-w-0 cursor-pointer rounded border px-2.5 py-2 font-mono text-sm ${c(
              "bg-ink-well border-ink-border text-text-normal hover:border-copper-dim",
              "bg-light-input border-light-border text-light-text-normal hover:border-copper",
            )}`}
            onMouseDown={expand}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") expand();
            }}
          >
            <span className="truncate">
              {firstLine
                ? collapsedSpans.map((span, i) => (
                    <span key={i} style={roleStyle(span.role, dark)}>{span.text}</span>
                  ))
                : "Search logs..."}
            </span>
            {hiddenCount > 0 && (
              <span className={`shrink-0 text-xs tabular-nums ${c("text-text-muted", "text-light-text-muted")}`}>
                +{hiddenCount} line{hiddenCount > 1 ? "s" : ""}
              </span>
            )}
          </div>
        ) : (
          /* ── Expanded: full query input ── */
          <div
            className="flex-1 relative"
            onFocusCapture={() => setFocused(true)}
            onBlurCapture={(e) => {
              // Keyboard blur (Tab): collapse immediately — no click to worry about.
              // Mouse blur: skip — the document-level click handler will collapse.
              if (
                !e.currentTarget.contains(e.relatedTarget as Node) &&
                !mouseDownRef.current
              ) {
                setFocused(false);
                setShowHistory(false);
                setShowSavedQueries(false);
              }
            }}
          >
            <QueryInput
              ref={queryInputRef}
              value={draft}
              highlightSpans={highlightSpans}
              highlightExpression={highlightExpression}
              errorMessage={errorMessage}
              onChange={(e) => {
                setDraft(e.target.value);
                cursorRef.current = e.target.selectionStart;
              }}
              // eslint-disable-next-line sonarjs/cognitive-complexity -- keyboard handler with autocomplete, pipe formatting, and submit logic
              onKeyDown={(e) => {
                if (autocomplete.isOpen) {
                  if (e.key === "Tab") {
                    e.preventDefault();
                    const result = autocomplete.accept();
                    if (result) {
                      const ta = queryInputRef.current;
                      if (ta) {
                        // Replace the entire value via insertText to preserve undo.
                        insertText(ta, result.newDraft, 0, draft.length);
                        // insertText places cursor at end of inserted text;
                        // we need it at the accept position.
                        ta.selectionStart = result.newCursor;
                        ta.selectionEnd = result.newCursor;
                        cursorRef.current = result.newCursor;
                      }
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
                  // Trim trailing whitespace on current line before inserting.
                  const before = draft.slice(0, start);
                  // eslint-disable-next-line sonarjs/slow-regex -- simple trailing whitespace pattern
                  const trimStart = before.replace(/[ \t]+$/, "").length;
                  const newCursor = insertText(ta, "\n| ", trimStart, end);
                  cursorRef.current = newCursor;
                  return;
                }
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  if (!draftHasErrors) executeQuery();
                }
              }}
              onClick={(e) => {
                const ta = e.target as HTMLTextAreaElement;
                cursorRef.current = ta.selectionStart;
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
                  "text-text-muted hover:text-copper",
                  "text-light-text-muted hover:text-copper",
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
                  "text-text-muted hover:text-copper",
                  "text-light-text-muted hover:text-copper",
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
                  const ta = queryInputRef.current;
                  if (ta) {
                    insertText(ta, formatPipeQuery(draft), 0, draft.length);
                  }
                }}
                className={`transition-colors ${c(
                  "text-text-muted hover:text-copper",
                  "text-light-text-muted hover:text-copper",
                )}`}
                aria-label="Format query"
                title="Format query"
              >
                <FormatIcon className="w-4 h-4" />
              </button>
              <button
                onClick={() => openHelp("query-language")}
                className={`transition-colors ${c(
                  "text-text-muted hover:text-copper",
                  "text-light-text-muted hover:text-copper",
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
                    const ta = queryInputRef.current;
                    if (ta) {
                      insertText(ta, result.newDraft, 0, draft.length);
                      ta.selectionStart = result.newCursor;
                      ta.selectionEnd = result.newCursor;
                      cursorRef.current = result.newCursor;
                    }
                  }
                }}
                onClose={autocomplete.dismiss}
              />
            )}
          </div>
        )}

        <QueryActionButtons
          dark={dark}
          executeQuery={executeQuery}
          cancelSearch={cancelSearch}
          isSearching={isSearching}
          isFollowMode={isFollowMode}
          startFollow={startFollow}
          stopFollowMode={stopFollowMode}
          draftHasErrors={draftHasErrors}
          draftCanFollow={draftCanFollow}
          showPlan={showPlan}
          handleShowPlan={handleShowPlan}
        />
      </div>
    </div>
  );
}

function followButtonTitle(isFollowMode: boolean, canFollow: boolean): string {
  if (isFollowMode) return "Stop following";
  if (!canFollow) return "This query cannot be followed";
  return "Follow";
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
  draftCanFollow,
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
  draftCanFollow: boolean;
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
          className="px-2 py-2.5 rounded border border-transparent bg-severity-error text-severity-error-text hover:bg-severity-error/80 transition-all duration-200"
        >
          <XIcon className="w-4.5 h-4.5" />
        </button>
      ) : (
        <button
          onClick={executeQuery}
          disabled={draftHasErrors}
          aria-label="Search"
          title="Search"
          className="px-2 py-2.5 rounded border border-transparent bg-copper text-text-on-copper hover:bg-copper-glow transition-all duration-200 disabled:opacity-40 disabled:cursor-not-allowed"
        >
          <SearchIcon className="w-4.5 h-4.5" />
        </button>
      )}
      <button
        onClick={isFollowMode ? stopFollowMode : startFollow}
        disabled={!isFollowMode && (draftHasErrors || !draftCanFollow)}
        aria-label={isFollowMode ? "Stop following" : "Follow"}
        title={followButtonTitle(isFollowMode, draftCanFollow)}
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
