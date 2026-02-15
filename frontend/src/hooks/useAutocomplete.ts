import { useState, useMemo, useCallback } from "react";
import type { FieldSummary } from "../utils";
import { timeRangeMs } from "../utils";
import { DIRECTIVES } from "../queryTokenizer";

const OPERATORS = ["AND", "OR", "NOT"];

// Known values for directives that have a fixed set of options.
// last= derives from the same timeRangeMs map used by TimeRangePicker.
const DIRECTIVE_VALUES: Record<string, string[]> = {
  last: Object.keys(timeRangeMs),
  reverse: ["true", "false"],
};

// Characters that break a word in the query language.
export function isWordBreak(ch: string): boolean {
  return " \t\n\r()=*\"'".includes(ch);
}

// Extract the word at cursor and its replace range.
export function wordAtCursor(
  text: string,
  cursor: number,
): { word: string; start: number; end: number } | null {
  if (cursor <= 0 || cursor > text.length) return null;

  // Scan backward from cursor to find word start.
  let start = cursor;
  while (start > 0 && !isWordBreak(text[start - 1]!)) start--;

  // Scan forward from cursor to find word end.
  let end = cursor;
  while (end < text.length && !isWordBreak(text[end]!)) end++;

  const word = text.slice(start, cursor); // only the part before cursor
  if (word.length === 0) return null;

  return { word, start, end };
}

// Determine if the cursor is in a "value" position (after key=).
export function getValueContext(text: string, wordStart: number): string | null {
  // Look backward from word start for `=` preceded by a key.
  let i = wordStart - 1;
  if (i < 0 || text[i] !== "=") return null;
  i--; // skip =

  // Find the key before =.
  const keyEnd = i + 1;
  while (i >= 0 && !isWordBreak(text[i]!)) i--;
  const key = text.slice(i + 1, keyEnd);
  return key.length > 0 ? key : null;
}

export interface AutocompleteState {
  suggestions: string[];
  selectedIndex: number;
  isOpen: boolean;
  replaceRange: { start: number; end: number } | null;
  suffix: string; // appended after accepted suggestion ("=" for keys, " " for values)
}

export function useAutocomplete(
  draft: string,
  cursorPos: number,
  fields: FieldSummary[],
) {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [dismissed, setDismissed] = useState(false);
  const [lastDraft, setLastDraft] = useState(draft);

  // Reset dismissed state when draft changes.
  if (draft !== lastDraft) {
    setLastDraft(draft);
    if (dismissed) setDismissed(false);
  }

  const { suggestions, replaceRange, suffix: _suffix } = useMemo(() => {
    const empty = {
      suggestions: [] as string[],
      replaceRange: null,
      suffix: "",
    };
    if (dismissed) return empty;

    const ctx = wordAtCursor(draft, cursorPos);

    // Cursor immediately after "=" with no value typed yet.
    if (!ctx && cursorPos > 0 && draft[cursorPos - 1] === "=") {
      const valueKey = getValueContext(draft, cursorPos);
      if (valueKey) {
        // Check directive values first, then field values.
        const dirVals = DIRECTIVE_VALUES[valueKey.toLowerCase()];
        if (dirVals) {
          return {
            suggestions: dirVals,
            replaceRange: { start: cursorPos, end: cursorPos },
            suffix: " ",
          };
        }
        const field = fields.find(
          (f) => f.key.toLowerCase() === valueKey.toLowerCase(),
        );
        if (field && field.values.length > 0) {
          return {
            suggestions: field.values.map((v) => v.value).slice(0, 10),
            replaceRange: { start: cursorPos, end: cursorPos },
            suffix: " ",
          };
        }
      }
      return empty;
    }

    if (!ctx) return empty;

    const prefix = ctx.word.toLowerCase();
    const valueKey = getValueContext(draft, ctx.start);

    if (valueKey) {
      // Value position: suggest known values for this key.
      // Check directive values first, then field values.
      const dirVals = DIRECTIVE_VALUES[valueKey.toLowerCase()];
      let candidates: string[];
      if (dirVals) {
        candidates = dirVals;
      } else {
        const field = fields.find(
          (f) => f.key.toLowerCase() === valueKey.toLowerCase(),
        );
        if (!field) return empty;
        candidates = field.values.map((v) => v.value);
      }
      const matches = candidates
        .filter(
          (v) =>
            v.toLowerCase().startsWith(prefix) && v.toLowerCase() !== prefix,
        )
        .slice(0, 10);
      return {
        suggestions: matches,
        replaceRange: { start: ctx.start, end: ctx.end },
        suffix: " ",
      };
    }

    // Key position: suggest attribute keys, directives, operators.
    const keySet = new Set<string>();
    for (const f of fields) keySet.add(f.key);
    for (const d of DIRECTIVES) keySet.add(d);

    const keyMatches = Array.from(keySet)
      .filter(
        (k) => k.toLowerCase().startsWith(prefix) && k.toLowerCase() !== prefix,
      )
      .sort();

    const opMatches = OPERATORS.filter(
      (op) =>
        op.toLowerCase().startsWith(prefix) && op.toLowerCase() !== prefix,
    );

    const suggestions = [...keyMatches, ...opMatches].slice(0, 10);
    return {
      suggestions,
      replaceRange: { start: ctx.start, end: ctx.end },
      suffix: "", // suffix is determined per-suggestion at accept time
    };
  }, [draft, cursorPos, fields, dismissed]);

  // Reset selection when suggestions change.
  const sugKey = suggestions.join("\0");
  const [prevSugKey, setPrevSugKey] = useState(sugKey);
  if (sugKey !== prevSugKey) {
    setPrevSugKey(sugKey);
    if (selectedIndex >= suggestions.length) {
      setSelectedIndex(0);
    }
  }

  const isOpen = suggestions.length > 0;

  const selectNext = useCallback(() => {
    setSelectedIndex((i) => (i + 1) % suggestions.length);
  }, [suggestions.length]);

  const selectPrev = useCallback(() => {
    setSelectedIndex((i) => (i - 1 + suggestions.length) % suggestions.length);
  }, [suggestions.length]);

  const dismiss = useCallback(() => {
    setDismissed(true);
  }, []);

  // Accept a suggestion: returns the new draft string and cursor position.
  const accept = useCallback(
    (index?: number): { newDraft: string; newCursor: number } | null => {
      const idx = index ?? selectedIndex;
      if (!isOpen || idx < 0 || idx >= suggestions.length || !replaceRange)
        return null;

      const suggestion = suggestions[idx]!;
      const valueKey = getValueContext(draft, replaceRange.start);

      // Keys get "=" appended, values and operators get " ".
      let suf: string;
      if (valueKey) {
        // Value position
        const needsQuotes = /[^a-zA-Z0-9_\-.]/.test(suggestion);
        const insert = needsQuotes ? `"${suggestion}"` : suggestion;
        suf = " ";
        const newDraft =
          draft.slice(0, replaceRange.start) +
          insert +
          suf +
          draft.slice(replaceRange.end);
        const newCursor = replaceRange.start + insert.length + suf.length;
        return { newDraft, newCursor };
      }

      // Check if this is an operator or a key.
      const isOperator = OPERATORS.includes(suggestion);
      suf = isOperator ? " " : "=";

      const newDraft =
        draft.slice(0, replaceRange.start) +
        suggestion +
        suf +
        draft.slice(replaceRange.end);
      const newCursor = replaceRange.start + suggestion.length + suf.length;
      return { newDraft, newCursor };
    },
    [selectedIndex, isOpen, suggestions, replaceRange, draft],
  );

  return {
    suggestions,
    selectedIndex,
    isOpen,
    selectNext,
    selectPrev,
    accept,
    dismiss,
    setSelectedIndex,
  };
}
