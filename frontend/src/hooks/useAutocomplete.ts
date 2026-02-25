import { useState, useMemo, useCallback, useEffect, useRef } from "react";
import type { FieldSummary } from "../utils";
import { timeRangeMs } from "../utils";
import { DEFAULT_SYNTAX, type SyntaxSets } from "../queryTokenizer";

const OPERATORS = ["AND", "OR", "NOT"];

// Aggregation functions valid in stats/timechart bodies.
// Scalar functions (abs, ceil, len, substr, etc.) belong in where/eval only.
const AGG_FUNCTIONS = new Set([
  "count", "avg", "sum", "min", "max",
]);

// Known values for directives that have a fixed set of options.
// last= derives from the same timeRangeMs map used by TimeRangePicker.
const DIRECTIVE_VALUES: Record<string, string[]> = {
  last: Object.keys(timeRangeMs),
  reverse: ["true", "false"],
};

// Characters that break a word in the query language.
export function isWordBreak(ch: string): boolean {
  return " \t\n\r()=*\"'|,".includes(ch);
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

// Detect whether the cursor is inside a pipe segment and what kind of position.
// bodyStart (returned for kind=body) is the absolute position right after the keyword.
export function getPipeContext(
  text: string,
  cursor: number,
): { kind: "keyword" } | { kind: "body"; keyword: string; bodyStart: number } | null {
  // Scan from 0 to cursor, tracking quote state, to find the last unquoted |.
  let lastPipe = -1;
  let inSingle = false;
  let inDouble = false;
  for (let i = 0; i < cursor; i++) {
    const ch = text[i]!;
    if (ch === "'" && !inDouble) inSingle = !inSingle;
    else if (ch === '"' && !inSingle) inDouble = !inDouble;
    else if (ch === "|" && !inSingle && !inDouble) lastPipe = i;
  }
  if (lastPipe === -1) return null;

  // Extract segment from after | to cursor.
  const segment = text.slice(lastPipe + 1, cursor);

  // Find the first word in the segment.
  let ws = 0;
  while (ws < segment.length && /\s/.test(segment[ws]!)) ws++;
  let we = ws;
  while (we < segment.length && !/\s/.test(segment[we]!)) we++;

  const keyword = segment.slice(ws, we);

  // No keyword yet or cursor is still within the first word.
  if (keyword.length === 0 || cursor <= lastPipe + 1 + we) {
    return { kind: "keyword" };
  }

  return { kind: "body", keyword, bodyStart: lastPipe + 1 + we };
}

// Check whether the body text contains an unquoted keyword token.
export function bodyHasToken(bodyText: string, token: string): boolean {
  const lower = token.toLowerCase();
  return bodyText
    .trim()
    .split(/\s+/)
    .some((t) => t.toLowerCase() === lower);
}

// Scan backward from wordStart to find the previous word, bounded by boundary.
export function prevWordBeforeCursor(
  text: string,
  wordStart: number,
  boundary: number,
): string | null {
  let i = wordStart - 1;
  // Skip whitespace.
  while (i >= boundary && /\s/.test(text[i]!)) i--;
  if (i < boundary) return null;
  const end = i + 1;
  while (i >= boundary && !/[\s|]/.test(text[i]!)) i--;
  const word = text.slice(i + 1, end);
  return word.length > 0 ? word : null;
}

// ── Pipe grammar table ──
//
// Each operator declares what to suggest given signals about the cursor
// position:  whether the body is empty, what the previous word is, and
// whether the body already contains a structural keyword like "by".
//
// A SuggestRule says which pools to draw from.  "none" means suppress
// the dropdown entirely.  Omitting a condition (undefined) means "skip
// this check and fall through to the next one."

export type SuggestRule = {
  fields?: boolean; // include field names
  aggs?: boolean; // include aggregation functions (AGG_FUNCTIONS)
  funcs?: boolean; // include all pipeFunctions (scalar + agg)
  literals?: string[]; // include these exact keywords
  lookupTables?: boolean; // include registered lookup table names from syntax
};

type Suggest = SuggestRule | "none";

export interface PipeGrammar {
  empty: Suggest; // body is empty (just typed "| kw ")
  afterAs?: Suggest; // previous word is "as"
  afterBy?: Suggest; // previous word is "by"
  pastBy?: Suggest; // body contains "by" somewhere earlier
  afterNumber?: Suggest; // previous word is a number
  fallback: Suggest; // everything else
}

export const PIPE_GRAMMARS: Record<string, PipeGrammar> = {
  // stats agg_list [by group_list]
  stats: {
    empty: { aggs: true },
    afterAs: "none",
    afterBy: { fields: true, literals: ["bin"] },
    pastBy: { fields: true, literals: ["bin"] },
    fallback: { funcs: true, fields: true, literals: ["as", "by"] },
  },
  // timechart NUMBER [by FIELD]
  timechart: {
    empty: "none",
    afterBy: { fields: true },
    pastBy: { fields: true },
    afterNumber: { literals: ["by"] },
    fallback: "none",
  },
  // where filter_expr
  where: {
    empty: { funcs: true, fields: true },
    fallback: { funcs: true, fields: true },
  },
  // eval IDENT = expr [, IDENT = expr]*
  eval: {
    empty: { fields: true },
    fallback: { funcs: true, fields: true },
  },
  // sort [-]field [, [-]field]*
  sort: {
    empty: { fields: true },
    fallback: { fields: true },
  },
  // rename field as newname [, field as newname]*
  rename: {
    empty: { fields: true },
    afterAs: "none",
    fallback: { fields: true, literals: ["as"] },
  },
  // fields [-] field_list
  fields: {
    empty: { fields: true },
    fallback: { fields: true },
  },
  // lookup TABLE FIELD
  lookup: {
    empty: { lookupTables: true },
    fallback: { fields: true },
  },
  // These take numeric/no arguments — never suggest.
  head: { empty: "none", fallback: "none" },
  tail: { empty: "none", fallback: "none" },
  slice: { empty: "none", fallback: "none" },
  raw: { empty: "none", fallback: "none" },
};

// Resolve a grammar + context signals into a concrete SuggestRule or "none".
// eslint-disable-next-line sonarjs/function-return-type -- returns Suggest after guards
export function resolveGrammar(
  grammar: PipeGrammar,
  prev: string | null,
  bodyText: string,
): Suggest {
  if (!prev) return grammar.empty;
  if (prev === "as" && grammar.afterAs !== undefined) return grammar.afterAs;
  if (prev === "by" && grammar.afterBy !== undefined) return grammar.afterBy;
  if (grammar.pastBy !== undefined && bodyHasToken(bodyText, "by"))
    return grammar.pastBy;
  if (
    grammar.afterNumber !== undefined &&
    /^\d+$/.test(prev)
  )
    return grammar.afterNumber;
  return grammar.fallback;
}

// eslint-disable-next-line sonarjs/class-name -- unused externally, underscore signals internal
interface _AutocompleteState {
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
  syntax: SyntaxSets = DEFAULT_SYNTAX,
  pipelineFields?: string[],
  pipelineCompletions?: string[],
) {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [dismissed, setDismissed] = useState(false);
  // Track which word the cursor was in when dismiss happened,
  // so we only re-open when the user moves to a different word.
  const [dismissedWordStart, setDismissedWordStart] = useState(-1);

  const currentWord = wordAtCursor(draft, cursorPos);
  const currentWordStart = currentWord ? currentWord.start : -1;

  // Reset dismissed state only when the cursor moves to a different word.
  if (dismissed && currentWordStart !== dismissedWordStart) {
    setDismissed(false);
    setDismissedWordStart(-1);
  }

  const { suggestions, replaceRange, suffix: _suffix, inPipeContext } =
    // eslint-disable-next-line sonarjs/cognitive-complexity -- autocomplete suggestion engine
    useMemo(() => {
      const empty = {
        suggestions: [] as string[],
        replaceRange: null,
        suffix: "",
        inPipeContext: false,
      };
      if (dismissed) return empty;

      const ctx = wordAtCursor(draft, cursorPos);
      const pipeCtx = getPipeContext(draft, cursorPos);

      // ── Pipe context suggestions ──
      if (pipeCtx) {
        const fieldNames = fields.map((f) => f.key);
        const range = ctx
          ? { start: ctx.start, end: ctx.end }
          : { start: cursorPos, end: cursorPos };
        const prefix = ctx ? ctx.word.toLowerCase() : "";

        if (pipeCtx.kind === "keyword") {
          const matches = Array.from(syntax.pipeKeywords)
            .filter(
              (k) =>
                k.toLowerCase().startsWith(prefix) &&
                k.toLowerCase() !== prefix,
            )
            .sort();
          return {
            suggestions: matches.slice(0, 10),
            replaceRange: range,
            suffix: " ",
            inPipeContext: true,
          };
        }

        // kind === "body" — look up the grammar table for this operator.
        const kw = pipeCtx.keyword.toLowerCase();
        const grammar = PIPE_GRAMMARS[kw];
        if (!grammar) return empty;

        const prevWord = ctx
          ? prevWordBeforeCursor(draft, ctx.start, pipeCtx.bodyStart)
          : prevWordBeforeCursor(draft, cursorPos, pipeCtx.bodyStart);
        const prev = prevWord?.toLowerCase() ?? null;
        const wordBoundary = ctx ? ctx.start : cursorPos;
        const bodyText = draft.slice(pipeCtx.bodyStart, wordBoundary);

        const rule = resolveGrammar(grammar, prev, bodyText);
        if (rule === "none") return empty;

        // Build candidates from the resolved rule.
        // When backend pipeline fields are available, use them instead of local fields.
        const backendHasFields = pipelineFields && pipelineFields.length > 0;
        const candidates: string[] = [];
        // Add backend completions (structural keywords like "by", "as") first.
        if (pipelineCompletions && pipelineCompletions.length > 0 && rule.literals) {
          // Merge: backend completions take priority, fallback to rule literals.
          const compSet = new Set(pipelineCompletions);
          for (const lit of rule.literals) compSet.add(lit);
          candidates.push(...Array.from(compSet));
        } else if (pipelineCompletions && pipelineCompletions.length > 0) {
          candidates.push(...pipelineCompletions);
        } else if (rule.literals) {
          candidates.push(...rule.literals);
        }
        if (rule.lookupTables) {
          candidates.push(...Array.from(syntax.lookupTables).sort());
        }
        if (rule.aggs) {
          candidates.push(
            ...Array.from(syntax.pipeFunctions)
              .filter((f) => AGG_FUNCTIONS.has(f))
              .sort(),
          );
        }
        if (rule.funcs) {
          candidates.push(...Array.from(syntax.pipeFunctions).sort());
        }
        if (rule.fields) {
          if (backendHasFields) {
            candidates.push(...[...pipelineFields].sort());
          } else {
            candidates.push(...fieldNames.toSorted());
          }
        }

        const matches = candidates
          .filter(
            (c) =>
              c.toLowerCase().startsWith(prefix) &&
              c.toLowerCase() !== prefix,
          )
          .slice(0, 10);
        return {
          suggestions: matches,
          replaceRange: range,
          suffix: " ",
          inPipeContext: true,
        };
      }

      // ── Filter context (no pipe) ──

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
              inPipeContext: false,
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
              inPipeContext: false,
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
              v.toLowerCase().startsWith(prefix) &&
              v.toLowerCase() !== prefix,
          )
          .slice(0, 10);
        return {
          suggestions: matches,
          replaceRange: { start: ctx.start, end: ctx.end },
          suffix: " ",
          inPipeContext: false,
        };
      }

      // Key position: suggest field names, directives, scalar functions, operators.
      const keySet = new Set<string>();
      for (const f of fields) keySet.add(f.key);
      for (const d of syntax.directives) keySet.add(d);
      for (const fn of syntax.pipeFunctions) {
        if (!AGG_FUNCTIONS.has(fn)) keySet.add(fn);
      }

      const keyMatches = Array.from(keySet)
        .filter(
          (k) =>
            k.toLowerCase().startsWith(prefix) &&
            k.toLowerCase() !== prefix,
        )
        .sort();

      const opMatches = OPERATORS.filter(
        (op) =>
          op.toLowerCase().startsWith(prefix) &&
          op.toLowerCase() !== prefix,
      );

      const suggestions = [...keyMatches, ...opMatches].slice(0, 10);
      return {
        suggestions,
        replaceRange: { start: ctx.start, end: ctx.end },
        suffix: "", // suffix is determined per-suggestion at accept time
        inPipeContext: false,
      };
    }, [draft, cursorPos, fields, dismissed, syntax, pipelineFields, pipelineCompletions]);

  // Reset selection when suggestions change.
  const sugKey = suggestions.join("\0");
  const [prevSugKey, setPrevSugKey] = useState(sugKey);
  if (sugKey !== prevSugKey) {
    setPrevSugKey(sugKey);
    if (selectedIndex >= suggestions.length) {
      setSelectedIndex(0);
    }
  }

  const hasSuggestions = suggestions.length > 0;
  const [isOpen, setIsOpen] = useState(false);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  useEffect(() => {
    clearTimeout(debounceRef.current);
    if (!hasSuggestions || dismissed) {
      setIsOpen(false);
    } else {
      debounceRef.current = setTimeout(() => setIsOpen(true), 150);
    }
    return () => clearTimeout(debounceRef.current);
  }, [hasSuggestions, dismissed, sugKey]);

  const dismiss = useCallback(() => {
    setDismissed(true);
    setDismissedWordStart(currentWordStart);
  }, [currentWordStart]);

  // Accept a suggestion: returns the new draft string and cursor position.
  const accept = useCallback(
    (index?: number): { newDraft: string; newCursor: number } | null => {
      const idx = index ?? selectedIndex;
      if (!isOpen || idx < 0 || idx >= suggestions.length || !replaceRange)
        return null;

      const suggestion = suggestions[idx]!;

      // Pipe context: always use " " as suffix.
      if (inPipeContext) {
        const suf = " ";
        const newDraft =
          draft.slice(0, replaceRange.start) +
          suggestion +
          suf +
          draft.slice(replaceRange.end);
        const newCursor = replaceRange.start + suggestion.length + suf.length;
        return { newDraft, newCursor };
      }

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
    [selectedIndex, isOpen, suggestions, replaceRange, draft, inPipeContext],
  );

  return {
    suggestions,
    selectedIndex,
    isOpen,
    accept,
    dismiss,
  };
}
