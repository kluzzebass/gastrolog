/**
 * Query manipulation handlers extracted from useSearchView.
 * All handlers modify the URL query via setUrlQuery — no internal state.
 */
import type { MutableRefObject } from "react";
import {
  stripSeverity,
  stripChunk,
  stripPos,
  buildSeverityExpr,
  injectVault,
  injectTimeRange,
} from "../utils/queryHelpers";
import { SEVERITY_LEVELS } from "../lib/severity";

interface QueryHandlerDeps {
  q: string;
  setUrlQuery: (newQ: string) => void;
  navigate: (opts: {
    to?: string;
    search?: Record<string, string | undefined> | ((prev: Record<string, string | undefined>) => Record<string, string | undefined>);
    replace?: boolean;
  }) => void;

  // Vault selection state
  selectedVault: string;
  setSelectedVault: (v: string) => void;

  // Reverse toggle state
  isFollowMode: boolean;
  isReversed: boolean;
  timeRange: string;
  followReversed: boolean;
  setFollowReversed: (fn: (prev: boolean) => boolean) => void;

  // Span click state (cursor insertion)
  draft: string;
  setDraft: (v: string) => void;
  cursorRef: MutableRefObject<number>;
  queryInputRef: MutableRefObject<HTMLTextAreaElement | null>;
}

export function useQueryHandlers(deps: QueryHandlerDeps) {
  const {
    q, setUrlQuery, navigate: _navigate,
    selectedVault, setSelectedVault,
    isFollowMode, isReversed, timeRange, followReversed: _followReversed, setFollowReversed,
    draft, setDraft, cursorRef, queryInputRef,
  } = deps;

  // ── Severity filtering ──────────────────────────────────────────────

  const allSeverities = SEVERITY_LEVELS;

  const activeSeverities = allSeverities.filter((s) =>
    q.includes(`level=${s}`),
  );

  const toggleSeverity = (level: string) => {
    const current = allSeverities.filter((s) => q.includes(`level=${s}`));
    const next: string[] = current.includes(level as typeof current[number])
      ? current.filter((s) => s !== level)
      : [...current, level];
    const base = stripSeverity(q);
    const sevExpr = buildSeverityExpr(next);
    const newQuery = sevExpr ? `${base} ${sevExpr}`.trim() : base;
    setUrlQuery(newQuery);
  };

  const handleSegmentClick = (level: string) => {
    if (level === "other") {
      const hasNoLevel = /\bnot\s+level=\*\b/i.test(q);
      const base = stripSeverity(q);
      const newQuery = hasNoLevel ? base : `${base} not level=*`.trim();
      setUrlQuery(newQuery);
    } else {
      toggleSeverity(level);
    }
  };

  // ── Reverse toggle ──────────────────────────────────────────────────

  const toggleReverse = () => {
    if (isFollowMode) {
      setFollowReversed((prev) => !prev);
    } else {
      const hasExplicitStartEnd =
        /\bstart=/.test(q) || /\bend=/.test(q);
      if (hasExplicitStartEnd) {
        const stripped = q
          .replace(/\breverse=\S+/g, "")
          .replace(/\s+/g, " ")
          .trim();
        const rev = `reverse=${!isReversed}`;
        setUrlQuery(stripped ? `${rev} ${stripped}` : rev);
      } else {
        const newQuery = injectTimeRange(q, timeRange, !isReversed);
        setUrlQuery(newQuery);
      }
    }
  };

  // ── Vault / chunk / position selection ──────────────────────────────

  const handleVaultSelect = (vaultId: string) => {
    const next = selectedVault === vaultId ? "all" : vaultId;
    setSelectedVault(next);
    const newQuery = injectVault(q, next);
    setUrlQuery(newQuery);
  };

  const handleChunkSelect = (chunkId: string) => {
    const token = `chunk=${chunkId}`;
    if (q.includes(token)) {
      setUrlQuery(stripChunk(q));
    } else {
      const base = stripChunk(q);
      setUrlQuery(base ? `${token} ${base}` : token);
    }
  };

  const handlePosSelect = (chunkId: string, pos: string) => {
    const posToken = `pos=${pos}`;
    const chunkToken = `chunk=${chunkId}`;
    if (q.includes(posToken)) {
      setUrlQuery(stripPos(stripChunk(q)));
    } else {
      const base = stripPos(stripChunk(q));
      const tokens = `${chunkToken} ${posToken}`;
      setUrlQuery(base ? `${tokens} ${base}` : tokens);
    }
  };

  // ── Field / token manipulation ──────────────────────────────────────

  const handleFieldSelect = (key: string, value: string) => {
    const needsQuotes = /[^a-zA-Z0-9_\-.]/.test(value);
    const token = needsQuotes ? `${key}="${value}"` : `${key}=${value}`;
    if (q.includes(token)) {
      const newQuery = q.replace(token, "").replace(/\s+/g, " ").trim();
      setUrlQuery(newQuery);
    } else {
      const newQuery = q.trim() ? `${q.trim()} ${token}` : token;
      setUrlQuery(newQuery);
    }
  };

  const handleMultiFieldSelect = (fields: [string, string][]) => {
    const tokens = fields.map(([key, value]) => {
      const needsQuotes = /[^a-zA-Z0-9_\-.]/.test(value);
      return needsQuotes ? `${key}="${value}"` : `${key}=${value}`;
    });
    let current = q.trim();
    for (const token of tokens) {
      if (!current.includes(token)) {
        current = current ? `${current} ${token}` : token;
      }
    }
    setUrlQuery(current);
  };

  const handleSpanClick = (value: string) => {
    const text = draft;
    const pos = cursorRef.current;
    const before = text.slice(0, pos);
    const after = text.slice(pos);
    const needSpaceBefore = before.length > 0 && !before.endsWith(" ") && !before.endsWith("(");
    const needSpaceAfter = after.length > 0 && !after.startsWith(" ") && !after.startsWith(")");
    const inserted = `${needSpaceBefore ? " " : ""}${value}${needSpaceAfter ? " " : ""}`;
    const newDraft = before + inserted + after;
    const newCursor = before.length + inserted.length;
    setDraft(newDraft);
    cursorRef.current = newCursor;
    requestAnimationFrame(() => {
      const ta = queryInputRef.current;
      if (ta) {
        ta.focus();
        ta.selectionStart = newCursor;
        ta.selectionEnd = newCursor;
      }
    });
  };

  const handleTokenToggle = (token: string) => {
    if (q.includes(token)) {
      const newQuery = q.replace(token, "").replace(/\s+/g, " ").trim();
      setUrlQuery(newQuery);
    } else {
      const newQuery = q.trim() ? `${q.trim()} ${token}` : token;
      setUrlQuery(newQuery);
    }
  };

  // ── Histogram brush / pan / zoom ────────────────────────────────────
  // These are included here because they're all "query → URL" manipulations.

  return {
    allSeverities,
    activeSeverities,
    toggleSeverity,
    handleSegmentClick,
    toggleReverse,
    handleVaultSelect,
    handleChunkSelect,
    handlePosSelect,
    handleFieldSelect,
    handleMultiFieldSelect,
    handleSpanClick,
    handleTokenToggle,
  };
}
