import { useState, useRef, useEffect, useDeferredValue } from "react";
import {
  useSearch as useRouterSearch,
  useNavigate,
  useLocation,
} from "@tanstack/react-router";
import {
  useSearch,
  useFollow,
  useExplain,
  useLiveHistogram,
  useRecordContext,
  extractTokens,
} from "../api/hooks";
import { tableResultToHistogramData } from "../utils/histogramData";
import { useVaults, useStats, useLogout, useCurrentUser } from "../api/hooks";
import { Record as ProtoRecord, getToken } from "../api/client";
import { TableResult, TableRow } from "../api/gen/gastrolog/v1/query_pb";

import { timeRangeMs, aggregateFields, sameRecord } from "../utils";
import {
  stripTimeRange,
  stripChunk,
  stripPos,
  stripSeverity,
  injectTimeRange,
  injectVault,
  buildSeverityExpr,
  resolveQueryEffectAction,
} from "../utils/queryHelpers";
import { normalizeTimeDirectives } from "../utils/normalizeTimeDirectives";
import { useThemeSync } from "./useThemeSync";
import { useThemeClass } from "./useThemeClass";
import { useDialogState } from "./useDialogState";
import { usePanelLayout } from "./usePanelLayout";
import { useTimeRange } from "./useTimeRange";
import { useToast } from "../components/Toast";
import { useQueryHistory } from "./useQueryHistory";
import {
  useSavedQueries,
  usePutSavedQuery,
  useDeleteSavedQuery,
} from "../api/hooks/useSavedQueries";
import { hasPipeOutsideQuotes } from "../lib/hasPipeOutsideQuotes";
import { SEVERITY_LEVELS } from "../lib/severity";
import type { SyntaxSets } from "../lib/syntaxSets";
import { useAutocomplete } from "./useAutocomplete";
import { useConfig } from "../api/hooks/useConfig";
import { useSettings } from "../api/hooks/useSettings";
import { useSyntax } from "../api/hooks/useSyntax";
import { useValidation } from "./useValidation";
import { usePipelineFields } from "./usePipelineFields";

// ── Types ──────────────────────────────────────────────────────────────

/** Search params shared by /search and /follow routes. */
export type ViewSearch = { q: string; help?: string; settings?: string; inspector?: string };

/** Navigate function typed for routes with ViewSearch params. */
export type ViewNavigate = (opts: {
  to?: string;
  search?: ViewSearch | ((prev: ViewSearch) => ViewSearch);
  replace?: boolean;
}) => void;

// ── Helpers (module-level) ─────────────────────────────────────────────

/** Check whether the query ends with a `| raw` pipe segment. */
function queryHasRaw(query: string): boolean {
  let inQuote: string | null = null;
  let lastPipe = -1;
  for (let i = 0; i < query.length; i++) {
    const ch = query[i]!;
    if (inQuote) {
      if (ch === "\\" && i + 1 < query.length) i++;
      else if (ch === inQuote) inQuote = null;
    } else if (ch === '"' || ch === "'") {
      inQuote = ch;
    } else if (ch === "|") {
      lastPipe = i;
    }
  }
  if (lastPipe < 0) return false;
  return query.slice(lastPipe + 1).trim().toLowerCase() === "raw";
}

/** Build a TableResult from streamed records (mirrors backend recordsToTable). */
function recordsToRawTable(records: ProtoRecord[]): TableResult {
  const keySet = new Set<string>();
  for (const rec of records) {
    for (const k of Object.keys(rec.attrs)) keySet.add(k);
  }
  const attrKeys = [...keySet].sort();
  const columns = ["_write_ts", "_ingest_ts", "_source_ts", ...attrKeys, "_raw"];
  const decoder = new TextDecoder();
  const rows = records.map((rec) => {
    const values: string[] = Array.from({ length: columns.length }, () => "");
    values[0] = rec.writeTs?.toDate().toISOString() ?? "";
    values[1] = rec.ingestTs?.toDate().toISOString() ?? "";
    values[2] = rec.sourceTs?.toDate().toISOString() ?? "";
    for (let j = 0; j < attrKeys.length; j++) {
      values[3 + j] = rec.attrs[attrKeys[j]!] ?? "";
    }
    values[columns.length - 1] = decoder.decode(rec.raw);
    return new TableRow({ values });
  });
  return new TableResult({ columns, rows, resultType: "raw" });
}

// ── Hook ───────────────────────────────────────────────────────────────

export function useSearchView() {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion -- useRouterSearch returns optional fields; assertion narrows q to string
  const { q, help: helpParam, settings: settingsParam, inspector: inspectorParam } = useRouterSearch({ strict: false }) as { q: string; help?: string; settings?: string; inspector?: string };
  // useNavigate() without `from` can't infer search types for shared routes.
  const navigate: ViewNavigate = useNavigate() as any;
  const location = useLocation();
  const isFollowMode = location.pathname === "/follow";

  // Redirect to setup wizard if no vaults are configured and wizard hasn't been dismissed.
  const config = useConfig();
  const settings = useSettings();
  useEffect(() => {
    if (config.data && settings.data && config.data.vaults.length === 0 && !settings.data.setupWizardDismissed) {
      navigate({ to: "/setup" });
    }
  }, [config.data, settings.data]); // eslint-disable-line react-hooks/exhaustive-deps

  const [draft, setDraft] = useState(q);
  const deferredDraft = useDeferredValue(draft);
  const cursorRef = useRef(0);
  const [selectedVault, setSelectedVault] = useState("all");

  // Follow buffer size — persisted to localStorage.
  const [followBufferSize, setFollowBufferSize] = useState(() => {
    const stored = localStorage.getItem("gastrolog:followBufferSize");
    const n = stored ? parseInt(stored, 10) : NaN;
    return n > 0 ? n : 5000;
  });
  const handleFollowBufferSizeChange = (size: number) => {
    setFollowBufferSize(size);
    localStorage.setItem("gastrolog:followBufferSize", String(size));
  };

  // Whether results are in reverse (newest-first) order.
  const isReversed = !q.includes("reverse=false");

  const {
    timeRange, setTimeRange,
    rangeStart, setRangeStart,
    rangeEnd, setRangeEnd,
    handleTimeRange, handleCustomRange,
  } = useTimeRange(q, isReversed);

  const {
    showPlan, setShowPlan,
    showHistory, setShowHistory,
    showSavedQueries, setShowSavedQueries,
    showChangePassword, setShowChangePassword,
    showPreferences, setShowPreferences,
    inspectorGlow,
  } = useDialogState();

  const openHelp = (topicId?: string) => {
    navigate({ search: (prev) => ({ ...prev, help: topicId || "general" }) });
  };

  const openSettings = (tab?: string) => {
    navigate({ search: (prev) => ({ ...prev, settings: tab || "service" }) });
  };

  const openInspector = (param?: string) => {
    const p = param || sessionStorage.getItem("inspector-last") || "entities:vaults";
    navigate({ search: (prev) => ({ ...prev, inspector: p }) });
  };

  const [selectedRecord, setSelectedRecord] = useState<ProtoRecord | null>(null);
  const { theme, setTheme, dark, highlightMode, setHighlightMode, palette, setPalette } = useThemeSync();

  const {
    isTablet,
    sidebarWidth, sidebarCollapsed, setSidebarCollapsed, sidebarResizeProps,
    detailWidth, detailCollapsed, setDetailCollapsed, detailPinned, setDetailPinned,
    detailResizeProps, resizing,
  } = usePanelLayout();

  // Auto-expand detail panel and fetch context when a record is selected.
  useEffect(() => {
    if (selectedRecord && detailCollapsed) setDetailCollapsed(false);
    if (selectedRecord?.ref) {
      fetchContext(selectedRecord.ref);
    } else {
      resetContext();
    }
  }, [selectedRecord]); // eslint-disable-line react-hooks/exhaustive-deps

  // Escape key: deselect record and collapse detail panel.
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        if (showPlan) {
          setShowPlan(false);
          return;
        }
        setSelectedRecord(null);
        if (!detailPinned) setDetailCollapsed(true);
      }
    };
    globalThis.addEventListener("keydown", handler);
    return () => globalThis.removeEventListener("keydown", handler);
  }, [detailPinned, showPlan, setDetailCollapsed, setShowPlan]);

  const queryHistory = useQueryHistory();
  const savedQueries = useSavedQueries();
  const putSavedQuery = usePutSavedQuery();
  const deleteSavedQuery = useDeleteSavedQuery();

  const queryInputRef = useRef<HTMLTextAreaElement>(null);
  const sentinelRef = useRef<HTMLDivElement>(null);
  const logScrollRef = useRef<HTMLDivElement>(null);
  const selectedRowRef = useRef<HTMLElement>(null);
  const expressionRef = useRef("");
  const skipNextSearchRef = useRef(false);
  const loadMoreGateRef = useRef(false);
  const [isScrolledDown, setIsScrolledDown] = useState(false);

  const { addToast } = useToast();
  const toastError = (err: Error) => addToast(err.message, "error");

  const {
    records,
    isSearching,
    hasMore,
    tableResult,
    search,
    loadMore,
    setRecords,
    cancel: cancelSearch,
    reset: resetSearch,
  } = useSearch({ onError: toastError });
  const {
    records: followRecords,
    isFollowing,
    reconnecting,
    reconnectAttempt,
    newCount: followNewCount,
    follow,
    stop: stopFollow,
    reset: resetFollow,
    resetNewCount: resetFollowNewCount,
  } = useFollow({ onError: toastError, maxRecords: followBufferSize });
  const {
    chunks: explainChunks,
    direction: explainDirection,
    totalChunks: explainTotalChunks,
    expression: explainExpression,
    pipelineStages: explainPipelineStages,
    isLoading: isExplaining,
    explain,
  } = useExplain({ onError: toastError });
  const {
    tableResult: histogramTableResult,
    search: histogramSearch,
    reset: histogramReset,
  } = useSearch({ onError: toastError });
  const {
    before: contextBefore,
    after: contextAfter,
    isLoading: contextLoading,
    fetchContext,
    reset: resetContext,
  } = useRecordContext({ onError: toastError });
  const { data: vaults, isLoading: vaultsLoading } = useVaults();
  const { data: stats, isLoading: statsLoading } = useStats();

  const logout = useLogout();
  const currentUser = useCurrentUser();
  const syntaxQuery = useSyntax();
  const syntax: SyntaxSets | undefined = syntaxQuery.data;
  const validation = useValidation(deferredDraft);

  // Navigate to a new query — pushes browser history, preserving current route.
  const setUrlQuery = (newQ: string) => {
    navigate({
      to: isFollowMode ? "/follow" : "/search",
      search: (prev) => ({ ...prev, q: newQ }),
      replace: false,
    });
  };

  // Auto-refresh polling for both pipeline and filter results.
  const [pollInterval, setPollInterval] = useState<number | null>(null);

  // Sync draft and reset poll interval when URL changes (browser back/forward).
  const [prevQ, setPrevQ] = useState(q);
  if (q !== prevQ) {
    setPrevQ(q);
    setDraft(q);
    setPollInterval(null);
  }

  // Fire search or follow depending on the current route.
  // eslint-disable-next-line sonarjs/cognitive-complexity -- query effect handles multiple route/mode transitions
  useEffect(() => {
    expressionRef.current = q;
    queryHistory.add(q);

    const action = resolveQueryEffectAction(q, isFollowMode, skipNextSearchRef.current);

    switch (action) {
      case "follow":
        resetSearch();
        resetFollow();
        follow(q);
        return;

      case "inject-default-range": {
        const defaultRange = "5m";
        setTimeRange(defaultRange);
        const ms = timeRangeMs[defaultRange];
        if (ms) {
          const now = new Date();
          setRangeStart(new Date(now.getTime() - ms));
          setRangeEnd(now);
        }
        const fixed = injectTimeRange(q, defaultRange, isReversed);
        navigate({ search: (prev) => ({ ...prev, q: fixed }), replace: true });
        return;
      }

      case "skip-search":
      case "search": {
        const lastMatch = /\blast=(\S+)/.exec(q);
        if (lastMatch?.[1]) {
          const key = lastMatch[1];
          const ms = timeRangeMs[key];
          if (ms) {
            setTimeRange(key);
            const now = new Date();
            setRangeStart(new Date(now.getTime() - ms));
            setRangeEnd(now);
          }
        } else if (q.includes("start=")) {
          setTimeRange("custom");
        }

        if (action === "skip-search") {
          skipNextSearchRef.current = false;
          return;
        }

        if (isFollowing) {
          stopFollow();
        }
        resetFollow();
        loadMoreGateRef.current = false;
        setSelectedRecord(null);
        scrollToSelectedRef.current = false;
        logScrollRef.current?.scrollTo(0, 0);
        search(q, false, !hasPipeOutsideQuotes(q));
        if (hasPipeOutsideQuotes(q)) {
          histogramReset();
        } else {
          const timechartExpr = q.trim()
            ? `${q.trim()} | timechart 50 by level`
            : `| timechart 50 by level`;
          histogramSearch(timechartExpr, false, true);
        }
        if (showPlan) explain(q);
        return;
      }
    }
  }, [q, isFollowMode]); // eslint-disable-line react-hooks/exhaustive-deps

  // On mount: focus input, seed default time range if no URL query.
  useEffect(() => {
    queryInputRef.current?.focus();
    if (!q) {
      const ms = timeRangeMs[timeRange];
      if (ms) {
        const now = new Date();
        setRangeStart(new Date(now.getTime() - ms));
        setRangeEnd(now);
      }
      const initial = injectTimeRange("", timeRange, isReversed);
      navigate({ search: (prev) => ({ ...prev, q: initial }), replace: true });
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Infinite scroll: observe a sentinel div at the bottom of the results.
  useEffect(() => {
    const sentinel = sentinelRef.current;
    const scrollEl = logScrollRef.current;
    if (!sentinel) return;

    const openGate = () => { loadMoreGateRef.current = true; };
    scrollEl?.addEventListener("scroll", openGate, { passive: true, once: true });

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting && hasMore && !isSearching && loadMoreGateRef.current) {
          loadMoreGateRef.current = false;
          loadMore(expressionRef.current);
        }
      },
      { root: scrollEl, rootMargin: "0px 0px 200px 0px" },
    );
    observer.observe(sentinel);
    return () => {
      observer.disconnect();
      scrollEl?.removeEventListener("scroll", openGate);
    };
  }, [hasMore, isSearching, loadMore]);

  // Follow mode: track scroll position and auto-reset new-record counter.
  useEffect(() => {
    const el = logScrollRef.current;
    if (!el || !isFollowMode) {
      setIsScrolledDown(false);
      return;
    }
    const onScroll = () => {
      const scrolled = el.scrollTop > 50;
      setIsScrolledDown(scrolled);
      if (!scrolled) resetFollowNewCount();
    };
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, [isFollowMode, resetFollowNewCount]);

  // After search completes, scroll the selected row into view.
  const prevSearchingRef = useRef(false);
  const scrollToSelectedRef = useRef(false);
  useEffect(() => {
    if (prevSearchingRef.current && !isSearching) {
      if (selectedRowRef.current) {
        selectedRowRef.current.scrollIntoView({ block: "center" });
        scrollToSelectedRef.current = false;
      } else if (selectedRecord && hasMore) {
        scrollToSelectedRef.current = true;
        loadMore(expressionRef.current);
      }
    }
    prevSearchingRef.current = isSearching;
  }, [isSearching]); // eslint-disable-line react-hooks/exhaustive-deps

  // When new records arrive during auto-pagination, check if selected row appeared.
  useEffect(() => {
    if (!scrollToSelectedRef.current || isSearching) return;
    if (selectedRowRef.current) {
      selectedRowRef.current.scrollIntoView({ block: "center" });
      scrollToSelectedRef.current = false;
    } else if (hasMore) {
      loadMore(expressionRef.current);
    } else {
      scrollToSelectedRef.current = false;
    }
  }, [records.length]); // eslint-disable-line react-hooks/exhaustive-deps

  const executeQuery = () => {
    setShowHistory(false);
    setShowSavedQueries(false);
    const normalized = normalizeTimeDirectives(draft);
    if (normalized === q && !isFollowMode) {
      setSelectedRecord(null);
      scrollToSelectedRef.current = false;
      logScrollRef.current?.scrollTo(0, 0);
      search(q, false, !hasPipeOutsideQuotes(q));
      if (hasPipeOutsideQuotes(q)) {
        histogramReset();
      } else {
        const timechartExpr = q.trim()
          ? `${q.trim()} | timechart 50 by level`
          : `| timechart 50 by level`;
        histogramSearch(timechartExpr, false, true);
      }
      if (showPlan) explain(q);
    } else {
      navigate({ to: "/search", search: (prev) => ({ ...prev, q: normalized }), replace: false });
    }
  };

  const startFollow = () => {
    setShowHistory(false);
    setShowSavedQueries(false);
    setFollowReversed(isReversed);
    const stripped = draft
      .replace(/\blast=\S+/g, "")
      .replace(/\bstart=\S+/g, "")
      .replace(/\bend=\S+/g, "")
      .replace(/\breverse=\S+/g, "")
      .replace(/\s+/g, " ")
      .trim();
    navigate({ to: "/follow", search: (prev) => ({ ...prev, q: stripped }), replace: false });
  };

  const stopFollowMode = () => {
    stopFollow();
    setRecords([...followRecords]);
    skipNextSearchRef.current = true;
    const base = stripTimeRange(draft);
    const restored = injectTimeRange(base, timeRange, followReversed);
    navigate({ to: "/search", search: (prev) => ({ ...prev, q: restored }), replace: false });
  };

  const handleShowPlan = () => {
    const next = !showPlan;
    setShowPlan(next);
    if (next) explain(q);
  };

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

  const [followReversed, setFollowReversed] = useState(true);

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

  const handleContextRecordSelect = (rec: ProtoRecord) => {
    const ts = rec.writeTs?.toDate();
    if (ts) {
      const start = new Date(ts.getTime() - 30_000);
      const end = new Date(ts.getTime() + 30_000);
      const newQuery = `start=${start.toISOString()} end=${end.toISOString()} reverse=true`;
      setTimeRange("custom");
      setRangeStart(start);
      setRangeEnd(end);
      setSelectedRecord(rec);
      navigate({
        to: "/search",
        search: (prev) => ({ ...prev, q: newQuery }),
        replace: false,
      });
    } else {
      setSelectedRecord(rec);
    }
  };

  const histogramData = histogramTableResult
    ? tableResultToHistogramData(histogramTableResult.columns, histogramTableResult.rows)
    : null;
  const liveHistogramData = useLiveHistogram(followRecords);
  const tokens = extractTokens(q);
  const draftHasErrors = !validation.valid;
  const draftIsPipeline = validation.expression === deferredDraft
    ? validation.hasPipeline
    : hasPipeOutsideQuotes(deferredDraft);
  const isRawQuery = queryHasRaw(q);
  const rawTableResult = isRawQuery && !tableResult && records.length > 0
    ? recordsToRawTable(records)
    : null;
  const effectiveTableResult = tableResult ?? rawTableResult;
  const isPipelineResult = effectiveTableResult !== null;
  const queryIsPipeline = hasPipeOutsideQuotes(q);

  useEffect(() => {
    if (!pollInterval || isFollowMode) return;
    const id = setInterval(() => {
      search(q, false, false, true);
      if (!hasPipeOutsideQuotes(q)) {
        const timechartExpr = q.trim()
          ? `${q.trim()} | timechart 50 by level`
          : `| timechart 50 by level`;
        histogramSearch(timechartExpr, false, false, true);
      }
    }, pollInterval);
    return () => clearInterval(id);
  }, [pollInterval, isFollowMode, q, search, histogramSearch]);

  const displayRecords = isFollowMode ? followRecords : records;
  const attrFields = aggregateFields(displayRecords, "attrs");
  const kvFields = aggregateFields(displayRecords, "kv");
  const allFields = (() => {
    const seen = new Set<string>();
    const merged = [];
    for (const f of [...attrFields, ...kvFields]) {
      if (!seen.has(f.key)) {
        seen.add(f.key);
        merged.push(f);
      }
    }
    return merged;
  })();
  const baseFieldNames = allFields.map((f) => f.key);
  const pipeFields = usePipelineFields(deferredDraft, cursorRef.current, baseFieldNames);
  const autocomplete = useAutocomplete(deferredDraft, cursorRef.current, allFields, syntax, pipeFields.fields, pipeFields.completions);

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

  const handleSpanClick = (value: string) => {
    const text = draft;
    const pos = cursorRef.current;

    // Insert at cursor position with surrounding spaces
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

  // Consolidated brush select: sets custom time range and navigates.
  const handleBrushSelect = (start: Date, end: Date) => {
    setRangeStart(start);
    setRangeEnd(end);
    setTimeRange("custom");
    const rangeTokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=${isReversed}`;
    const base = stripTimeRange(q);
    const newQuery = base ? `${rangeTokens} ${base}` : rangeTokens;
    setUrlQuery(newQuery);
  };

  // Follow-mode brush: same as handleBrushSelect but navigates to /search with reverse=true.
  const handleFollowBrushSelect = (start: Date, end: Date) => {
    setRangeStart(start);
    setRangeEnd(end);
    setTimeRange("custom");
    const rangeTokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=true`;
    const base = stripTimeRange(q);
    const newQuery = base ? `${rangeTokens} ${base}` : rangeTokens;
    navigate({
      to: "/search",
      search: (prev) => ({ ...prev, q: newQuery }),
      replace: false,
    });
  };

  // Pan handler — identical to search-mode brush.
  const handlePan = (start: Date, end: Date) => {
    handleBrushSelect(start, end);
  };

  // Zoom out from selected record: doubles the visible time span.
  const handleZoomOut = () => {
    const anchor = selectedRecord?.writeTs?.toDate();
    if (!anchor) return;
    const curStart = rangeStart?.getTime() ?? anchor.getTime() - 30_000;
    const curEnd = rangeEnd?.getTime() ?? anchor.getTime() + 30_000;
    const span = curEnd - curStart;
    const mid = anchor.getTime();
    const newStart = new Date(mid - span);
    const newEnd = new Date(mid + span);
    setTimeRange("custom");
    setRangeStart(newStart);
    setRangeEnd(newEnd);
    const newQuery = `start=${newStart.toISOString()} end=${newEnd.toISOString()} reverse=${isReversed}`;
    navigate({
      to: "/search",
      search: (prev) => ({ ...prev, q: newQuery }),
      replace: false,
    });
  };

  const cpuPercent = stats?.processCpuPercent ?? 0;
  const memoryBytes = stats?.processMemoryBytes ?? BigInt(0);
  const totalBytes = stats?.totalBytes ?? BigInt(0);
  const totalRecords = stats?.totalRecords ?? BigInt(0);

  const c = useThemeClass(dark);

  return {
    // URL / routing
    q, navigate, isFollowMode,
    helpParam, settingsParam, inspectorParam,

    // Draft / input
    draft, setDraft, cursorRef, queryInputRef,
    draftHasErrors, draftIsPipeline,
    autocomplete, validation,

    // Theme
    dark, theme, setTheme, highlightMode, setHighlightMode, palette, setPalette, c,

    // Panel layout
    isTablet,
    sidebarWidth, sidebarCollapsed, setSidebarCollapsed, sidebarResizeProps,
    detailWidth, detailCollapsed, setDetailCollapsed, detailPinned, setDetailPinned,
    detailResizeProps, resizing,

    // Time range
    timeRange, rangeStart, rangeEnd,
    handleTimeRange, handleCustomRange,

    // Dialogs
    showPlan, setShowPlan, showHistory, setShowHistory,
    showSavedQueries, setShowSavedQueries,
    showChangePassword, setShowChangePassword,
    showPreferences, setShowPreferences,
    inspectorGlow,
    openHelp, openSettings, openInspector,

    // Search
    records, isSearching, hasMore, effectiveTableResult,
    isPipelineResult, isRawQuery, queryIsPipeline,
    search, cancelSearch,
    tokens, displayRecords,
    selectedRecord, setSelectedRecord,
    selectedRowRef, logScrollRef, sentinelRef,
    executeQuery,

    // Follow
    followRecords, isFollowing, reconnecting, reconnectAttempt,
    followNewCount, resetFollowNewCount,
    followBufferSize, handleFollowBufferSizeChange,
    followReversed, isReversed,
    isScrolledDown,
    startFollow, stopFollowMode,

    // Histogram
    histogramData, histogramTableResult,
    liveHistogramData,
    handleBrushSelect, handleFollowBrushSelect, handlePan,
    handleSegmentClick,

    // Handlers
    toggleReverse, handleShowPlan, handleZoomOut,
    handleVaultSelect, handleChunkSelect, handlePosSelect,
    handleContextRecordSelect, handleFieldSelect,
    handleSpanClick, handleTokenToggle,
    toggleSeverity,

    // Sidebar data
    vaults, vaultsLoading, statsLoading,
    selectedVault, activeSeverities,
    attrFields, kvFields, totalRecords,
    cpuPercent, memoryBytes, totalBytes,

    // Query history / saved
    queryHistory, savedQueries, putSavedQuery, deleteSavedQuery,

    // Explain
    explainChunks, explainDirection, explainTotalChunks,
    explainExpression, explainPipelineStages, isExplaining,

    // Context (for detail panel)
    contextBefore, contextAfter, contextLoading,

    // Polling
    pollInterval, setPollInterval,

    // Auth
    logout, currentUser,
    addToast,
  };
}
