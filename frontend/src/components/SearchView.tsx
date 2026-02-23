import { useState, useRef, useEffect } from "react";
import {
  useSearch as useRouterSearch,
  useNavigate,
  useLocation,
} from "@tanstack/react-router";
import {
  useSearch,
  useFollow,
  useExplain,
  useHistogram,
  useLiveHistogram,
  useRecordContext,
  extractTokens,
} from "../api/hooks";
import { useStores, useStats, useLogout, useCurrentUser } from "../api/hooks";
import { Record as ProtoRecord, getToken } from "../api/client";

import { timeRangeMs, aggregateFields, sameRecord } from "../utils";
import {
  stripTimeRange,
  stripChunk,
  stripPos,
  stripSeverity,
  injectTimeRange,
  injectStore,
  buildSeverityExpr,
  resolveQueryEffectAction,
} from "../utils/queryHelpers";
import { normalizeTimeDirectives } from "../utils/normalizeTimeDirectives";
import { useThemeSync } from "../hooks/useThemeSync";
import { useThemeClass } from "../hooks/useThemeClass";
import { useDialogState } from "../hooks/useDialogState";
import { usePanelLayout } from "../hooks/usePanelLayout";
import { useTimeRange } from "../hooks/useTimeRange";
import { EmptyState } from "./EmptyState";
import { LogEntry } from "./LogEntry";
import { HistogramChart } from "./HistogramChart";
import { SearchSidebar } from "./SearchSidebar";
import { DetailSidebar } from "./DetailSidebar";
import { useToast } from "./Toast";
import { useQueryHistory } from "../hooks/useQueryHistory";
import {
  useSavedQueries,
  usePutSavedQuery,
  useDeleteSavedQuery,
} from "../api/hooks/useSavedQueries";
import { Dialog } from "./Dialog";
import { tokenize, hasPipeOutsideQuotes, type SyntaxSets } from "../queryTokenizer";
import { useAutocomplete } from "../hooks/useAutocomplete";
import { HeaderBar } from "./HeaderBar";
import { PipelineResults } from "./PipelineResults";

import { ExplainPanel } from "./ExplainPanel";
import { SettingsDialog } from "./settings/SettingsDialog";
import { InspectorDialog } from "./inspector/InspectorDialog";
import { ChangePasswordDialog } from "./ChangePasswordDialog";
import { PreferencesDialog } from "./PreferencesDialog";
import { HelpDialog } from "./HelpDialog";
import { HelpProvider } from "../hooks/useHelp";
import { ResultsToolbar } from "./ResultsToolbar";
import { QueryBar } from "./QueryBar";
import { useConfig, useServerConfig } from "../api/hooks/useConfig";
import { useSyntax } from "../api/hooks/useSyntax";

export function SearchView() {
  const { q, help: helpParam, settings: settingsParam, inspector: inspectorParam } = useRouterSearch({ strict: false }) as { q: string; help?: string; settings?: string; inspector?: string };
  const navigate = useNavigate();
  const location = useLocation();
  const isFollowMode = location.pathname === "/follow";

  // Redirect to setup wizard if no stores are configured and wizard hasn't been dismissed.
  const config = useConfig();
  const serverConfig = useServerConfig();
  useEffect(() => {
    if (config.data && serverConfig.data && config.data.stores.length === 0 && !serverConfig.data.setupWizardDismissed) {
      navigate({ to: "/setup" } as any);
    }
  }, [config.data, serverConfig.data]); // eslint-disable-line react-hooks/exhaustive-deps
  const [draft, setDraft] = useState(q);
  const [cursorPos, setCursorPos] = useState(0);
  const [selectedStore, setSelectedStore] = useState("all");

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
    navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, help: topicId || "general" }) } as any);
  };

  const openSettings = (tab?: string) => {
    navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, settings: tab || "service" }) } as any);
  };

  const openInspector = (tab?: string) => {
    navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, inspector: tab || "stores" }) } as any);
  };

  const [selectedRecord, setSelectedRecord] = useState<ProtoRecord | null>(
    null,
  );
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
  } = useFollow({ onError: toastError });
  const {
    chunks: explainChunks,
    direction: explainDirection,
    totalChunks: explainTotalChunks,
    expression: explainExpression,
    isLoading: isExplaining,
    explain,
  } = useExplain();
  const {
    data: histogramData,
    isLoading: _isHistogramLoading,
    fetchHistogram,
  } = useHistogram();
  const {
    before: contextBefore,
    after: contextAfter,
    isLoading: contextLoading,
    fetchContext,
    reset: resetContext,
  } = useRecordContext();
  const { data: stores, isLoading: storesLoading } = useStores();
  const { data: stats, isLoading: statsLoading } = useStats();

  const logout = useLogout();
  const currentUser = useCurrentUser();
  const syntaxQuery = useSyntax();
  const syntax: SyntaxSets | undefined = syntaxQuery.data;

  // Navigate to a new query — pushes browser history, preserving current route.
  const setUrlQuery = (newQ: string) => {
    navigate({
      to: isFollowMode ? "/follow" : "/search",
      search: (prev: Record<string, unknown>) => ({ ...prev, q: newQ }),
      replace: false,
    } as any);
  };

  // Sync draft when URL changes (browser back/forward).
  useEffect(() => {
    setDraft(q);
  }, [q]);

  // Fire search or follow depending on the current route.
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
        // No time directives — inject default range so the query is bounded.
        const defaultRange = "5m";
        setTimeRange(defaultRange);
        const ms = timeRangeMs[defaultRange];
        if (ms) {
          const now = new Date();
          setRangeStart(new Date(now.getTime() - ms));
          setRangeEnd(now);
        }
        const fixed = injectTimeRange(q, defaultRange, isReversed);
        navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, q: fixed }), replace: true } as any);
        return;
      }

      case "skip-search":
      case "search": {
        // Sync sidebar preset and range display from the URL.
        const lastMatch = q.match(/\blast=(\S+)/);
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

        // On /search: stop any active follow, start searching.
        if (isFollowing) {
          stopFollow();
        }
        resetFollow();
        loadMoreGateRef.current = false;
        search(q, false, true);
        fetchHistogram(q);
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
      navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, q: initial }), replace: true } as any);
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Infinite scroll: observe a sentinel div at the bottom of the results.
  // The gate prevents runaway loading — it opens only when the user scrolls.
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

  // After search completes (e.g. zoom-out), scroll the selected row into view.
  // If the selected record isn't in the loaded page yet, keep loading more
  // until it appears or there are no more pages.
  const prevSearchingRef = useRef(false);
  const scrollToSelectedRef = useRef(false);
  useEffect(() => {
    if (prevSearchingRef.current && !isSearching) {
      if (selectedRowRef.current) {
        selectedRowRef.current.scrollIntoView({ block: "center" });
        scrollToSelectedRef.current = false;
      } else if (selectedRecord && hasMore) {
        // Selected record not in loaded results yet — auto-paginate.
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
      // No more pages — give up.
      scrollToSelectedRef.current = false;
    }
  }, [records.length]); // eslint-disable-line react-hooks/exhaustive-deps

  const executeQuery = () => {
    // Always search from the search route.
    setShowHistory(false);
    setShowSavedQueries(false);
    const normalized = normalizeTimeDirectives(draft);
    if (normalized === q && !isFollowMode) {
      // Query unchanged — re-run the search directly since the URL
      // won't change and the effect won't fire.
      search(q, false, true);
      fetchHistogram(q);
      if (showPlan) explain(q);
    } else {
      navigate({ to: "/search", search: (prev: Record<string, unknown>) => ({ ...prev, q: normalized }), replace: false } as any);
    }
  };

  const startFollow = () => {
    setShowHistory(false);
    setShowSavedQueries(false);
    setFollowReversed(isReversed);
    // Strip time bounds and reverse= (follow mode uses local state for sort).
    const stripped = draft
      .replace(/\blast=\S+/g, "")
      .replace(/\bstart=\S+/g, "")
      .replace(/\bend=\S+/g, "")
      .replace(/\breverse=\S+/g, "")
      .replace(/\s+/g, " ")
      .trim();
    navigate({ to: "/follow", search: (prev: Record<string, unknown>) => ({ ...prev, q: stripped }), replace: false } as any);
  };

  const stopFollowMode = () => {
    // Stop the stream and adopt follow records into search results
    // so they stay visible after the route change.
    stopFollow();
    setRecords([...followRecords]);
    skipNextSearchRef.current = true;

    // Restore time range when switching back to search,
    // preserving the sort direction from follow mode.
    const base = stripTimeRange(draft);
    const restored = injectTimeRange(base, timeRange, followReversed);
    navigate({ to: "/search", search: (prev: Record<string, unknown>) => ({ ...prev, q: restored }), replace: false } as any);
  };

  const handleShowPlan = () => {
    const next = !showPlan;
    setShowPlan(next);
    if (next) explain(q);
  };

  const allSeverities = ["error", "warn", "info", "debug", "trace"];

  // Parse which severities are active from the query string.
  const activeSeverities = allSeverities.filter((s) =>
    q.includes(`level=${s}`),
  );

  const toggleSeverity = (level: string) => {
    const current = allSeverities.filter((s) => q.includes(`level=${s}`));
    const next = current.includes(level)
      ? current.filter((s) => s !== level)
      : [...current, level];
    const base = stripSeverity(q);
    const sevExpr = buildSeverityExpr(next);
    const newQuery = sevExpr ? `${base} ${sevExpr}`.trim() : base;
    setUrlQuery(newQuery);
  };

  const handleSegmentClick = (level: string) => {
    if (level === "other") {
      // Toggle "not level=*" (records with no level attribute).
      const hasNoLevel = /\bnot\s+level=\*\b/i.test(q);
      const base = stripSeverity(q);
      const newQuery = hasNoLevel ? base : `${base} not level=*`.trim();
      setUrlQuery(newQuery);
    } else {
      toggleSeverity(level);
    }
  };

  // In follow mode, sort direction is purely a display concern (local state).
  const [followReversed, setFollowReversed] = useState(true);

  const toggleReverse = () => {
    if (isFollowMode) {
      setFollowReversed((prev) => !prev);
    } else {
      // Swap reverse= without touching start=/end=/last= tokens.
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

  const handleStoreSelect = (storeId: string) => {
    const next = selectedStore === storeId ? "all" : storeId;
    setSelectedStore(next);
    const newQuery = injectStore(q, next);
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
      // Toggle off: remove pos= and chunk=
      setUrlQuery(stripPos(stripChunk(q)));
    } else {
      // Toggle on: ensure chunk= is present, add pos=
      const base = stripPos(stripChunk(q));
      const tokens = `${chunkToken} ${posToken}`;
      setUrlQuery(base ? `${tokens} ${base}` : tokens);
    }
  };

  const liveHistogramData = useLiveHistogram(followRecords);
  const tokens = extractTokens(q);
  const { hasErrors: draftHasErrors, hasPipeline: draftIsPipeline } = tokenize(draft, syntax);
  const isPipelineResult = tableResult !== null;
  const queryIsPipeline = hasPipeOutsideQuotes(q);

  // Auto-refresh polling for pipeline results.
  const [pollInterval, setPollInterval] = useState<number | null>(null);

  useEffect(() => {
    if (!pollInterval || !isPipelineResult) return;
    const id = setInterval(() => {
      search(q, false, true);
    }, pollInterval);
    return () => clearInterval(id);
  }, [pollInterval, isPipelineResult, q, search]);

  // Clear poll interval when query changes or leaves pipeline mode.
  useEffect(() => {
    setPollInterval(null);
  }, [q]);
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
  const autocomplete = useAutocomplete(draft, cursorPos, allFields, syntax);

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

  const handleSpanClick = (value: string, shiftKey: boolean) => {
    const trimmed = draft.trim();
    if (shiftKey) {
      // Shift-click: build/extend an OR group at the end of the draft.
      if (trimmed.endsWith(")")) {
        // Find the matching ( for the trailing ).
        let depth = 0;
        let matchIdx = -1;
        for (let i = trimmed.length - 1; i >= 0; i--) {
          if (trimmed[i] === ")") depth++;
          else if (trimmed[i] === "(") {
            depth--;
            if (depth === 0) { matchIdx = i; break; }
          }
        }
        const atBoundary = matchIdx === 0 || trimmed[matchIdx - 1] === " ";
        if (matchIdx >= 0 && atBoundary && /\bOR\b/.test(trimmed.slice(matchIdx))) {
          // Extend existing OR group.
          setDraft(trimmed.slice(0, -1) + " OR " + value + ")");
          queryInputRef.current?.focus();
          return;
        }
      }
      // Wrap last token + new value in an OR group.
      const lastSpace = trimmed.lastIndexOf(" ");
      if (lastSpace >= 0) {
        const prefix = trimmed.slice(0, lastSpace);
        const lastToken = trimmed.slice(lastSpace + 1);
        setDraft(`${prefix} (${lastToken} OR ${value})`);
      } else if (trimmed) {
        setDraft(`(${trimmed} OR ${value})`);
      } else {
        setDraft(value);
      }
    } else {
      // Plain click: append value to existing query.
      setDraft(trimmed ? `${trimmed} ${value}` : value);
    }
    // Focus the query input so the user can review and press Enter.
    queryInputRef.current?.focus();
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

  const cpuPercent = stats?.processCpuPercent ?? 0;
  const memoryBytes = stats?.processMemoryBytes ?? BigInt(0);
  const totalBytes = stats?.totalBytes ?? BigInt(0);
  const totalRecords = stats?.totalRecords ?? BigInt(0);

  const c = useThemeClass(dark);

  return (
    <HelpProvider onOpen={openHelp}>
    <div className="flex flex-col flex-1 overflow-hidden">
      <a href="#main-content" className="skip-link">Skip to main content</a>

      <HeaderBar
        dark={dark}
        statsLoading={statsLoading}
        cpuPercent={cpuPercent}
        memoryBytes={memoryBytes}
        totalBytes={totalBytes}
        inspectorGlow={inspectorGlow}
        onShowHelp={() => openHelp()}
        onShowInspector={() => openInspector()}
        onShowSettings={() => openSettings()}
        currentUser={currentUser ? { username: currentUser.username, role: currentUser.role } : null}
        onPreferences={() => setShowPreferences(true)}
        onChangePassword={() => setShowChangePassword(true)}
        onLogout={logout}
      />

      {/* ── Main Layout ── */}
      <div className="flex flex-1 overflow-hidden">
        {isTablet && !sidebarCollapsed && (
          <div className="fixed inset-0 bg-black/30 z-20" onClick={() => setSidebarCollapsed(true)} />
        )}
        <SearchSidebar
          dark={dark}
          isTablet={isTablet}
          sidebarWidth={sidebarWidth}
          sidebarCollapsed={sidebarCollapsed}
          setSidebarCollapsed={setSidebarCollapsed}
          sidebarResizeProps={sidebarResizeProps}
          resizing={resizing}
          rangeStart={rangeStart}
          rangeEnd={rangeEnd}
          timeRange={timeRange}
          onTimeRangeChange={handleTimeRange}
          onCustomRange={handleCustomRange}
          stores={stores}
          storesLoading={storesLoading}
          statsLoading={statsLoading}
          totalRecords={totalRecords}
          selectedStore={selectedStore}
          onStoreSelect={handleStoreSelect}
          activeSeverities={activeSeverities}
          onToggleSeverity={toggleSeverity}
          attrFields={attrFields}
          kvFields={kvFields}
          onFieldSelect={handleFieldSelect}
          activeQuery={q}
        />

        {/* ── Main Content ── */}
        <main
          id="main-content"
          className={`flex-1 flex flex-col overflow-hidden ${c("bg-ink-raised", "bg-light-bg")}`}
        >
          <QueryBar
            dark={dark}
            draft={draft}
            setDraft={setDraft}
            setCursorPos={setCursorPos}
            queryInputRef={queryInputRef}
            autocomplete={autocomplete}
            showHistory={showHistory}
            setShowHistory={setShowHistory}
            showSavedQueries={showSavedQueries}
            setShowSavedQueries={setShowSavedQueries}
            historyEntries={queryHistory.entries}
            onHistoryRemove={queryHistory.remove}
            onHistoryClear={queryHistory.clear}
            savedQueries={savedQueries.data ?? []}
            onSaveQuery={(name, query) => putSavedQuery.mutate({ name, query })}
            onDeleteSavedQuery={(name) => deleteSavedQuery.mutate(name)}
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
            syntax={syntax}
          />

          {/* Execution Plan Dialog */}
          {showPlan && (
            <Dialog
              onClose={() => setShowPlan(false)}
              ariaLabel="Query Execution Plan"
              dark={dark}
              size="lg"
            >
              {isExplaining ? (
                <div
                  className={`text-[0.9em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Analyzing query plan...
                </div>
              ) : explainChunks.length === 0 ? (
                <div
                  className={`text-[0.9em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Run a query to see the execution plan.
                </div>
              ) : (
                  <ExplainPanel
                    chunks={explainChunks}
                    direction={explainDirection}
                    totalChunks={explainTotalChunks}
                    expression={explainExpression}
                    dark={dark}
                  />
              )}
            </Dialog>
          )}

          {/* Settings Dialog */}
          {showPreferences && (
              <PreferencesDialog
                dark={dark}
                theme={theme}
                setTheme={setTheme}
                highlightMode={highlightMode}
                setHighlightMode={setHighlightMode}
                palette={palette}
                setPalette={setPalette}
                onClose={() => setShowPreferences(false)}
              />
          )}

          {showChangePassword && currentUser && (
              <ChangePasswordDialog
                username={currentUser.username}
                dark={dark}
                onClose={() => setShowChangePassword(false)}
                onSuccess={() => {
                  setShowChangePassword(false);
                  addToast("Password changed successfully", "info");
                }}
              />
          )}

          {settingsParam && (
              <SettingsDialog
                dark={dark}
                tab={settingsParam as any}
                onTabChange={(tab) => navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, settings: tab }) } as any)}
                onClose={() => navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, settings: undefined }) } as any)}
                isAdmin={currentUser?.role === "admin" || getToken() === "no-auth"}
                noAuth={getToken() === "no-auth"}
              />
          )}

          {inspectorParam && (
              <InspectorDialog
                dark={dark}
                tab={inspectorParam as any}
                onTabChange={(tab) => navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, inspector: tab }) } as any)}
                onClose={() => navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, inspector: undefined }) } as any)}
              />
          )}

          {helpParam && (
              <HelpDialog
                dark={dark}
                topicId={helpParam}
                onClose={() => navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, help: undefined }) } as any)}
                onNavigate={(id) => navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, help: id }) } as any)}
                onOpenSettings={(tab) => navigate({ search: (prev: Record<string, unknown>) => ({ ...prev, help: undefined, settings: tab }) } as any)}
              />
          )}

          {/* Histogram — server-side for search, client-side for follow */}
          {!isFollowMode &&
            !isPipelineResult &&
            histogramData &&
            histogramData.buckets.length > 0 && (
              <div
                className={`px-5 py-3 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
              >
                <HistogramChart
                  data={histogramData}
                  dark={dark}
                  onBrushSelect={(start, end) => {
                    setRangeStart(start);
                    setRangeEnd(end);
                    setTimeRange("custom");
                    const tokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=${isReversed}`;
                    const base = stripTimeRange(q);
                    const newQuery = base ? `${tokens} ${base}` : tokens;
                    setUrlQuery(newQuery);
                  }}
                  onPan={(start, end) => {
                    setRangeStart(start);
                    setRangeEnd(end);
                    setTimeRange("custom");
                    const tokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=${isReversed}`;
                    const base = stripTimeRange(q);
                    const newQuery = base ? `${tokens} ${base}` : tokens;
                    setUrlQuery(newQuery);
                  }}
                  onSegmentClick={handleSegmentClick}
                />
              </div>
            )}
          {isFollowMode && (
            <div
              className={`px-5 py-3 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
            >
              {liveHistogramData && liveHistogramData.buckets.length > 0 ? (
                <HistogramChart
                  data={liveHistogramData}
                  dark={dark}
                  onBrushSelect={(start, end) => {
                    setRangeStart(start);
                    setRangeEnd(end);
                    setTimeRange("custom");
                    const tokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=true`;
                    const base = stripTimeRange(q);
                    const newQuery = base ? `${tokens} ${base}` : tokens;
                    navigate({
                      to: "/search",
                      search: (prev: Record<string, unknown>) => ({ ...prev, q: newQuery }),
                      replace: false,
                    } as any);
                  }}
                  onSegmentClick={handleSegmentClick}
                />
              ) : (
                <div className="relative">
                  <div className="flex items-baseline justify-between mb-1.5">
                    <span
                      className={`text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                    >
                      Volume
                    </span>
                    <span
                      className={`font-mono text-[0.75em] ${c("text-text-muted", "text-light-text-muted")}`}
                    >
                      0 records
                    </span>
                  </div>
                  <div
                    className={`rounded h-12 ${c("bg-ink-surface/30", "bg-light-hover/30")}`}
                  />
                  <div className="flex justify-between mt-1 min-h-5">
                    <span
                      className={`text-[0.65em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
                    >
                      &mdash;
                    </span>
                    <span
                      className={`text-[0.65em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
                    >
                      &mdash;
                    </span>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Results */}
          <div className="flex-1 flex flex-col overflow-hidden">
            {isSearching && !tableResult && queryIsPipeline ? (
              <div className="flex-1 flex items-center justify-center">
                <div className={`text-center font-mono text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
                  <div className="inline-block w-5 h-5 border-2 border-current border-t-transparent rounded-full animate-spin mb-3" />
                  <div>Running pipeline...</div>
                </div>
              </div>
            ) : isPipelineResult ? (
              <PipelineResults
                tableResult={tableResult}
                dark={dark}
                pollInterval={pollInterval}
                onPollIntervalChange={setPollInterval}
              />
            ) : (
            <>
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
              onZoomOut={() => {
                const anchor = selectedRecord?.writeTs?.toDate();
                if (!anchor) return;
                const curStart =
                  rangeStart?.getTime() ?? anchor.getTime() - 30_000;
                const curEnd =
                  rangeEnd?.getTime() ?? anchor.getTime() + 30_000;
                const span = curEnd - curStart;
                const mid = anchor.getTime();
                const newStart = new Date(mid - span);
                const newEnd = new Date(mid + span);
                setTimeRange("custom");
                setRangeStart(newStart);
                setRangeEnd(newEnd);
                const newQuery = `start=${newStart.toISOString()} end=${newEnd.toISOString()} reverse=${isReversed}`;
                setSelectedRecord(selectedRecord);
                navigate({
                  to: "/search",
                  search: (prev: Record<string, unknown>) => ({ ...prev, q: newQuery }),
                  replace: false,
                } as any);
              }}
            />

            <div className="relative flex-1 overflow-hidden">
              {/* "N new logs" floating badge */}
              {isFollowMode && isScrolledDown && followNewCount > 0 && (
                <button
                  onClick={() => {
                    logScrollRef.current?.scrollTo({
                      top: 0,
                      behavior: "smooth",
                    });
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
                {(isFollowMode ? followRecords : records).length === 0 &&
                !isSearching &&
                !isFollowMode ? (
                  <EmptyState dark={dark} />
                ) : (isFollowMode ? followRecords : records).length === 0 &&
                  isFollowMode ? (
                  <div
                    className={`py-8 text-center text-[0.85em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
                  >
                    Waiting for new records...
                  </div>
                ) : (
                  <div>
                    {(isFollowMode
                      ? followReversed
                        ? followRecords
                        : [...followRecords].reverse()
                      : records
                    ).map((record, i) => {
                      const selected = sameRecord(selectedRecord, record);
                      return (
                        <LogEntry
                          key={record.ref ? `${record.ref.storeId}:${record.ref.chunkId}:${record.ref.pos}` : `follow-${i}`}
                          ref={selected ? selectedRowRef : undefined}
                          record={record}
                          tokens={tokens}
                          isSelected={selected}
                          onSelect={() =>
                            setSelectedRecord(selected ? null : record)
                          }
                          onFilterToggle={handleTokenToggle}
                          onSpanClick={handleSpanClick}
                          dark={dark}
                          highlightMode={highlightMode}
                        />
                      );
                    })}
                    {/* Infinite scroll sentinel (search only) */}
                    {!isFollowMode && <div ref={sentinelRef} className="h-1" />}
                    {isSearching && records.length > 0 && (
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
            </>
            )}
          </div>
        </main>

        {isTablet && !detailCollapsed && (
          <div className="fixed inset-0 bg-black/30 z-20" onClick={() => setDetailCollapsed(true)} />
        )}
        <DetailSidebar
          dark={dark}
          isTablet={isTablet}
          detailWidth={detailWidth}
          detailCollapsed={detailCollapsed}
          setDetailCollapsed={setDetailCollapsed}
          detailPinned={detailPinned}
          setDetailPinned={setDetailPinned}
          detailResizeProps={detailResizeProps}
          resizing={resizing}
          selectedRecord={selectedRecord}
          onFieldSelect={handleFieldSelect}
          onChunkSelect={handleChunkSelect}
          onStoreSelect={handleStoreSelect}
          onPosSelect={handlePosSelect}
          contextBefore={contextBefore}
          contextAfter={contextAfter}
          contextLoading={contextLoading}
          contextReversed={isReversed}
          highlightMode={highlightMode}
          onContextRecordSelect={(rec) => {
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
                search: (prev: Record<string, unknown>) => ({ ...prev, q: newQuery }),
                replace: false,
              } as any);
            } else {
              setSelectedRecord(rec);
            }
          }}
        />
      </div>
    </div>
    </HelpProvider>
  );
}
