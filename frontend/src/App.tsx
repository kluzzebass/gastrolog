import { useState, useRef, useEffect, useCallback, useMemo } from "react";
import { useIsFetching } from "@tanstack/react-query";
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
} from "./api/hooks";
import { useStores, useStats, useLogout, useCurrentUser } from "./api/hooks";
import { Record as ProtoRecord } from "./api/client";

import { timeRangeMs, aggregateFields, sameRecord } from "./utils";
import {
  stripTimeRange,
  stripChunk,
  stripPos,
  stripSeverity,
  injectTimeRange,
  injectStore,
  buildSeverityExpr,
} from "./utils/queryHelpers";
import { usePanelResize } from "./hooks/usePanelResize";
import { useThemeSync } from "./hooks/useThemeSync";
import { EmptyState } from "./components/EmptyState";
import { LogEntry } from "./components/LogEntry";
import { HistogramChart } from "./components/HistogramChart";
import { ExplainPanel } from "./components/ExplainPanel";
import { SearchSidebar } from "./components/SearchSidebar";
import { DetailSidebar } from "./components/DetailSidebar";
import { ToastProvider, useToast } from "./components/Toast";
import {
  SettingsDialog,
  type SettingsTab,
} from "./components/settings/SettingsDialog";
import {
  InspectorDialog,
  type InspectorTab,
} from "./components/inspector/InspectorDialog";
import { useQueryHistory } from "./hooks/useQueryHistory";
import {
  useSavedQueries,
  usePutSavedQuery,
  useDeleteSavedQuery,
} from "./api/hooks/useSavedQueries";
import { ChangePasswordDialog } from "./components/ChangePasswordDialog";
import { Dialog, CloseButton } from "./components/Dialog";
import { tokenize } from "./queryTokenizer";
import { useAutocomplete } from "./hooks/useAutocomplete";
import { HeaderBar } from "./components/HeaderBar";
import { ResultsToolbar } from "./components/ResultsToolbar";
import { QueryBar } from "./components/QueryBar";

export function App() {
  return (
    <ToastProvider>
      <AppContent />
    </ToastProvider>
  );
}

function AppContent() {
  const { q } = useRouterSearch({ strict: false }) as { q: string };
  const navigate = useNavigate({ from: "/search" });
  const location = useLocation();
  const isFollowMode = location.pathname === "/follow";
  const [draft, setDraft] = useState(q);
  const [cursorPos, setCursorPos] = useState(0);
  const [selectedStore, setSelectedStore] = useState("all");
  const [timeRange, setTimeRange] = useState("1h");
  const [rangeStart, setRangeStart] = useState<Date | null>(null);
  const [rangeEnd, setRangeEnd] = useState<Date | null>(null);
  const [showPlan, setShowPlan] = useState(false);
  const [showHelp, setShowHelp] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [settingsTab, setSettingsTab] = useState<SettingsTab>("service");
  const [showInspector, setShowInspector] = useState(false);
  const [inspectorTab, setInspectorTab] = useState<InspectorTab>("stores");
  const fetchCount = useIsFetching();
  const [inspectorGlow, setInspectorGlow] = useState(false);
  const glowTimer = useRef<ReturnType<typeof setTimeout>>(null);
  useEffect(() => {
    if (fetchCount > 0) {
      setInspectorGlow(true);
      if (glowTimer.current) clearTimeout(glowTimer.current);
      glowTimer.current = setTimeout(() => setInspectorGlow(false), 800);
    }
  }, [fetchCount]);
  const [selectedRecord, setSelectedRecord] = useState<ProtoRecord | null>(
    null,
  );
  const { theme, setTheme, dark } = useThemeSync();
  const [detailWidth, setDetailWidth] = useState(320);
  const [sidebarWidth, setSidebarWidth] = useState(224);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [detailCollapsed, setDetailCollapsed] = useState(true);
  const [detailPinned, setDetailPinned] = useState(false);
  const { handleResize: handleDetailResize, resizing: detailResizing } =
    usePanelResize(setDetailWidth, 240, 600, "right");
  const { handleResize: handleSidebarResize, resizing: sidebarResizing } =
    usePanelResize(setSidebarWidth, 160, 400, "left");
  const resizing = detailResizing || sidebarResizing;

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
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [detailPinned, showPlan]);

  const [showHistory, setShowHistory] = useState(false);
  const [showSavedQueries, setShowSavedQueries] = useState(false);
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
  const [isScrolledDown, setIsScrolledDown] = useState(false);


  const {
    records,
    isSearching,
    error: searchError,
    hasMore,
    search,
    loadMore,
    setRecords,
    reset: resetSearch,
  } = useSearch();
  const {
    records: followRecords,
    isFollowing,
    reconnecting,
    reconnectAttempt,
    error: followError,
    newCount: followNewCount,
    follow,
    stop: stopFollow,
    reset: resetFollow,
    resetNewCount: resetFollowNewCount,
  } = useFollow();
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

  const { addToast } = useToast();
  const logout = useLogout();
  const currentUser = useCurrentUser();
  const [showChangePassword, setShowChangePassword] = useState(false);

  // Push errors from hooks to the toast system.
  useEffect(() => {
    if (searchError) {
      addToast(searchError.message, "error");
    }
  }, [searchError]); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => {
    if (followError) {
      addToast(followError.message, "error");
    }
  }, [followError]); // eslint-disable-line react-hooks/exhaustive-deps

  // Whether results are in reverse (newest-first) order.
  const isReversed = !q.includes("reverse=false");

  // Navigate to a new query — pushes browser history, preserving current route.
  const setUrlQuery = useCallback(
    (newQ: string) => {
      navigate({
        to: isFollowMode ? "/follow" : "/search",
        search: { q: newQ },
        replace: false,
      });
    },
    [navigate, isFollowMode],
  );

  // Sync draft when URL changes (browser back/forward).
  useEffect(() => {
    setDraft(q);
  }, [q]);

  // Fire search or follow depending on the current route.
  useEffect(() => {
    expressionRef.current = q;
    queryHistory.add(q);

    // Sync sidebar preset and range display from last= in the URL.
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

    if (isFollowMode) {
      // On /follow: stop any in-flight search, start following.
      resetSearch();
      resetFollow();
      follow(q);
    } else {
      // On /search: stop any active follow, start searching.
      if (isFollowing) {
        stopFollow();
      }
      resetFollow();
      // When transitioning from follow → search via the stop button,
      // skip the auto-search so the accumulated follow records stay visible.
      if (skipNextSearchRef.current) {
        skipNextSearchRef.current = false;
        return;
      }
      search(q);
      fetchHistogram(q);
      if (showPlan) explain(q);
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
      navigate({ search: { q: initial }, replace: true });
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Infinite scroll: observe a sentinel div at the bottom of the results.
  useEffect(() => {
    const sentinel = sentinelRef.current;
    if (!sentinel) return;

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting && hasMore && !isSearching) {
          loadMore(expressionRef.current);
        }
      },
      { root: logScrollRef.current, rootMargin: "0px 0px 200px 0px" },
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
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
    if (draft === q && !isFollowMode) {
      // Query unchanged — re-run the search directly since the URL
      // won't change and the effect won't fire.
      search(q);
      fetchHistogram(q);
      if (showPlan) explain(q);
    } else {
      navigate({ to: "/search", search: { q: draft }, replace: false });
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
    navigate({ to: "/follow", search: { q: stripped }, replace: false });
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
    navigate({ to: "/search", search: { q: restored }, replace: false });
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

  const handleTimeRange = (range: string) => {
    setTimeRange(range);
    if (range === "All") {
      setRangeStart(null);
      setRangeEnd(null);
    } else {
      const ms = timeRangeMs[range];
      if (ms) {
        const now = new Date();
        setRangeStart(new Date(now.getTime() - ms));
        setRangeEnd(now);
      }
    }
    const newQuery = injectTimeRange(q, range, isReversed);
    // Time ranges imply search mode — switch away from follow if active.
    navigate({ to: "/search", search: { q: newQuery }, replace: false });
  };

  const handleCustomRange = (start: Date, end: Date) => {
    setTimeRange("custom");
    setRangeStart(start);
    setRangeEnd(end);
    const tokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=${isReversed}`;
    const base = stripTimeRange(q);
    const newQuery = base ? `${tokens} ${base}` : tokens;
    // Time ranges imply search mode — switch away from follow if active.
    navigate({ to: "/search", search: { q: newQuery }, replace: false });
  };

  // In follow mode, sort direction is purely a display concern (local state).
  const [followReversed, setFollowReversed] = useState(true);

  const toggleReverse = () => {
    if (isFollowMode) {
      setFollowReversed((prev) => !prev);
    } else {
      const newQuery = injectTimeRange(q, timeRange, !isReversed);
      setUrlQuery(newQuery);
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
  const draftHasErrors = useMemo(() => tokenize(draft).hasErrors, [draft]);
  const displayRecords = isFollowMode ? followRecords : records;
  const attrFields = useMemo(
    () => aggregateFields(displayRecords, "attrs"),
    [displayRecords],
  );
  const kvFields = useMemo(
    () => aggregateFields(displayRecords, "kv"),
    [displayRecords],
  );
  const allFields = useMemo(() => {
    const seen = new Set<string>();
    const merged = [];
    for (const f of [...attrFields, ...kvFields]) {
      if (!seen.has(f.key)) {
        seen.add(f.key);
        merged.push(f);
      }
    }
    return merged;
  }, [attrFields, kvFields]);
  const autocomplete = useAutocomplete(draft, cursorPos, allFields);

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

  const handleTokenToggle = (token: string) => {
    if (q.includes(token)) {
      const newQuery = q.replace(token, "").replace(/\s+/g, " ").trim();
      setUrlQuery(newQuery);
    } else {
      const newQuery = q.trim() ? `${q.trim()} ${token}` : token;
      setUrlQuery(newQuery);
    }
  };

  const totalRecords = stats?.totalRecords ?? BigInt(0);
  const totalStores = stats?.totalStores ?? BigInt(0);
  const sealedChunks = stats?.sealedChunks ?? BigInt(0);
  const totalBytes = stats?.totalBytes ?? BigInt(0);

  // Theme-aware class helper
  const c = (darkCls: string, lightCls: string) => (dark ? darkCls : lightCls);

  return (
    <div
      className={`grain h-screen overflow-hidden flex flex-col font-body text-base ${c("bg-ink text-text-normal", "light-theme bg-light-bg text-light-text-normal")}`}
    >
      <a href="#main-content" className="skip-link">Skip to main content</a>

      <HeaderBar
        dark={dark}
        theme={theme}
        setTheme={setTheme}
        statsLoading={statsLoading}
        totalRecords={totalRecords}
        totalStores={totalStores}
        sealedChunks={sealedChunks}
        totalBytes={totalBytes}
        inspectorGlow={inspectorGlow}
        onShowInspector={() => setShowInspector(true)}
        onShowSettings={() => setShowSettings(true)}
        currentUser={currentUser ? { username: currentUser.username, role: currentUser.role } : null}
        onChangePassword={() => setShowChangePassword(true)}
        onLogout={logout}
      />

      {/* ── Main Layout ── */}
      <div className="flex flex-1 overflow-hidden">
        <SearchSidebar
          dark={dark}
          sidebarWidth={sidebarWidth}
          sidebarCollapsed={sidebarCollapsed}
          setSidebarCollapsed={setSidebarCollapsed}
          handleSidebarResize={handleSidebarResize}
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
            showHelp={showHelp}
            setShowHelp={setShowHelp}
            executeQuery={executeQuery}
            isSearching={isSearching}
            isFollowMode={isFollowMode}
            startFollow={startFollow}
            stopFollowMode={stopFollowMode}
            draftHasErrors={draftHasErrors}
            showPlan={showPlan}
            handleShowPlan={handleShowPlan}
          />

          {/* Execution Plan Dialog */}
          {showPlan && (
            <Dialog
              onClose={() => setShowPlan(false)}
              ariaLabel="Query Execution Plan"
              dark={dark}
              size="lg"
            >
              <CloseButton onClick={() => setShowPlan(false)} dark={dark} />
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

          {showSettings && (
            <SettingsDialog
              dark={dark}
              tab={settingsTab}
              onTabChange={setSettingsTab}
              onClose={() => setShowSettings(false)}
              isAdmin={currentUser?.role === "admin"}
            />
          )}

          {showInspector && (
            <InspectorDialog
              dark={dark}
              tab={inspectorTab}
              onTabChange={setInspectorTab}
              onClose={() => setShowInspector(false)}
            />
          )}

          {/* Histogram — server-side for search, client-side for follow */}
          {!isFollowMode &&
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
                      search: { q: newQuery },
                      replace: false,
                    });
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
            <ResultsToolbar
              dark={dark}
              isFollowMode={isFollowMode}
              isReversed={isReversed}
              followReversed={followReversed}
              toggleReverse={toggleReverse}
              selectedRecord={selectedRecord}
              rangeStart={rangeStart}
              rangeEnd={rangeEnd}
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
                  search: { q: newQuery },
                  replace: false,
                });
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
                          key={i}
                          ref={selected ? selectedRowRef : undefined}
                          record={record}
                          tokens={tokens}
                          isSelected={selected}
                          onSelect={() =>
                            setSelectedRecord(selected ? null : record)
                          }
                          onFilterToggle={handleTokenToggle}
                          dark={dark}
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
          </div>
        </main>

        <DetailSidebar
          dark={dark}
          detailWidth={detailWidth}
          detailCollapsed={detailCollapsed}
          setDetailCollapsed={setDetailCollapsed}
          detailPinned={detailPinned}
          setDetailPinned={setDetailPinned}
          handleDetailResize={handleDetailResize}
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
                search: { q: newQuery },
                replace: false,
              });
            } else {
              setSelectedRecord(rec);
            }
          }}
        />
      </div>
    </div>
  );
}

export default App;
