import { useState, useRef, useEffect, useCallback, useMemo } from "react";
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
  extractTokens,
} from "./api/hooks";
import { useStores, useStats } from "./api/hooks";
import { Record as ProtoRecord } from "./api/client";

import { timeRangeMs, aggregateFields, sameRecord } from "./utils";
import type { Theme } from "./utils";
import { StatPill } from "./components/StatPill";
import { EmptyState } from "./components/EmptyState";
import { QueryHelp } from "./components/QueryHelp";
import { LogEntry } from "./components/LogEntry";
import { HistogramChart } from "./components/HistogramChart";
import { TimeRangePicker } from "./components/TimeRangePicker";
import { DetailPanelContent } from "./components/DetailPanel";
import { ExplainPanel } from "./components/ExplainPanel";
import {
  SidebarSection,
  FieldExplorer,
  StoreButton,
} from "./components/Sidebar";

export function App() {
  const { q } = useRouterSearch({ strict: false }) as { q: string };
  const navigate = useNavigate({ from: "/search" });
  const location = useLocation();
  const isFollowMode = location.pathname === "/follow";
  const [draft, setDraft] = useState(q);
  const [selectedStore, setSelectedStore] = useState("all");
  const [timeRange, setTimeRange] = useState("1h");
  const [rangeStart, setRangeStart] = useState<Date | null>(null);
  const [rangeEnd, setRangeEnd] = useState<Date | null>(null);
  const [showPlan, setShowPlan] = useState(false);
  const [showHelp, setShowHelp] = useState(false);
  const [selectedRecord, setSelectedRecord] = useState<ProtoRecord | null>(
    null,
  );
  const [theme, setTheme] = useState<Theme>("system");
  const [systemDark, setSystemDark] = useState(
    () => window.matchMedia("(prefers-color-scheme: dark)").matches,
  );

  useEffect(() => {
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = (e: MediaQueryListEvent) => setSystemDark(e.matches);
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);
  const [detailWidth, setDetailWidth] = useState(320);
  const [sidebarWidth, setSidebarWidth] = useState(224);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [detailCollapsed, setDetailCollapsed] = useState(false);
  const [detailPinned, setDetailPinned] = useState(false);
  const [resizing, setResizing] = useState(false);

  // Auto-expand detail panel when a record is selected.
  useEffect(() => {
    if (selectedRecord && detailCollapsed) setDetailCollapsed(false);
  }, [selectedRecord]); // eslint-disable-line react-hooks/exhaustive-deps

  // Escape key: deselect record and collapse detail panel.
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setSelectedRecord(null);
        if (!detailPinned) setDetailCollapsed(true);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [detailPinned]);

  const queryInputRef = useRef<HTMLInputElement>(null);
  const sentinelRef = useRef<HTMLDivElement>(null);
  const expressionRef = useRef("");

  const handleDetailResize = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setResizing(true);
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    const onMouseMove = (e: MouseEvent) => {
      setDetailWidth(
        Math.max(240, Math.min(600, window.innerWidth - e.clientX)),
      );
    };
    const onMouseUp = () => {
      setResizing(false);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
    };
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
  }, []);

  const handleSidebarResize = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setResizing(true);
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    const onMouseMove = (e: MouseEvent) => {
      setSidebarWidth(Math.max(160, Math.min(400, e.clientX)));
    };
    const onMouseUp = () => {
      setResizing(false);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
    };
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
  }, []);

  const {
    records,
    isSearching,
    error: searchError,
    hasMore,
    search,
    loadMore,
  } = useSearch();
  const {
    records: followRecords,
    isFollowing,
    error: followError,
    follow,
    stop: stopFollow,
    reset: resetFollow,
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
    isLoading: isHistogramLoading,
    fetchHistogram,
  } = useHistogram();
  const { data: stores, isLoading: storesLoading } = useStores();
  const { data: stats, isLoading: statsLoading } = useStats();

  const dark = theme === "dark" || (theme === "system" && systemDark);

  // Build the full expression sent to the server.
  // Strip start=/end=/reverse tokens from the query string.
  const stripTimeRange = (q: string): string =>
    q
      .replace(/\bstart=\S+/g, "")
      .replace(/\bend=\S+/g, "")
      .replace(/\breverse=\S+/g, "")
      .replace(/\s+/g, " ")
      .trim();

  // Strip store= token from the query string.
  const stripStore = (q: string): string =>
    q
      .replace(/\bstore=\S+/g, "")
      .replace(/\s+/g, " ")
      .trim();

  // Build time range tokens for the given range key.
  const buildTimeTokens = (range: string): string => {
    if (range === "All") return "reverse=true";
    const ms = timeRangeMs[range];
    if (!ms) return "reverse=true";
    const now = Date.now();
    return `start=${new Date(now - ms).toISOString()} end=${new Date(now).toISOString()} reverse=true`;
  };

  // Inject time range + reverse into query, replacing any existing time tokens.
  const injectTimeRange = (q: string, range: string): string => {
    const base = stripTimeRange(q);
    const timeTokens = buildTimeTokens(range);
    return base ? `${timeTokens} ${base}` : timeTokens;
  };

  // Inject store= into query, replacing any existing store token.
  const injectStore = (q: string, storeId: string): string => {
    const base = stripStore(q);
    if (storeId === "all") return base;
    const token = `store=${storeId}`;
    return base ? `${token} ${base}` : token;
  };

  const stripChunk = (q: string): string =>
    q
      .replace(/\bchunk=\S+/g, "")
      .replace(/\s+/g, " ")
      .trim();

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
    if (isFollowMode) {
      // On /follow: stop any in-flight search, start following.
      resetFollow();
      follow(q);
    } else {
      // On /search: stop any active follow, start searching.
      if (isFollowing) {
        stopFollow();
        resetFollow();
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
      const initial = injectTimeRange("", timeRange);
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
      { threshold: 0 },
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [hasMore, isSearching, loadMore]);

  const executeQuery = () => {
    // Always search from the search route.
    navigate({ to: "/search", search: { q: draft }, replace: false });
  };

  const startFollow = () => {
    // Strip time bounds but keep reverse=.
    const stripped = draft
      .replace(/\bstart=\S+/g, "")
      .replace(/\bend=\S+/g, "")
      .replace(/\s+/g, " ")
      .trim();
    navigate({ to: "/follow", search: { q: stripped }, replace: false });
  };

  const stopFollowMode = () => {
    // Restore time range when switching back to search.
    const base = stripTimeRange(draft);
    const restored = injectTimeRange(base, timeRange);
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

  // Build the severity portion of the query: single predicate or OR group.
  const buildSeverityExpr = (severities: string[]): string => {
    if (severities.length === 0) return "";
    if (severities.length === 1) return `level=${severities[0]}`;
    return `(${severities.map((s) => `level=${s}`).join(" OR ")})`;
  };

  // Remove any existing severity expression from the query string.
  const stripSeverity = (qs: string): string =>
    qs
      .replace(/\((?:level=\w+\s+OR\s+)*level=\w+\)/g, "")
      .replace(/\blevel=(?:error|warn|info|debug|trace)\b/g, "")
      .replace(/\s+/g, " ")
      .trim();

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
    const newQuery = injectTimeRange(q, range);
    setUrlQuery(newQuery);
  };

  const handleCustomRange = (start: Date, end: Date) => {
    setTimeRange("custom");
    setRangeStart(start);
    setRangeEnd(end);
    const tokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=true`;
    const base = stripTimeRange(q);
    const newQuery = base ? `${tokens} ${base}` : tokens;
    setUrlQuery(newQuery);
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

  const stripPos = (q: string): string =>
    q
      .replace(/\bpos=\S+/g, "")
      .replace(/\s+/g, " ")
      .trim();

  const handlePosSelect = (chunkId: string, pos: string) => {
    const posToken = `pos=${pos}`;
    const chunkToken = `chunk=${chunkId}`;
    if (q.includes(posToken)) {
      // Toggle off: remove pos= and chunk=
      setUrlQuery(stripPos(stripChunk(q)));
    } else {
      // Toggle on: ensure chunk= is present, add pos=
      let base = stripPos(stripChunk(q));
      const tokens = `${chunkToken} ${posToken}`;
      setUrlQuery(base ? `${tokens} ${base}` : tokens);
    }
  };

  const tokens = extractTokens(q);
  const displayRecords = isFollowMode ? followRecords : records;
  const attrFields = useMemo(
    () => aggregateFields(displayRecords, "attrs"),
    [displayRecords],
  );
  const kvFields = useMemo(
    () => aggregateFields(displayRecords, "kv"),
    [displayRecords],
  );

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
      className={`grain h-screen overflow-hidden flex flex-col font-body text-[16px] ${c("bg-ink text-text-normal", "light-theme bg-light-bg text-light-text-normal")}`}
    >
      {/* ── Header ── */}
      <header
        className={`flex items-center justify-between px-7 py-3.5 border-b ${c("border-ink-border-subtle bg-ink", "border-light-border-subtle bg-light-raised")}`}
      >
        <div className="flex items-center gap-3">
          <img src="/favicon.svg" alt="" className="w-6 h-6" />
          <h1
            className={`font-display text-[1.6em] font-semibold tracking-tight leading-none ${c("text-text-bright", "text-light-text-bright")}`}
          >
            GastroLog
          </h1>
        </div>

        <div className="flex items-center gap-6">
          {/* Stats ribbon */}
          <div className="flex items-center gap-5">
            <StatPill
              label="Records"
              value={statsLoading ? "..." : totalRecords.toLocaleString()}
              dark={dark}
            />
            <span
              className={`text-xs ${c("text-ink-border", "text-light-border")}`}
            >
              |
            </span>
            <StatPill
              label="Stores"
              value={statsLoading ? "..." : totalStores.toString()}
              dark={dark}
            />
            <span
              className={`text-xs ${c("text-ink-border", "text-light-border")}`}
            >
              |
            </span>
            <StatPill
              label="Sealed"
              value={statsLoading ? "..." : sealedChunks.toString()}
              dark={dark}
            />
            <span
              className={`text-xs ${c("text-ink-border", "text-light-border")}`}
            >
              |
            </span>
            <StatPill
              label="Storage"
              value={
                statsLoading
                  ? "..."
                  : `${(Number(totalBytes) / 1024 / 1024).toFixed(1)} MB`
              }
              dark={dark}
            />
          </div>

          <div className="flex items-center gap-1">
            {[
              { mode: "light" as Theme, icon: "\u2600", title: "Light" },
              { mode: "dark" as Theme, icon: "\u263E", title: "Dark" },
              { mode: "system" as Theme, icon: "\u25D1", title: "System" },
            ].map(({ mode, icon, title }) => (
              <button
                key={mode}
                onClick={() => setTheme(mode)}
                title={title}
                className={`w-7 h-7 flex items-center justify-center text-sm rounded transition-all duration-200 ${
                  theme === mode
                    ? c(
                        "bg-ink-hover text-copper",
                        "bg-light-hover text-copper",
                      )
                    : c(
                        "text-text-ghost hover:text-text-muted",
                        "text-light-text-ghost hover:text-light-text-muted",
                      )
                }`}
              >
                {icon}
              </button>
            ))}
          </div>
        </div>
      </header>

      {/* ── Main Layout ── */}
      <div className="flex flex-1 overflow-hidden">
        {/* ── Sidebar ── */}
        {sidebarCollapsed && (
          <button
            onClick={() => setSidebarCollapsed(false)}
            className={`shrink-0 px-1 flex items-center border-r transition-colors ${c(
              "border-ink-border-subtle bg-ink text-text-ghost hover:text-text-muted hover:bg-ink-hover",
              "border-light-border-subtle bg-light-raised text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
            )}`}
            title="Expand sidebar"
          >
            {"\u25B8"}
          </button>
        )}
        <aside
          style={{ width: sidebarCollapsed ? 0 : sidebarWidth }}
          className={`shrink-0 overflow-hidden ${resizing ? "" : "transition-[width] duration-200"} ${
            sidebarCollapsed
              ? ""
              : `p-4 border-r app-scroll overflow-y-auto ${c("border-ink-border-subtle bg-ink", "border-light-border-subtle bg-light-raised")}`
          }`}
        >
          {/* Time Range */}
          <SidebarSection title="Time Range" dark={dark}>
            <TimeRangePicker
              dark={dark}
              rangeStart={rangeStart}
              rangeEnd={rangeEnd}
              activePreset={timeRange}
              onPresetClick={handleTimeRange}
              onApply={handleCustomRange}
            />
          </SidebarSection>

          {/* Stores */}
          <SidebarSection title="Stores" dark={dark}>
            <div className="flex flex-col gap-px">
              {storesLoading ? (
                <div
                  className={`px-2.5 py-1.5 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Loading...
                </div>
              ) : (
                stores?.map((store) => (
                  <StoreButton
                    key={store.id}
                    label={store.id}
                    count={store.recordCount.toLocaleString()}
                    active={selectedStore === store.id}
                    onClick={() => handleStoreSelect(store.id)}
                    dark={dark}
                  />
                ))
              )}
            </div>
            <div
              className={`flex justify-between items-center px-2.5 pt-2 mt-1 border-t text-[0.8em] ${c("border-ink-border-subtle text-text-ghost", "border-light-border-subtle text-light-text-ghost")}`}
            >
              <span>Total</span>
              <span className="font-mono">
                {statsLoading ? "..." : totalRecords.toLocaleString()}
              </span>
            </div>
          </SidebarSection>

          {/* Quick Filters */}
          <SidebarSection title="Severity" dark={dark}>
            <div className="flex flex-wrap gap-1.5">
              {[
                { label: "Error", level: "error", color: "severity-error" },
                { label: "Warn", level: "warn", color: "severity-warn" },
                { label: "Info", level: "info", color: "severity-info" },
                { label: "Debug", level: "debug", color: "severity-debug" },
                { label: "Trace", level: "trace", color: "severity-trace" },
              ].map(({ label, level, color }) => {
                const active = activeSeverities.includes(level);
                const styles: Record<
                  string,
                  { active: string; inactive: string }
                > = {
                  "severity-error": {
                    active:
                      "bg-severity-error border-severity-error text-white",
                    inactive:
                      "border-severity-error/40 text-severity-error hover:border-severity-error hover:bg-severity-error/10",
                  },
                  "severity-warn": {
                    active: "bg-severity-warn border-severity-warn text-white",
                    inactive:
                      "border-severity-warn/40 text-severity-warn hover:border-severity-warn hover:bg-severity-warn/10",
                  },
                  "severity-info": {
                    active: "bg-severity-info border-severity-info text-white",
                    inactive:
                      "border-severity-info/40 text-severity-info hover:border-severity-info hover:bg-severity-info/10",
                  },
                  "severity-debug": {
                    active:
                      "bg-severity-debug border-severity-debug text-white",
                    inactive:
                      "border-severity-debug/40 text-severity-debug hover:border-severity-debug hover:bg-severity-debug/10",
                  },
                  "severity-trace": {
                    active:
                      "bg-severity-trace border-severity-trace text-white",
                    inactive:
                      "border-severity-trace/40 text-severity-trace hover:border-severity-trace hover:bg-severity-trace/10",
                  },
                };
                const s = styles[color]!;
                return (
                  <button
                    key={level}
                    onClick={() => toggleSeverity(level)}
                    className={`px-2 py-0.5 text-[0.8em] font-medium uppercase tracking-wider rounded-sm border transition-all duration-150 ${
                      active ? s.active : s.inactive
                    }`}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
          </SidebarSection>

          {/* Field Explorers */}
          <SidebarSection title="Attributes" dark={dark}>
            <FieldExplorer
              fields={attrFields}
              dark={dark}
              onSelect={handleFieldSelect}
              activeQuery={q}
            />
          </SidebarSection>

          <SidebarSection title="Extracted Fields" dark={dark}>
            <FieldExplorer
              fields={kvFields}
              dark={dark}
              onSelect={handleFieldSelect}
              activeQuery={q}
            />
          </SidebarSection>
        </aside>

        {/* Sidebar resize handle + collapse toggle */}
        {!sidebarCollapsed && (
          <div className="relative shrink-0 flex">
            <div
              onMouseDown={handleSidebarResize}
              className={`w-1 cursor-col-resize transition-colors ${c("hover:bg-copper-muted/30", "hover:bg-copper-muted/20")}`}
            />
            <button
              onClick={() => setSidebarCollapsed(true)}
              className={`absolute top-2 -right-3 w-4 h-6 flex items-center justify-center text-[0.6em] rounded-r z-10 transition-colors ${c(
                "bg-ink-surface border border-l-0 border-ink-border-subtle text-text-ghost hover:text-text-muted",
                "bg-light-surface border border-l-0 border-light-border-subtle text-light-text-ghost hover:text-light-text-muted",
              )}`}
              title="Collapse sidebar"
            >
              {"\u25C2"}
            </button>
          </div>
        )}

        {/* ── Main Content ── */}
        <main
          className={`flex-1 flex flex-col overflow-hidden ${c("bg-ink-raised", "bg-light-bg")}`}
        >
          {/* Query Bar */}
          <div
            className={`px-5 py-4 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
          >
            <div className="flex gap-3 items-center">
              <div className="flex-1 relative">
                <input
                  ref={queryInputRef}
                  type="text"
                  value={draft}
                  onChange={(e) => setDraft(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      executeQuery();
                    }
                  }}
                  placeholder="Search logs... tokens for full-text, key=value for attributes"
                  className={`query-input w-full pl-3 pr-8 h-9.5 text-[1em] font-mono border rounded transition-all duration-200 focus:outline-none ${c(
                    "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost",
                    "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost",
                  )}`}
                />
                <button
                  onClick={() => setShowHelp(true)}
                  className={`absolute right-2 top-1/2 -translate-y-1/2 transition-colors ${c(
                    "text-text-ghost hover:text-copper",
                    "text-light-text-ghost hover:text-copper",
                  )}`}
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
              </div>
              <button
                onClick={executeQuery}
                disabled={isSearching}
                className="px-5 h-9.5 text-[0.9em] font-medium rounded bg-copper text-white hover:bg-copper-glow transition-all duration-200 disabled:opacity-40 disabled:cursor-not-allowed whitespace-nowrap"
              >
                Search
              </button>
              <button
                onClick={isFollowMode ? stopFollowMode : startFollow}
                className={`px-4 h-9.5 text-[0.9em] font-medium rounded border transition-all duration-200 whitespace-nowrap ${
                  isFollowMode
                    ? "bg-severity-error/15 border-severity-error text-severity-error hover:bg-severity-error/25"
                    : c(
                        "border-ink-border text-text-muted hover:border-copper-dim hover:text-copper-dim",
                        "border-light-border text-light-text-muted hover:border-copper hover:text-copper",
                      )
                }`}
              >
                {isFollowMode ? "Stop" : "Follow"}
              </button>
              <button
                onClick={handleShowPlan}
                className={`px-3 h-9.5 text-[0.9em] font-medium border rounded transition-all duration-200 whitespace-nowrap ${
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
                {showPlan ? "Hide Plan" : "Explain"}
              </button>
            </div>

            {(searchError || followError) && (
              <div className="mt-2.5 px-3 py-2 text-[0.9em] bg-severity-error/10 border border-severity-error/25 rounded text-severity-error">
                {(searchError || followError)!.message}
              </div>
            )}
            {showHelp && (
              <QueryHelp
                dark={dark}
                onClose={() => setShowHelp(false)}
                onExample={(ex) => {
                  setDraft(ex);
                  setShowHelp(false);
                }}
              />
            )}
          </div>

          {/* Execution Plan Dialog */}
          {showPlan && (
            <div
              className="fixed inset-0 z-50 flex items-center justify-center"
              onClick={() => setShowPlan(false)}
            >
              <div className="absolute inset-0 bg-black/80 backdrop-blur-sm" />
              <div
                className={`relative w-[90vw] max-w-4xl h-[80vh] flex flex-col rounded-lg shadow-2xl p-6 ${c("bg-ink-bg border border-ink-border-subtle", "bg-light-bg border border-light-border-subtle")}`}
                onClick={(e) => e.stopPropagation()}
              >
                <button
                  onClick={() => setShowPlan(false)}
                  className={`absolute top-3 right-3 w-7 h-7 flex items-center justify-center rounded text-lg leading-none transition-colors ${c("text-text-muted hover:text-text-bright", "text-light-text-muted hover:text-light-text-bright")}`}
                >
                  &times;
                </button>
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
              </div>
            </div>
          )}

          {/* Histogram (hidden during follow) */}
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
                    const tokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=true`;
                    const base = stripTimeRange(q);
                    const newQuery = base ? `${tokens} ${base}` : tokens;
                    setUrlQuery(newQuery);
                  }}
                  onPan={(start, end) => {
                    setRangeStart(start);
                    setRangeEnd(end);
                    setTimeRange("custom");
                    const tokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=true`;
                    const base = stripTimeRange(q);
                    const newQuery = base ? `${tokens} ${base}` : tokens;
                    setUrlQuery(newQuery);
                  }}
                />
              </div>
            )}

          {/* Results */}
          <div className="flex-1 flex flex-col overflow-hidden">
            <div
              className={`flex justify-between items-center px-5 py-2.5 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
            >
              <div className="flex items-center gap-3">
                <h3
                  className={`font-display text-[1.15em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
                >
                  {isFollowMode ? "Following" : "Results"}
                </h3>
                {isFollowMode && (
                  <span className="relative flex h-2 w-2">
                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-severity-error opacity-75" />
                    <span className="relative inline-flex rounded-full h-2 w-2 bg-severity-error" />
                  </span>
                )}
                <span
                  className={`font-mono text-[0.8em] px-2 py-0.5 rounded ${c("bg-ink-surface text-text-muted", "bg-light-hover text-light-text-muted")}`}
                >
                  {isFollowMode ? followRecords.length : records.length}
                  {!isFollowMode && hasMore ? "+" : ""}
                </span>
              </div>
              {(isFollowMode ? followRecords : records).length > 0 && (
                <span
                  className={`font-mono text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  {new Date().toLocaleTimeString("en-US", { hour12: false })}
                </span>
              )}
            </div>

            <div className="flex-1 overflow-y-auto app-scroll">
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
                  {(isFollowMode ? followRecords : records).map((record, i) => (
                    <LogEntry
                      key={i}
                      record={record}
                      tokens={tokens}
                      isSelected={sameRecord(selectedRecord, record)}
                      onSelect={() =>
                        setSelectedRecord(
                          sameRecord(selectedRecord, record) ? null : record,
                        )
                      }
                      onFilterToggle={handleTokenToggle}
                      dark={dark}
                    />
                  ))}
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
        </main>

        {/* ── Detail Panel ── */}
        {/* Detail resize handle + collapse toggle */}
        {!detailCollapsed && (
          <div className="relative shrink-0 flex">
            <button
              onClick={() => setDetailCollapsed(true)}
              className={`absolute top-2 -left-3 w-4 h-6 flex items-center justify-center text-[0.6em] rounded-l z-10 transition-colors ${c(
                "bg-ink-surface border border-r-0 border-ink-border-subtle text-text-ghost hover:text-text-muted",
                "bg-light-surface border border-r-0 border-light-border-subtle text-light-text-ghost hover:text-light-text-muted",
              )}`}
              title="Collapse detail panel"
            >
              {"\u25B8"}
            </button>
            <div
              onMouseDown={handleDetailResize}
              className={`w-1 cursor-col-resize transition-colors ${c("hover:bg-copper-muted/30", "hover:bg-copper-muted/20")}`}
            />
          </div>
        )}
        {detailCollapsed && (
          <button
            onClick={() => setDetailCollapsed(false)}
            className={`shrink-0 px-1 flex items-center border-l transition-colors ${c(
              "border-ink-border-subtle bg-ink-surface text-text-ghost hover:text-text-muted hover:bg-ink-hover",
              "border-light-border-subtle bg-light-surface text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
            )}`}
            title="Expand detail panel"
          >
            {"\u25C2"}
          </button>
        )}
        <aside
          style={{ width: detailCollapsed ? 0 : detailWidth }}
          className={`shrink-0 overflow-hidden ${resizing ? "" : "transition-[width] duration-200"} ${
            detailCollapsed
              ? ""
              : `border-l overflow-y-auto app-scroll ${c("border-ink-border-subtle bg-ink-surface", "border-light-border-subtle bg-light-surface")}`
          }`}
        >
          <div
            className={`flex items-center justify-between px-4 py-3 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
          >
            <h3
              className={`font-display text-[1.15em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
            >
              Details
            </h3>
            <button
              onClick={() => setDetailPinned((p) => !p)}
              title={detailPinned ? "Unpin detail panel" : "Pin detail panel"}
              className={`w-6 h-6 flex items-center justify-center rounded transition-colors ${
                detailPinned
                  ? c("text-copper", "text-copper")
                  : c(
                      "text-text-ghost hover:text-text-muted",
                      "text-light-text-ghost hover:text-light-text-muted",
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
                className="w-4 h-4"
                style={
                  detailPinned ? undefined : { transform: "rotate(45deg)" }
                }
              >
                <line x1="12" y1="17" x2="12" y2="22" />
                <path d="M5 17h14v-1.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V6h1a2 2 0 0 0 0-4H8a2 2 0 0 0 0 4h1v4.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24Z" />
              </svg>
            </button>
          </div>

          {selectedRecord ? (
            <DetailPanelContent
              record={selectedRecord}
              dark={dark}
              onFieldSelect={handleFieldSelect}
              onChunkSelect={handleChunkSelect}
              onStoreSelect={handleStoreSelect}
              onPosSelect={handlePosSelect}
            />
          ) : (
            <div className="flex flex-col items-center justify-center h-48 px-4">
              <p
                className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
              >
                Select a record to view details
              </p>
            </div>
          )}
        </aside>
      </div>
    </div>
  );
}

export default App;
