import { useState, useRef, useEffect, useCallback, useMemo } from "react";
import {
  useSearch as useRouterSearch,
  useNavigate,
  useLocation,
} from "@tanstack/react-router";
import {
  startOfMonth,
  endOfMonth,
  startOfWeek,
  endOfWeek,
  eachDayOfInterval,
  format,
  isSameDay,
  isSameMonth,
  isWithinInterval,
  addMonths,
  subMonths,
  isAfter,
  isBefore,
} from "date-fns";
import {
  useSearch,
  useFollow,
  useExplain,
  useHistogram,
  extractTokens,
} from "../../api/hooks";
import { useStores, useStats } from "../../api/hooks";
import {
  Record as ProtoRecord,
  ChunkPlan,
  BranchPlan,
  PipelineStep,
} from "../../api/client";
import type { HistogramData } from "../../api/hooks/useHistogram";

type Theme = "dark" | "light" | "system";

/* ── Client-side extraction utilities ── */

/** Extract key=value pairs from raw log text (simplified port of Go tokenizer.ExtractKeyValues). */
function extractKVPairs(raw: string): { key: string; value: string }[] {
  const results: { key: string; value: string }[] = [];
  const seen = new Set<string>();
  // Match key=value, key="quoted value", key='quoted value'
  const keyRe =
    /(?:^|[\s,;:()\[\]{}])([a-zA-Z_][a-zA-Z0-9_.]*?)=(?:"([^"]*)"|'([^']*)'|([^\s,;)\]}"'=&{[]+))/g;
  let m: RegExpExecArray | null;
  while ((m = keyRe.exec(raw)) !== null) {
    const key = m[1].toLowerCase();
    const value = (m[2] ?? m[3] ?? m[4] ?? "").toLowerCase();
    if (key.length > 64 || value.length > 64 || value.length === 0) continue;
    const dedup = `${key}\0${value}`;
    if (seen.has(dedup)) continue;
    seen.add(dedup);
    results.push({ key, value });
  }
  return results;
}

/** Format a relative time string (e.g., "3s ago", "2m ago"). */
function relativeTime(date: Date): string {
  const now = Date.now();
  const diffMs = now - date.getTime();
  if (diffMs < 0) return "in the future";
  const sec = Math.floor(diffMs / 1000);
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const days = Math.floor(hr / 24);
  return `${days}d ago`;
}

/** Format byte size to human-readable string. */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

type FieldSummary = {
  key: string;
  count: number;
  values: { value: string; count: number }[];
};

function aggregateFields(
  records: ProtoRecord[],
  source: "attrs" | "kv",
): FieldSummary[] {
  const keyMap = new Map<string, Map<string, number>>();
  const decoder = new TextDecoder();
  for (const record of records) {
    const pairs: [string, string][] =
      source === "attrs"
        ? Object.entries(record.attrs)
        : extractKVPairs(decoder.decode(record.raw)).map((p) => [
            p.key,
            p.value,
          ]);
    for (const [key, value] of pairs) {
      if (source === "kv" && key === "level") continue;
      let valMap = keyMap.get(key);
      if (!valMap) {
        valMap = new Map();
        keyMap.set(key, valMap);
      }
      valMap.set(value, (valMap.get(value) ?? 0) + 1);
    }
  }
  return Array.from(keyMap.entries())
    .map(([key, valMap]) => ({
      key,
      count: Array.from(valMap.values()).reduce((a, b) => a + b, 0),
      values: Array.from(valMap.entries())
        .map(([value, count]) => ({ value, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 10),
    }))
    .sort((a, b) => b.count - a.count);
}

const timeRangeMs: Record<string, number> = {
  "5m": 5 * 60 * 1000,
  "15m": 15 * 60 * 1000,
  "30m": 30 * 60 * 1000,
  "1h": 60 * 60 * 1000,
  "3h": 3 * 60 * 60 * 1000,
  "6h": 6 * 60 * 60 * 1000,
  "12h": 12 * 60 * 60 * 1000,
  "24h": 24 * 60 * 60 * 1000,
  "3d": 3 * 24 * 60 * 60 * 1000,
  "7d": 7 * 24 * 60 * 60 * 1000,
  "30d": 30 * 24 * 60 * 60 * 1000,
};

export function EditorialDesign() {
  const { q } = useRouterSearch({ strict: false }) as { q: string };
  const navigate = useNavigate();
  const location = useLocation();
  const isFollowMode = location.pathname === "/follow";
  const [draft, setDraft] = useState(q);
  const [selectedStore, setSelectedStore] = useState("all");
  const [timeRange, setTimeRange] = useState("1h");
  const [rangeStart, setRangeStart] = useState<Date | null>(null);
  const [rangeEnd, setRangeEnd] = useState<Date | null>(null);
  const [showPlan, setShowPlan] = useState(false);
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
    setSelectedRecord(null);
    if (!detailPinned) setDetailCollapsed(true);
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
        if (entries[0].isIntersecting && hasMore && !isSearching) {
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
              : `p-4 border-r editorial-scroll overflow-y-auto ${c("border-ink-border-subtle bg-ink", "border-light-border-subtle bg-light-raised")}`
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
                const s = styles[color];
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
                  className={`query-input w-full px-3 h-[38px] text-[1em] font-mono border rounded transition-all duration-200 focus:outline-none ${c(
                    "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost",
                    "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost",
                  )}`}
                />
              </div>
              <button
                onClick={executeQuery}
                disabled={isSearching}
                className="px-5 h-[38px] text-[0.9em] font-medium rounded bg-copper text-white hover:bg-copper-glow transition-all duration-200 disabled:opacity-40 disabled:cursor-not-allowed whitespace-nowrap"
              >
                Search
              </button>
              <button
                onClick={isFollowMode ? stopFollowMode : startFollow}
                className={`px-4 h-[38px] text-[0.9em] font-medium rounded border transition-all duration-200 whitespace-nowrap ${
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
                className={`px-3 h-[38px] text-[0.9em] font-medium border rounded transition-all duration-200 whitespace-nowrap ${
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

            <div
              className={`flex items-center gap-2 mt-2.5 text-[0.75em] ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              <span className="uppercase tracking-wider font-medium">
                Syntax
              </span>
              <span className={c("text-ink-border", "text-light-border")}>
                |
              </span>
              {[
                "error timeout",
                "level=error host=*",
                "start=2024-02-05T10:00:00Z limit=100",
              ].map((ex) => (
                <button
                  key={ex}
                  onClick={() => setDraft(ex)}
                  className={`font-mono px-1.5 py-0.5 rounded transition-colors ${c(
                    "text-text-muted hover:text-copper hover:bg-ink-hover",
                    "text-light-text-muted hover:text-copper hover:bg-light-hover",
                  )}`}
                >
                  {ex}
                </button>
              ))}
            </div>
          </div>

          {/* Execution Plan */}
          {showPlan && (
            <div
              className={`px-5 py-4 border-b animate-fade-up max-h-[50vh] overflow-y-auto overflow-x-hidden editorial-scroll ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
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

            <div className="flex-1 overflow-y-auto editorial-scroll">
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
                      isSelected={selectedRecord === record}
                      onSelect={() =>
                        setSelectedRecord(
                          selectedRecord === record ? null : record,
                        )
                      }
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
              : `border-l overflow-y-auto editorial-scroll ${c("border-ink-border-subtle bg-ink-surface", "border-light-border-subtle bg-light-surface")}`
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

/* ── Sub-components ── */

function StatPill({
  label,
  value,
  dark,
}: {
  label: string;
  value: string;
  dark: boolean;
}) {
  return (
    <div className="flex items-baseline gap-1.5">
      <span
        className={`font-mono text-[0.9em] font-medium ${dark ? "text-text-bright" : "text-light-text-bright"}`}
      >
        {value}
      </span>
      <span
        className={`text-[0.7em] uppercase tracking-wider ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {label}
      </span>
    </div>
  );
}

function TimeRangePicker({
  dark,
  rangeStart,
  rangeEnd,
  activePreset,
  onPresetClick,
  onApply,
}: {
  dark: boolean;
  rangeStart: Date | null;
  rangeEnd: Date | null;
  activePreset: string;
  onPresetClick: (preset: string) => void;
  onApply: (start: Date, end: Date) => void;
}) {
  const c = (d: string, l: string) => (dark ? d : l);
  const [viewMonth, setViewMonth] = useState(() => rangeEnd ?? new Date());
  const [pendingStart, setPendingStart] = useState<Date | null>(rangeStart);
  const [pendingEnd, setPendingEnd] = useState<Date | null>(rangeEnd);
  const [startTime, setStartTime] = useState(() =>
    rangeStart ? format(rangeStart, "HH:mm") : "00:00",
  );
  const [endTime, setEndTime] = useState(() =>
    rangeEnd ? format(rangeEnd, "HH:mm") : "23:59",
  );
  const [picking, setPicking] = useState<"start" | "end">("start");

  // Sync from parent when presets or histogram brush update the range.
  useEffect(() => {
    setPendingStart(rangeStart);
    setPendingEnd(rangeEnd);
    if (rangeStart) setStartTime(format(rangeStart, "HH:mm"));
    if (rangeEnd) setEndTime(format(rangeEnd, "HH:mm"));
    if (rangeEnd) setViewMonth(rangeEnd);
    setPicking("start");
  }, [rangeStart, rangeEnd]);

  const handleDayClick = (day: Date) => {
    if (picking === "start") {
      setPendingStart(day);
      setPendingEnd(null);
      setPicking("end");
    } else {
      let s = pendingStart!;
      let e = day;
      if (isBefore(e, s)) [s, e] = [e, s];
      setPendingStart(s);
      setPendingEnd(e);
      setPicking("start");
    }
  };

  const handleApply = () => {
    if (!pendingStart || !pendingEnd) return;
    const [sh, sm] = startTime.split(":").map(Number);
    const [eh, em] = endTime.split(":").map(Number);
    const start = new Date(pendingStart);
    start.setHours(sh, sm, 0, 0);
    const end = new Date(pendingEnd);
    end.setHours(eh, em, 59, 999);
    onApply(start, end);
  };

  // Calendar grid
  const monthStart = startOfMonth(viewMonth);
  const monthEnd = endOfMonth(viewMonth);
  const calStart = startOfWeek(monthStart, { weekStartsOn: 1 });
  const calEnd = endOfWeek(monthEnd, { weekStartsOn: 1 });
  const days = eachDayOfInterval({ start: calStart, end: calEnd });
  const today = new Date();

  const presets = Object.keys(timeRangeMs);

  return (
    <div className="space-y-2.5">
      {/* Preset buttons */}
      <div className="flex flex-wrap gap-1">
        {[...presets, "All"].map((range) => (
          <button
            key={range}
            onClick={() => onPresetClick(range)}
            className={`px-2 py-0.5 text-[0.75em] font-mono rounded transition-all duration-150 ${
              activePreset === range
                ? c("bg-copper text-ink", "bg-copper text-white")
                : c(
                    "text-text-muted hover:text-text-normal hover:bg-ink-hover",
                    "text-light-text-muted hover:text-light-text-normal hover:bg-light-hover",
                  )
            }`}
          >
            {range}
          </button>
        ))}
      </div>

      {/* From / To display */}
      <div className="space-y-1">
        <div className="flex items-center gap-1.5">
          <span
            className={`text-[0.7em] w-8 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            From
          </span>
          <span
            className={`flex-1 text-[0.75em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {pendingStart ? format(pendingStart, "yyyy-MM-dd") : "\u2014"}
          </span>
          <input
            type="text"
            value={startTime}
            onChange={(e) => {
              const v = e.target.value.replace(/[^0-9:]/g, "");
              if (v.length <= 5) setStartTime(v);
            }}
            placeholder="HH:mm"
            className={`text-[0.75em] font-mono w-14 px-1 py-0.5 rounded border text-center ${c(
              "bg-ink-surface border-ink-border text-text-normal",
              "bg-light-surface border-light-border text-light-text-normal",
            )}`}
          />
        </div>
        <div className="flex items-center gap-1.5">
          <span
            className={`text-[0.7em] w-8 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            To
          </span>
          <span
            className={`flex-1 text-[0.75em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {pendingEnd ? format(pendingEnd, "yyyy-MM-dd") : "\u2014"}
          </span>
          <input
            type="text"
            value={endTime}
            onChange={(e) => {
              const v = e.target.value.replace(/[^0-9:]/g, "");
              if (v.length <= 5) setEndTime(v);
            }}
            placeholder="HH:mm"
            className={`text-[0.75em] font-mono w-14 px-1 py-0.5 rounded border text-center ${c(
              "bg-ink-surface border-ink-border text-text-normal",
              "bg-light-surface border-light-border text-light-text-normal",
            )}`}
          />
        </div>
      </div>

      {/* Month navigation */}
      <div className="flex items-center justify-between">
        <button
          onClick={() => setViewMonth((m) => subMonths(m, 1))}
          className={`text-[0.8em] px-1 rounded ${c("text-text-ghost hover:text-text-muted", "text-light-text-ghost hover:text-light-text-muted")}`}
        >
          {"\u25C2"}
        </button>
        <span
          className={`text-[0.75em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          {format(viewMonth, "MMMM yyyy")}
        </span>
        <button
          onClick={() => setViewMonth((m) => addMonths(m, 1))}
          className={`text-[0.8em] px-1 rounded ${c("text-text-ghost hover:text-text-muted", "text-light-text-ghost hover:text-light-text-muted")}`}
        >
          {"\u25B8"}
        </button>
      </div>

      {/* Calendar grid */}
      <div>
        <div className="grid grid-cols-7 gap-px mb-0.5">
          {["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"].map((d) => (
            <div
              key={d}
              className={`text-center text-[0.65em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              {d}
            </div>
          ))}
        </div>
        <div className="grid grid-cols-7 gap-px">
          {days.map((day) => {
            const inMonth = isSameMonth(day, viewMonth);
            const isToday = isSameDay(day, today);
            const isStart = pendingStart && isSameDay(day, pendingStart);
            const isEnd = pendingEnd && isSameDay(day, pendingEnd);
            const inRange =
              pendingStart &&
              pendingEnd &&
              isWithinInterval(day, {
                start: pendingStart,
                end: pendingEnd,
              });
            const selected = isStart || isEnd;

            return (
              <button
                key={day.toISOString()}
                onClick={() => handleDayClick(day)}
                className={`text-center text-[0.7em] font-mono py-0.5 rounded transition-colors ${
                  selected
                    ? "bg-copper text-white"
                    : inRange
                      ? c(
                          "bg-copper/10 text-text-normal",
                          "bg-copper/10 text-light-text-normal",
                        )
                      : inMonth
                        ? c(
                            "text-text-muted hover:bg-ink-hover hover:text-text-normal",
                            "text-light-text-muted hover:bg-light-hover hover:text-light-text-normal",
                          )
                        : c("text-text-ghost/40", "text-light-text-ghost/40")
                }${isToday && !selected ? ` ${c("underline decoration-copper", "underline decoration-copper")}` : ""}`}
              >
                {format(day, "d")}
              </button>
            );
          })}
        </div>
      </div>

      {/* Apply button */}
      <button
        onClick={handleApply}
        disabled={!pendingStart || !pendingEnd}
        className="w-full py-1 text-[0.8em] font-medium rounded bg-copper text-white hover:bg-copper-glow transition-all duration-200 disabled:opacity-30 disabled:cursor-not-allowed"
      >
        Apply
      </button>
    </div>
  );
}

function SidebarSection({
  title,
  dark,
  children,
}: {
  title: string;
  dark: boolean;
  children: React.ReactNode;
}) {
  return (
    <section className="mb-5">
      <h3
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {title}
      </h3>
      {children}
    </section>
  );
}

function FieldExplorer({
  fields,
  dark,
  onSelect,
  activeQuery,
}: {
  fields: FieldSummary[];
  dark: boolean;
  onSelect: (key: string, value: string) => void;
  activeQuery: string;
}) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  if (fields.length === 0) {
    return (
      <div
        className={`text-[0.8em] italic ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        No fields
      </div>
    );
  }

  const toggleKey = (key: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  return (
    <div className="space-y-px">
      {fields.map(({ key, count, values }) => {
        const isExpanded = expanded.has(key);
        return (
          <div key={key}>
            <button
              onClick={() => toggleKey(key)}
              className={`w-full flex items-center gap-1.5 px-1.5 py-1 text-left text-[0.8em] rounded transition-colors ${dark ? "hover:bg-ink-hover text-text-muted hover:text-text-normal" : "hover:bg-light-hover text-light-text-muted hover:text-light-text-normal"}`}
            >
              <span
                className={`text-[0.7em] ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
              >
                {isExpanded ? "\u25be" : "\u25b8"}
              </span>
              <span className="flex-1 font-mono truncate">{key}</span>
              <span
                className={`text-[0.85em] tabular-nums ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
              >
                {count}
              </span>
            </button>
            {isExpanded && (
              <div className="ml-4 space-y-px">
                {values.map(({ value, count: vCount }) => {
                  const needsQuotes = /[^a-zA-Z0-9_\-.]/.test(value);
                  const token = needsQuotes
                    ? `${key}="${value}"`
                    : `${key}=${value}`;
                  const isActive = activeQuery.includes(token);
                  return (
                    <button
                      key={value}
                      onClick={() => onSelect(key, value)}
                      className={`w-full flex items-center gap-1.5 px-1.5 py-0.5 text-left text-[0.75em] rounded transition-colors ${
                        isActive
                          ? dark
                            ? "bg-copper/15 text-copper"
                            : "bg-copper/10 text-copper"
                          : dark
                            ? "hover:bg-ink-hover text-text-ghost hover:text-copper-glow"
                            : "hover:bg-light-hover text-light-text-ghost hover:text-copper"
                      }`}
                    >
                      <span className="flex-1 font-mono truncate">{value}</span>
                      <span className="tabular-nums">{vCount}</span>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function StoreButton({
  label,
  count,
  active,
  onClick,
  dark,
}: {
  label: string;
  count: string;
  active: boolean;
  onClick: () => void;
  dark: boolean;
}) {
  return (
    <button
      onClick={onClick}
      className={`flex justify-between items-center px-2.5 py-1.5 text-[0.9em] rounded text-left transition-all duration-150 ${
        active
          ? dark
            ? "bg-copper/15 text-copper border border-copper/25"
            : "bg-copper/10 text-copper border border-copper/25"
          : dark
            ? "text-text-muted hover:text-text-normal hover:bg-ink-hover border border-transparent"
            : "text-light-text-muted hover:text-light-text-normal hover:bg-light-hover border border-transparent"
      }`}
    >
      <span className="font-medium">{label}</span>
      <span
        className={`font-mono text-[0.8em] ${active ? "text-copper-dim" : dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {count}
      </span>
    </button>
  );
}

function EmptyState({ dark }: { dark: boolean }) {
  return (
    <div className="flex flex-col items-center justify-center h-full py-20 animate-fade-up">
      <div
        className={`font-display text-[3em] font-light leading-none mb-3 ${dark ? "text-ink-border" : "text-light-border"}`}
      >
        &empty;
      </div>
      <p
        className={`text-[0.9em] ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        Enter a query to search your logs
      </p>
      <p
        className={`text-[0.8em] mt-1 font-mono ${dark ? "text-text-ghost/60" : "text-light-text-ghost/60"}`}
      >
        press Enter to execute
      </p>
    </div>
  );
}

function LogEntry({
  record,
  tokens,
  isSelected,
  onSelect,
  dark,
}: {
  record: ProtoRecord;
  tokens: string[];
  isSelected: boolean;
  onSelect: () => void;
  dark: boolean;
}) {
  const rawText = new TextDecoder().decode(record.raw);
  const parts = highlightMatches(rawText, tokens);
  const ingestTime = record.ingestTs ? record.ingestTs.toDate() : new Date();

  const ts = ingestTime.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
    hour12: false,
  });

  return (
    <article
      onClick={onSelect}
      className={`grid grid-cols-[13ch_1fr] gap-2.5 px-5 py-[5px] border-b cursor-pointer transition-colors duration-100 ${
        isSelected
          ? dark
            ? "bg-ink-hover"
            : "bg-light-hover"
          : dark
            ? "hover:bg-ink-surface border-b-ink-border-subtle"
            : "hover:bg-light-hover border-b-light-border-subtle"
      }`}
    >
      <span
        className={`font-mono text-[0.8em] tabular-nums self-center ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {ts}
      </span>
      <div
        className={`font-mono text-[0.85em] leading-relaxed truncate self-center ${dark ? "text-text-normal" : "text-light-text-normal"}`}
      >
        {parts.map((part, i) => (
          <span
            key={i}
            className={
              part.highlighted
                ? dark
                  ? "bg-highlight-bg border border-highlight-border text-highlight-text px-0.5 rounded-sm"
                  : "bg-light-highlight-bg border border-light-highlight-border text-light-highlight-text px-0.5 rounded-sm"
                : ""
            }
          >
            {part.text}
          </span>
        ))}
      </div>
    </article>
  );
}

/* ── Explain Visualizer ── */

function ExplainPanel({
  chunks,
  direction,
  totalChunks,
  expression,
  dark,
}: {
  chunks: ChunkPlan[];
  direction: string;
  totalChunks: number;
  expression: string;
  dark: boolean;
}) {
  const c = (d: string, l: string) => (dark ? d : l);
  const [collapsed, setCollapsed] = useState<Record<number, boolean>>(() => {
    // Auto-collapse if more than 3 chunks
    if (chunks.length <= 3) return {};
    const m: Record<number, boolean> = {};
    for (let i = 0; i < chunks.length; i++) m[i] = true;
    return m;
  });

  const toggle = (i: number) =>
    setCollapsed((prev) => ({ ...prev, [i]: !prev[i] }));

  return (
    <div>
      {/* Query summary */}
      <div className="flex items-center gap-3 mb-3">
        <h3
          className={`font-display text-[1.15em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Execution Plan
        </h3>
        <span
          className={`text-[0.65em] px-1.5 py-0.5 rounded uppercase tracking-wider font-semibold ${
            direction === "reverse"
              ? "bg-copper/15 text-copper border border-copper/25"
              : "bg-severity-info/15 text-severity-info border border-severity-info/25"
          }`}
        >
          {direction || "forward"}
        </span>
        <span
          className={`font-mono text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          {chunks.length} of {totalChunks} chunks
        </span>
      </div>
      {expression && (
        <div
          className={`font-mono text-[0.8em] px-3 py-1.5 rounded mb-3 ${c("bg-ink-surface text-text-normal", "bg-light-surface text-light-text-normal")}`}
        >
          {expression}
        </div>
      )}

      {/* Chunk plans */}
      <div className="flex flex-col gap-2 stagger-children">
        {chunks.map((plan, i) => (
          <ExplainChunk
            key={i}
            plan={plan}
            dark={dark}
            collapsed={!!collapsed[i]}
            onToggle={() => toggle(i)}
          />
        ))}
      </div>
    </div>
  );
}

function ExplainChunk({
  plan,
  dark,
  collapsed,
  onToggle,
}: {
  plan: ChunkPlan;
  dark: boolean;
  collapsed: boolean;
  onToggle: () => void;
}) {
  const c = (d: string, l: string) => (dark ? d : l);
  const isSkipped = plan.scanMode === "skipped";
  const hasBranches = plan.branchPlans.length > 0;
  const totalRecords = Number(plan.recordCount);

  const formatTs = (ts: { toDate(): Date } | undefined) => {
    if (!ts) return "";
    return ts.toDate().toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  };

  return (
    <div
      className={`rounded border overflow-hidden ${
        isSkipped
          ? c(
              "bg-ink-surface/50 border-ink-border-subtle/50",
              "bg-light-surface/50 border-light-border-subtle/50",
            )
          : c(
              "bg-ink-surface border-ink-border-subtle",
              "bg-light-surface border-light-border-subtle",
            )
      }`}
    >
      {/* Chunk header — clickable to toggle */}
      <button
        onClick={onToggle}
        className={`w-full min-w-0 relative px-3.5 pt-2.5 pb-4 text-left transition-colors ${
          !collapsed
            ? c(
                "border-b border-ink-border-subtle",
                "border-b border-light-border-subtle",
              )
            : ""
        }`}
      >
        <div className="flex items-center gap-2 w-full min-w-0">
          <span
            className={`text-xs transition-transform ${collapsed ? "" : "rotate-90"} ${c("text-text-muted", "text-light-text-muted")}`}
          >
            &#x25B6;
          </span>
          <span
            className={`font-mono text-sm font-medium ${
              isSkipped
                ? c("text-text-muted", "text-light-text-muted")
                : c("text-text-bright", "text-light-text-bright")
            }`}
          >
            {formatChunkId(plan.chunkId)}
          </span>
          <span
            className={`text-xs px-1.5 py-0.5 rounded uppercase tracking-wider font-semibold ${
              isSkipped
                ? "bg-severity-error/15 text-severity-error border border-severity-error/25"
                : plan.sealed
                  ? "bg-severity-info/15 text-severity-info border border-severity-info/25"
                  : "bg-copper/15 text-copper border border-copper/25"
            }`}
          >
            {isSkipped ? "Skip" : plan.sealed ? "Sealed" : "Active"}
          </span>
          {plan.storeId && (
            <span
              className={`font-mono text-sm ${c("text-text-muted", "text-light-text-muted")}`}
            >
              [{plan.storeId}]
            </span>
          )}
          <span
            className={`font-mono text-sm ${c("text-text-normal", "text-light-text-normal")}`}
          >
            {totalRecords.toLocaleString()} rec
          </span>
          {(plan.startTs || plan.endTs) && (
            <span
              className={`font-mono text-xs ${c("text-text-muted", "text-light-text-muted")}`}
            >
              {formatTs(plan.startTs)}
              {plan.startTs && plan.endTs ? " \u2013 " : ""}
              {formatTs(plan.endTs)}
            </span>
          )}
          {isSkipped && plan.skipReason && (
            <span
              className={`ml-auto font-mono text-sm ${c("text-severity-error/80", "text-severity-error/90")}`}
            >
              {plan.skipReason}
            </span>
          )}
          {!isSkipped && (
            <span
              className={`ml-auto font-mono text-sm ${c("text-text-normal", "text-light-text-normal")}`}
            >
              scan ~{Number(plan.estimatedRecords).toLocaleString()}
            </span>
          )}
        </div>
        {/* Aggregate narrowing bar */}
        {!isSkipped && totalRecords > 0 && (
          <div className="absolute bottom-0 left-3.5 right-3.5 h-1.5 rounded-full overflow-hidden">
            <div
              className={`absolute inset-0 ${c("bg-ink-border-subtle/60", "bg-light-border/50")}`}
            />
            <div
              className="absolute inset-y-0 left-0 bg-copper/80"
              style={{
                width: `${Math.min(Math.max((Number(plan.estimatedRecords) / totalRecords) * 100, 0.5), 100)}%`,
              }}
            />
          </div>
        )}
      </button>

      {/* Chunk body — pipeline */}
      {!collapsed && !isSkipped && (
        <div className="px-3.5 py-4">
          {hasBranches ? (
            <div className="flex flex-col gap-4">
              {plan.branchPlans.map((bp, j) => (
                <ExplainBranch
                  key={j}
                  branch={bp}
                  index={j}
                  totalRecords={totalRecords}
                  dark={dark}
                />
              ))}
            </div>
          ) : plan.steps.length > 0 ? (
            <PipelineFunnel
              steps={plan.steps}
              totalRecords={totalRecords}
              dark={dark}
            />
          ) : null}

          {/* Footer */}
          <div
            className={`flex flex-wrap items-center gap-x-4 gap-y-1.5 mt-4 pt-3 text-xs font-mono border-t ${c("border-ink-border-subtle text-text-normal", "border-light-border-subtle text-light-text-normal")}`}
          >
            <span>
              scan{" "}
              <strong
                className={c("text-text-bright", "text-light-text-bright")}
              >
                {plan.scanMode}
              </strong>
            </span>
            <span>
              records to scan{" "}
              <strong
                className={c("text-text-bright", "text-light-text-bright")}
              >
                ~{Number(plan.estimatedRecords).toLocaleString()}
              </strong>
            </span>
            {plan.runtimeFilters
              .filter((f) => f && f !== "none")
              .map((f, i) => (
                <span
                  key={i}
                  className={`px-1.5 py-px rounded ${c("bg-severity-warn/10 text-severity-warn", "bg-severity-warn/10 text-severity-warn")}`}
                >
                  {f}
                </span>
              ))}
          </div>
        </div>
      )}
    </div>
  );
}

function ExplainBranch({
  branch,
  index,
  totalRecords,
  dark,
}: {
  branch: BranchPlan;
  index: number;
  totalRecords: number;
  dark: boolean;
}) {
  const c = (d: string, l: string) => (dark ? d : l);

  return (
    <div
      className={`rounded border px-3.5 py-3 ${
        branch.skipped
          ? c(
              "bg-ink/30 border-ink-border-subtle/50",
              "bg-light-bg/50 border-light-border-subtle/50",
            )
          : c(
              "bg-ink border-ink-border-subtle",
              "bg-light-bg border-light-border-subtle",
            )
      }`}
    >
      <div className="flex items-center gap-2.5 mb-4">
        <span
          className={`text-xs font-medium uppercase tracking-wider ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Branch {index + 1}
        </span>
        <span
          className={`font-mono text-sm ${
            branch.skipped
              ? c("text-text-muted", "text-light-text-muted")
              : c("text-text-bright", "text-light-text-bright")
          }`}
        >
          {branch.expression}
        </span>
        {branch.skipped && (
          <span className="text-xs px-1.5 py-0.5 rounded uppercase tracking-wider font-semibold bg-severity-error/15 text-severity-error border border-severity-error/25">
            Skip
          </span>
        )}
        {branch.skipped && branch.skipReason && (
          <span
            className={`font-mono text-sm ${c("text-severity-error/80", "text-severity-error/90")}`}
          >
            {branch.skipReason}
          </span>
        )}
        {!branch.skipped && (
          <span
            className={`ml-auto font-mono text-sm ${c("text-text-normal", "text-light-text-normal")}`}
          >
            scan ~{Number(branch.estimatedRecords).toLocaleString()}
          </span>
        )}
      </div>
      {!branch.skipped && branch.steps.length > 0 && (
        <PipelineFunnel
          steps={branch.steps}
          totalRecords={totalRecords}
          dark={dark}
        />
      )}
    </div>
  );
}

function PipelineFunnel({
  steps,
  totalRecords,
  dark,
}: {
  steps: PipelineStep[];
  totalRecords: number;
  dark: boolean;
}) {
  const c = (d: string, l: string) => (dark ? d : l);
  const maxVal = Math.max(
    totalRecords,
    ...steps.map((s) => Number(s.inputEstimate)),
    1,
  );

  const actionColor = (action: string) => {
    switch (action) {
      case "seek":
      case "indexed":
        return "bg-severity-info/20 text-severity-info border-severity-info/30";
      case "skipped":
        return "bg-severity-error/15 text-severity-error border-severity-error/25";
      default:
        return "bg-severity-warn/15 text-severity-warn border-severity-warn/25";
    }
  };

  return (
    <div className="flex flex-col gap-4">
      {steps.map((step, i) => {
        const inVal = Number(step.inputEstimate);
        const outVal = Number(step.outputEstimate);
        const inPct = maxVal > 0 ? (inVal / maxVal) * 100 : 0;
        const outPct = maxVal > 0 ? (outVal / maxVal) * 100 : 0;
        const reduced = inVal > 0 && outVal < inVal;

        return (
          <div key={i} className="relative pb-3 min-w-0">
            {/* Top row: step info */}
            <div className="flex items-center gap-3 min-w-0">
              {/* Step number */}
              <span
                className={`w-5 text-right text-xs font-mono ${c("text-text-muted", "text-light-text-muted")}`}
              >
                {i + 1}
              </span>

              {/* Name */}
              <span
                className={`text-sm font-semibold capitalize ${c("text-text-bright", "text-light-text-bright")}`}
              >
                {step.name}
              </span>

              {/* Action badge */}
              <span
                className={`text-xs px-1.5 py-0.5 rounded border uppercase tracking-wide font-semibold ${actionColor(step.action)}`}
              >
                {step.action}
              </span>

              {/* Counts */}
              <span
                className={`font-mono text-sm ${c("text-text-normal", "text-light-text-normal")}`}
              >
                <span className={c("text-text-muted", "text-light-text-muted")}>
                  candidates{" "}
                </span>
                {inVal.toLocaleString()}
                {" \u2192 "}
                <span
                  className={
                    reduced
                      ? "text-copper font-semibold"
                      : c("text-text-normal", "text-light-text-normal")
                  }
                >
                  {outVal.toLocaleString()}
                </span>
              </span>

              {/* Predicate + reason */}
              <span className="ml-auto flex items-center gap-2 min-w-0 max-w-[60%]">
                {step.predicate && (
                  <span
                    className={`font-mono text-xs truncate ${c("text-text-normal", "text-light-text-normal")}`}
                    title={step.predicate}
                  >
                    {step.predicate}
                  </span>
                )}
                {(step.reason || step.detail) && (
                  <span
                    className={`font-mono text-xs truncate ${c("text-text-muted", "text-light-text-muted")}`}
                    title={step.detail || step.reason}
                  >
                    {step.detail || step.reason}
                  </span>
                )}
              </span>
            </div>

            {/* Narrowing bar */}
            <div className="absolute bottom-0 left-8 right-0 h-1.5 rounded-full overflow-hidden">
              <div
                className={`absolute inset-0 ${c("bg-ink-border-subtle/60", "bg-light-border/50")}`}
                style={{ width: `${Math.min(Math.max(inPct, 1), 100)}%` }}
              />
              <div
                className="absolute inset-y-0 left-0 bg-copper/80"
                style={{ width: `${Math.min(Math.max(outPct, 0.5), 100)}%` }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}

function DetailPanelContent({
  record,
  dark,
  onFieldSelect,
  onChunkSelect,
}: {
  record: ProtoRecord;
  dark: boolean;
  onFieldSelect?: (key: string, value: string) => void;
  onChunkSelect?: (chunkId: string) => void;
}) {
  const c = (d: string, l: string) => (dark ? d : l);
  const rawText = new TextDecoder().decode(record.raw);
  const rawBytes = record.raw.length;
  const kvPairs = extractKVPairs(rawText);

  const tsRows: { label: string; date: Date | null }[] = [
    { label: "Write", date: record.writeTs ? record.writeTs.toDate() : null },
    {
      label: "Ingest",
      date: record.ingestTs ? record.ingestTs.toDate() : null,
    },
    {
      label: "Source",
      date: record.sourceTs ? record.sourceTs.toDate() : null,
    },
  ];

  return (
    <div className="p-4 space-y-4">
      {/* Timestamps */}
      <DetailSection label="Timestamps" dark={dark}>
        <div className="space-y-1.5">
          {tsRows.map(({ label, date }) => (
            <div
              key={label}
              className={`flex py-1 border-b last:border-b-0 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
            >
              <dt
                className={`w-16 shrink-0 text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
              >
                {label}
              </dt>
              <dd className="flex-1 min-w-0">
                {date ? (
                  <>
                    <div
                      className={`text-[0.85em] font-mono break-all ${c("text-text-normal", "text-light-text-normal")}`}
                    >
                      {date.toISOString()}
                    </div>
                    <div
                      className={`text-[0.75em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
                    >
                      {relativeTime(date)}
                    </div>
                  </>
                ) : (
                  <>
                    <div
                      className={`text-[0.85em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
                    >
                      {"\u2014"}
                    </div>
                    <div className="text-[0.75em] font-mono">&nbsp;</div>
                  </>
                )}
              </dd>
            </div>
          ))}
        </div>
      </DetailSection>

      {/* Message */}
      <DetailSection label={`Message (${formatBytes(rawBytes)})`} dark={dark}>
        <pre
          className={`text-[0.85em] font-mono p-3 rounded whitespace-pre-wrap break-words leading-relaxed ${c("bg-ink text-text-normal", "bg-light-bg text-light-text-normal")}`}
        >
          {rawText}
        </pre>
      </DetailSection>

      {/* Attributes */}
      {Object.keys(record.attrs).length > 0 && (
        <DetailSection label="Attributes" dark={dark}>
          <div className="space-y-0">
            {Object.entries(record.attrs).map(([k, v]) => (
              <DetailRow
                key={k}
                label={k}
                value={v}
                dark={dark}
                onClick={onFieldSelect ? () => onFieldSelect(k, v) : undefined}
              />
            ))}
          </div>
        </DetailSection>
      )}

      {/* Extracted Fields */}
      {kvPairs.length > 0 && (
        <DetailSection label="Extracted Fields" dark={dark}>
          <div className="space-y-0">
            {kvPairs.map(({ key, value }, i) => (
              <DetailRow
                key={`${key}-${i}`}
                label={key}
                value={value}
                dark={dark}
                onClick={
                  onFieldSelect ? () => onFieldSelect(key, value) : undefined
                }
              />
            ))}
          </div>
        </DetailSection>
      )}

      {/* Reference */}
      <DetailSection label="Reference" dark={dark}>
        <div className="space-y-0">
          <DetailRow
            label="Store"
            value={record.ref?.storeId ?? "N/A"}
            dark={dark}
          />
          <DetailRow
            label="Chunk"
            value={
              record.ref?.chunkId ? formatChunkId(record.ref.chunkId) : "N/A"
            }
            dark={dark}
            onClick={
              record.ref?.chunkId
                ? () => onChunkSelect?.(record.ref!.chunkId)
                : undefined
            }
          />
          <DetailRow
            label="Position"
            value={record.ref?.pos?.toString() ?? "N/A"}
            dark={dark}
          />
        </div>
      </DetailSection>
    </div>
  );
}

function DetailSection({
  label,
  dark,
  children,
}: {
  label: string;
  dark: boolean;
  children: React.ReactNode;
}) {
  return (
    <div>
      <h4
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {label}
      </h4>
      {children}
    </div>
  );
}

function DetailRow({
  label,
  value,
  dark,
  onClick,
}: {
  label: string;
  value: string;
  dark: boolean;
  onClick?: () => void;
}) {
  return (
    <div
      className={`flex py-1 border-b last:border-b-0 ${dark ? "border-ink-border-subtle" : "border-light-border-subtle"}`}
    >
      <dt
        className={`w-24 shrink-0 text-[0.8em] ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {label}
      </dt>
      <dd
        className={`flex-1 text-[0.85em] font-mono break-all ${
          onClick
            ? `cursor-pointer transition-colors ${dark ? "text-text-muted hover:text-copper" : "text-light-text-muted hover:text-copper"}`
            : dark
              ? "text-text-normal"
              : "text-light-text-normal"
        }`}
        onClick={onClick}
      >
        {value}
      </dd>
    </div>
  );
}

function HistogramChart({
  data,
  dark,
  onBrushSelect,
  onPan,
}: {
  data: HistogramData;
  dark: boolean;
  onBrushSelect?: (start: Date, end: Date) => void;
  onPan?: (start: Date, end: Date) => void;
}) {
  const { buckets } = data;
  const barsRef = useRef<HTMLDivElement>(null);
  const [brushStart, setBrushStart] = useState<number | null>(null);
  const [brushEnd, setBrushEnd] = useState<number | null>(null);
  const brushingRef = useRef(false);

  if (buckets.length === 0) return null;

  const maxCount = Math.max(...buckets.map((b) => b.count), 1);
  const totalCount = buckets.reduce((sum, b) => sum + b.count, 0);
  const barHeight = 48;
  const c = (d: string, l: string) => (dark ? d : l);

  const getBucketIndex = (clientX: number): number => {
    const el = barsRef.current;
    if (!el) return 0;
    const rect = el.getBoundingClientRect();
    const x = clientX - rect.left;
    const idx = Math.floor((x / rect.width) * buckets.length);
    return Math.max(0, Math.min(buckets.length - 1, idx));
  };

  const handleMouseDown = (e: React.MouseEvent) => {
    if (!onBrushSelect) return;
    e.preventDefault();
    const idx = getBucketIndex(e.clientX);
    setBrushStart(idx);
    setBrushEnd(idx);
    brushingRef.current = true;

    const onMouseMove = (e: MouseEvent) => {
      if (!brushingRef.current) return;
      setBrushEnd(getBucketIndex(e.clientX));
    };
    const onMouseUp = (e: MouseEvent) => {
      if (!brushingRef.current) return;
      brushingRef.current = false;
      const endIdx = getBucketIndex(e.clientX);
      const lo = Math.min(idx, endIdx);
      const hi = Math.max(idx, endIdx);
      if (lo !== hi) {
        onBrushSelect(buckets[lo].ts, buckets[hi].ts);
      }
      setBrushStart(null);
      setBrushEnd(null);
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
    };
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
  };

  const brushLo =
    brushStart !== null && brushEnd !== null
      ? Math.min(brushStart, brushEnd)
      : null;
  const brushHi =
    brushStart !== null && brushEnd !== null
      ? Math.max(brushStart, brushEnd)
      : null;

  // Pan handlers.
  const axisRef = useRef<HTMLDivElement>(null);
  const panStartX = useRef<number>(0);
  const panAxisWidth = useRef<number>(1);
  const panningRef = useRef(false);
  const [panOffset, setPanOffset] = useState(0);

  const handlePanStep = (direction: -1 | 1) => {
    if (!onPan || buckets.length < 2) return;
    const windowMs =
      buckets[buckets.length - 1].ts.getTime() - buckets[0].ts.getTime();
    const stepMs = windowMs / 2;
    const first = buckets[0].ts.getTime();
    const last = buckets[buckets.length - 1].ts.getTime();
    onPan(
      new Date(first + direction * stepMs),
      new Date(last + direction * stepMs),
    );
  };

  const handleAxisMouseDown = (e: React.MouseEvent) => {
    if (!onPan || buckets.length < 2) return;
    e.preventDefault();
    panStartX.current = e.clientX;
    panAxisWidth.current = axisRef.current?.getBoundingClientRect().width || 1;
    panningRef.current = true;
    document.body.style.cursor = "grabbing";
    document.body.style.userSelect = "none";

    const onMouseMove = (e: MouseEvent) => {
      setPanOffset(e.clientX - panStartX.current);
    };
    const onMouseUp = (e: MouseEvent) => {
      panningRef.current = false;
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      setPanOffset(0);
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);

      const el = axisRef.current;
      if (!el) return;
      const deltaX = panStartX.current - e.clientX; // drag left = positive = go back
      const axisWidth = el.getBoundingClientRect().width;
      if (Math.abs(deltaX) < 3) return; // ignore tiny movements
      const windowMs =
        buckets[buckets.length - 1].ts.getTime() - buckets[0].ts.getTime();
      const deltaMs = (deltaX / axisWidth) * windowMs;
      const first = buckets[0].ts.getTime();
      const last = buckets[buckets.length - 1].ts.getTime();
      onPan(new Date(first + deltaMs), new Date(last + deltaMs));
    };
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
  };

  // Format time label based on range span.
  const rangeMs =
    buckets.length > 1
      ? buckets[buckets.length - 1].ts.getTime() - buckets[0].ts.getTime()
      : 0;

  const formatTime = (d: Date) => {
    if (rangeMs > 24 * 60 * 60 * 1000) {
      return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    }
    if (rangeMs < 60 * 60 * 1000) {
      return d.toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      });
    }
    return d.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
  };

  // Show ~5 evenly spaced time labels.
  const labelCount = Math.min(5, buckets.length);
  const labelStep = Math.max(1, Math.floor(buckets.length / labelCount));

  // Compute human-readable pan delta during drag.
  const windowMs =
    buckets.length > 1
      ? buckets[buckets.length - 1].ts.getTime() - buckets[0].ts.getTime()
      : 0;
  const panDeltaMs =
    panOffset !== 0 ? -((panOffset / panAxisWidth.current) * windowMs) : 0;

  const formatDuration = (ms: number): string => {
    const abs = Math.abs(ms);
    const sign = ms < 0 ? "-" : "+";
    if (abs < 1000) return `${sign}${Math.round(abs)}ms`;
    if (abs < 60_000) return `${sign}${(abs / 1000).toFixed(1)}s`;
    if (abs < 3_600_000) {
      const m = Math.floor(abs / 60_000);
      const s = Math.round((abs % 60_000) / 1000);
      return s > 0 ? `${sign}${m}m ${s}s` : `${sign}${m}m`;
    }
    if (abs < 86_400_000) {
      const h = Math.floor(abs / 3_600_000);
      const m = Math.round((abs % 3_600_000) / 60_000);
      return m > 0 ? `${sign}${h}h ${m}m` : `${sign}${h}h`;
    }
    const d = Math.floor(abs / 86_400_000);
    const h = Math.round((abs % 86_400_000) / 3_600_000);
    return h > 0 ? `${sign}${d}d ${h}h` : `${sign}${d}d`;
  };

  return (
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
          {totalCount.toLocaleString()} records
        </span>
      </div>
      <div className="relative" style={{ height: barHeight }}>
        {/* Pan delta indicator — centered over bars */}
        {panOffset !== 0 && (
          <div
            className={`absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 px-2 py-0.5 text-[0.7em] font-mono rounded whitespace-nowrap pointer-events-none z-20 ${c(
              "bg-ink-surface text-copper border border-copper/30",
              "bg-light-surface text-copper border border-copper/30",
            )}`}
          >
            {formatDuration(panDeltaMs)}
          </div>
        )}
        <div
          ref={barsRef}
          onMouseDown={handleMouseDown}
          className={`relative flex items-end h-full gap-px ${onBrushSelect ? "cursor-crosshair" : ""}`}
        >
          {brushLo !== null && brushHi !== null && (
            <div
              className="absolute inset-y-0 bg-copper/20 pointer-events-none z-10 rounded-sm"
              style={{
                left: `${(brushLo / buckets.length) * 100}%`,
                width: `${((brushHi - brushLo + 1) / buckets.length) * 100}%`,
              }}
            />
          )}
          {buckets.map((bucket, i) => {
            const pct = maxCount > 0 ? bucket.count / maxCount : 0;
            return (
              <div
                key={i}
                className="flex-1 min-w-0 group relative"
                style={{ height: "100%" }}
              >
                {bucket.count > 0 && (
                  <div
                    className={`absolute bottom-0 inset-x-0 rounded-t-sm transition-colors ${c(
                      "bg-copper/60 group-hover:bg-copper",
                      "bg-copper/50 group-hover:bg-copper/80",
                    )}`}
                    style={{
                      height: `${Math.max(pct * 100, 4)}%`,
                    }}
                  />
                )}
                {/* Tooltip */}
                <div
                  className={`absolute bottom-full left-1/2 -translate-x-1/2 mb-1 px-2 py-1 text-[0.7em] font-mono rounded whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity z-10 ${c("bg-ink-surface text-text-bright border border-ink-border-subtle", "bg-light-surface text-light-text-bright border border-light-border-subtle")}`}
                >
                  {bucket.count.toLocaleString()} &middot;{" "}
                  {formatTime(bucket.ts)}
                </div>
              </div>
            );
          })}
        </div>
      </div>
      {/* Time axis with pan arrows + draggable labels */}
      <div className="flex items-center mt-1 gap-1">
        {onPan && (
          <button
            onClick={() => handlePanStep(-1)}
            className={`text-[0.7em] px-1 rounded transition-colors shrink-0 ${c(
              "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
              "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
            )}`}
            title="Pan backward"
          >
            {"\u25C2"}
          </button>
        )}
        <div
          ref={axisRef}
          onMouseDown={handleAxisMouseDown}
          className={`flex-1 flex justify-between overflow-hidden ${onPan ? "cursor-grab active:cursor-grabbing" : ""}`}
          style={
            panOffset ? { transform: `translateX(${panOffset}px)` } : undefined
          }
        >
          {Array.from({ length: labelCount }, (_, i) => {
            const idx = Math.min(i * labelStep, buckets.length - 1);
            return (
              <span
                key={i}
                className={`text-[0.65em] font-mono select-none ${c("text-text-ghost", "text-light-text-ghost")}`}
              >
                {formatTime(buckets[idx].ts)}
              </span>
            );
          })}
        </div>
        {onPan && (
          <button
            onClick={() => handlePanStep(1)}
            className={`text-[0.7em] px-1 rounded transition-colors shrink-0 ${c(
              "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
              "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
            )}`}
            title="Pan forward"
          >
            {"\u25B8"}
          </button>
        )}
      </div>
    </div>
  );
}

/* ── Utilities ── */

function formatChunkId(chunkId: string): string {
  return chunkId || "N/A";
}

function highlightMatches(
  text: string,
  tokens: string[],
): { text: string; highlighted: boolean }[] {
  if (tokens.length === 0) return [{ text, highlighted: false }];

  const pattern = new RegExp(
    `(${tokens.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|")})`,
    "gi",
  );
  const parts: { text: string; highlighted: boolean }[] = [];
  let lastIndex = 0;
  let match;

  while ((match = pattern.exec(text)) !== null) {
    if (match.index > lastIndex) {
      parts.push({
        text: text.slice(lastIndex, match.index),
        highlighted: false,
      });
    }
    parts.push({ text: match[0], highlighted: true });
    lastIndex = pattern.lastIndex;
  }

  if (lastIndex < text.length) {
    parts.push({ text: text.slice(lastIndex), highlighted: false });
  }

  return parts.length > 0 ? parts : [{ text, highlighted: false }];
}

export default EditorialDesign;
