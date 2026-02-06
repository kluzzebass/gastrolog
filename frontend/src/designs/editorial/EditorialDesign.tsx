import { useState, useRef, useEffect, useCallback } from "react";
import {
  useSearch,
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

type Theme = "dark" | "light";

/* ── Client-side extraction utilities ── */

/** Extract key=value pairs from raw log text (simplified port of Go tokenizer.ExtractKeyValues). */
function extractKVPairs(raw: string): { key: string; value: string }[] {
  const results: { key: string; value: string }[] = [];
  const seen = new Set<string>();
  const keyRe =
    /(?:^|[\s,;:()\[\]{}])([a-zA-Z_][a-zA-Z0-9_.]*?)=([^\s,;)\]}"'=&{[]+)/g;
  let m: RegExpExecArray | null;
  while ((m = keyRe.exec(raw)) !== null) {
    const key = m[1].toLowerCase();
    const value = m[2].toLowerCase();
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

const timeRangeMs: Record<string, number> = {
  "15m": 15 * 60 * 1000,
  "1h": 60 * 60 * 1000,
  "6h": 6 * 60 * 60 * 1000,
  "24h": 24 * 60 * 60 * 1000,
  "7d": 7 * 24 * 60 * 60 * 1000,
};

export function EditorialDesign() {
  const [query, setQuery] = useState("");
  const [selectedStore, setSelectedStore] = useState("all");
  const [timeRange, setTimeRange] = useState("1h");
  const [showPlan, setShowPlan] = useState(false);
  const [selectedRecord, setSelectedRecord] = useState<ProtoRecord | null>(
    null,
  );
  const [theme, setTheme] = useState<Theme>("dark");
  const queryInputRef = useRef<HTMLTextAreaElement>(null);
  const sentinelRef = useRef<HTMLDivElement>(null);
  const expressionRef = useRef("");

  const {
    records,
    isSearching,
    error: searchError,
    hasMore,
    search,
    loadMore,
  } = useSearch();
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

  const dark = theme === "dark";

  // Build the full expression sent to the server.
  // The query text box only contains user filters (tokens, kv, severity).
  // Time range and store are sidebar state, injected here at call time.
  const buildExpression = (
    q: string,
    range?: string,
    store?: string,
  ): string => {
    const r = range ?? timeRange;
    const s = store ?? selectedStore;
    const parts: string[] = [];

    if (r !== "All") {
      const ms = timeRangeMs[r];
      if (ms) {
        const now = Date.now();
        parts.push(`start=${new Date(now - ms).toISOString()}`);
        parts.push(`end=${new Date(now).toISOString()}`);
        parts.push("reverse");
      }
    } else {
      parts.push("reverse");
    }
    if (s !== "all") {
      parts.push(`store=${s}`);
    }
    if (q.trim()) {
      parts.push(q.trim());
    }
    return parts.join(" ");
  };

  // Fire search + histogram + explain. Tracks expression for infinite scroll.
  const fireSearch = (q?: string, range?: string, store?: string) => {
    const expr = buildExpression(q ?? query, range, store);
    expressionRef.current = expr;
    search(expr);
    fetchHistogram(expr);
    if (showPlan) explain(expr);
  };

  // On mount: focus input and load initial results for the default time range.
  useEffect(() => {
    queryInputRef.current?.focus();
    fireSearch();
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
    fireSearch();
  };

  const handleShowPlan = () => {
    const next = !showPlan;
    setShowPlan(next);
    if (next) explain(buildExpression(query));
  };

  const allSeverities = ["error", "warn", "info", "debug"];

  // Parse which severities are active from the query string.
  const activeSeverities = allSeverities.filter((s) =>
    query.includes(`level=${s}`),
  );

  // Build the severity portion of the query: single predicate or OR group.
  const buildSeverityExpr = (severities: string[]): string => {
    if (severities.length === 0) return "";
    if (severities.length === 1) return `level=${severities[0]}`;
    return `(${severities.map((s) => `level=${s}`).join(" OR ")})`;
  };

  // Remove any existing severity expression from the query string.
  const stripSeverity = (q: string): string =>
    q
      .replace(/\((?:level=\w+\s+OR\s+)*level=\w+\)/g, "")
      .replace(/\blevel=(?:error|warn|info|debug)\b/g, "")
      .replace(/\s+/g, " ")
      .trim();

  const toggleSeverity = (level: string) => {
    const current = allSeverities.filter((s) => query.includes(`level=${s}`));
    const next = current.includes(level)
      ? current.filter((s) => s !== level)
      : [...current, level];
    const base = stripSeverity(query);
    const sevExpr = buildSeverityExpr(next);
    const newQuery = sevExpr ? `${base} ${sevExpr}`.trim() : base;
    setQuery(newQuery);
    fireSearch(newQuery);
  };

  const handleTimeRange = (range: string) => {
    setTimeRange(range);
    fireSearch(undefined, range);
  };

  const handleStoreSelect = (storeId: string) => {
    setSelectedStore(storeId);
    fireSearch(undefined, undefined, storeId);
  };

  const tokens = extractTokens(query);
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
        <div className="flex items-baseline gap-3">
          <h1
            className={`font-display text-[1.6em] font-semibold tracking-tight leading-none ${c("text-text-bright", "text-light-text-bright")}`}
          >
            GastroLog
          </h1>
          <span
            className={`text-[0.7em] font-body font-medium uppercase tracking-[0.2em] ${c("text-copper-dim", "text-copper")}`}
          >
            Observatory
          </span>
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

          <button
            onClick={() => setTheme((t) => (t === "dark" ? "light" : "dark"))}
            className={`px-2.5 py-1 text-[0.85em] font-mono border rounded transition-all duration-200 ${c(
              "border-ink-border text-text-muted hover:border-copper hover:text-copper",
              "border-light-border text-light-text-muted hover:border-copper hover:text-copper",
            )}`}
          >
            {dark ? "light" : "dark"}
          </button>
        </div>
      </header>

      {/* ── Main Layout ── */}
      <div className="flex flex-1 overflow-hidden">
        {/* ── Sidebar ── */}
        <aside
          className={`w-56 shrink-0 p-4 border-r editorial-scroll overflow-y-auto ${c("border-ink-border-subtle bg-ink", "border-light-border-subtle bg-light-raised")}`}
        >
          {/* Time Range */}
          <SidebarSection title="Time Range" dark={dark}>
            <div className="flex flex-wrap gap-1">
              {["15m", "1h", "6h", "24h", "7d", "All"].map((range) => (
                <button
                  key={range}
                  onClick={() => handleTimeRange(range)}
                  className={`px-2 py-1 text-[0.85em] font-mono rounded transition-all duration-150 ${
                    timeRange === range
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
          </SidebarSection>

          {/* Stores */}
          <SidebarSection title="Stores" dark={dark}>
            <div className="flex flex-col gap-px">
              <StoreButton
                label="All Stores"
                count={statsLoading ? "..." : totalRecords.toLocaleString()}
                active={selectedStore === "all"}
                onClick={() => handleStoreSelect("all")}
                dark={dark}
              />
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
          </SidebarSection>

          {/* Quick Filters */}
          <SidebarSection title="Severity" dark={dark}>
            <div className="flex flex-wrap gap-1.5">
              {[
                { label: "Error", level: "error", color: "severity-error" },
                { label: "Warn", level: "warn", color: "severity-warn" },
                { label: "Info", level: "info", color: "severity-info" },
                { label: "Debug", level: "debug", color: "severity-debug" },
              ].map(({ label, level, color }) => {
                const active = activeSeverities.includes(level);
                return (
                  <button
                    key={level}
                    onClick={() => toggleSeverity(level)}
                    className={`px-2 py-0.5 text-[0.8em] font-medium uppercase tracking-wider rounded-sm border transition-all duration-150 ${
                      active
                        ? `bg-${color} border-${color} text-white`
                        : `border-${color}/40 text-${color} hover:border-${color} hover:bg-${color}/10`
                    }`}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
          </SidebarSection>
        </aside>

        {/* ── Main Content ── */}
        <main
          className={`flex-1 flex flex-col overflow-hidden ${c("bg-ink-raised", "bg-light-bg")}`}
        >
          {/* Query Bar */}
          <div
            className={`px-5 py-4 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
          >
            <div className="flex gap-3 items-start">
              <div className="flex-1 relative">
                <textarea
                  ref={queryInputRef}
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      executeQuery();
                    }
                  }}
                  placeholder="Search logs... tokens for full-text, key=value for attributes"
                  rows={1}
                  className={`query-input w-full px-3 py-2 text-[1em] font-mono border rounded resize-none transition-all duration-200 focus:outline-none ${c(
                    "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost",
                    "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost",
                  )}`}
                />
              </div>
              <button
                onClick={executeQuery}
                disabled={isSearching}
                className="px-5 py-2 text-[0.9em] font-medium rounded bg-copper text-white hover:bg-copper-glow transition-all duration-200 disabled:opacity-40 disabled:cursor-not-allowed whitespace-nowrap"
              >
                {isSearching ? "Searching..." : "Search"}
              </button>
              <button
                onClick={handleShowPlan}
                className={`px-3 py-2 text-[0.85em] font-mono border rounded transition-all duration-200 whitespace-nowrap ${
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

            {searchError && (
              <div className="mt-2.5 px-3 py-2 text-[0.9em] bg-severity-error/10 border border-severity-error/25 rounded text-severity-error">
                {searchError.message}
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
                  onClick={() => setQuery(ex)}
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

          {/* Histogram */}
          {histogramData && histogramData.buckets.length > 0 && (
            <div
              className={`px-5 py-3 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
            >
              <HistogramChart data={histogramData} dark={dark} />
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
                  Results
                </h3>
                <span
                  className={`font-mono text-[0.8em] px-2 py-0.5 rounded ${c("bg-ink-surface text-text-muted", "bg-light-hover text-light-text-muted")}`}
                >
                  {records.length}
                  {hasMore ? "+" : ""}
                </span>
              </div>
              {records.length > 0 && (
                <span
                  className={`font-mono text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  {new Date().toLocaleTimeString("en-US", { hour12: false })}
                </span>
              )}
            </div>

            <div className="flex-1 overflow-y-auto editorial-scroll">
              {records.length === 0 && !isSearching ? (
                <EmptyState dark={dark} />
              ) : (
                <div>
                  {records.map((record, i) => (
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
                  {/* Infinite scroll sentinel */}
                  <div ref={sentinelRef} className="h-1" />
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
        {selectedRecord && (
          <aside
            className={`w-80 shrink-0 border-l overflow-y-auto editorial-scroll animate-fade-in ${c("border-ink-border-subtle bg-ink-surface", "border-light-border-subtle bg-light-surface")}`}
          >
            <div
              className={`flex justify-between items-center px-4 py-3 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
            >
              <h3
                className={`font-display text-[1.15em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
              >
                Detail
              </h3>
              <button
                onClick={() => setSelectedRecord(null)}
                className={`text-sm leading-none w-6 h-6 flex items-center justify-center rounded transition-colors ${c(
                  "text-text-ghost hover:text-text-bright hover:bg-ink-hover",
                  "text-light-text-ghost hover:text-light-text-bright hover:bg-light-hover",
                )}`}
              >
                &times;
              </button>
            </div>

            <DetailPanelContent record={selectedRecord} dark={dark} />
          </aside>
        )}
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
                ? "bg-highlight-bg border border-highlight-border text-highlight-text px-0.5 rounded-sm"
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
}: {
  record: ProtoRecord;
  dark: boolean;
}) {
  const c = (d: string, l: string) => (dark ? d : l);
  const rawText = new TextDecoder().decode(record.raw);
  const rawBytes = record.raw.length;
  const kvPairs = extractKVPairs(rawText);

  const tsRows: { label: string; date: Date }[] = [];
  if (record.sourceTs)
    tsRows.push({ label: "Source", date: record.sourceTs.toDate() });
  if (record.ingestTs)
    tsRows.push({ label: "Ingest", date: record.ingestTs.toDate() });
  if (record.writeTs)
    tsRows.push({ label: "Write", date: record.writeTs.toDate() });

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
              />
            ))}
          </div>
        </DetailSection>
      )}

      {/* Attributes */}
      {Object.keys(record.attrs).length > 0 && (
        <DetailSection label="Attributes" dark={dark}>
          <div className="space-y-0">
            {Object.entries(record.attrs).map(([k, v]) => (
              <DetailRow key={k} label={k} value={v} dark={dark} />
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
}: {
  label: string;
  value: string;
  dark: boolean;
}) {
  return (
    <div
      className={`flex py-1 border-b last:border-b-0 ${dark ? "border-ink-border-subtle" : "border-light-border-subtle"}`}
    >
      <dt
        className={`w-16 shrink-0 text-[0.8em] ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {label}
      </dt>
      <dd
        className={`flex-1 text-[0.85em] font-mono break-all ${dark ? "text-text-normal" : "text-light-text-normal"}`}
      >
        {value}
      </dd>
    </div>
  );
}

function HistogramChart({
  data,
  dark,
}: {
  data: HistogramData;
  dark: boolean;
}) {
  const { buckets } = data;
  if (buckets.length === 0) return null;

  const maxCount = Math.max(...buckets.map((b) => b.count), 1);
  const totalCount = buckets.reduce((sum, b) => sum + b.count, 0);
  const barHeight = 48;
  const c = (d: string, l: string) => (dark ? d : l);

  // Format time label based on range span.
  const rangeMs =
    buckets.length > 1
      ? buckets[buckets.length - 1].ts.getTime() - buckets[0].ts.getTime()
      : 0;

  const formatTime = (d: Date) => {
    if (rangeMs > 24 * 60 * 60 * 1000) {
      return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
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

  return (
    <div>
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
        <div className="flex items-end h-full gap-px">
          {buckets.map((bucket, i) => {
            const pct = maxCount > 0 ? bucket.count / maxCount : 0;
            return (
              <div
                key={i}
                className="flex-1 min-w-0 group relative"
                style={{ height: "100%" }}
              >
                <div
                  className={`absolute bottom-0 inset-x-0 rounded-t-sm transition-colors ${
                    bucket.count > 0
                      ? c(
                          "bg-copper/60 group-hover:bg-copper",
                          "bg-copper/50 group-hover:bg-copper/80",
                        )
                      : c("bg-ink-border-subtle/30", "bg-light-border/30")
                  }`}
                  style={{
                    height:
                      bucket.count > 0 ? `${Math.max(pct * 100, 4)}%` : "2px",
                  }}
                />
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
      {/* Time axis labels */}
      <div className="flex justify-between mt-1">
        {Array.from({ length: labelCount }, (_, i) => {
          const idx = Math.min(i * labelStep, buckets.length - 1);
          return (
            <span
              key={i}
              className={`text-[0.65em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              {formatTime(buckets[idx].ts)}
            </span>
          );
        })}
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
