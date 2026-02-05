import { useState, useRef, useEffect } from "react";
import { useSearch, useExplain, parseQuery } from "../../api/hooks";
import { useStores, useStats } from "../../api/hooks";
import { Record as ProtoRecord, ChunkPlan } from "../../api/client";

type Theme = "dark" | "light";

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
    isLoading: isExplaining,
    explain,
  } = useExplain();
  const { data: stores, isLoading: storesLoading } = useStores();
  const { data: stats, isLoading: statsLoading } = useStats();

  const dark = theme === "dark";

  // Focus query input on mount
  useEffect(() => {
    queryInputRef.current?.focus();
  }, []);

  const executeQuery = () => {
    let fullQuery = query;
    if (!query.includes("start=") && timeRange !== "Custom") {
      const now = new Date();
      const ranges: Record<string, number> = {
        "15m": 15 * 60 * 1000,
        "1h": 60 * 60 * 1000,
        "6h": 6 * 60 * 60 * 1000,
        "24h": 24 * 60 * 60 * 1000,
        "7d": 7 * 24 * 60 * 60 * 1000,
      };
      const start = new Date(
        now.getTime() - (ranges[timeRange] ?? ranges["1h"]!),
      );
      fullQuery = `start=${start.toISOString()} ${query}`.trim();
    }
    if (selectedStore !== "all" && !query.includes("store=")) {
      fullQuery = `store=${selectedStore} ${fullQuery}`.trim();
    }
    search(fullQuery);
    if (showPlan) explain(fullQuery);
  };

  const handleShowPlan = () => {
    const next = !showPlan;
    setShowPlan(next);
    if (next && query) explain(query);
  };

  const toggleFilter = (filter: string) => {
    setQuery((q) =>
      q.includes(filter)
        ? q.replace(filter, "").trim()
        : `${q} ${filter}`.trim(),
    );
  };

  const tokens = parseQuery(query).tokens;
  const totalRecords = stats?.totalRecords ?? BigInt(0);
  const totalStores = stats?.totalStores ?? BigInt(0);
  const sealedChunks = stats?.sealedChunks ?? BigInt(0);
  const totalBytes = stats?.totalBytes ?? BigInt(0);

  // Theme-aware class helper
  const c = (darkCls: string, lightCls: string) => (dark ? darkCls : lightCls);

  return (
    <div
      className={`grain min-h-screen font-body text-[13px] ${c("bg-ink text-text-normal", "light-theme bg-light-bg text-light-text-normal")}`}
    >
      {/* ── Header ── */}
      <header
        className={`flex items-center justify-between px-7 py-3.5 border-b ${c("border-ink-border-subtle bg-ink", "border-light-border-subtle bg-light-raised")}`}
      >
        <div className="flex items-baseline gap-3">
          <h1
            className={`font-display text-[22px] font-semibold tracking-tight leading-none ${c("text-text-bright", "text-light-text-bright")}`}
          >
            GastroLog
          </h1>
          <span
            className={`text-[9px] font-body font-medium uppercase tracking-[0.2em] ${c("text-copper-dim", "text-copper")}`}
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
            className={`px-2.5 py-1 text-[11px] font-mono border rounded transition-all duration-200 ${c(
              "border-ink-border text-text-muted hover:border-copper hover:text-copper",
              "border-light-border text-light-text-muted hover:border-copper hover:text-copper",
            )}`}
          >
            {dark ? "light" : "dark"}
          </button>
        </div>
      </header>

      {/* ── Main Layout ── */}
      <div className="flex min-h-[calc(100vh-52px)]">
        {/* ── Sidebar ── */}
        <aside
          className={`w-56 shrink-0 p-4 border-r editorial-scroll overflow-y-auto ${c("border-ink-border-subtle bg-ink", "border-light-border-subtle bg-light-raised")}`}
        >
          {/* Time Range */}
          <SidebarSection title="Time Range" dark={dark}>
            <div className="flex flex-wrap gap-1">
              {["15m", "1h", "6h", "24h", "7d", "Custom"].map((range) => (
                <button
                  key={range}
                  onClick={() => setTimeRange(range)}
                  className={`px-2 py-1 text-[11px] font-mono rounded transition-all duration-150 ${
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
                onClick={() => setSelectedStore("all")}
                dark={dark}
              />
              {storesLoading ? (
                <div
                  className={`px-2.5 py-1.5 text-[11px] ${c("text-text-ghost", "text-light-text-ghost")}`}
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
                    onClick={() => setSelectedStore(store.id)}
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
                {
                  label: "Error",
                  filter: "level=error",
                  color: "severity-error",
                },
                { label: "Warn", filter: "level=warn", color: "severity-warn" },
                { label: "Info", filter: "level=info", color: "severity-info" },
                {
                  label: "Debug",
                  filter: "level=debug",
                  color: "severity-debug",
                },
              ].map(({ label, filter, color }) => {
                const active = query.includes(filter);
                return (
                  <button
                    key={filter}
                    onClick={() => toggleFilter(filter)}
                    className={`px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider rounded-sm border transition-all duration-150 ${
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
                  className={`query-input w-full px-3 py-2 text-[13px] font-mono border rounded resize-none transition-all duration-200 focus:outline-none ${c(
                    "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost",
                    "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost",
                  )}`}
                />
              </div>
              <button
                onClick={executeQuery}
                disabled={isSearching}
                className="px-5 py-2 text-[12px] font-medium rounded bg-copper text-white hover:bg-copper-glow transition-all duration-200 disabled:opacity-40 disabled:cursor-not-allowed whitespace-nowrap"
              >
                {isSearching ? "Searching..." : "Search"}
              </button>
              <button
                onClick={handleShowPlan}
                className={`px-3 py-2 text-[11px] font-mono border rounded transition-all duration-200 whitespace-nowrap ${
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
              <div className="mt-2.5 px-3 py-2 text-[12px] bg-severity-error/10 border border-severity-error/25 rounded text-severity-error">
                {searchError.message}
              </div>
            )}

            <div
              className={`flex items-center gap-2 mt-2.5 text-[10px] ${c("text-text-ghost", "text-light-text-ghost")}`}
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
              className={`px-5 py-4 border-b animate-fade-up ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
            >
              <h3
                className={`font-display text-base font-semibold mb-3 ${c("text-text-bright", "text-light-text-bright")}`}
              >
                Execution Plan
              </h3>
              {isExplaining ? (
                <div
                  className={`text-[12px] ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Analyzing query plan...
                </div>
              ) : explainChunks.length === 0 ? (
                <div
                  className={`text-[12px] ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Run a query to see the execution plan.
                </div>
              ) : (
                <div className="flex flex-col gap-3 stagger-children">
                  {explainChunks.map((plan, i) => (
                    <PlanChunk key={i} plan={plan} dark={dark} />
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Results */}
          <div className="flex-1 flex flex-col overflow-hidden">
            <div
              className={`flex justify-between items-center px-5 py-2.5 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
            >
              <div className="flex items-center gap-3">
                <h3
                  className={`font-display text-[15px] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
                >
                  Results
                </h3>
                <span
                  className={`font-mono text-[10px] px-2 py-0.5 rounded ${c("bg-ink-surface text-text-muted", "bg-light-hover text-light-text-muted")}`}
                >
                  {records.length}
                  {hasMore ? "+" : ""}
                </span>
              </div>
              {records.length > 0 && (
                <span
                  className={`font-mono text-[10px] ${c("text-text-ghost", "text-light-text-ghost")}`}
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
                  {hasMore && (
                    <button
                      onClick={() => loadMore(query)}
                      disabled={isSearching}
                      className={`w-full py-3 text-[11px] font-mono transition-colors ${c(
                        "text-text-muted hover:text-copper hover:bg-ink-hover",
                        "text-light-text-muted hover:text-copper hover:bg-light-hover",
                      )}`}
                    >
                      {isSearching ? "loading..." : "load more"}
                    </button>
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
                className={`font-display text-[15px] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
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

            <div className="p-4 space-y-4">
              <DetailSection label="Timestamps" dark={dark}>
                <div className="space-y-1.5">
                  {selectedRecord.ingestTs && (
                    <DetailRow
                      label="Ingest"
                      value={selectedRecord.ingestTs.toDate().toISOString()}
                      dark={dark}
                    />
                  )}
                  {selectedRecord.writeTs && (
                    <DetailRow
                      label="Write"
                      value={selectedRecord.writeTs.toDate().toISOString()}
                      dark={dark}
                    />
                  )}
                </div>
              </DetailSection>

              <DetailSection label="Message" dark={dark}>
                <pre
                  className={`text-[11px] font-mono p-3 rounded whitespace-pre-wrap break-words leading-relaxed ${c("bg-ink text-text-normal", "bg-light-bg text-light-text-normal")}`}
                >
                  {new TextDecoder().decode(selectedRecord.raw)}
                </pre>
              </DetailSection>

              {Object.keys(selectedRecord.attrs).length > 0 && (
                <DetailSection label="Attributes" dark={dark}>
                  <div className="space-y-0">
                    {Object.entries(selectedRecord.attrs).map(([k, v]) => (
                      <DetailRow key={k} label={k} value={v} dark={dark} />
                    ))}
                  </div>
                </DetailSection>
              )}

              <DetailSection label="Reference" dark={dark}>
                <div className="space-y-0">
                  <DetailRow
                    label="Store"
                    value={selectedRecord.ref?.storeId ?? "N/A"}
                    dark={dark}
                  />
                  <DetailRow
                    label="Chunk"
                    value={
                      selectedRecord.ref?.chunkId
                        ? formatChunkId(selectedRecord.ref.chunkId)
                        : "N/A"
                    }
                    dark={dark}
                  />
                  <DetailRow
                    label="Position"
                    value={selectedRecord.ref?.pos?.toString() ?? "N/A"}
                    dark={dark}
                  />
                </div>
              </DetailSection>
            </div>
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
        className={`font-mono text-[12px] font-medium ${dark ? "text-text-bright" : "text-light-text-bright"}`}
      >
        {value}
      </span>
      <span
        className={`text-[9px] uppercase tracking-wider ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
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
        className={`text-[9px] font-medium uppercase tracking-[0.15em] mb-2 ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
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
      className={`flex justify-between items-center px-2.5 py-1.5 text-[12px] rounded text-left transition-all duration-150 ${
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
        className={`font-mono text-[10px] ${active ? "text-copper-dim" : dark ? "text-text-ghost" : "text-light-text-ghost"}`}
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
        className={`font-display text-[42px] font-light leading-none mb-3 ${dark ? "text-ink-border" : "text-light-border"}`}
      >
        &empty;
      </div>
      <p
        className={`text-[12px] ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        Enter a query to search your logs
      </p>
      <p
        className={`text-[10px] mt-1 font-mono ${dark ? "text-text-ghost/60" : "text-light-text-ghost/60"}`}
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
  const level = (record.attrs["level"] ?? "info").toUpperCase();
  const rawText = new TextDecoder().decode(record.raw);
  const parts = highlightMatches(rawText, tokens);
  const ingestTime = record.ingestTs ? record.ingestTs.toDate() : new Date();

  const severityColors: Record<string, { border: string; text: string }> = {
    ERROR: { border: "border-l-severity-error", text: "text-severity-error" },
    WARN: { border: "border-l-severity-warn", text: "text-severity-warn" },
    INFO: { border: "border-l-severity-info", text: "text-severity-info" },
    DEBUG: { border: "border-l-severity-debug", text: "text-severity-debug" },
  };
  const sev = severityColors[level] ?? severityColors["INFO"]!;

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
      className={`grid grid-cols-[72px_42px_1fr] gap-2.5 px-5 py-[5px] border-b border-l-2 cursor-pointer transition-colors duration-100 ${sev.border} ${
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
        className={`font-mono text-[10px] tabular-nums self-center ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {ts}
      </span>
      <span
        className={`font-mono text-[9px] font-medium uppercase self-center ${sev.text}`}
      >
        {level.slice(0, 4)}
      </span>
      <div
        className={`font-mono text-[11px] leading-relaxed truncate self-center ${dark ? "text-text-normal" : "text-light-text-normal"}`}
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

function PlanChunk({ plan, dark }: { plan: ChunkPlan; dark: boolean }) {
  return (
    <div
      className={`p-3.5 rounded border ${dark ? "bg-ink-surface border-ink-border-subtle" : "bg-light-surface border-light-border-subtle"}`}
    >
      <div className="flex items-center gap-2 mb-2.5">
        <span
          className={`font-mono text-[11px] font-medium ${dark ? "text-text-bright" : "text-light-text-bright"}`}
        >
          {formatChunkId(plan.chunkId)}
        </span>
        <span
          className={`text-[8px] px-1.5 py-0.5 rounded uppercase tracking-wider font-semibold ${
            plan.sealed
              ? "bg-severity-info/15 text-severity-info border border-severity-info/25"
              : "bg-copper/15 text-copper border border-copper/25"
          }`}
        >
          {plan.sealed ? "Sealed" : "Active"}
        </span>
        <span
          className={`font-mono text-[10px] ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
        >
          {plan.recordCount.toLocaleString()} records
        </span>
        {plan.storeId && (
          <span
            className={`font-mono text-[10px] ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
          >
            [{plan.storeId}]
          </span>
        )}
      </div>

      <div className="flex gap-1.5 mb-2.5 overflow-x-auto pb-1">
        {plan.steps.map((step, j) => (
          <div
            key={j}
            className={`min-w-[110px] p-2 rounded border ${dark ? "bg-ink border-ink-border-subtle" : "bg-light-bg border-light-border-subtle"}`}
          >
            <div className="flex justify-between items-center mb-1">
              <span
                className={`text-[10px] font-semibold capitalize ${dark ? "text-text-bright" : "text-light-text-bright"}`}
              >
                {step.name}
              </span>
              <span
                className={`text-[8px] px-1 py-px rounded uppercase tracking-wide font-medium ${
                  step.action === "seek"
                    ? "bg-severity-info/15 text-severity-info"
                    : step.action === "indexed"
                      ? "bg-severity-info/15 text-severity-info"
                      : "bg-severity-warn/15 text-severity-warn"
                }`}
              >
                {step.action}
              </span>
            </div>
            <div
              className={`font-mono text-[10px] ${dark ? "text-text-muted" : "text-light-text-muted"}`}
            >
              {step.inputEstimate.toLocaleString()} &rarr;{" "}
              {step.outputEstimate.toLocaleString()}
            </div>
            <div
              className={`text-[9px] mt-0.5 ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
            >
              {step.reason}
            </div>
          </div>
        ))}
      </div>

      <div
        className={`flex gap-4 text-[10px] font-mono pt-2 border-t ${dark ? "border-ink-border-subtle text-text-muted" : "border-light-border-subtle text-light-text-muted"}`}
      >
        <span>
          scan:{" "}
          <strong
            className={dark ? "text-text-bright" : "text-light-text-bright"}
          >
            {plan.scanMode}
          </strong>
        </span>
        <span>
          est:{" "}
          <strong
            className={dark ? "text-text-bright" : "text-light-text-bright"}
          >
            ~{plan.estimatedRecords.toLocaleString()}
          </strong>
        </span>
      </div>
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
        className={`text-[9px] font-medium uppercase tracking-[0.15em] mb-1.5 ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
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
        className={`w-16 shrink-0 text-[10px] ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {label}
      </dt>
      <dd
        className={`flex-1 text-[11px] font-mono break-all ${dark ? "text-text-normal" : "text-light-text-normal"}`}
      >
        {value}
      </dd>
    </div>
  );
}

/* ── Utilities ── */

function formatChunkId(chunkId: Uint8Array): string {
  if (chunkId.length !== 16) return "invalid";
  const hex = Array.from(chunkId, (b) => b.toString(16).padStart(2, "0")).join(
    "",
  );
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}...${hex.slice(28)}`;
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
