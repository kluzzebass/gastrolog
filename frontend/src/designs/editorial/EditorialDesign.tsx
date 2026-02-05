import { useState, useEffect } from "react";
import {
  generateMockRecords,
  highlightMatches,
  mockStats,
  mockStores,
  mockExplainPlan,
} from "../../api/mock";
import type { Record } from "../../types/api";

export function EditorialDesign() {
  const [query, setQuery] = useState("");
  const [records, setRecords] = useState<Record[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [selectedStore, setSelectedStore] = useState("all");
  const [timeRange, setTimeRange] = useState("1h");
  const [showPlan, setShowPlan] = useState(false);
  const [selectedRecord, setSelectedRecord] = useState<Record | null>(null);
  const [theme, setTheme] = useState<"light" | "dark">("light");

  useEffect(() => {
    setRecords(generateMockRecords(30));
  }, []);

  const executeQuery = () => {
    setIsSearching(true);
    setTimeout(() => {
      setRecords(generateMockRecords(50));
      setIsSearching(false);
    }, 400);
  };

  const tokens = query.split(/\s+/).filter((t) => t && !t.includes("="));

  const formatTime = (date: Date) => {
    return date.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  };

  const formatDate = (date: Date) => {
    return date.toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  };

  const isDark = theme === "dark";

  return (
    <div
      className={`min-h-screen font-sans ${isDark ? "bg-dark-bg text-dark-text" : "bg-cream text-text-primary"}`}
    >
      {/* Header */}
      <header
        className={`flex items-center justify-between px-8 py-4 border-b ${isDark ? "border-dark-border bg-dark-bg" : "border-border bg-cream"}`}
      >
        <div className="flex flex-col">
          <h1
            className={`font-serif text-2xl font-semibold tracking-tight ${isDark ? "text-dark-text" : "text-charcoal"}`}
          >
            GastroLog
          </h1>
          <span
            className={`text-[10px] uppercase tracking-widest ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
          >
            Intelligent Log Management
          </span>
        </div>

        <nav className="flex gap-1.5">
          {["Query", "Stores", "Analytics", "Settings"].map((item, i) => (
            <button
              key={item}
              className={`px-4 py-2 text-sm rounded transition-colors ${
                i === 0
                  ? isDark
                    ? "bg-dark-text text-dark-bg"
                    : "bg-charcoal text-cream"
                  : isDark
                    ? "text-dark-text-secondary hover:text-dark-text hover:bg-dark-bg-secondary"
                    : "text-text-secondary hover:text-text-primary hover:bg-cream-dark"
              }`}
            >
              {item}
            </button>
          ))}
        </nav>

        <div className="flex items-center gap-4">
          <button
            onClick={() => setTheme((t) => (t === "light" ? "dark" : "light"))}
            className={`px-3 py-1.5 text-sm border rounded transition-colors ${
              isDark
                ? "border-dark-border text-dark-text-secondary hover:border-gold"
                : "border-border text-text-secondary hover:border-gold"
            }`}
          >
            {isDark ? "○ Light" : "◐ Dark"}
          </button>
          <div className="flex items-center gap-4">
            <div className="flex flex-col items-end">
              <span
                className={`font-serif text-xl font-medium ${isDark ? "text-dark-text" : "text-charcoal"}`}
              >
                {mockStats.totalRecords.toLocaleString()}
              </span>
              <span
                className={`text-[10px] uppercase tracking-wide ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
              >
                Records
              </span>
            </div>
            <div
              className={`w-px h-8 ${isDark ? "bg-dark-border" : "bg-border"}`}
            />
            <div className="flex flex-col items-end">
              <span
                className={`font-serif text-xl font-medium ${isDark ? "text-dark-text" : "text-charcoal"}`}
              >
                {mockStats.totalStores}
              </span>
              <span
                className={`text-[10px] uppercase tracking-wide ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
              >
                Stores
              </span>
            </div>
          </div>
        </div>
      </header>

      {/* Main Layout */}
      <div className="flex min-h-[calc(100vh-70px)]">
        {/* Sidebar */}
        <aside
          className={`w-60 p-5 border-r ${isDark ? "border-dark-border bg-dark-bg" : "border-border bg-cream"}`}
        >
          {/* Time Range */}
          <section className="mb-6">
            <h3
              className={`text-xs font-bold uppercase tracking-wider mb-3 ${isDark ? "text-dark-text" : "text-text-primary"}`}
            >
              Time Range
            </h3>
            <div className="flex flex-wrap gap-1.5">
              {["15m", "1h", "6h", "24h", "7d", "Custom"].map((range) => (
                <button
                  key={range}
                  onClick={() => setTimeRange(range)}
                  className={`px-2.5 py-1.5 text-xs border rounded transition-colors ${
                    timeRange === range
                      ? isDark
                        ? "bg-dark-text border-dark-text text-dark-bg"
                        : "bg-charcoal border-charcoal text-cream"
                      : isDark
                        ? "border-dark-border text-dark-text-secondary hover:border-dark-text hover:text-dark-text"
                        : "border-border text-text-secondary hover:border-charcoal hover:text-charcoal"
                  }`}
                >
                  {range}
                </button>
              ))}
            </div>
          </section>

          {/* Stores */}
          <section className="mb-6">
            <h3
              className={`text-xs font-bold uppercase tracking-wider mb-3 ${isDark ? "text-dark-text" : "text-text-primary"}`}
            >
              Stores
            </h3>
            <div className="flex flex-col gap-0.5">
              <button
                onClick={() => setSelectedStore("all")}
                className={`flex justify-between items-center px-3 py-2 text-sm rounded text-left transition-colors ${
                  selectedStore === "all"
                    ? "bg-gold text-charcoal"
                    : isDark
                      ? "text-dark-text-secondary hover:bg-dark-bg-secondary hover:text-dark-text"
                      : "text-text-secondary hover:bg-cream-dark hover:text-text-primary"
                }`}
              >
                <span className="font-medium">All Stores</span>
                <span
                  className={`text-xs ${selectedStore === "all" ? "text-charcoal/70" : isDark ? "text-dark-text-muted" : "text-text-muted"}`}
                >
                  {mockStats.totalRecords.toLocaleString()}
                </span>
              </button>
              {mockStores.map((store) => (
                <button
                  key={store.id}
                  onClick={() => setSelectedStore(store.id)}
                  className={`flex justify-between items-center px-3 py-2 text-sm rounded text-left transition-colors ${
                    selectedStore === store.id
                      ? "bg-gold text-charcoal"
                      : isDark
                        ? "text-dark-text-secondary hover:bg-dark-bg-secondary hover:text-dark-text"
                        : "text-text-secondary hover:bg-cream-dark hover:text-text-primary"
                  }`}
                >
                  <span className="font-medium">{store.id}</span>
                  <span
                    className={`text-xs ${selectedStore === store.id ? "text-charcoal/70" : isDark ? "text-dark-text-muted" : "text-text-muted"}`}
                  >
                    {store.recordCount.toLocaleString()}
                  </span>
                </button>
              ))}
            </div>
          </section>

          {/* Quick Filters */}
          <section className="mb-6">
            <h3
              className={`text-xs font-bold uppercase tracking-wider mb-3 ${isDark ? "text-dark-text" : "text-text-primary"}`}
            >
              Quick Filters
            </h3>
            <div className="flex flex-wrap gap-1.5">
              <button className="px-2.5 py-1 text-xs border border-error text-error rounded-full hover:bg-error hover:text-white transition-colors">
                Errors
              </button>
              <button className="px-2.5 py-1 text-xs border border-warn text-warn rounded-full hover:bg-warn hover:text-white transition-colors">
                Warnings
              </button>
              <button className="px-2.5 py-1 text-xs border border-info text-info rounded-full hover:bg-info hover:text-white transition-colors">
                Info
              </button>
              <button
                className={`px-2.5 py-1 text-xs border rounded-full transition-colors ${isDark ? "border-dark-border text-dark-text-muted hover:bg-dark-text-muted hover:text-white" : "border-debug text-debug hover:bg-debug hover:text-white"}`}
              >
                Debug
              </button>
            </div>
          </section>

          {/* Statistics */}
          <section>
            <h3
              className={`text-xs font-bold uppercase tracking-wider mb-3 ${isDark ? "text-dark-text" : "text-text-primary"}`}
            >
              Statistics
            </h3>
            <div className="grid grid-cols-2 gap-2">
              <div
                className={`p-3 rounded-md text-center ${isDark ? "bg-dark-bg-secondary" : "bg-cream-dark"}`}
              >
                <span
                  className={`font-serif text-xl font-medium block ${isDark ? "text-dark-text" : "text-charcoal"}`}
                >
                  {mockStats.sealedChunks}
                </span>
                <span
                  className={`text-[10px] uppercase tracking-wide ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
                >
                  Sealed Chunks
                </span>
              </div>
              <div
                className={`p-3 rounded-md text-center ${isDark ? "bg-dark-bg-secondary" : "bg-cream-dark"}`}
              >
                <span
                  className={`font-serif text-xl font-medium block ${isDark ? "text-dark-text" : "text-charcoal"}`}
                >
                  {(mockStats.totalBytes / 1024 / 1024).toFixed(0)}
                </span>
                <span
                  className={`text-[10px] uppercase tracking-wide ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
                >
                  MB Stored
                </span>
              </div>
            </div>
          </section>
        </aside>

        {/* Main Content */}
        <main
          className={`flex-1 p-6 overflow-y-auto ${isDark ? "bg-dark-bg-secondary" : "bg-cream"}`}
        >
          {/* Query Card */}
          <div
            className={`rounded-lg p-5 mb-4 border shadow-sm ${isDark ? "bg-dark-bg-card border-dark-border" : "bg-white border-border"}`}
          >
            <div className="flex justify-between items-center mb-4">
              <h2
                className={`font-serif text-lg font-semibold ${isDark ? "text-dark-text" : "text-charcoal"}`}
              >
                Search Logs
              </h2>
              <button
                onClick={() => setShowPlan(!showPlan)}
                className={`px-3 py-1.5 text-xs border rounded transition-colors ${
                  isDark
                    ? "border-dark-border text-dark-text-secondary hover:border-gold hover:text-gold"
                    : "border-border text-text-secondary hover:border-gold hover:text-gold-muted"
                }`}
              >
                {showPlan ? "Hide Plan" : "View Execution Plan"}
              </button>
            </div>

            <div className="flex gap-3 mb-3">
              <textarea
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Enter your query... Use tokens for full-text search, key=value for attribute filters"
                rows={2}
                className={`flex-1 px-3 py-2.5 text-sm font-mono border rounded-md resize-none transition-colors focus:outline-none focus:ring-2 focus:ring-gold/20 focus:border-gold ${
                  isDark
                    ? "bg-dark-bg-secondary border-dark-border text-dark-text placeholder:text-dark-text-muted"
                    : "bg-cream-dark border-border text-text-primary placeholder:text-text-muted"
                }`}
              />
              <button
                onClick={executeQuery}
                disabled={isSearching}
                className={`px-6 py-2.5 text-sm font-medium rounded-md whitespace-nowrap transition-colors disabled:opacity-60 disabled:cursor-not-allowed ${
                  isDark
                    ? "bg-dark-text text-dark-bg hover:opacity-90"
                    : "bg-charcoal text-cream hover:bg-charcoal-light"
                }`}
              >
                {isSearching ? "Searching..." : "Search"}
              </button>
            </div>

            <div
              className={`flex items-center gap-2 text-xs ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
            >
              <span>Examples:</span>
              <code
                className={`px-1.5 py-0.5 rounded font-mono text-[10px] ${isDark ? "bg-dark-bg-secondary text-dark-text-secondary" : "bg-cream-dark text-text-secondary"}`}
              >
                error timeout
              </code>
              <code
                className={`px-1.5 py-0.5 rounded font-mono text-[10px] ${isDark ? "bg-dark-bg-secondary text-dark-text-secondary" : "bg-cream-dark text-text-secondary"}`}
              >
                level=ERROR service=payment
              </code>
              <code
                className={`px-1.5 py-0.5 rounded font-mono text-[10px] ${isDark ? "bg-dark-bg-secondary text-dark-text-secondary" : "bg-cream-dark text-text-secondary"}`}
              >
                start=2024-02-05T10:00:00Z limit=100
              </code>
            </div>
          </div>

          {/* Execution Plan */}
          {showPlan && (
            <div
              className={`rounded-lg p-5 mb-4 border shadow-sm ${isDark ? "bg-dark-bg-card border-dark-border" : "bg-white border-border"}`}
            >
              <h3
                className={`font-serif text-base font-semibold mb-4 ${isDark ? "text-dark-text" : "text-charcoal"}`}
              >
                Execution Plan
              </h3>
              <div className="flex flex-col gap-4">
                {mockExplainPlan.map((plan, i) => (
                  <div
                    key={i}
                    className={`p-4 rounded-md ${isDark ? "bg-dark-bg-secondary" : "bg-cream-dark"}`}
                  >
                    <div className="flex items-center gap-2.5 mb-3">
                      <span
                        className={`font-mono text-xs font-medium ${isDark ? "text-dark-text" : "text-charcoal"}`}
                      >
                        {plan.chunkId}
                      </span>
                      <span
                        className={`text-[9px] px-1.5 py-0.5 rounded uppercase font-semibold ${
                          plan.sealed
                            ? "bg-info text-white"
                            : "bg-gold text-charcoal"
                        }`}
                      >
                        {plan.sealed ? "Sealed" : "Active"}
                      </span>
                      <span
                        className={`text-xs ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
                      >
                        {plan.recordCount.toLocaleString()} records
                      </span>
                    </div>
                    <div className="flex gap-2 mb-3 overflow-x-auto pb-1">
                      {plan.steps.map((step, j) => (
                        <div
                          key={j}
                          className={`min-w-[120px] p-2.5 rounded-md border ${isDark ? "bg-dark-bg-card border-dark-border" : "bg-white border-border"}`}
                        >
                          <div className="flex justify-between items-center mb-1.5">
                            <span
                              className={`text-xs font-semibold capitalize ${isDark ? "text-dark-text" : "text-charcoal"}`}
                            >
                              {step.name}
                            </span>
                            <span
                              className={`text-[9px] px-1.5 py-0.5 rounded uppercase font-medium ${
                                step.action === "seek"
                                  ? "bg-blue-100 text-blue-700"
                                  : step.action === "indexed"
                                    ? "bg-green-100 text-green-700"
                                    : "bg-orange-100 text-orange-700"
                              }`}
                            >
                              {step.action}
                            </span>
                          </div>
                          <div
                            className={`font-mono text-xs mb-0.5 ${isDark ? "text-dark-text-secondary" : "text-text-secondary"}`}
                          >
                            {step.inputEstimate.toLocaleString()} →{" "}
                            {step.outputEstimate.toLocaleString()}
                          </div>
                          <div
                            className={`text-[10px] ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
                          >
                            {step.reason}
                          </div>
                        </div>
                      ))}
                    </div>
                    <div
                      className={`flex gap-5 text-xs pt-2.5 border-t ${isDark ? "border-dark-border text-dark-text-secondary" : "border-border text-text-secondary"}`}
                    >
                      <span>
                        Scan:{" "}
                        <strong
                          className={
                            isDark ? "text-dark-text" : "text-charcoal"
                          }
                        >
                          {plan.scanMode}
                        </strong>
                      </span>
                      <span>
                        Estimated:{" "}
                        <strong
                          className={
                            isDark ? "text-dark-text" : "text-charcoal"
                          }
                        >
                          ~{plan.estimatedRecords} records
                        </strong>
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Results */}
          <div
            className={`rounded-lg overflow-hidden border shadow-sm ${isDark ? "bg-dark-bg-card border-dark-border" : "bg-white border-border"}`}
          >
            <div
              className={`flex justify-between items-center px-5 py-3.5 border-b ${isDark ? "border-dark-border" : "border-border"}`}
            >
              <h3
                className={`font-serif text-base font-semibold flex items-center gap-2.5 ${isDark ? "text-dark-text" : "text-charcoal"}`}
              >
                Results
                <span
                  className={`font-sans text-xs font-medium px-2 py-0.5 rounded-full ${isDark ? "bg-dark-bg-secondary text-dark-text-muted" : "bg-cream-dark text-text-muted"}`}
                >
                  {records.length} records
                </span>
              </h3>
              <div
                className={`text-xs ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
              >
                {formatDate(new Date())} · {formatTime(new Date())}
              </div>
            </div>

            <div className="max-h-[calc(100vh-320px)] overflow-y-auto">
              {records.map((record, i) => (
                <LogEntry
                  key={i}
                  record={record}
                  tokens={tokens}
                  isSelected={selectedRecord === record}
                  onSelect={() =>
                    setSelectedRecord(selectedRecord === record ? null : record)
                  }
                  isDark={isDark}
                />
              ))}
            </div>
          </div>
        </main>

        {/* Detail Panel */}
        {selectedRecord && (
          <aside
            className={`w-80 p-5 border-l overflow-y-auto ${isDark ? "bg-dark-bg-card border-dark-border" : "bg-white border-border"}`}
          >
            <div className="flex justify-between items-center mb-5">
              <h3
                className={`font-serif text-base font-semibold ${isDark ? "text-dark-text" : "text-charcoal"}`}
              >
                Log Details
              </h3>
              <button
                onClick={() => setSelectedRecord(null)}
                className={`text-xl leading-none ${isDark ? "text-dark-text-muted hover:text-dark-text" : "text-text-muted hover:text-charcoal"}`}
              >
                ×
              </button>
            </div>

            <div className="mb-5">
              <h4
                className={`text-[10px] font-semibold uppercase tracking-wide mb-1.5 ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
              >
                Timestamp
              </h4>
              <p
                className={`text-sm ${isDark ? "text-dark-text" : "text-text-primary"}`}
              >
                {selectedRecord.ingestTs.toISOString()}
              </p>
            </div>

            <div className="mb-5">
              <h4
                className={`text-[10px] font-semibold uppercase tracking-wide mb-1.5 ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
              >
                Message
              </h4>
              <pre
                className={`text-xs font-mono p-3 rounded-md whitespace-pre-wrap break-words leading-relaxed ${isDark ? "bg-dark-bg-secondary text-dark-text" : "bg-cream-dark text-text-primary"}`}
              >
                {selectedRecord.raw}
              </pre>
            </div>

            <div className="mb-5">
              <h4
                className={`text-[10px] font-semibold uppercase tracking-wide mb-1.5 ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
              >
                Attributes
              </h4>
              <dl>
                {Object.entries(selectedRecord.attrs).map(([k, v]) => (
                  <div
                    key={k}
                    className={`flex py-1.5 border-b last:border-b-0 ${isDark ? "border-dark-border" : "border-border"}`}
                  >
                    <dt
                      className={`w-20 text-xs shrink-0 ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
                    >
                      {k}
                    </dt>
                    <dd
                      className={`flex-1 text-xs font-mono break-all ${isDark ? "text-dark-text" : "text-text-primary"}`}
                    >
                      {v}
                    </dd>
                  </div>
                ))}
              </dl>
            </div>

            <div>
              <h4
                className={`text-[10px] font-semibold uppercase tracking-wide mb-1.5 ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
              >
                Reference
              </h4>
              <dl>
                {[
                  ["Store", selectedRecord.ref.storeId],
                  ["Chunk", selectedRecord.ref.chunkId],
                  ["Position", selectedRecord.ref.pos],
                ].map(([k, v]) => (
                  <div
                    key={k}
                    className={`flex py-1.5 border-b last:border-b-0 ${isDark ? "border-dark-border" : "border-border"}`}
                  >
                    <dt
                      className={`w-20 text-xs shrink-0 ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
                    >
                      {k}
                    </dt>
                    <dd
                      className={`flex-1 text-xs font-mono break-all ${isDark ? "text-dark-text" : "text-text-primary"}`}
                    >
                      {v}
                    </dd>
                  </div>
                ))}
              </dl>
            </div>
          </aside>
        )}
      </div>
    </div>
  );
}

function LogEntry({
  record,
  tokens,
  isSelected,
  onSelect,
  isDark,
}: {
  record: Record;
  tokens: string[];
  isSelected: boolean;
  onSelect: () => void;
  isDark: boolean;
}) {
  const level = record.attrs.level || "INFO";
  const parts = highlightMatches(record.raw, tokens);

  const formatTime = (date: Date) => {
    return date.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      fractionalSecondDigits: 3,
      hour12: false,
    });
  };

  const levelColors: Record<string, string> = {
    ERROR: "border-l-error text-error",
    WARN: "border-l-warn text-warn",
    INFO: "border-l-info text-info",
    DEBUG: isDark
      ? "border-l-dark-text-muted text-dark-text-muted"
      : "border-l-debug text-debug",
  };

  return (
    <article
      onClick={onSelect}
      className={`grid grid-cols-[80px_50px_140px_1fr] gap-3 px-5 py-1.5 border-b border-l-2 cursor-pointer text-xs leading-relaxed items-center transition-colors ${
        levelColors[level] || levelColors.INFO
      } ${
        isSelected
          ? isDark
            ? "bg-dark-bg-secondary"
            : "bg-cream-dark"
          : isDark
            ? "hover:bg-dark-bg-secondary border-dark-border"
            : "hover:bg-cream-dark border-border"
      }`}
    >
      <span
        className={`font-mono text-[11px] ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
      >
        {formatTime(record.ingestTs)}
      </span>
      <span
        className={`text-[10px] font-semibold uppercase tracking-tight ${levelColors[level]?.split(" ")[1] || ""}`}
      >
        {level}
      </span>
      <div className="flex flex-col min-w-0">
        <span
          className={`text-xs font-medium truncate ${isDark ? "text-dark-text" : "text-text-primary"}`}
        >
          {record.attrs.host}
        </span>
        <span
          className={`text-[11px] truncate ${isDark ? "text-dark-text-muted" : "text-text-muted"}`}
        >
          {record.attrs.service}
        </span>
      </div>
      <div
        className={`font-mono text-xs truncate ${isDark ? "text-dark-text-secondary" : "text-text-secondary"}`}
      >
        {parts.map((part, i) => (
          <span
            key={i}
            className={
              part.highlighted
                ? "bg-highlight-bg text-highlight-text px-0.5 rounded font-medium"
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

export default EditorialDesign;
