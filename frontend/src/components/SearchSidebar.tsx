import { TimeRangePicker } from "./TimeRangePicker";
import {
  SidebarSection,
  FieldExplorer,
  StoreButton,
} from "./Sidebar";
import type { StoreInfo } from "../api/gen/gastrolog/v1/store_pb";
import type { FieldSummary } from "../utils";
import { useThemeClass } from "../hooks/useThemeClass";

interface SearchSidebarProps {
  dark: boolean;
  sidebarWidth: number;
  sidebarCollapsed: boolean;
  setSidebarCollapsed: (v: boolean) => void;
  handleSidebarResize: (e: React.MouseEvent) => void;
  resizing: boolean;
  rangeStart: Date | null;
  rangeEnd: Date | null;
  timeRange: string;
  onTimeRangeChange: (range: string) => void;
  onCustomRange: (start: Date, end: Date) => void;
  stores: StoreInfo[] | undefined;
  storesLoading: boolean;
  statsLoading: boolean;
  totalRecords: bigint;
  selectedStore: string;
  onStoreSelect: (storeId: string) => void;
  activeSeverities: string[];
  onToggleSeverity: (level: string) => void;
  attrFields: FieldSummary[];
  kvFields: FieldSummary[];
  onFieldSelect: (key: string, value: string) => void;
  activeQuery: string;
}

export function SearchSidebar({
  dark,
  sidebarWidth,
  sidebarCollapsed,
  setSidebarCollapsed,
  handleSidebarResize,
  resizing,
  rangeStart,
  rangeEnd,
  timeRange,
  onTimeRangeChange,
  onCustomRange,
  stores,
  storesLoading,
  statsLoading,
  totalRecords,
  selectedStore,
  onStoreSelect,
  activeSeverities,
  onToggleSeverity,
  attrFields,
  kvFields,
  onFieldSelect,
  activeQuery,
}: Readonly<SearchSidebarProps>) {
  const c = useThemeClass(dark);

  const allSeverities = [
    { label: "Error", level: "error", color: "severity-error" },
    { label: "Warn", level: "warn", color: "severity-warn" },
    { label: "Info", level: "info", color: "severity-info" },
    { label: "Debug", level: "debug", color: "severity-debug" },
    { label: "Trace", level: "trace", color: "severity-trace" },
  ];

  const severityStyles: Record<string, { active: string; inactive: string }> = {
    "severity-error": {
      active: "bg-severity-error border-severity-error text-white",
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
      active: "bg-severity-debug border-severity-debug text-white",
      inactive:
        "border-severity-debug/40 text-severity-debug hover:border-severity-debug hover:bg-severity-debug/10",
    },
    "severity-trace": {
      active: "bg-severity-trace border-severity-trace text-white",
      inactive:
        "border-severity-trace/40 text-severity-trace hover:border-severity-trace hover:bg-severity-trace/10",
    },
  };

  return (
    <>
      {sidebarCollapsed && (
        <button
          onClick={() => setSidebarCollapsed(false)}
          className={`shrink-0 px-1 flex items-center border-r transition-colors ${c(
            "border-ink-border-subtle bg-ink text-text-ghost hover:text-text-muted hover:bg-ink-hover",
            "border-light-border-subtle bg-light-raised text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
          )}`}
          aria-label="Expand sidebar"
          title="Expand sidebar"
        >
          {"\u25B8"}
        </button>
      )}
      <aside
        aria-label="Sidebar"
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
            onPresetClick={onTimeRangeChange}
            onApply={onCustomRange}
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
                  label={store.name || store.id}
                  count={store.recordCount.toLocaleString()}
                  active={selectedStore === store.id}
                  onClick={() => onStoreSelect(store.id)}
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
            {allSeverities.map(({ label, level, color }) => {
              const active = activeSeverities.includes(level);
              const s = severityStyles[color]!;
              return (
                <button
                  key={level}
                  onClick={() => onToggleSeverity(level)}
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
            onSelect={onFieldSelect}
            activeQuery={activeQuery}
          />
        </SidebarSection>

        <SidebarSection title="Extracted Fields" dark={dark}>
          <FieldExplorer
            fields={kvFields}
            dark={dark}
            onSelect={onFieldSelect}
            activeQuery={activeQuery}
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
            aria-label="Collapse sidebar"
            title="Collapse sidebar"
          >
            {"\u25C2"}
          </button>
        </div>
      )}
    </>
  );
}
