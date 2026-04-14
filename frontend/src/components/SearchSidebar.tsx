import { TimeRangePicker } from "./TimeRangePicker";
import {
  SidebarSection,
  FieldExplorer,
  VaultButton,
} from "./Sidebar";
import type { VaultInfo } from "../api/gen/gastrolog/v1/vault_pb";
import type { FieldSummary } from "../utils";
import type { ResizeProps } from "../hooks/usePanelResize";
import { useThemeClass } from "../hooks/useThemeClass";
import { SEVERITY_LEVELS, SEVERITIES } from "../lib/severity";
import { LoadingPlaceholder } from "./LoadingPlaceholder";
import { encode } from "../api/glid";

interface SearchSidebarProps {
  dark: boolean;
  isTablet: boolean;
  sidebarWidth: number;
  sidebarCollapsed: boolean;
  setSidebarCollapsed: (v: boolean) => void;
  sidebarResizeProps: ResizeProps;
  resizing: boolean;
  rangeStart: Date | null;
  rangeEnd: Date | null;
  timeRange: string;
  onTimeRangeChange: (range: string) => void;
  onCustomRange: (start: Date, end: Date) => void;
  vaults: VaultInfo[] | undefined;
  vaultsLoading: boolean;
  statsLoading: boolean;
  totalRecords: bigint;
  selectedVault: string;
  onVaultSelect: (vaultId: string) => void;
  activeSeverities: string[];
  onToggleSeverity: (level: string) => void;
  attrFields: FieldSummary[];
  kvFields: FieldSummary[];
  onFieldSelect: (key: string, value: string) => void;
  activeQuery: string;
}

export function SearchSidebar({
  dark,
  isTablet,
  sidebarWidth,
  sidebarCollapsed,
  setSidebarCollapsed,
  sidebarResizeProps,
  resizing,
  rangeStart,
  rangeEnd,
  timeRange,
  onTimeRangeChange,
  onCustomRange,
  vaults,
  vaultsLoading,
  statsLoading,
  totalRecords,
  selectedVault,
  onVaultSelect,
  activeSeverities,
  onToggleSeverity,
  attrFields,
  kvFields,
  onFieldSelect,
  activeQuery,
}: Readonly<SearchSidebarProps>) {
  const c = useThemeClass(dark);

  const allSeverities = SEVERITY_LEVELS.map((l) => ({
    label: SEVERITIES[l].label,
    level: l,
    color: SEVERITIES[l].color,
  }));

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
        className={(() => {
          const transitionCls = resizing ? "" : "transition-[width,opacity] duration-200 will-change-[width,opacity]";
          const layoutCls = isTablet && !sidebarCollapsed
            ? `fixed left-0 top-0 h-full z-30 ${c("bg-ink", "bg-light-raised")}`
            : `shrink-0 ${transitionCls}`;
          const collapseCls = sidebarCollapsed
            ? ""
            : `p-4 border-r app-scroll overflow-y-auto ${c("border-ink-border-subtle bg-ink", "border-light-border-subtle bg-light-raised")}`;
          return `${layoutCls} overflow-hidden ${collapseCls}`;
        })()}
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

        {/* Vaults */}
        <SidebarSection title="Vaults" dark={dark}>
          <div className="flex flex-col gap-px">
            {vaultsLoading ? (
              <LoadingPlaceholder dark={dark} className="px-2.5 py-1.5" />
            ) : (
              vaults?.toSorted((a, b) => (a.name || encode(a.id)).localeCompare(b.name || encode(b.id))).map((vault) => (
                <VaultButton
                  key={encode(vault.id)}
                  label={vault.name || encode(vault.id)}
                  count={vault.recordCount.toLocaleString()}
                  active={selectedVault === encode(vault.id)}
                  onClick={() => onVaultSelect(encode(vault.id))}
                  dark={dark}
                  nodeId={encode(vault.nodeId)}
                  remote={vault.remote}
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
            {allSeverities.map(({ label, level }) => {
              const active = activeSeverities.includes(level);
              const sev = SEVERITIES[level];
              return (
                <button
                  key={level}
                  onClick={() => onToggleSeverity(level)}
                  className={`px-2 py-1.5 text-[0.8em] font-medium uppercase tracking-wider rounded-sm border transition-all duration-150 ${
                    active ? sev.toggleActive : sev.toggleInactive
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
        <div className="relative shrink-0 flex" style={isTablet ? { position: "fixed", left: sidebarWidth, top: 0, height: "100%", zIndex: 30 } : undefined}>
          <div
            {...sidebarResizeProps}
            className={`w-3 cursor-col-resize ${c("bg-ink hover:bg-copper-muted/30", "bg-light-bg hover:bg-copper-muted/20")}`}
          />
          <button
            onClick={() => setSidebarCollapsed(true)}
            className={`absolute top-2 -right-3 w-6 h-8 flex items-center justify-center text-[0.6em] rounded-r z-10 transition-colors ${c(
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
