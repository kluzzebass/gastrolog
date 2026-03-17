import { getToken } from "../api/client";
import { useSearchView } from "../hooks/useSearchView";
import { Dialog } from "./Dialog";
import { ExplainPanel } from "./ExplainPanel";
import { HeaderBar } from "./HeaderBar";
import { HistogramChart } from "./HistogramChart";
import { SearchSidebar } from "./SearchSidebar";
import { DetailSidebar } from "./DetailSidebar";
import { SearchResults } from "./SearchResults";
import { parseOrderBy } from "./LogEntry";
import { SettingsDialog } from "./settings/SettingsDialog";
import { InspectorDialog } from "./inspector/InspectorDialog";
import { ChangePasswordDialog } from "./ChangePasswordDialog";
import { PreferencesDialog } from "./PreferencesDialog";
import { HelpDialog } from "./HelpDialog";
import { ExportToVaultDialog } from "./ExportToVaultDialog";
import { HelpProvider } from "../hooks/useHelp";
import { DetailPanelProvider } from "../hooks/useDetailPanel";
import { QueryBar } from "./QueryBar";

export function SearchView() {
  const sv = useSearchView();

  return (
    <HelpProvider onOpen={sv.openHelp}>
    <div className="flex flex-col flex-1 overflow-hidden">
      <a href="#main-content" className="skip-link">Skip to main content</a>

      <HeaderBar
        dark={sv.dark}
        onShowHelp={() => sv.openHelp()}
        onShowInspector={() => sv.openInspector()}
        onShowSettings={() => sv.openSettings()}
        currentUser={sv.currentUser ? { username: sv.currentUser.username, role: sv.currentUser.role } : null}
        onPreferences={() => sv.setShowPreferences(true)}
        onChangePassword={() => sv.setShowChangePassword(true)}
        onLogout={sv.logout}
      />

      {/* ── Main Layout ── */}
      <div className="flex flex-1 overflow-hidden">
        {sv.isTablet && !sv.sidebarCollapsed && (
          <div className="fixed inset-0 bg-black/30 z-20" role="presentation" onClick={() => sv.setSidebarCollapsed(true)} />
        )}
        <SearchSidebar
          dark={sv.dark}
          isTablet={sv.isTablet}
          sidebarWidth={sv.sidebarWidth}
          sidebarCollapsed={sv.sidebarCollapsed}
          setSidebarCollapsed={sv.setSidebarCollapsed}
          sidebarResizeProps={sv.sidebarResizeProps}
          resizing={sv.resizing}
          rangeStart={sv.rangeStart}
          rangeEnd={sv.rangeEnd}
          timeRange={sv.timeRange}
          onTimeRangeChange={sv.handleTimeRange}
          onCustomRange={sv.handleCustomRange}
          vaults={sv.vaults}
          vaultsLoading={sv.vaultsLoading}
          statsLoading={sv.statsLoading}
          totalRecords={sv.totalRecords}
          selectedVault={sv.selectedVault}
          onVaultSelect={sv.handleVaultSelect}
          activeSeverities={sv.activeSeverities}
          onToggleSeverity={sv.toggleSeverity}
          attrFields={sv.attrFields}
          kvFields={sv.kvFields}
          onFieldSelect={sv.handleFieldSelect}
          activeQuery={sv.q}
        />

        {/* ── Main Content ── */}
        <main
          id="main-content"
          className={`flex-1 flex flex-col overflow-hidden ${sv.c("bg-ink-raised", "bg-light-bg")}`}
        >
          <QueryBar
            dark={sv.dark}
            draft={sv.draft}
            setDraft={sv.setDraft}
            cursorRef={sv.cursorRef}
            queryInputRef={sv.queryInputRef}
            autocomplete={sv.autocomplete}
            showHistory={sv.showHistory}
            setShowHistory={sv.setShowHistory}
            showSavedQueries={sv.showSavedQueries}
            setShowSavedQueries={sv.setShowSavedQueries}
            historyEntries={sv.queryHistory.entries}
            onHistoryRemove={sv.queryHistory.remove}
            onHistoryClear={sv.queryHistory.clear}
            savedQueries={sv.savedQueries.data ?? []}
            onSaveQuery={(name, query) => sv.putSavedQuery.mutate({ name, query })}
            onDeleteSavedQuery={(name) => sv.deleteSavedQuery.mutate(name)}
            executeQuery={sv.executeQuery}
            cancelSearch={sv.cancelSearch}
            isSearching={sv.isSearching}
            isFollowMode={sv.isFollowMode}
            startFollow={sv.startFollow}
            stopFollowMode={sv.stopFollowMode}
            draftHasErrors={sv.draftHasErrors}
            draftCanFollow={sv.draftCanFollow}
            showPlan={sv.showPlan}
            handleShowPlan={sv.handleShowPlan}
            highlightSpans={sv.validation.spans}
            highlightExpression={sv.validation.expression}
            errorMessage={sv.validation.errorMessage}
          />

          {/* Execution Plan Dialog */}
          {sv.showPlan && (
            <Dialog
              onClose={() => sv.setShowPlan(false)}
              ariaLabel="Query Execution Plan"
              dark={sv.dark}
              size="lg"
            >
              {sv.isExplaining && (
                <div className={`text-[0.9em] ${sv.c("text-text-ghost", "text-light-text-ghost")}`}>
                  Analyzing query plan...
                </div>
              )}
              {!sv.isExplaining && sv.explainChunks.length === 0 && (
                <div className={`text-[0.9em] ${sv.c("text-text-ghost", "text-light-text-ghost")}`}>
                  Run a query to see the execution plan.
                </div>
              )}
              {!sv.isExplaining && sv.explainChunks.length > 0 && (
                <ExplainPanel
                  chunks={sv.explainChunks}
                  direction={sv.explainDirection}
                  totalChunks={sv.explainTotalChunks}
                  expression={sv.explainExpression}
                  pipelineStages={sv.explainPipelineStages}
                  dark={sv.dark}
                />
              )}
            </Dialog>
          )}

          {/* Preferences Dialog */}
          {sv.showPreferences && (
            <PreferencesDialog
              dark={sv.dark}
              theme={sv.theme}
              setTheme={sv.setTheme}
              highlightMode={sv.highlightMode}
              setHighlightMode={sv.setHighlightMode}
              palette={sv.palette}
              setPalette={sv.setPalette}
              onClose={() => sv.setShowPreferences(false)}
            />
          )}

          {sv.showChangePassword && sv.currentUser && (
            <ChangePasswordDialog
              username={sv.currentUser.username}
              dark={sv.dark}
              onClose={() => sv.setShowChangePassword(false)}
              onSuccess={() => {
                sv.setShowChangePassword(false);
                sv.addToast("Password changed successfully", "info");
              }}
            />
          )}

          {sv.settingsParam && (
            <SettingsDialog
              dark={sv.dark}
              tab={sv.settingsParam}
              onTabChange={(tab) => sv.navigate({ search: (prev) => ({ ...prev, settings: tab }) })}
              onClose={() => sv.navigate({ search: (prev) => ({ ...prev, settings: undefined }) })}
              onOpenInspector={(param) => sv.navigate({ search: (prev) => ({ ...prev, settings: undefined, inspector: param }) })}
              isAdmin={sv.currentUser?.role === "admin" || getToken() === "no-auth"}
              noAuth={getToken() === "no-auth"}
            />
          )}

          {sv.inspectorParam && (
            <InspectorDialog
              dark={sv.dark}
              inspectorParam={sv.inspectorParam}
              onNavigate={(p) => { sessionStorage.setItem("inspector-last", p); sv.navigate({ search: (prev) => ({ ...prev, inspector: p }) }); }}
              onClose={() => sv.navigate({ search: (prev) => ({ ...prev, inspector: undefined }) })}
              onOpenSettings={(tab, entityName) => sv.navigate({ search: (prev) => ({ ...prev, inspector: undefined, settings: entityName ? `${tab}:${entityName}` : tab }) })}
            />
          )}

          {sv.helpParam && (
            <HelpDialog
              dark={sv.dark}
              topicId={sv.helpParam}
              onClose={() => sv.navigate({ search: (prev) => ({ ...prev, help: undefined }) })}
              onNavigate={(id) => sv.navigate({ search: (prev) => ({ ...prev, help: id }) })}
              onOpenSettings={(tab) => sv.navigate({ search: (prev) => ({ ...prev, help: undefined, settings: tab }) })}
            />
          )}

          {sv.showExportToVault && (
            <ExportToVaultDialog
              dark={sv.dark}
              expression={sv.q}
              onClose={() => sv.setShowExportToVault(false)}
            />
          )}

          {/* Histogram — server-side for search, client-side for follow */}
          {!sv.isFollowMode &&
            !sv.isPipelineResult &&
            sv.histogramData &&
            sv.histogramData.buckets.length > 0 && (
              <div className={`px-5 py-3 border-b ${sv.c("border-ink-border-subtle", "border-light-border-subtle")}`}>
                <HistogramChart
                  data={sv.histogramData}
                  dark={sv.dark}
                  truncated={false}
                  elapsedMs={sv.searchElapsedMs}
                  onBrushSelect={sv.handleBrushSelect}
                  onPan={sv.handlePan}
                  onSegmentClick={sv.handleSegmentClick}
                />
              </div>
            )}
          {sv.isFollowMode && (
            <div className={`px-5 py-3 border-b ${sv.c("border-ink-border-subtle", "border-light-border-subtle")}`}>
              {sv.liveHistogramData && sv.liveHistogramData.buckets.length > 0 ? (
                <HistogramChart
                  data={sv.liveHistogramData}
                  dark={sv.dark}
                  onBrushSelect={sv.handleFollowBrushSelect}
                  onSegmentClick={sv.handleSegmentClick}
                />
              ) : (
                <div className="relative">
                  <div className="flex items-baseline justify-between mb-1.5">
                    <span className={`text-[0.7em] font-medium uppercase tracking-[0.15em] ${sv.c("text-text-ghost", "text-light-text-ghost")}`}>
                      Volume
                    </span>
                    <span className={`font-mono text-[0.75em] ${sv.c("text-text-muted", "text-light-text-muted")}`}>
                      0 records
                    </span>
                  </div>
                  <div className={`rounded h-12 ${sv.c("bg-ink-surface/30", "bg-light-hover/30")}`} />
                  <div className="flex justify-between mt-1 min-h-5">
                    <span className={`text-[0.65em] font-mono ${sv.c("text-text-ghost", "text-light-text-ghost")}`}>
                      &mdash;
                    </span>
                    <span className={`text-[0.65em] font-mono ${sv.c("text-text-ghost", "text-light-text-ghost")}`}>
                      &mdash;
                    </span>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Results */}
          <SearchResults
            dark={sv.dark}
            c={sv.c}
            isFollowMode={sv.isFollowMode}
            isReversed={sv.isReversed}
            followReversed={sv.followReversed}
            records={sv.records}
            followRecords={sv.followRecords}
            displayRecords={sv.displayRecords}
            selectedRecord={sv.selectedRecord}
            effectiveTableResult={sv.effectiveTableResult}
            isSearching={sv.isSearching}
            hasMore={sv.hasMore}
            isPipelineResult={sv.isPipelineResult}
            isRawQuery={sv.isRawQuery}
            queryIsPipeline={sv.queryIsPipeline}
            isScrolledDown={sv.isScrolledDown}
            followNewCount={sv.followNewCount}
            reconnecting={sv.reconnecting}
            reconnectAttempt={sv.reconnectAttempt}
            followBufferSize={sv.followBufferSize}
            onSelectRecord={sv.setSelectedRecord}
            toggleReverse={sv.toggleReverse}
            onTokenToggle={sv.handleTokenToggle}
            onSpanClick={sv.handleSpanClick}
            onZoomOut={sv.handleZoomOut}
            onFollowBufferSizeChange={sv.handleFollowBufferSizeChange}
            resetFollowNewCount={sv.resetFollowNewCount}
            onPollIntervalChange={sv.setPollInterval}
            tokens={sv.tokens}
            highlightMode={sv.highlightMode}
            pollInterval={sv.pollInterval}
            orderBy={parseOrderBy(sv.q)}
            logScrollRef={sv.logScrollRef}
            sentinelRef={sv.sentinelRef}
            selectedRowRef={sv.selectedRowRef}
            onExportToVault={() => sv.setShowExportToVault(true)}
            queryExpression={sv.q}
          />
        </main>

        <DetailPanelProvider value={{
          onFieldSelect: sv.handleFieldSelect,
          onMultiFieldSelect: sv.handleMultiFieldSelect,
          onSpanClick: sv.handleSpanClick,
          onChunkSelect: sv.handleChunkSelect,
          onVaultSelect: sv.handleVaultSelect,
          onPosSelect: sv.handlePosSelect,
          contextBefore: sv.contextBefore,
          contextAfter: sv.contextAfter,
          contextLoading: sv.contextLoading,
          contextReversed: sv.isReversed,
          onContextRecordSelect: sv.handleContextRecordSelect,
          highlightMode: sv.highlightMode,
        }}>
          {sv.isTablet && !sv.detailCollapsed && (
            <div className="fixed inset-0 bg-black/30 z-20" role="presentation" onClick={() => sv.setDetailCollapsed(true)} />
          )}
          <DetailSidebar
            dark={sv.dark}
            isTablet={sv.isTablet}
            detailWidth={sv.detailWidth}
            detailCollapsed={sv.detailCollapsed}
            setDetailCollapsed={sv.setDetailCollapsed}
            detailPinned={sv.detailPinned}
            setDetailPinned={sv.setDetailPinned}
            detailResizeProps={sv.detailResizeProps}
            resizing={sv.resizing}
            selectedRecord={sv.selectedRecord}
          />
        </DetailPanelProvider>
      </div>
    </div>
    </HelpProvider>
  );
}
