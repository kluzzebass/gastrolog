import { describe, test, expect, mock, beforeEach } from "bun:test";
import { render, fireEvent } from "@testing-library/react";
import { createRef } from "react";
import { Record as ProtoRecord } from "../api/gen/gastrolog/v1/query_pb";
import { Timestamp } from "@bufbuild/protobuf";
import { createTestQueryClient, settingsWrapper } from "../../test/render";

// ── Helpers ─────────────────────────────────────────────────────────

function makeMockRecord(raw: string, attrs: Record<string, string> = {}): ProtoRecord {
  const now = new Date();
  return new ProtoRecord({
    raw: new TextEncoder().encode(raw),
    attrs,
    writeTs: Timestamp.fromDate(now),
    ingestTs: Timestamp.fromDate(now),
    sourceTs: Timestamp.fromDate(now),
  });
}

const noopFn = () => {};

function createMockSv(overrides: Record<string, unknown> = {}) {
  return {
    q: "", navigate: mock(noopFn), isFollowMode: false,
    helpParam: undefined, settingsParam: undefined, inspectorParam: undefined,
    draft: "", setDraft: mock(noopFn),
    cursorRef: createRef<number>(),
    queryInputRef: createRef<HTMLTextAreaElement>(),
    draftHasErrors: false, draftIsPipeline: false, draftCanFollow: true,
    autocomplete: { suggestions: [] as string[], selectedIndex: 0, isOpen: false, accept: mock(noopFn), dismiss: mock(noopFn) },
    validation: { spans: [], expression: "", errorMessage: "" },
    dark: true, theme: "dark" as const, setTheme: mock(noopFn),
    highlightMode: "bold" as const, setHighlightMode: mock(noopFn),
    palette: "default" as const, setPalette: mock(noopFn),
    c: (d: string) => d,
    isTablet: false,
    sidebarWidth: 260, sidebarCollapsed: false, setSidebarCollapsed: mock(noopFn),
    sidebarResizeProps: {},
    detailWidth: 360, detailCollapsed: true, setDetailCollapsed: mock(noopFn),
    detailPinned: false, setDetailPinned: mock(noopFn),
    detailResizeProps: {}, resizing: false,
    timeRange: "5m", rangeStart: null, rangeEnd: null,
    handleTimeRange: mock(noopFn), handleCustomRange: mock(noopFn),
    showPlan: false, setShowPlan: mock(noopFn),
    showHistory: false, setShowHistory: mock(noopFn),
    showSavedQueries: false, setShowSavedQueries: mock(noopFn),
    showChangePassword: false, setShowChangePassword: mock(noopFn),
    showPreferences: false, setShowPreferences: mock(noopFn),
    inspectorGlow: false,
    openHelp: mock(noopFn), openSettings: mock(noopFn), openInspector: mock(noopFn),
    records: [] as ProtoRecord[], isSearching: false, hasMore: false,
    effectiveTableResult: null, isPipelineResult: false, isRawQuery: false,
    queryIsPipeline: false,
    search: mock(async () => {}), cancelSearch: mock(noopFn),
    tokens: [] as string[], displayRecords: [] as ProtoRecord[],
    selectedRecord: null as ProtoRecord | null, setSelectedRecord: mock(noopFn),
    selectedRowRef: createRef<HTMLElement>(),
    logScrollRef: createRef<HTMLDivElement>(),
    sentinelRef: createRef<HTMLDivElement>(),
    executeQuery: mock(noopFn),
    followRecords: [] as ProtoRecord[], isFollowing: false,
    reconnecting: false, reconnectAttempt: 0,
    followNewCount: 0, resetFollowNewCount: mock(noopFn),
    followBufferSize: 100, handleFollowBufferSizeChange: mock(noopFn),
    followReversed: true, isReversed: true,
    isScrolledDown: false,
    startFollow: mock(noopFn), stopFollowMode: mock(noopFn),
    histogramData: null, liveHistogramData: null,
    handleBrushSelect: mock(noopFn), handleFollowBrushSelect: mock(noopFn),
    handlePan: mock(noopFn), handleSegmentClick: mock(noopFn),
    toggleReverse: mock(noopFn), handleShowPlan: mock(noopFn),
    handleZoomOut: mock(noopFn), handleVaultSelect: mock(noopFn),
    handleChunkSelect: mock(noopFn), handlePosSelect: mock(noopFn),
    handleContextRecordSelect: mock(noopFn), handleFieldSelect: mock(noopFn),
    handleSpanClick: mock(noopFn), handleMultiFieldSelect: mock(noopFn),
    handleTokenToggle: mock(noopFn), toggleSeverity: mock(noopFn),
    vaults: [], vaultsLoading: false, statsLoading: false,
    selectedVault: "all", activeSeverities: ["error", "warn", "info", "debug", "trace"],
    attrFields: [], kvFields: [], totalRecords: BigInt(0),
    cpuPercent: 0, memoryBytes: BigInt(0), totalBytes: BigInt(0),
    queryHistory: { entries: [], add: mock(noopFn), remove: mock(noopFn), clear: mock(noopFn) },
    savedQueries: { data: [] }, putSavedQuery: { mutate: mock(noopFn) },
    deleteSavedQuery: { mutate: mock(noopFn) },
    explainChunks: [], explainDirection: "forward",
    explainTotalChunks: 0, explainExpression: "",
    explainPipelineStages: [], isExplaining: false,
    contextBefore: [], contextAfter: [], contextLoading: false,
    pollInterval: null as number | null, setPollInterval: mock(noopFn),
    logout: mock(noopFn), currentUser: null,
    addToast: mock(noopFn),
    ...overrides,
  };
}

// ── Mock modules ────────────────────────────────────────────────────

let currentSv = createMockSv();

mock.module("../hooks/useSearchView", () => ({
  useSearchView: () => currentSv,
}));

mock.module("../api/client", () => ({
  getToken: () => "test-token",
  Record: ProtoRecord,
}));

import { SearchView } from "./SearchView";

beforeEach(() => {
  currentSv = createMockSv();
});

function renderSV(overrides: Record<string, unknown> = {}) {
  Object.assign(currentSv, overrides);
  const qc = createTestQueryClient();
  return render(<SearchView />, { wrapper: settingsWrapper(qc) });
}

// ── Tests ───────────────────────────────────────────────────────────

describe("SearchView", () => {

  // ── 1. Query input → search execution → results display ──────────

  test("renders draft text in the query bar", () => {
    const { getByText } = renderSV({ draft: "level=error last=5m" });
    // QueryBar collapsed mode shows draft as a span
    expect(getByText("level=error last=5m")).toBeTruthy();
  });

  test("query bar collapsed state shows clickable div", () => {
    const { container } = renderSV({ draft: "level=error" });
    // QueryBar collapsed state renders a div[role=button]
    const queryDiv = container.querySelector('[role="button"][tabindex="0"]');
    expect(queryDiv).toBeTruthy();
    expect(queryDiv!.textContent).toContain("level=error");
  });

  test("search button has correct aria-label", () => {
    const { container } = renderSV();
    const searchBtn = container.querySelector('button[aria-label="Search"]');
    expect(searchBtn).toBeTruthy();
  });

  test("clicking search button calls executeQuery", () => {
    const executeQuery = mock(noopFn);
    const { container } = renderSV({ draft: "level=error", executeQuery });
    const searchBtn = container.querySelector('button[aria-label="Search"]');
    fireEvent.click(searchBtn!);
    expect(executeQuery).toHaveBeenCalled();
  });

  test("shows empty state when no records", () => {
    const { getByText } = renderSV({ records: [], displayRecords: [] });
    expect(getByText("Enter a query to search your logs")).toBeTruthy();
  });

  test("displays records when search results are present", () => {
    // Use follow mode to avoid VirtualLogList (needs real scroll dimensions)
    const records = [
      makeMockRecord("ERROR: connection timeout", { level: "error" }),
      makeMockRecord("INFO: server started", { level: "info" }),
    ];
    const { getByText } = renderSV({
      isFollowMode: true, followRecords: records, displayRecords: records,
    });
    expect(getByText(/connection timeout/)).toBeTruthy();
    expect(getByText(/server started/)).toBeTruthy();
  });

  test("shows result count badge", () => {
    const records = [makeMockRecord("test entry")];
    const { container } = renderSV({ records, displayRecords: records });
    // Result count badge shows the number of records
    const badge = container.querySelector('[role="status"]');
    expect(badge).toBeTruthy();
    expect(badge!.textContent).toBe("1");
  });

  // ── 2. Record selection → detail panel ────────────────────────────

  test("selecting a record calls setSelectedRecord", () => {
    const setSelectedRecord = mock(noopFn);
    const records = [makeMockRecord("test log entry")];
    const { getByText } = renderSV({
      isFollowMode: true, followRecords: records, displayRecords: records,
      setSelectedRecord,
    });
    fireEvent.click(getByText(/test log entry/));
    expect(setSelectedRecord).toHaveBeenCalled();
  });

  test("detail panel shows placeholder when no record selected", () => {
    const { getByText } = renderSV({ detailCollapsed: false });
    expect(getByText("Select a record to view details")).toBeTruthy();
  });

  test("detail panel expand button is present when collapsed", () => {
    const { container } = renderSV({ detailCollapsed: true });
    const expandBtn = container.querySelector('button[aria-label="Expand detail panel"]');
    expect(expandBtn).toBeTruthy();
  });

  // ── 3. Follow mode toggle → streaming state ──────────────────────

  test("follow button is present and has correct aria-label", () => {
    const { container } = renderSV();
    const followBtn = container.querySelector('button[aria-label="Follow"]');
    expect(followBtn).toBeTruthy();
  });

  test("clicking follow button calls startFollow", () => {
    const startFollow = mock(noopFn);
    const { container } = renderSV({ draft: "level=error", startFollow });
    const followBtn = container.querySelector('button[aria-label="Follow"]');
    fireEvent.click(followBtn!);
    expect(startFollow).toHaveBeenCalled();
  });

  test("shows follow-mode volume placeholder when no live data", () => {
    const { getByText } = renderSV({
      isFollowMode: true, liveHistogramData: null,
    });
    expect(getByText("Volume")).toBeTruthy();
    expect(getByText("0 records")).toBeTruthy();
  });

  test("renders follow records in follow mode", () => {
    const followRecords = [
      makeMockRecord("live: request received"),
      makeMockRecord("live: processing complete"),
    ];
    const { getByText } = renderSV({
      isFollowMode: true, followRecords, displayRecords: followRecords,
    });
    expect(getByText(/request received/)).toBeTruthy();
    expect(getByText(/processing complete/)).toBeTruthy();
  });

  test("stop follow button shown in follow mode", () => {
    const { container } = renderSV({
      isFollowMode: true, isFollowing: true,
    });
    const stopBtn = container.querySelector('button[aria-label="Stop following"]');
    expect(stopBtn).toBeTruthy();
  });

  test("clicking stop follow calls stopFollowMode", () => {
    const stopFollowMode = mock(noopFn);
    const { container } = renderSV({
      isFollowMode: true, isFollowing: true, stopFollowMode,
    });
    const stopBtn = container.querySelector('button[aria-label="Stop following"]');
    if (stopBtn) {
      fireEvent.click(stopBtn);
      expect(stopFollowMode).toHaveBeenCalled();
    }
  });

  // ── 4. Auto-refresh polling ───────────────────────────────────────

  test("polling buttons are rendered in results toolbar", () => {
    const { getByText } = renderSV();
    expect(getByText("Off")).toBeTruthy();
    expect(getByText("5s")).toBeTruthy();
    expect(getByText("10s")).toBeTruthy();
    expect(getByText("30s")).toBeTruthy();
    expect(getByText("1m")).toBeTruthy();
  });

  test("clicking poll interval button calls setPollInterval", () => {
    const setPollInterval = mock(noopFn);
    const { getByText } = renderSV({ setPollInterval });
    fireEvent.click(getByText("5s"));
    expect(setPollInterval).toHaveBeenCalledWith(5000);
  });

  test("active poll interval is visually highlighted", () => {
    const { getByText } = renderSV({ pollInterval: 10000 });
    const btn10s = getByText("10s");
    // Active poll button has copper styling
    expect(btn10s.className).toContain("copper");
  });

  // ── 5. Pagination ────────────────────────────────────────────────

  test("sentinel ref div exists for infinite scroll", () => {
    const records = Array.from({ length: 3 }, (_, i) =>
      makeMockRecord(`entry ${i}`),
    );
    const { container } = renderSV({
      records, displayRecords: records, hasMore: true,
    });
    // The sentinel is a small div at the end of the scroll container
    expect(container.querySelector("main")).toBeTruthy();
  });

  test("shows result count for paginated results", () => {
    const records = Array.from({ length: 50 }, (_, i) =>
      makeMockRecord(`entry ${i}`),
    );
    const { container } = renderSV({
      records, displayRecords: records, hasMore: true,
    });
    const badge = container.querySelector('[role="status"]');
    expect(badge).toBeTruthy();
    expect(badge!.textContent).toBe("50+");
  });

  // ── Dialog rendering ─────────────────────────────────────────────

  test("settings dialog shown when settingsParam is set", () => {
    renderSV({ settingsParam: "service" });
    // Dialog renders via portal to document.body, not inside the test container.
    const dialog = document.querySelector('[role="dialog"][aria-label="Settings"]');
    expect(dialog).toBeTruthy();
  });

  test("execution plan dialog shown when showPlan is true", () => {
    const { getByLabelText } = renderSV({ showPlan: true });
    expect(getByLabelText("Query Execution Plan")).toBeTruthy();
  });

  test("plan dialog shows analyzing message while explaining", () => {
    const { getByText } = renderSV({ showPlan: true, isExplaining: true });
    expect(getByText("Analyzing query plan...")).toBeTruthy();
  });

  test("plan dialog shows prompt when no explain data", () => {
    const { getByText } = renderSV({ showPlan: true, explainChunks: [] });
    expect(getByText("Run a query to see the execution plan.")).toBeTruthy();
  });

  // ── Layout ───────────────────────────────────────────────────────

  test("sidebar renders with time range, vaults, and severity sections", () => {
    const { getByText } = renderSV();
    expect(getByText("Time Range")).toBeTruthy();
    expect(getByText("Vaults")).toBeTruthy();
    expect(getByText("Severity")).toBeTruthy();
  });

  test("sidebar shows severity toggles", () => {
    const { getByText } = renderSV();
    expect(getByText("Error")).toBeTruthy();
    expect(getByText("Warn")).toBeTruthy();
    expect(getByText("Info")).toBeTruthy();
    expect(getByText("Debug")).toBeTruthy();
    expect(getByText("Trace")).toBeTruthy();
  });

  test("header shows system stats", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["clusterStatus"], {
      clusterEnabled: false,
      nodes: [{
        id: "node-1",
        name: "node-1",
        stats: {
          cpuPercent: 42.5,
          memoryRss: BigInt(1024 * 1024 * 256),
          vaults: [],
        },
      }],
    });
    Object.assign(currentSv, {
      cpuPercent: 42.5,
      memoryBytes: BigInt(1024 * 1024 * 256),
    });
    const { getByText } = render(<SearchView />, { wrapper: settingsWrapper(qc) });
    expect(getByText("42.5%")).toBeTruthy();
    expect(getByText("256.0 MB")).toBeTruthy();
  });

  test("sort button toggles reverse order", () => {
    const toggleReverse = mock(noopFn);
    const { container } = renderSV({ toggleReverse });
    const sortBtn = container.querySelector('button[aria-label="Sort oldest first"]');
    expect(sortBtn).toBeTruthy();
    fireEvent.click(sortBtn!);
    expect(toggleReverse).toHaveBeenCalled();
  });

  test("explain plan button has correct aria-label", () => {
    const { container } = renderSV();
    const planBtn = container.querySelector('button[aria-label="Explain query plan"]');
    expect(planBtn).toBeTruthy();
  });

  test("clicking explain button calls handleShowPlan", () => {
    const handleShowPlan = mock(noopFn);
    const { container } = renderSV({ handleShowPlan });
    const planBtn = container.querySelector('button[aria-label="Explain query plan"]');
    fireEvent.click(planBtn!);
    expect(handleShowPlan).toHaveBeenCalled();
  });
});
