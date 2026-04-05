import { describe, test, expect, mock } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { useHistogramHandlers } from "./useHistogramHandlers";

/** Build a minimal ProtoRecord-like object with a writeTs. */
function makeRecord(epochMs: number) {
  const secs = BigInt(Math.floor(epochMs / 1000));
  const nanos = (epochMs % 1000) * 1_000_000;
  return {
    writeTs: { seconds: secs, nanos },
    ref: { chunkId: "c1", pos: BigInt(0), vaultId: "v1" },
    attrs: {},
    raw: new Uint8Array(0),
    ingestSeq: 0,
    ingesterId: new Uint8Array(0),
  } as unknown as import("../api/client").Record;
}

function makeDeps(overrides: Partial<Parameters<typeof useHistogramHandlers>[0]> = {}) {
  return {
    q: "",
    isReversed: true,
    setUrlQuery: mock(() => {}),
    navigate: mock(() => {}),
    rangeStart: null,
    rangeEnd: null,
    setTimeRange: mock(() => {}),
    setRangeStart: mock(() => {}),
    setRangeEnd: mock(() => {}),
    selectedRecord: null,
    setSelectedRecord: mock(() => {}),
    ...overrides,
  } satisfies Parameters<typeof useHistogramHandlers>[0];
}

// ── handleBrushSelect ────────────────────────────────────────────────

describe("useHistogramHandlers", () => {
  describe("handleBrushSelect", () => {
    test("sets time range, start/end dates, and calls setUrlQuery", () => {
      const deps = makeDeps({ q: "foo", isReversed: true });
      const { result } = renderHook(() => useHistogramHandlers(deps));
      const start = new Date("2026-04-01T10:00:00Z");
      const end = new Date("2026-04-01T11:00:00Z");
      act(() => result.current.handleBrushSelect(start, end));

      expect(deps.setRangeStart).toHaveBeenCalledWith(start);
      expect(deps.setRangeEnd).toHaveBeenCalledWith(end);
      expect(deps.setTimeRange).toHaveBeenCalledWith("custom");

      const call = (deps.setUrlQuery as ReturnType<typeof mock>).mock.calls[0]![0] as string;
      expect(call).toContain("start=2026-04-01T10:00:00.000Z");
      expect(call).toContain("end=2026-04-01T11:00:00.000Z");
      expect(call).toContain("reverse=true");
      expect(call).toContain("foo");
    });

    test("strips existing time tokens before injecting new ones", () => {
      const deps = makeDeps({ q: "start=old end=old reverse=false bar" });
      const { result } = renderHook(() => useHistogramHandlers(deps));
      const start = new Date("2026-01-01T00:00:00Z");
      const end = new Date("2026-01-02T00:00:00Z");
      act(() => result.current.handleBrushSelect(start, end));

      const call = (deps.setUrlQuery as ReturnType<typeof mock>).mock.calls[0]![0] as string;
      expect(call).not.toContain("start=old");
      expect(call).not.toContain("end=old");
      expect(call).toContain("bar");
    });

    test("works with empty base query", () => {
      const deps = makeDeps({ q: "" });
      const { result } = renderHook(() => useHistogramHandlers(deps));
      const start = new Date("2026-01-01T00:00:00Z");
      const end = new Date("2026-01-02T00:00:00Z");
      act(() => result.current.handleBrushSelect(start, end));

      const call = (deps.setUrlQuery as ReturnType<typeof mock>).mock.calls[0]![0] as string;
      expect(call).toContain("start=");
      expect(call).toContain("end=");
      // No trailing space from empty base
      expect(call).not.toMatch(/\s$/);
    });
  });

  // ── handleFollowBrushSelect ────────────────────────────────────────

  describe("handleFollowBrushSelect", () => {
    test("navigates to /search with time range", () => {
      const deps = makeDeps({ q: "foo" });
      const { result } = renderHook(() => useHistogramHandlers(deps));
      const start = new Date("2026-04-01T10:00:00Z");
      const end = new Date("2026-04-01T11:00:00Z");
      act(() => result.current.handleFollowBrushSelect(start, end));

      expect(deps.setRangeStart).toHaveBeenCalledWith(start);
      expect(deps.setRangeEnd).toHaveBeenCalledWith(end);
      expect(deps.setTimeRange).toHaveBeenCalledWith("custom");
      expect(deps.navigate).toHaveBeenCalledTimes(1);

      const navCall = (deps.navigate as ReturnType<typeof mock>).mock.calls[0]![0] as {
        to: string;
        search: (prev: Record<string, string | undefined>) => Record<string, string | undefined>;
        replace: boolean;
      };
      expect(navCall.to).toBe("/search");
      expect(navCall.replace).toBe(false);

      // The search function should produce a query with start/end/reverse=true
      const searchResult = navCall.search({ q: "old" });
      expect(searchResult.q).toContain("start=2026-04-01T10:00:00.000Z");
      expect(searchResult.q).toContain("reverse=true");
    });

    test("always sets reverse=true regardless of isReversed dep", () => {
      const deps = makeDeps({ q: "", isReversed: false });
      const { result } = renderHook(() => useHistogramHandlers(deps));
      act(() => result.current.handleFollowBrushSelect(
        new Date("2026-01-01T00:00:00Z"),
        new Date("2026-01-02T00:00:00Z"),
      ));

      const navCall = (deps.navigate as ReturnType<typeof mock>).mock.calls[0]![0] as {
        search: (prev: Record<string, string | undefined>) => Record<string, string | undefined>;
      };
      const searchResult = navCall.search({});
      expect(searchResult.q).toContain("reverse=true");
    });
  });

  // ── handlePan ──────────────────────────────────────────────────────

  describe("handlePan", () => {
    test("delegates to handleBrushSelect", () => {
      const deps = makeDeps({ q: "" });
      const { result } = renderHook(() => useHistogramHandlers(deps));
      const start = new Date("2026-01-01T00:00:00Z");
      const end = new Date("2026-01-02T00:00:00Z");
      act(() => result.current.handlePan(start, end));

      expect(deps.setRangeStart).toHaveBeenCalledWith(start);
      expect(deps.setRangeEnd).toHaveBeenCalledWith(end);
      expect(deps.setUrlQuery).toHaveBeenCalledTimes(1);
    });
  });

  // ── handleZoomOut ──────────────────────────────────────────────────

  describe("handleZoomOut", () => {
    test("is no-op when no selectedRecord", () => {
      const deps = makeDeps({ selectedRecord: null });
      const { result } = renderHook(() => useHistogramHandlers(deps));
      act(() => result.current.handleZoomOut());
      expect(deps.navigate).not.toHaveBeenCalled();
      expect(deps.setTimeRange).not.toHaveBeenCalled();
    });

    test("is no-op when selectedRecord has no writeTs", () => {
      const rec = { ref: { chunkId: "c1", pos: BigInt(0), vaultId: "v1" } } as unknown as import("../api/client").Record;
      const deps = makeDeps({ selectedRecord: rec });
      const { result } = renderHook(() => useHistogramHandlers(deps));
      act(() => result.current.handleZoomOut());
      expect(deps.navigate).not.toHaveBeenCalled();
    });

    test("doubles the span around selectedRecord timestamp", () => {
      const anchorMs = 1_712_000_000_000; // ~2026-04-01
      const rec = makeRecord(anchorMs);
      const rangeStart = new Date(anchorMs - 30_000); // 30s before
      const rangeEnd = new Date(anchorMs + 30_000); // 30s after
      // span = 60_000ms, so new range should be anchor ±60_000ms
      const deps = makeDeps({
        selectedRecord: rec,
        rangeStart,
        rangeEnd,
      });
      const { result } = renderHook(() => useHistogramHandlers(deps));
      act(() => result.current.handleZoomOut());

      expect(deps.setTimeRange).toHaveBeenCalledWith("custom");
      const newStart = (deps.setRangeStart as ReturnType<typeof mock>).mock.calls[0]![0] as Date;
      const newEnd = (deps.setRangeEnd as ReturnType<typeof mock>).mock.calls[0]![0] as Date;
      expect(newStart.getTime()).toBe(anchorMs - 60_000);
      expect(newEnd.getTime()).toBe(anchorMs + 60_000);
    });

    test("uses default ±30s range when rangeStart/rangeEnd are null", () => {
      const anchorMs = 1_712_000_000_000;
      const rec = makeRecord(anchorMs);
      const deps = makeDeps({
        selectedRecord: rec,
        rangeStart: null,
        rangeEnd: null,
      });
      const { result } = renderHook(() => useHistogramHandlers(deps));
      act(() => result.current.handleZoomOut());

      // Default span = (anchorMs + 30_000) - (anchorMs - 30_000) = 60_000
      // New range = anchorMs ± 60_000
      const newStart = (deps.setRangeStart as ReturnType<typeof mock>).mock.calls[0]![0] as Date;
      const newEnd = (deps.setRangeEnd as ReturnType<typeof mock>).mock.calls[0]![0] as Date;
      expect(newStart.getTime()).toBe(anchorMs - 60_000);
      expect(newEnd.getTime()).toBe(anchorMs + 60_000);
    });

    test("navigates to /search with new time range", () => {
      const anchorMs = 1_712_000_000_000;
      const rec = makeRecord(anchorMs);
      const deps = makeDeps({
        selectedRecord: rec,
        rangeStart: new Date(anchorMs - 10_000),
        rangeEnd: new Date(anchorMs + 10_000),
      });
      const { result } = renderHook(() => useHistogramHandlers(deps));
      act(() => result.current.handleZoomOut());

      expect(deps.navigate).toHaveBeenCalledTimes(1);
      const navCall = (deps.navigate as ReturnType<typeof mock>).mock.calls[0]![0] as {
        to: string;
        search: (prev: Record<string, string | undefined>) => Record<string, string | undefined>;
      };
      expect(navCall.to).toBe("/search");

      const searchResult = navCall.search({});
      expect(searchResult.q).toContain("start=");
      expect(searchResult.q).toContain("end=");
    });
  });

  // ── handleContextRecordSelect ──────────────────────────────────────

  describe("handleContextRecordSelect", () => {
    test("navigates to /search with ±30s range around record timestamp", () => {
      const tsMs = 1_712_000_000_000;
      const rec = makeRecord(tsMs);
      const deps = makeDeps();
      const { result } = renderHook(() => useHistogramHandlers(deps));
      act(() => result.current.handleContextRecordSelect(rec));

      expect(deps.setTimeRange).toHaveBeenCalledWith("custom");
      const newStart = (deps.setRangeStart as ReturnType<typeof mock>).mock.calls[0]![0] as Date;
      const newEnd = (deps.setRangeEnd as ReturnType<typeof mock>).mock.calls[0]![0] as Date;
      expect(newStart.getTime()).toBe(tsMs - 30_000);
      expect(newEnd.getTime()).toBe(tsMs + 30_000);

      expect(deps.setSelectedRecord).toHaveBeenCalledWith(rec);
      expect(deps.navigate).toHaveBeenCalledTimes(1);

      const navCall = (deps.navigate as ReturnType<typeof mock>).mock.calls[0]![0] as {
        to: string;
        search: (prev: Record<string, string | undefined>) => Record<string, string | undefined>;
      };
      expect(navCall.to).toBe("/search");
      const searchResult = navCall.search({});
      expect(searchResult.q).toContain("reverse=true");
    });

    test("is no-op when record has no writeTs", () => {
      const rec = { ref: { chunkId: "c1", pos: BigInt(0), vaultId: "v1" } } as unknown as import("../api/client").Record;
      const deps = makeDeps();
      const { result } = renderHook(() => useHistogramHandlers(deps));
      act(() => result.current.handleContextRecordSelect(rec));

      expect(deps.navigate).not.toHaveBeenCalled();
      expect(deps.setSelectedRecord).not.toHaveBeenCalled();
    });
  });
});
