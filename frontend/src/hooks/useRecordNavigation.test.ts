import { describe, test, expect, mock } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { useRecordNavigation } from "./useRecordNavigation";

/** Build a minimal ProtoRecord-like object. */
function makeRecord(chunkId: string, pos: number, vaultId = "v1") {
  return {
    ref: { chunkId, pos: BigInt(pos), vaultId },
    writeTs: { seconds: BigInt(1712000000), nanos: 0 },
    attrs: {},
    raw: new Uint8Array(0),
    ingestSeq: 0,
    ingesterId: new Uint8Array(0),
  } as unknown as import("../api/client").Record;
}

function makeDeps(overrides: Partial<Parameters<typeof useRecordNavigation>[0]> = {}) {
  return {
    isFollowMode: false,
    recordsRef: { current: [] as import("../api/client").Record[] },
    followRecordsRef: { current: [] as import("../api/client").Record[] },
    selectedRowRef: { current: null },
    detailCollapsed: true,
    setDetailCollapsed: mock(() => {}),
    detailPinned: false,
    showPlan: false,
    setShowPlan: mock(() => {}),
    fetchContext: mock(() => Promise.resolve()),
    resetContext: mock(() => {}),
    ...overrides,
  } satisfies Parameters<typeof useRecordNavigation>[0];
}

describe("useRecordNavigation", () => {
  // ── Initial state ──────────────────────────────────────────────────

  describe("initial state", () => {
    test("selectedRecord starts as null", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useRecordNavigation(deps));
      expect(result.current.selectedRecord).toBeNull();
    });

    test("selectedRecordRef starts as null", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useRecordNavigation(deps));
      expect(result.current.selectedRecordRef.current).toBeNull();
    });
  });

  // ── setSelectedRecord ──────────────────────────────────────────────

  describe("setSelectedRecord", () => {
    test("updates selectedRecord state", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useRecordNavigation(deps));
      const rec = makeRecord("c1", 0);
      act(() => result.current.setSelectedRecord(rec));
      expect(result.current.selectedRecord).toBe(rec);
    });

    test("updates selectedRecordRef", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useRecordNavigation(deps));
      const rec = makeRecord("c1", 0);
      act(() => result.current.setSelectedRecord(rec));
      expect(result.current.selectedRecordRef.current).toBe(rec);
    });

    test("setting to null clears the selection", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useRecordNavigation(deps));
      const rec = makeRecord("c1", 0);
      act(() => result.current.setSelectedRecord(rec));
      act(() => result.current.setSelectedRecord(null));
      expect(result.current.selectedRecord).toBeNull();
    });

    test("selecting a record expands the detail panel", () => {
      const deps = makeDeps({ detailCollapsed: true });
      const { result } = renderHook(() => useRecordNavigation(deps));
      const rec = makeRecord("c1", 0);
      act(() => result.current.setSelectedRecord(rec));
      expect(deps.setDetailCollapsed).toHaveBeenCalledWith(false);
    });

    test("deselecting collapses the detail panel when not pinned", () => {
      const deps = makeDeps({ detailCollapsed: false, detailPinned: false });
      const { result } = renderHook(() => useRecordNavigation(deps));
      act(() => result.current.setSelectedRecord(null));
      expect(deps.setDetailCollapsed).toHaveBeenCalledWith(true);
    });

    test("deselecting does NOT collapse the detail panel when pinned", () => {
      const deps = makeDeps({ detailCollapsed: false, detailPinned: true });
      const { result } = renderHook(() => useRecordNavigation(deps));
      act(() => result.current.setSelectedRecord(null));
      // setDetailCollapsed should not be called with true because detailPinned is true
      const calls = (deps.setDetailCollapsed as ReturnType<typeof mock>).mock.calls;
      const calledWithTrue = calls.some((c) => c[0] === true);
      expect(calledWithTrue).toBe(false);
    });

    test("selecting a record with a ref fetches context", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useRecordNavigation(deps));
      const rec = makeRecord("c1", 42);
      act(() => result.current.setSelectedRecord(rec));
      expect(deps.fetchContext).toHaveBeenCalledWith(rec.ref);
    });

    test("deselecting resets context", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useRecordNavigation(deps));
      act(() => result.current.setSelectedRecord(null));
      expect(deps.resetContext).toHaveBeenCalled();
    });
  });

  // ── Keyboard navigation ────────────────────────────────────────────

  describe("keyboard navigation", () => {
    test("ArrowDown selects first record when nothing selected", () => {
      const rec1 = makeRecord("c1", 0);
      const rec2 = makeRecord("c1", 1);
      const deps = makeDeps({
        recordsRef: { current: [rec1, rec2] },
      });
      const { result } = renderHook(() => useRecordNavigation(deps));

      act(() => {
        globalThis.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown" }));
      });
      expect(result.current.selectedRecord).toBe(rec1);
    });

    test("ArrowUp selects last record when nothing selected", () => {
      const rec1 = makeRecord("c1", 0);
      const rec2 = makeRecord("c1", 1);
      const deps = makeDeps({
        recordsRef: { current: [rec1, rec2] },
      });
      const { result } = renderHook(() => useRecordNavigation(deps));

      act(() => {
        globalThis.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowUp" }));
      });
      expect(result.current.selectedRecord).toBe(rec2);
    });

    test("ArrowDown advances to next record", () => {
      const rec1 = makeRecord("c1", 0);
      const rec2 = makeRecord("c1", 1);
      const deps = makeDeps({
        recordsRef: { current: [rec1, rec2] },
      });
      const { result } = renderHook(() => useRecordNavigation(deps));

      // Select first
      act(() => result.current.setSelectedRecord(rec1));
      // Navigate down
      act(() => {
        globalThis.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown" }));
      });
      expect(result.current.selectedRecord).toBe(rec2);
    });

    test("ArrowUp moves to previous record", () => {
      const rec1 = makeRecord("c1", 0);
      const rec2 = makeRecord("c1", 1);
      const deps = makeDeps({
        recordsRef: { current: [rec1, rec2] },
      });
      const { result } = renderHook(() => useRecordNavigation(deps));

      act(() => result.current.setSelectedRecord(rec2));
      act(() => {
        globalThis.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowUp" }));
      });
      expect(result.current.selectedRecord).toBe(rec1);
    });

    test("ArrowDown at last record stays on last record", () => {
      const rec1 = makeRecord("c1", 0);
      const rec2 = makeRecord("c1", 1);
      const deps = makeDeps({
        recordsRef: { current: [rec1, rec2] },
      });
      const { result } = renderHook(() => useRecordNavigation(deps));

      act(() => result.current.setSelectedRecord(rec2));
      act(() => {
        globalThis.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown" }));
      });
      expect(result.current.selectedRecord).toBe(rec2);
    });

    test("ArrowUp at first record stays on first record", () => {
      const rec1 = makeRecord("c1", 0);
      const rec2 = makeRecord("c1", 1);
      const deps = makeDeps({
        recordsRef: { current: [rec1, rec2] },
      });
      const { result } = renderHook(() => useRecordNavigation(deps));

      act(() => result.current.setSelectedRecord(rec1));
      act(() => {
        globalThis.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowUp" }));
      });
      expect(result.current.selectedRecord).toBe(rec1);
    });

    test("uses followRecordsRef in follow mode", () => {
      const searchRec = makeRecord("s1", 0);
      const followRec = makeRecord("f1", 0);
      const deps = makeDeps({
        isFollowMode: true,
        recordsRef: { current: [searchRec] },
        followRecordsRef: { current: [followRec] },
      });
      const { result } = renderHook(() => useRecordNavigation(deps));

      act(() => {
        globalThis.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown" }));
      });
      expect(result.current.selectedRecord).toBe(followRec);
    });

    test("no-op when records list is empty", () => {
      const deps = makeDeps({ recordsRef: { current: [] } });
      const { result } = renderHook(() => useRecordNavigation(deps));
      act(() => {
        globalThis.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown" }));
      });
      expect(result.current.selectedRecord).toBeNull();
    });

    test("ignores keyboard when target is an INPUT element", () => {
      const rec = makeRecord("c1", 0);
      const deps = makeDeps({ recordsRef: { current: [rec] } });
      const { result } = renderHook(() => useRecordNavigation(deps));

      const input = document.createElement("input");
      document.body.appendChild(input);
      act(() => {
        input.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }));
      });
      expect(result.current.selectedRecord).toBeNull();
      input.remove();
    });
  });

  // ── Escape key ─────────────────────────────────────────────────────

  describe("Escape key", () => {
    test("deselects current record", () => {
      const rec = makeRecord("c1", 0);
      const deps = makeDeps({ recordsRef: { current: [rec] } });
      const { result } = renderHook(() => useRecordNavigation(deps));

      act(() => result.current.setSelectedRecord(rec));
      expect(result.current.selectedRecord).toBe(rec);

      act(() => {
        globalThis.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
      });
      expect(result.current.selectedRecord).toBeNull();
    });

    test("closes plan dialog first if showPlan is true", () => {
      const rec = makeRecord("c1", 0);
      const deps = makeDeps({
        recordsRef: { current: [rec] },
        showPlan: true,
      });
      const { result } = renderHook(() => useRecordNavigation(deps));

      act(() => result.current.setSelectedRecord(rec));
      act(() => {
        globalThis.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
      });
      // Should close plan, NOT deselect record
      expect(deps.setShowPlan).toHaveBeenCalledWith(false);
      // Record should still be selected
      expect(result.current.selectedRecord).toBe(rec);
    });

    test("collapses detail panel when not pinned", () => {
      const deps = makeDeps({ detailPinned: false });
      const { result } = renderHook(() => useRecordNavigation(deps));
      const rec = makeRecord("c1", 0);

      act(() => result.current.setSelectedRecord(rec));
      // Clear previous calls from selection effect
      (deps.setDetailCollapsed as ReturnType<typeof mock>).mockClear();

      act(() => {
        globalThis.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
      });
      expect(deps.setDetailCollapsed).toHaveBeenCalledWith(true);
    });
  });
});
