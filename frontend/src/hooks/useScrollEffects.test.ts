import { describe, test, expect, mock } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { useScrollEffects } from "./useScrollEffects";

function makeDeps(overrides: Partial<Parameters<typeof useScrollEffects>[0]> = {}) {
  return {
    isFollowMode: false,
    isSearching: false,
    hasMore: false,
    selectedRecord: null,
    recordsLength: 0,
    loadMoreRef: { current: mock(() => {}) },
    resetFollowNewCountRef: { current: mock(() => {}) },
    expressionRef: { current: "" },
    selectedRowRef: { current: null },
    ...overrides,
  } satisfies Parameters<typeof useScrollEffects>[0];
}

describe("useScrollEffects", () => {
  // ── Return values ──────────────────────────────────────────────────

  describe("return values", () => {
    test("returns sentinelRef as a ref object", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useScrollEffects(deps));
      expect(result.current.sentinelRef).toBeDefined();
      expect(result.current.sentinelRef.current).toBeNull();
    });

    test("returns logScrollRef as a ref object", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useScrollEffects(deps));
      expect(result.current.logScrollRef).toBeDefined();
      expect(result.current.logScrollRef.current).toBeNull();
    });

    test("isScrolledDown starts as false", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useScrollEffects(deps));
      expect(result.current.isScrolledDown).toBe(false);
    });

    test("scrollToSelectedRef starts as false", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useScrollEffects(deps));
      expect(result.current.scrollToSelectedRef.current).toBe(false);
    });

    test("resetScroll is a function", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useScrollEffects(deps));
      expect(typeof result.current.resetScroll).toBe("function");
    });
  });

  // ── resetScroll ────────────────────────────────────────────────────

  describe("resetScroll", () => {
    test("can be called without error", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useScrollEffects(deps));
      expect(() => {
        act(() => result.current.resetScroll());
      }).not.toThrow();
    });

    test("resets scrollToSelectedRef to false", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useScrollEffects(deps));
      // Manually set it to true to simulate state
      result.current.scrollToSelectedRef.current = true;
      act(() => result.current.resetScroll());
      expect(result.current.scrollToSelectedRef.current).toBe(false);
    });

    test("calls scrollTo on logScrollRef element if present", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useScrollEffects(deps));
      const scrollTo = mock(() => {});
      // Attach a mock element to logScrollRef
      (result.current.logScrollRef as { current: unknown }).current = { scrollTo };
      act(() => result.current.resetScroll());
      expect(scrollTo).toHaveBeenCalledWith(0, 0);
    });
  });

  // ── isScrolledDown in follow mode ──────────────────────────────────

  describe("isScrolledDown tracking", () => {
    test("stays false when not in follow mode", () => {
      const deps = makeDeps({ isFollowMode: false });
      const { result } = renderHook(() => useScrollEffects(deps));
      expect(result.current.isScrolledDown).toBe(false);
    });

    test("stays false in follow mode with no scroll element", () => {
      const deps = makeDeps({ isFollowMode: true });
      const { result } = renderHook(() => useScrollEffects(deps));
      expect(result.current.isScrolledDown).toBe(false);
    });
  });

  // ── Stability across re-renders ────────────────────────────────────

  describe("stability", () => {
    test("refs are stable across re-renders", () => {
      const deps = makeDeps();
      const { result, rerender } = renderHook(() => useScrollEffects(deps));
      const sentinel1 = result.current.sentinelRef;
      const logScroll1 = result.current.logScrollRef;
      rerender();
      expect(result.current.sentinelRef).toBe(sentinel1);
      expect(result.current.logScrollRef).toBe(logScroll1);
    });
  });

  // ── Edge: rapid dep changes ────────────────────────────────────────

  describe("edge cases", () => {
    test("handles transition from searching to not searching", () => {
      const deps = makeDeps({ isSearching: true });
      const { result, rerender } = renderHook(
        (props: { isSearching: boolean }) => useScrollEffects({ ...deps, isSearching: props.isSearching }),
        { initialProps: { isSearching: true } },
      );
      // Transition to not-searching
      rerender({ isSearching: false });
      // Should not crash; selectedRowRef is null so no scrollIntoView
      expect(result.current.isScrolledDown).toBe(false);
    });

    test("handles changing recordsLength", () => {
      const deps = makeDeps({ recordsLength: 0 });
      const { result, rerender } = renderHook(
        (props: { recordsLength: number }) => useScrollEffects({ ...deps, recordsLength: props.recordsLength }),
        { initialProps: { recordsLength: 0 } },
      );
      rerender({ recordsLength: 100 });
      // Should not crash
      expect(result.current.sentinelRef).toBeDefined();
    });

    test("handles hasMore toggling", () => {
      const deps = makeDeps({ hasMore: true });
      const { result, rerender } = renderHook(
        (props: { hasMore: boolean }) => useScrollEffects({ ...deps, hasMore: props.hasMore }),
        { initialProps: { hasMore: true } },
      );
      rerender({ hasMore: false });
      rerender({ hasMore: true });
      expect(result.current.sentinelRef).toBeDefined();
    });
  });
});
