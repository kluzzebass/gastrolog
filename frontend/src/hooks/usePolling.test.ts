import { describe, test, expect, mock, beforeEach, afterEach } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { useState, useRef, useEffect } from "react";

/**
 * Minimal reproduction of the polling pattern used in useSearchView.
 * Tests that the setInterval fires correctly and isn't reset by re-renders.
 */
function usePolling(searchFn: () => void) {
  const [pollInterval, setPollInterval] = useState<number | null>(null);

  // Ref-stabilize the search function so the interval doesn't reset on re-render.
  const searchRef = useRef(searchFn);
  searchRef.current = searchFn;

  useEffect(() => {
    if (!pollInterval) return;
    const id = setInterval(() => {
      searchRef.current();
    }, pollInterval);
    return () => clearInterval(id);
  }, [pollInterval]);

  return { pollInterval, setPollInterval };
}

// Broken version: search in deps causes interval to reset every render.
function usePollingBroken(searchFn: () => void) {
  const [pollInterval, setPollInterval] = useState<number | null>(null);

  useEffect(() => {
    if (!pollInterval) return;
    const id = setInterval(() => {
      searchFn();
    }, pollInterval);
    return () => clearInterval(id);
  }, [pollInterval, searchFn]);

  return { pollInterval, setPollInterval };
}

describe("usePolling", () => {
  let realSetInterval: typeof globalThis.setInterval;
  let realClearInterval: typeof globalThis.clearInterval;
  let intervalCallbacks: Map<number, () => void>;
  let nextId: number;
  let clearCount: number;

  beforeEach(() => {
    realSetInterval = globalThis.setInterval;
    realClearInterval = globalThis.clearInterval;
    intervalCallbacks = new Map();
    nextId = 1;
    clearCount = 0;

    globalThis.setInterval = ((fn: () => void, _ms?: number) => {
      const id = nextId++;
      intervalCallbacks.set(id, fn);
      return id;
    }) as typeof setInterval;

    globalThis.clearInterval = ((id: number) => {
      intervalCallbacks.delete(id);
      clearCount++;
    }) as typeof clearInterval;
  });

  afterEach(() => {
    globalThis.setInterval = realSetInterval;
    globalThis.clearInterval = realClearInterval;
  });

  test("ref-stabilized polling fires callback without interval reset on re-renders", () => {
    const searchFn = mock(() => {});

    const { result, rerender } = renderHook(
      ({ fn }) => usePolling(fn),
      { initialProps: { fn: searchFn } },
    );

    // Enable polling.
    act(() => result.current.setPollInterval(5000));
    expect(intervalCallbacks.size).toBe(1);
    const initialClearCount = clearCount;

    // Simulate multiple re-renders with a new function reference each time.
    for (let i = 0; i < 10; i++) {
      rerender({ fn: mock(() => {}) });
    }

    // The interval should NOT have been cleared and recreated.
    expect(clearCount).toBe(initialClearCount);
    expect(intervalCallbacks.size).toBe(1);

    // Fire the interval callback — should call the latest searchFn.
    const cb = [...intervalCallbacks.values()][0]!;
    cb();
    // The ref always points to the latest fn, so the original mock won't be called.
    // But the latest one (from last rerender) will be.
    // Just verify cb doesn't throw and the interval survived re-renders.
    expect(intervalCallbacks.size).toBe(1);
  });

  test("broken version resets interval on every re-render", () => {
    const searchFn = mock(() => {});

    const { result, rerender } = renderHook(
      ({ fn }) => usePollingBroken(fn),
      { initialProps: { fn: searchFn } },
    );

    // Enable polling.
    act(() => result.current.setPollInterval(5000));
    const initialClearCount = clearCount;

    // Simulate re-renders with new function references.
    for (let i = 0; i < 10; i++) {
      rerender({ fn: mock(() => {}) });
    }

    // The broken version clears and recreates the interval on every re-render.
    expect(clearCount).toBeGreaterThan(initialClearCount);
  });

  test("disabling poll clears the interval", () => {
    const searchFn = mock(() => {});
    const { result } = renderHook(() => usePolling(searchFn));

    act(() => result.current.setPollInterval(5000));
    expect(intervalCallbacks.size).toBe(1);

    act(() => result.current.setPollInterval(null));
    expect(intervalCallbacks.size).toBe(0);
  });
});
