import { describe, test, expect, afterEach } from "bun:test";
import { renderHook, act } from "@testing-library/react";

import { useMediaQuery } from "./useMediaQuery";

// happy-dom provides matchMedia — we override it per test.
let listeners: Array<(e: MediaQueryListEvent) => void> = [];
let currentMatches = false;

const origMatchMedia = globalThis.matchMedia;
globalThis.matchMedia = (query: string) =>
  ({
    matches: currentMatches,
    media: query,
    addEventListener: (_: string, fn: (e: MediaQueryListEvent) => void) => {
      listeners.push(fn);
    },
    removeEventListener: (_: string, fn: (e: MediaQueryListEvent) => void) => {
      listeners = listeners.filter((l) => l !== fn);
    },
  }) as unknown as MediaQueryList;

afterEach(() => {
  listeners = [];
  currentMatches = false;
});

describe("useMediaQuery", () => {
  test("returns initial match state", () => {
    currentMatches = true;
    const { result } = renderHook(() => useMediaQuery("(max-width: 1023px)"));
    expect(result.current).toBe(true);
  });

  test("returns false when query does not match", () => {
    currentMatches = false;
    const { result } = renderHook(() => useMediaQuery("(max-width: 1023px)"));
    expect(result.current).toBe(false);
  });

  test("updates when media query changes", () => {
    currentMatches = false;
    const { result } = renderHook(() => useMediaQuery("(max-width: 1023px)"));
    expect(result.current).toBe(false);

    act(() => {
      for (const fn of listeners) fn({ matches: true } as MediaQueryListEvent);
    });
    expect(result.current).toBe(true);
  });

  test("cleans up listener on unmount", () => {
    const { unmount } = renderHook(() => useMediaQuery("(max-width: 1023px)"));
    expect(listeners).toHaveLength(1);
    unmount();
    expect(listeners).toHaveLength(0);
  });
});
