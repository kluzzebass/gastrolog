import { describe, expect, test, beforeEach } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { useQueryHistory } from "./useQueryHistory";

describe("useQueryHistory", () => {
  beforeEach(() => {
    localStorage.removeItem("gastrolog:query-history");
  });

  test("starts empty with no localStorage data", () => {
    const { result } = renderHook(() => useQueryHistory());
    expect(result.current.entries).toEqual([]);
  });

  test("add pushes an entry", () => {
    const { result } = renderHook(() => useQueryHistory());

    act(() => {
      result.current.add("level=error");
    });

    expect(result.current.entries).toHaveLength(1);
    expect(result.current.entries[0].query).toBe("level=error");
  });

  test("add trims whitespace", () => {
    const { result } = renderHook(() => useQueryHistory());

    act(() => {
      result.current.add("  level=error  ");
    });

    expect(result.current.entries[0].query).toBe("level=error");
  });

  test("add ignores empty strings", () => {
    const { result } = renderHook(() => useQueryHistory());

    act(() => {
      result.current.add("");
      result.current.add("   ");
    });

    expect(result.current.entries).toHaveLength(0);
  });

  test("add deduplicates (moves existing to top)", () => {
    const { result } = renderHook(() => useQueryHistory());

    act(() => {
      result.current.add("first");
    });
    act(() => {
      result.current.add("second");
    });
    act(() => {
      result.current.add("first");
    });

    expect(result.current.entries).toHaveLength(2);
    expect(result.current.entries[0].query).toBe("first");
    expect(result.current.entries[1].query).toBe("second");
  });

  test("add caps at 50 entries", () => {
    const { result } = renderHook(() => useQueryHistory());

    act(() => {
      for (let i = 0; i < 60; i++) {
        result.current.add(`query-${i}`);
      }
    });

    expect(result.current.entries).toHaveLength(50);
    // Most recent should be first
    expect(result.current.entries[0].query).toBe("query-59");
  });

  test("add persists to localStorage", () => {
    const { result } = renderHook(() => useQueryHistory());

    act(() => {
      result.current.add("persisted");
    });

    const stored = JSON.parse(
      localStorage.getItem("gastrolog:query-history") ?? "[]",
    );
    expect(stored).toHaveLength(1);
    expect(stored[0].query).toBe("persisted");
  });

  test("remove deletes a specific entry", () => {
    const { result } = renderHook(() => useQueryHistory());

    act(() => {
      result.current.add("keep");
      result.current.add("delete-me");
    });

    act(() => {
      result.current.remove("delete-me");
    });

    expect(result.current.entries).toHaveLength(1);
    expect(result.current.entries[0].query).toBe("keep");
  });

  test("clear removes all entries", () => {
    const { result } = renderHook(() => useQueryHistory());

    act(() => {
      result.current.add("one");
      result.current.add("two");
    });

    act(() => {
      result.current.clear();
    });

    expect(result.current.entries).toHaveLength(0);
    expect(localStorage.getItem("gastrolog:query-history")).toBeNull();
  });

  test("loads from localStorage on init", () => {
    const entries = [
      { query: "from-storage", timestamp: Date.now() },
    ];
    localStorage.setItem("gastrolog:query-history", JSON.stringify(entries));

    const { result } = renderHook(() => useQueryHistory());
    expect(result.current.entries).toHaveLength(1);
    expect(result.current.entries[0].query).toBe("from-storage");
  });

  test("handles corrupt localStorage gracefully", () => {
    localStorage.setItem("gastrolog:query-history", "not-json!!!");

    const { result } = renderHook(() => useQueryHistory());
    expect(result.current.entries).toEqual([]);
  });
});
