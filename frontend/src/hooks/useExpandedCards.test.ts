import { describe, test, expect } from "bun:test";
import { renderHook, act } from "@testing-library/react";

import { useExpandedCard, useExpandedCards } from "./useExpandedCards";

describe("useExpandedCard (single-expand)", () => {
  test("starts with no card expanded by default", () => {
    const { result } = renderHook(() => useExpandedCard());
    expect(result.current.expanded).toBeNull();
    expect(result.current.isExpanded("a")).toBe(false);
  });

  test("starts with initial card expanded", () => {
    const { result } = renderHook(() => useExpandedCard("a"));
    expect(result.current.expanded).toBe("a");
    expect(result.current.isExpanded("a")).toBe(true);
  });

  test("toggle opens a card", () => {
    const { result } = renderHook(() => useExpandedCard());
    act(() => result.current.toggle("a"));
    expect(result.current.isExpanded("a")).toBe(true);
  });

  test("toggle closes an already-open card", () => {
    const { result } = renderHook(() => useExpandedCard("a"));
    act(() => result.current.toggle("a"));
    expect(result.current.expanded).toBeNull();
  });

  test("only one card at a time", () => {
    const { result } = renderHook(() => useExpandedCard());
    act(() => result.current.toggle("a"));
    act(() => result.current.toggle("b"));
    expect(result.current.isExpanded("a")).toBe(false);
    expect(result.current.isExpanded("b")).toBe(true);
  });
});

describe("useExpandedCards (multi-expand)", () => {
  test("starts with no cards expanded", () => {
    const { result } = renderHook(() => useExpandedCards());
    expect(result.current.isExpanded("a")).toBe(false);
  });

  test("starts with initial state", () => {
    const { result } = renderHook(() => useExpandedCards({ a: true, b: false }));
    expect(result.current.isExpanded("a")).toBe(true);
    expect(result.current.isExpanded("b")).toBe(false);
  });

  test("toggle opens and closes independently", () => {
    const { result } = renderHook(() => useExpandedCards());
    act(() => result.current.toggle("a"));
    act(() => result.current.toggle("b"));
    expect(result.current.isExpanded("a")).toBe(true);
    expect(result.current.isExpanded("b")).toBe(true);

    act(() => result.current.toggle("a"));
    expect(result.current.isExpanded("a")).toBe(false);
    expect(result.current.isExpanded("b")).toBe(true);
  });
});
