import { describe, expect, test } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { useEditState } from "./useEditState";

const defaults = (id: string) => ({
  name: `default-${id}`,
  count: 0,
});

describe("useEditState", () => {
  test("getEdit returns defaults when no edits", () => {
    const { result } = renderHook(() => useEditState(defaults));
    expect(result.current.getEdit("foo")).toEqual({
      name: "default-foo",
      count: 0,
    });
  });

  test("setEdit merges partial updates", () => {
    const { result } = renderHook(() => useEditState(defaults));

    act(() => {
      result.current.setEdit("foo", { count: 5 });
    });

    expect(result.current.getEdit("foo")).toEqual({
      name: "default-foo",
      count: 5,
    });
  });

  test("setEdit preserves previous edits", () => {
    const { result } = renderHook(() => useEditState(defaults));

    act(() => {
      result.current.setEdit("foo", { name: "edited" });
    });
    act(() => {
      result.current.setEdit("foo", { count: 10 });
    });

    expect(result.current.getEdit("foo")).toEqual({
      name: "edited",
      count: 10,
    });
  });

  test("different ids are independent", () => {
    const { result } = renderHook(() => useEditState(defaults));

    act(() => {
      result.current.setEdit("a", { count: 1 });
      result.current.setEdit("b", { count: 2 });
    });

    expect(result.current.getEdit("a").count).toBe(1);
    expect(result.current.getEdit("b").count).toBe(2);
  });

  test("clearEdit resets to defaults immediately", () => {
    const { result } = renderHook(() => useEditState(defaults));

    act(() => {
      result.current.setEdit("foo", { count: 99 });
    });
    expect(result.current.getEdit("foo").count).toBe(99);

    act(() => {
      result.current.clearEdit("foo");
    });
    expect(result.current.getEdit("foo")).toEqual({
      name: "default-foo",
      count: 0,
    });
  });

  test("clearEdit on non-existent id is a no-op", () => {
    const { result } = renderHook(() => useEditState(defaults));
    act(() => {
      result.current.clearEdit("nonexistent");
    });
    // Should not throw, defaults still work
    expect(result.current.getEdit("nonexistent")).toEqual({
      name: "default-nonexistent",
      count: 0,
    });
  });

  test("stale edit is discarded when defaults change", () => {
    // Mutable defaults that we can change externally (simulating config update).
    let externalNodeId = "node-1";
    const dynamicDefaults = (id: string) => ({
      name: `vault-${id}`,
      count: 0,
      nodeId: externalNodeId,
    });

    const { result, rerender } = renderHook(() =>
      useEditState(dynamicDefaults),
    );

    // User edits nodeId to "node-2".
    act(() => {
      result.current.setEdit("v1", { nodeId: "node-2" });
    });
    expect(result.current.getEdit("v1").nodeId).toBe("node-2");

    // Simulate config changing externally (e.g. WatchConfig refetch).
    externalNodeId = "node-3";
    rerender();

    // The edit's baseline no longer matches current defaults — stale edit discarded.
    expect(result.current.getEdit("v1").nodeId).toBe("node-3");
  });

  test("isDirty returns false for stale edits", () => {
    let externalName = "original";
    const dynamicDefaults = (_id: string) => ({
      name: externalName,
      count: 0,
    });

    const { result, rerender } = renderHook(() =>
      useEditState(dynamicDefaults),
    );

    act(() => {
      result.current.setEdit("x", { name: "edited" });
    });
    expect(result.current.isDirty("x")).toBe(true);

    // Defaults change externally — edit becomes stale, not dirty.
    externalName = "updated-externally";
    rerender();
    expect(result.current.isDirty("x")).toBe(false);
  });
});
