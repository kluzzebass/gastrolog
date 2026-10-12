import { describe, expect, test } from "bun:test";
import { StrictMode } from "react";
import { renderHook, act } from "@testing-library/react";
import { renderHookWithAbandonedRender } from "../../test/abandonedRender";
import { useEditState } from "./useEditState";

const defaults = (id: string) => ({
  name: `default-${id}`,
  count: 0,
});
const configA = (id: string) => ({ name: `a-${id}`, count: 0 });
const configB = (id: string) => ({ name: `b-${id}`, count: 0 });

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

  test("editing after defaults changed starts over from the fresh defaults", () => {
    let configNodeId = "node-1";
    const configDefaults = (id: string) => ({ name: `vault-${id}`, count: 0, nodeId: configNodeId });
    const { result, rerender } = renderHook(() => useEditState(configDefaults));

    act(() => {
      result.current.setEdit("v1", { nodeId: "node-2" });
    });
    configNodeId = "node-3";
    rerender();
    expect(result.current.getEdit("v1").nodeId).toBe("node-3");

    // The operator sees node-3 and edits another field: the stale node-2 must not come back.
    act(() => {
      result.current.setEdit("v1", { count: 4 });
    });
    expect(result.current.getEdit("v1")).toEqual({ name: "vault-v1", count: 4, nodeId: "node-3" });
    expect(result.current.isDirty("v1")).toBe(true);
  });

  test("an edit that matches the defaults again is not dirty", () => {
    const { result } = renderHook(() => useEditState(defaults));

    act(() => {
      result.current.setEdit("foo", { count: 3 });
    });
    expect(result.current.isDirty("foo")).toBe(true);
    act(() => {
      result.current.setEdit("foo", { count: 0 });
    });
    expect(result.current.isDirty("foo")).toBe(false);
  });

  test("setEdit merges correctly under StrictMode's double-invoked updaters", () => {
    const { result } = renderHook(() => useEditState(defaults), { wrapper: StrictMode });

    act(() => {
      result.current.setEdit("foo", { name: "edited" });
    });
    act(() => {
      result.current.setEdit("foo", { count: 2 });
    });

    expect(result.current.getEdit("foo")).toEqual({ name: "edited", count: 2 });
    expect(result.current.isDirty("foo")).toBe(true);
  });

  test("a render React threw away cannot discard a live edit", async () => {
    const { result, abandon } = renderHookWithAbandonedRender((d: typeof configA) => {
      const edits = useEditState(d);
      edits.getEdit("foo");
      return edits;
    }, configA);

    act(() => {
      result.current.setEdit("foo", { count: 9 });
    });
    // A render against other defaults starts and is thrown away; the committed defaults never changed.
    await abandon(configB);

    expect(result.current.getEdit("foo")).toEqual({ name: "a-foo", count: 9 });
    expect(result.current.isDirty("foo")).toBe(true);
  });
});
