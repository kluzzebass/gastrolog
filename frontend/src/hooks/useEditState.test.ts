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

  test("clearEdit resets to defaults", () => {
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
});
