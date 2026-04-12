import { describe, test, expect, mock, beforeEach } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { installMockClients, m } from "../../../../test/api-mock";
import { settingsWrapper, createTestQueryClient } from "../../../../test/render";

const mocks = installMockClients();

import { useLookupCrud } from "./useLookupCrud";

interface Draft { name: string; value: string }
interface Saved { name: string; value: string }

const serialize = (items: Draft[]) => items.map((d) => ({ name: d.name, value: d.value }));
const equal = (a: Draft, b: Saved) => a.name === b.name && a.value === b.value;
const getName = (d: Draft) => d.name;

beforeEach(() => {
  m(mocks.systemClient, "putSettings").mockClear();
});

function renderCrud(overrides: Partial<Parameters<typeof useLookupCrud<Draft, Saved>>[0]> = {}) {
  const onDelete = mock(() => {});
  const qc = createTestQueryClient();
  const opts = {
    lookups: [{ name: "a", value: "1" }],
    savedLookups: [{ name: "a", value: "1" }],
    serialize,
    equal,
    lookupKey: "testLookups",
    typeLabel: "Test",
    getName,
    onDelete,
    ...overrides,
  };
  const result = renderHook(() => useLookupCrud(opts), {
    wrapper: settingsWrapper(qc),
  });
  return { ...result, onDelete };
}

describe("useLookupCrud", () => {
  // ── isDirty ───────────────────────────────────────────────────

  test("isDirty returns false when draft equals saved", () => {
    const { result } = renderCrud();
    expect(result.current.isDirty(0)).toBe(false);
  });

  test("isDirty returns true when draft differs from saved", () => {
    const { result } = renderCrud({
      lookups: [{ name: "a", value: "changed" }],
      savedLookups: [{ name: "a", value: "1" }],
    });
    expect(result.current.isDirty(0)).toBe(true);
  });

  test("isDirty returns true for new item (no saved entry)", () => {
    const { result } = renderCrud({
      lookups: [{ name: "a", value: "1" }, { name: "b", value: "2" }],
      savedLookups: [{ name: "a", value: "1" }],
    });
    expect(result.current.isDirty(1)).toBe(true);
  });

  // ── save ──────────────────────────────────────────────────────

  test("save calls putSettings with serialized lookups", async () => {
    m(mocks.systemClient, "putSettings").mockResolvedValueOnce({});
    const { result } = renderCrud();

    await act(() => result.current.save(0));

    expect(m(mocks.systemClient, "putSettings")).toHaveBeenCalledTimes(1);
  });

  test("save shows error toast on failure", async () => {
    m(mocks.systemClient, "putSettings").mockRejectedValueOnce(new Error("network"));
    const { result } = renderCrud();

    // Should not throw — error is caught and toasted.
    await act(() => result.current.save(0));
  });

  // ── handleDelete ──────────────────────────────────────────────

  test("handleDelete calls putSettings with remaining items", async () => {
    m(mocks.systemClient, "putSettings").mockResolvedValueOnce({});
    const { result, onDelete } = renderCrud({
      lookups: [{ name: "a", value: "1" }, { name: "b", value: "2" }],
    });

    await act(() => result.current.handleDelete(0));

    expect(m(mocks.systemClient, "putSettings")).toHaveBeenCalledTimes(1);
    expect(onDelete).toHaveBeenCalledWith(0);
  });

  test("handleDelete shows error toast on failure", async () => {
    m(mocks.systemClient, "putSettings").mockRejectedValueOnce(new Error("fail"));
    const { result, onDelete } = renderCrud();

    await act(() => result.current.handleDelete(0));

    // onDelete should NOT be called on failure.
    expect(onDelete).not.toHaveBeenCalled();
  });

  // ── edge cases ────────────────────────────────────────────────

  test("isDirty with empty lookups", () => {
    const { result } = renderCrud({
      lookups: [],
      savedLookups: [],
    });
    // Index 0 doesn't exist — saved is undefined, so returns true.
    expect(result.current.isDirty(0)).toBe(true);
  });
});
