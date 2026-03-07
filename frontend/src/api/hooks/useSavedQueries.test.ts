import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, wrapper } from "../../../test/render";

const mocks = installMockClients();

import { useSavedQueries, usePutSavedQuery, useDeleteSavedQuery } from "./useSavedQueries";

beforeEach(() => {
  m(mocks.configClient, "getSavedQueries").mockClear();
  m(mocks.configClient, "putSavedQuery").mockClear();
  m(mocks.configClient, "deleteSavedQuery").mockClear();
});

describe("useSavedQueries", () => {
  test("fetches saved queries", async () => {
    const queries = [
      { name: "errors", query: "level=error" },
      { name: "slow", query: "duration>1000" },
    ];
    m(mocks.configClient, "getSavedQueries").mockResolvedValueOnce({ queries });

    const { result } = renderHook(() => useSavedQueries(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
  });
});

describe("usePutSavedQuery", () => {
  test("saves query and invalidates cache", async () => {
    m(mocks.configClient, "putSavedQuery").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["savedQueries"], []);

    const { result } = renderHook(() => usePutSavedQuery(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({ name: "errors", query: "level=error" });
    });

    expect(m(mocks.configClient, "putSavedQuery")).toHaveBeenCalledWith({
      query: { name: "errors", query: "level=error" },
    });
    expect(qc.getQueryState(["savedQueries"])?.isInvalidated).toBe(true);
  });
});

describe("useDeleteSavedQuery", () => {
  test("deletes query by name", async () => {
    m(mocks.configClient, "deleteSavedQuery").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["savedQueries"], []);

    const { result } = renderHook(() => useDeleteSavedQuery(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync("errors");
    });

    expect(m(mocks.configClient, "deleteSavedQuery")).toHaveBeenCalledWith({ name: "errors" });
  });
});
