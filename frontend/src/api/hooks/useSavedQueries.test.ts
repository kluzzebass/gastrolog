import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, wrapper } from "../../../test/render";

const mocks = installMockClients();

import { useSavedQueries, usePutSavedQuery, useDeleteSavedQuery } from "./useSavedQueries";

beforeEach(() => {
  m(mocks.systemClient, "getSavedQueries").mockClear();
  m(mocks.systemClient, "putSavedQuery").mockClear();
  m(mocks.systemClient, "deleteSavedQuery").mockClear();
});

describe("useSavedQueries", () => {
  test("fetches saved queries", async () => {
    const queries = [
      { name: "errors", query: "level=error" },
      { name: "slow", query: "duration>1000" },
    ];
    m(mocks.systemClient, "getSavedQueries").mockResolvedValueOnce({ queries });

    const { result } = renderHook(() => useSavedQueries(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
  });
});

describe("usePutSavedQuery", () => {
  test("saves query and writes saved_queries list into cache", async () => {
    const queries = [{ name: "errors", query: "level=error" }];
    m(mocks.systemClient, "putSavedQuery").mockResolvedValueOnce({ savedQueries: { queries } });
    const qc = createTestQueryClient();
    qc.setQueryData(["savedQueries"], []);

    const { result } = renderHook(() => usePutSavedQuery(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({ name: "errors", query: "level=error" });
    });

    expect(m(mocks.systemClient, "putSavedQuery")).toHaveBeenCalledWith({
      query: { name: "errors", query: "level=error" },
    });
    expect(qc.getQueryState(["savedQueries"])?.isInvalidated).toBeFalsy();
    expect(qc.getQueryData<Array<{ name: string; query: string }>>(["savedQueries"])).toEqual(queries);
  });
});

describe("useDeleteSavedQuery", () => {
  test("deletes query by name", async () => {
    m(mocks.systemClient, "deleteSavedQuery").mockResolvedValueOnce({ savedQueries: { queries: [] } });
    const qc = createTestQueryClient();
    qc.setQueryData(["savedQueries"], [{ name: "errors", query: "level=error" }]);

    const { result } = renderHook(() => useDeleteSavedQuery(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync("errors");
    });

    expect(m(mocks.systemClient, "deleteSavedQuery")).toHaveBeenCalledWith({ name: "errors" });
    expect(qc.getQueryData<Array<{ name: string; query: string }>>(["savedQueries"])).toEqual([]);
  });
});
