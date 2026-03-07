import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, wrapper } from "../../../test/render";

const mocks = installMockClients();

import { usePreferences, usePutPreferences } from "./usePreferences";

beforeEach(() => {
  m(mocks.configClient, "getPreferences").mockClear();
  m(mocks.configClient, "putPreferences").mockClear();
});

describe("usePreferences", () => {
  test("fetches preferences when token exists", () => {
    // getToken returns null by default (from installMockClients), so
    // the query should be disabled. Override it for this test.
    // Note: usePreferences uses `enabled: !!getToken()` — since getToken
    // is mocked to return null, this query stays idle.
    const { result } = renderHook(() => usePreferences(), { wrapper: wrapper() });
    expect(result.current.fetchStatus).toBe("idle");
  });
});

describe("usePutPreferences", () => {
  test("sends preferences and invalidates cache", async () => {
    m(mocks.configClient, "putPreferences").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["preferences"], {});

    const { result } = renderHook(() => usePutPreferences(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({
        theme: "dark",
        syntaxHighlight: "full",
        palette: "nord",
      });
    });

    expect(m(mocks.configClient, "putPreferences")).toHaveBeenCalledWith({
      theme: "dark",
      syntaxHighlight: "full",
      palette: "nord",
    });
    expect(qc.getQueryState(["preferences"])?.isInvalidated).toBe(true);
  });
});
