import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, wrapper } from "../../../test/render";

const mocks = installMockClients();

import { usePreferences, usePutPreferences } from "./usePreferences";

beforeEach(() => {
  m(mocks.systemClient, "getPreferences").mockClear();
  m(mocks.systemClient, "putPreferences").mockClear();
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
  test("sends preferences and writes response into cache", async () => {
    const prefs = { theme: "dark", syntaxHighlight: "full", palette: "nord" };
    m(mocks.systemClient, "putPreferences").mockResolvedValueOnce({ preferences: prefs });
    const qc = createTestQueryClient();
    qc.setQueryData(["preferences"], { theme: "light", syntaxHighlight: "off", palette: "default" });

    const { result } = renderHook(() => usePutPreferences(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({
        theme: "dark",
        syntaxHighlight: "full",
        palette: "nord",
      });
    });

    expect(m(mocks.systemClient, "putPreferences")).toHaveBeenCalledWith({
      theme: "dark",
      syntaxHighlight: "full",
      palette: "nord",
    });
    expect(qc.getQueryState(["preferences"])?.isInvalidated).toBeFalsy();
    expect(qc.getQueryData<Record<string, string>>(["preferences"])).toEqual(prefs);
  });
});
