import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, wrapper } from "../../../test/render";

const mocks = installMockClients();

import { useSettings, usePutServiceSettings } from "./useSettings";

beforeEach(() => {
  m(mocks.systemClient, "getSettings").mockClear();
  m(mocks.systemClient, "putServiceSettings").mockClear();
});

describe("useSettings", () => {
  test("fetches settings", async () => {
    m(mocks.systemClient, "getSettings").mockResolvedValueOnce({
      auth: { tokenDuration: "24h" },
      query: { timeout: "30s" },
    });

    const { result } = renderHook(() => useSettings(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.auth?.tokenDuration).toBe("24h");
  });
});

describe("usePutServiceSettings", () => {
  test("sends settings and applies echo to settings cache", async () => {
    m(mocks.systemClient, "putServiceSettings").mockResolvedValueOnce({
      echo: {
        settings: {
          auth: {},
          query: { timeout: "60s", maxFollowDuration: "", maxResultCount: 500 },
        },
        systemRaftIndex: 2n,
      },
    });
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], { auth: {} });

    const { result } = renderHook(() => usePutServiceSettings(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({
        query: { timeout: "60s", maxResultCount: 500 },
      });
    });

    expect(m(mocks.systemClient, "putServiceSettings")).toHaveBeenCalledTimes(1);
    expect(qc.getQueryState(["settings"])?.isInvalidated).toBeFalsy();
    const cached = qc.getQueryData<{ query?: { timeout?: string; maxResultCount?: number } }>(["settings"]);
    expect(cached?.query?.timeout).toBe("60s");
    expect(cached?.query?.maxResultCount).toBe(500);
  });
});
