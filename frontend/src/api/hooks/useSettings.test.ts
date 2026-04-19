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
  test("sends settings and invalidates cache", async () => {
    m(mocks.systemClient, "putServiceSettings").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], { auth: {} });

    const { result } = renderHook(() => usePutServiceSettings(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({
        query: { timeout: "60s", maxResultCount: 500 },
      });
    });

    expect(m(mocks.systemClient, "putServiceSettings")).toHaveBeenCalledTimes(1);
    expect(qc.getQueryState(["settings"])?.isInvalidated).toBe(true);
  });
});
