import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, wrapper } from "../../../test/render";

const mocks = installMockClients();

import { useSettings, usePutSettings } from "./useSettings";

beforeEach(() => {
  m(mocks.configClient, "getSettings").mockClear();
  m(mocks.configClient, "putSettings").mockClear();
});

describe("useSettings", () => {
  test("fetches settings", async () => {
    m(mocks.configClient, "getSettings").mockResolvedValueOnce({
      auth: { tokenDuration: "24h" },
      query: { timeout: "30s" },
    });

    const { result } = renderHook(() => useSettings(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.auth?.tokenDuration).toBe("24h");
  });
});

describe("usePutSettings", () => {
  test("sends settings and invalidates cache", async () => {
    m(mocks.configClient, "putSettings").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], { auth: {} });

    const { result } = renderHook(() => usePutSettings(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({
        query: { timeout: "60s", maxResultCount: 500 },
      });
    });

    expect(m(mocks.configClient, "putSettings")).toHaveBeenCalledTimes(1);
    expect(qc.getQueryState(["settings"])?.isInvalidated).toBe(true);
  });
});
