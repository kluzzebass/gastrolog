import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { wrapper } from "../../../test/render";

// Install mocks before importing hooks (bun hoists mock.module).
const mocks = installMockClients();

import { useConfig, useGenerateName } from "./useSystem";
import { GetSystemResponse } from "../gen/gastrolog/v1/system_pb";
import { decode } from "../glid";

beforeEach(() => {
  m(mocks.systemClient, "getSystem").mockClear();
  m(mocks.systemClient, "generateName").mockClear();
});

describe("useConfig", () => {
  test("fetches config and returns data", async () => {
    const fakeConfig = new GetSystemResponse({
      vaults: [{ id: decode("v1"), name: "default", enabled: true }],
    });
    m(mocks.systemClient, "getSystem").mockResolvedValueOnce(fakeConfig);

    const { result } = renderHook(() => useConfig(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toBe(fakeConfig);
    expect(m(mocks.systemClient, "getSystem")).toHaveBeenCalledTimes(1);
  });

  test("surfaces fetch errors", async () => {
    m(mocks.systemClient, "getSystem").mockRejectedValueOnce(new Error("network down"));

    const { result } = renderHook(() => useConfig(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error?.message).toBe("network down");
  });
});

describe("useGenerateName", () => {
  test("returns generated name on mutate", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "crimson-owl" });

    const { result } = renderHook(() => useGenerateName(), { wrapper: wrapper() });
    result.current.mutate();

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toBe("crimson-owl");
  });
});
