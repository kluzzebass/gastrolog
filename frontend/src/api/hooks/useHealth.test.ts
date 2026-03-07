import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { wrapper } from "../../../test/render";
import { Status } from "../gen/gastrolog/v1/lifecycle_pb";

const mocks = installMockClients();

import { useHealth } from "./useHealth";

beforeEach(() => {
  m(mocks.lifecycleClient, "health").mockClear();
});

describe("useHealth", () => {
  test("fetches health status", async () => {
    m(mocks.lifecycleClient, "health").mockResolvedValueOnce({
      status: Status.HEALTHY,
      version: "1.0.0",
    });

    const { result } = renderHook(() => useHealth(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.status).toBe(Status.HEALTHY);
    expect(result.current.data?.version).toBe("1.0.0");
  });
});
