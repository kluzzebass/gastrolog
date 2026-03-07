import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, wrapper } from "../../../test/render";

const mocks = installMockClients();

import {
  useIngesters,
  useIngesterStatus,
  usePutIngester,
  useDeleteIngester,
  useTestIngester,
} from "./useIngesters";
import { IngesterConfig } from "../gen/gastrolog/v1/config_pb";

beforeEach(() => {
  m(mocks.configClient, "listIngesters").mockClear();
  m(mocks.configClient, "getIngesterStatus").mockClear();
  m(mocks.configClient, "putIngester").mockClear();
  m(mocks.configClient, "deleteIngester").mockClear();
  m(mocks.configClient, "testIngester").mockClear();
});

describe("useIngesters", () => {
  test("fetches ingester list", async () => {
    const ingesters = [
      new IngesterConfig({ id: "i1", name: "syslog", type: "syslog", enabled: true }),
    ];
    m(mocks.configClient, "listIngesters").mockResolvedValueOnce({ ingesters });

    const { result } = renderHook(() => useIngesters(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(1);
    expect(result.current.data?.[0]?.name).toBe("syslog");
  });
});

describe("useIngesterStatus", () => {
  test("fetches status when id is provided", async () => {
    m(mocks.configClient, "getIngesterStatus").mockResolvedValueOnce({
      running: true,
      messagesIngested: BigInt(500),
    });

    const { result } = renderHook(() => useIngesterStatus("i1"), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.running).toBe(true);
  });

  test("does not fetch when id is empty", () => {
    const { result } = renderHook(() => useIngesterStatus(""), { wrapper: wrapper() });
    expect(result.current.fetchStatus).toBe("idle");
  });
});

describe("usePutIngester", () => {
  test("strips empty params before sending", async () => {
    m(mocks.configClient, "putIngester").mockResolvedValueOnce({});
    const qc = createTestQueryClient();

    const { result } = renderHook(() => usePutIngester(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({
        id: "i1",
        name: "test",
        type: "syslog",
        enabled: true,
        params: { addr: ":514", format: "", tls: "true" },
      });
    });

    const call = m(mocks.configClient, "putIngester").mock.calls[0]?.[0] as {
      config: { params: Record<string, string> };
    };
    // Empty "format" should be stripped.
    expect(call.config.params).toEqual({ addr: ":514", tls: "true" });
  });
});

describe("useDeleteIngester", () => {
  test("deletes and invalidates config", async () => {
    m(mocks.configClient, "deleteIngester").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], {});

    const { result } = renderHook(() => useDeleteIngester(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync("i1");
    });

    expect(m(mocks.configClient, "deleteIngester")).toHaveBeenCalledWith({ id: "i1" });
    expect(qc.getQueryState(["config"])?.isInvalidated).toBe(true);
  });
});

describe("useTestIngester", () => {
  test("sends test request and returns response", async () => {
    m(mocks.configClient, "testIngester").mockResolvedValueOnce({
      success: true,
      message: "connected",
    });

    const { result } = renderHook(() => useTestIngester(), { wrapper: wrapper() });

    let response: unknown;
    await act(async () => {
      response = await result.current.mutateAsync({
        type: "syslog",
        params: { addr: ":514", empty: "" },
      });
    });

    const call = m(mocks.configClient, "testIngester").mock.calls[0]?.[0] as {
      params: Record<string, string>;
    };
    // Empty params stripped here too.
    expect(call.params).toEqual({ addr: ":514" });
    expect((response as { success: boolean }).success).toBe(true);
  });
});
