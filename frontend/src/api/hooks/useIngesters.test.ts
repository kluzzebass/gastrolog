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
import { IngesterConfig } from "../gen/gastrolog/v1/system_pb";
import { decode } from "../glid";

beforeEach(() => {
  m(mocks.systemClient, "listIngesters").mockClear();
  m(mocks.systemClient, "getIngesterStatus").mockClear();
  m(mocks.systemClient, "putIngester").mockClear();
  m(mocks.systemClient, "deleteIngester").mockClear();
  m(mocks.systemClient, "testIngester").mockClear();
});

describe("useIngesters", () => {
  test("fetches ingester list", async () => {
    const ingesters = [
      new IngesterConfig({ id: decode("i1"), name: "syslog", type: "syslog", enabled: true }),
    ];
    m(mocks.systemClient, "listIngesters").mockResolvedValueOnce({ ingesters });

    const { result } = renderHook(() => useIngesters(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(1);
    expect(result.current.data?.[0]?.name).toBe("syslog");
  });
});

describe("useIngesterStatus", () => {
  test("streams status when id is provided", async () => {
    async function* one() {
      yield { running: true, messagesIngested: BigInt(500) };
    }
    m(mocks.systemClient, "watchIngesterStatus").mockReturnValueOnce(one() as never);

    const { result } = renderHook(() => useIngesterStatus("i1"), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.data?.running).toBe(true));
    expect(result.current.isLoading).toBe(false);
  });

  test("does not subscribe when id is empty", () => {
    const { result } = renderHook(() => useIngesterStatus(""), { wrapper: wrapper() });
    expect(result.current.data).toBeUndefined();
    expect(result.current.isLoading).toBe(false);
  });
});

describe("usePutIngester", () => {
  test("strips empty params before sending", async () => {
    m(mocks.systemClient, "putIngester").mockResolvedValueOnce({});
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

    const call = m(mocks.systemClient, "putIngester").mock.calls[0]?.[0] as {
      config: { params: Record<string, string> };
    };
    // Empty "format" should be stripped.
    expect(call.config.params).toEqual({ addr: ":514", tls: "true" });
  });
});

describe("useDeleteIngester", () => {
  test("deletes and invalidates config", async () => {
    m(mocks.systemClient, "deleteIngester").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], {});
    qc.setQueryData(["ingesters"], []);

    const { result } = renderHook(() => useDeleteIngester(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync("i1");
    });

    expect(m(mocks.systemClient, "deleteIngester")).toHaveBeenCalledWith({ id: decode("i1") });
    expect(qc.getQueryState(["system"])?.isInvalidated).toBe(true);
    expect(qc.getQueryState(["ingesters"])?.isInvalidated).toBe(true);
  });
});

describe("useTestIngester", () => {
  test("sends test request and returns response", async () => {
    m(mocks.systemClient, "testIngester").mockResolvedValueOnce({
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

    const call = m(mocks.systemClient, "testIngester").mock.calls[0]?.[0] as {
      params: Record<string, string>;
    };
    // Empty params stripped here too.
    expect(call.params).toEqual({ addr: ":514" });
    expect((response as { success: boolean }).success).toBe(true);
  });
});
