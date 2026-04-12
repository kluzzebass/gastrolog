import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, wrapper } from "../../../test/render";

const mocks = installMockClients();

import { useVaults, useVault, useStats, useSealVault, useDeleteVault } from "./useVaults";
import { VaultInfo, GetStatsResponse } from "../gen/gastrolog/v1/vault_pb";

beforeEach(() => {
  m(mocks.vaultClient, "listVaults").mockClear();
  m(mocks.vaultClient, "getVault").mockClear();
  m(mocks.vaultClient, "getStats").mockClear();
  m(mocks.vaultClient, "sealVault").mockClear();
  m(mocks.systemClient, "deleteVault").mockClear();
});

describe("useVaults", () => {
  test("fetches vault list", async () => {
    const vaults = [
      new VaultInfo({ id: "v1", name: "logs", type: "file", enabled: true }),
      new VaultInfo({ id: "v2", name: "metrics", type: "file", enabled: false }),
    ];
    m(mocks.vaultClient, "listVaults").mockResolvedValueOnce({ vaults });

    const { result } = renderHook(() => useVaults(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
    expect(result.current.data?.[0]?.name).toBe("logs");
  });
});

describe("useVault", () => {
  test("fetches single vault when enabled", async () => {
    const vault = new VaultInfo({ id: "v1", name: "logs" });
    m(mocks.vaultClient, "getVault").mockResolvedValueOnce({ vault });

    const { result } = renderHook(() => useVault("v1"), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.name).toBe("logs");
  });

  test("does not fetch when id is empty", () => {
    const { result } = renderHook(() => useVault(""), { wrapper: wrapper() });
    expect(result.current.fetchStatus).toBe("idle");
  });
});

describe("useStats", () => {
  test("fetches stats for all vaults", async () => {
    const stats = new GetStatsResponse({ totalRecords: BigInt(1000) });
    m(mocks.vaultClient, "getStats").mockResolvedValueOnce(stats);

    const { result } = renderHook(() => useStats(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.totalRecords).toBe(BigInt(1000));
  });
});

describe("useSealVault", () => {
  test("calls sealVault and invalidates queries", async () => {
    m(mocks.vaultClient, "sealVault").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["vaults"], []);

    const { result } = renderHook(() => useSealVault(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync("v1");
    });

    expect(m(mocks.vaultClient, "sealVault")).toHaveBeenCalledWith({ vault: "v1" });
  });
});

describe("useDeleteVault", () => {
  test("calls deleteVault with force flag", async () => {
    m(mocks.systemClient, "deleteVault").mockResolvedValueOnce({});
    const qc = createTestQueryClient();

    const { result } = renderHook(() => useDeleteVault(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({ id: "v1", force: true });
    });

    expect(m(mocks.systemClient, "deleteVault")).toHaveBeenCalledWith({
      id: "v1",
      force: true,
      deleteData: false,
    });
  });
});
