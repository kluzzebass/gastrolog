import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { wrapper } from "../../../test/render";

const mocks = installMockClients();

import { useStorages } from "./useStorages";
import { StorageState } from "../gen/gastrolog/v1/storage_pb";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

beforeEach(() => {
  m(mocks.systemClient, "listStorages").mockClear();
});

describe("useStorages", () => {
  test("fetches the storage list and wraps each entry in a Storage model", async () => {
    const storages = [
      new StorageState({ id: testId(1), name: "nvme-fast", nodeName: "node-1", storageClass: 1 }),
      new StorageState({ id: testId(2), name: "hdd-archive", nodeName: "node-2", storageClass: 3 }),
    ];
    m(mocks.systemClient, "listStorages").mockResolvedValueOnce({ storages });

    const { result } = renderHook(() => useStorages(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.data.length).toBe(2));
    expect(result.current.data[0]?.name).toBe("nvme-fast");
    expect(result.current.data[0]?.nodeName).toBe("node-1");
    expect(result.current.data[1]?.storageClass).toBe(3);
  });

  test("empty response yields an empty list, not loading forever", async () => {
    m(mocks.systemClient, "listStorages").mockResolvedValueOnce({ storages: [] });

    const { result } = renderHook(() => useStorages(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.data).toEqual([]);
  });
});
