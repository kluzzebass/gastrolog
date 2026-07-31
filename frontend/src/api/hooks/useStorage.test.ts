import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, wrapper } from "../../../test/render";

const mocks = installMockClients();

import { useSetNodeStorageConfig } from "./useStorage";

beforeEach(() => {
  m(mocks.systemClient, "setNodeStorageConfig").mockClear();
});

describe("useSetNodeStorageConfig", () => {
  // Deleting the LAST storage on a node left its card stranded because
  // this mutation never invalidated the storage entity list
  // (["storages"], ListStorages) — only the push stream did. Both must
  // invalidate/write on removal.
  test("invalidates the storage list", async () => {
    m(mocks.systemClient, "setNodeStorageConfig").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["storages"], []);

    const { result } = renderHook(() => useSetNodeStorageConfig(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({ nodeId: "node-1", fileStorages: [] });
    });

    expect(qc.getQueryState(["storages"])?.isInvalidated).toBe(true);
  });
});
