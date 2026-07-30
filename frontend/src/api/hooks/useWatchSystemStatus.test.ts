import { describe, test, expect } from "bun:test";
import { createTestQueryClient } from "../../../test/render";
import { applyStatusMessage } from "./useWatchSystemStatus";
import { WatchSystemStatusResponse } from "../gen/gastrolog/v1/lifecycle_pb";
import { StorageState } from "../gen/gastrolog/v1/storage_pb";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

describe("applyStatusMessage", () => {
  // buildSystemStatus (backend) always populates `storages`, including as
  // an empty slice once the last storage is removed. A
  // `msg.storages.length > 0` guard here dropped that empty write, so the
  // cache kept showing the deleted storage's card forever — this pins the
  // merge writing empty.
  test("writes an empty storages list, not skipping it", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["storages"], [new StorageState({ id: testId(1), name: "old-storage" })]);

    applyStatusMessage(qc, new WatchSystemStatusResponse({ storages: [] }));

    expect(qc.getQueryData<StorageState[]>(["storages"])).toEqual([]);
  });

  test("writes a non-empty storages list", () => {
    const qc = createTestQueryClient();
    const storages = [new StorageState({ id: testId(1), name: "nvme-fast" })];

    applyStatusMessage(qc, new WatchSystemStatusResponse({ storages }));

    expect(qc.getQueryData<StorageState[]>(["storages"])).toEqual(storages);
  });
});
