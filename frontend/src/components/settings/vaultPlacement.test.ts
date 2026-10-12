import { describe, test, expect } from "bun:test";
import { classStorageCounts, placementStatus, rfCap } from "./vaultPlacement";

const twoClassOne = new Map([[1, 2]]);

describe("rfCap", () => {
  test("a file vault is capped by the file storages in its class, same-node storages included", () => {
    const counts = classStorageCounts([
      { fileStorages: [{ storageClass: 1 }, { storageClass: 1 }] },
      { fileStorages: [{ storageClass: 1 }, { storageClass: 2 }] },
    ]);
    expect(rfCap({ type: "file", storageClass: "1" }, counts, 2)).toEqual({
      max: 3,
      eligible: 3,
      cause: "3 file storages with class 1",
    });
    expect(rfCap({ type: "file", storageClass: "2" }, counts, 2)).toEqual({
      max: 1,
      eligible: 1,
      cause: "1 file storage with class 2",
    });
  });

  test("dropping a storage lowers the cap and its stated cause", () => {
    expect(rfCap({ type: "file", storageClass: "1" }, twoClassOne, 1).cause).toBe("2 file storages with class 1");
    expect(rfCap({ type: "file", storageClass: "1" }, new Map([[1, 1]]), 1)).toEqual({
      max: 1,
      eligible: 1,
      cause: "1 file storage with class 1",
    });
  });

  test("a class nobody serves, or none selected, caps at 1 with nothing eligible", () => {
    expect(rfCap({ type: "file", storageClass: "7" }, twoClassOne, 3)).toEqual({
      max: 1,
      eligible: 0,
      cause: "no file storage has class 7",
    });
    expect(rfCap({ type: "file", storageClass: "" }, twoClassOne, 3)).toEqual({
      max: 1,
      eligible: 0,
      cause: "no storage class selected",
    });
  });

  test("a memory vault is capped by the node count", () => {
    expect(rfCap({ type: "memory", storageClass: "" }, twoClassOne, 4)).toEqual({
      max: 4,
      eligible: 4,
      cause: "4 nodes in the cluster",
    });
  });
});

describe("placementStatus", () => {
  const file = { type: "file" as const, storageClass: "1" };

  test("a placed vault at its replication factor has no cause and no action", () => {
    const s = placementStatus(
      { replicationFactor: 2, placements: [{ leader: true }, { leader: false }] },
      file,
      twoClassOne,
      2,
    );
    expect(s).toEqual({ placed: 2, rf: 2, cap: { max: 2, eligible: 2, cause: "2 file storages with class 1" } });
  });

  test("a shortfall below the cap names the storages to add; at or above it, placement is pending", () => {
    const short = placementStatus({ replicationFactor: 3, placements: [{ leader: true }, { leader: false }] }, file, twoClassOne, 2);
    expect(short.action).toBe("add 1 file storage with class 1");
    const pending = placementStatus(
      { replicationFactor: 3, placements: [{ leader: true }, { leader: false }] },
      file,
      new Map([[1, 3]]),
      2,
    );
    expect(pending.action).toBe("placement pending");
  });

  test("an unplaced vault names the cause: no storage of its class, or no leader yet", () => {
    const noStorage = placementStatus({ replicationFactor: 1, placements: [] }, { type: "file", storageClass: "3" }, twoClassOne, 2);
    expect(noStorage.unplacedCause).toBe("no file storage has class 3");
    expect(noStorage.action).toBe("add 1 file storage with class 3");

    const noLeader = placementStatus({ replicationFactor: 1, placements: [] }, file, twoClassOne, 2);
    expect(noLeader.unplacedCause).toBe("no leader assigned yet");
    expect(noLeader.action).toBe("placement pending");

    const noClass = placementStatus({ replicationFactor: 1, placements: [] }, { type: "file", storageClass: "" }, twoClassOne, 2);
    expect(noClass.unplacedCause).toBe("no storage class selected");
    expect(noClass.action).toBe("select a storage class");
  });

  test("a memory vault short of nodes says how many to add", () => {
    const s = placementStatus({ replicationFactor: 3, placements: [{ leader: true }] }, { type: "memory", storageClass: "" }, twoClassOne, 1);
    expect(s.action).toBe("add 2 nodes");
  });
});
