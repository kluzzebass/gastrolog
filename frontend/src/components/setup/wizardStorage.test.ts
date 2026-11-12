import { describe, test, expect } from "bun:test";
import { NodeConfig } from "../../api/gen/gastrolog/v1/system_pb";
import { FileStorage, NodeStorageConfig } from "../../api/gen/gastrolog/v1/storage_pb";
import { encodeString } from "../../api/glid";
import { DEFAULT_STORAGE_CLASS, planWizardStorage } from "./wizardStorage";

const NODE_IDS = ["06gsor0000000000000000000a", "06gsor0000000000000000000b", "06gsor0000000000000000000c", "06gsor0000000000000000000d"];

function nodes(ids: string[]): NodeConfig[] {
  return ids.map((id, i) => new NodeConfig({ id: encodeString(id), name: `node-${i + 1}` }));
}

function storageOn(nodeId: string, ...storages: { path: string; storageClass: number; name?: string }[]): NodeStorageConfig {
  return new NodeStorageConfig({
    nodeId: encodeString(nodeId),
    fileStorages: storages.map((s, i) =>
      new FileStorage({
        id: crypto.getRandomValues(new Uint8Array(16)),
        name: s.name ?? `existing-${i}`,
        path: s.path,
        storageClass: s.storageClass,
        diskFreeWarn: "15%",
      }),
    ),
  });
}

describe("planWizardStorage", () => {
  test("a fresh cluster gets one storage at the path on every node, in the vault's class", () => {
    const plan = planWizardStorage(nodes(NODE_IDS), [], { name: "brisk-otter", path: "storage/brisk-otter" });

    expect(plan.storageClass).toBe(DEFAULT_STORAGE_CLASS);
    expect(plan.updates.map((u) => u.nodeId).sort()).toEqual([...NODE_IDS].sort());
    for (const u of plan.updates) {
      expect(u.fileStorages).toHaveLength(1);
      const [fs] = u.fileStorages;
      expect(fs!.path).toBe("storage/brisk-otter");
      expect(fs!.name).toBe("brisk-otter");
      expect(fs!.storageClass).toBe(plan.storageClass);
      expect(fs!.id).not.toBe("");
    }
    const ids = new Set(plan.updates.map((u) => u.fileStorages[0]!.id));
    expect(ids.size).toBe(NODE_IDS.length);
  });

  test("a node's existing storages are kept, since SetNodeStorageConfig replaces the node's whole list", () => {
    const [a, b] = NODE_IDS as [string, string];
    const plan = planWizardStorage(
      nodes([a, b]),
      [storageOn(a, { path: "/mnt/fast", storageClass: 3, name: "fast" })],
      { name: "brisk-otter", path: "/var/lib/gastrolog" },
    );

    const onA = plan.updates.find((u) => u.nodeId === a)!;
    expect(onA.fileStorages.map((fs) => fs.path)).toEqual(["/mnt/fast", "/var/lib/gastrolog"]);
    const kept = onA.fileStorages[0]!;
    expect(kept.name).toBe("fast");
    expect(kept.storageClass).toBe(3);
    expect(kept.diskFreeWarn).toBe("15%");
    expect(plan.updates.find((u) => u.nodeId === b)!.fileStorages).toHaveLength(1);
  });

  test("a node that already has a storage at the path is left alone and the vault takes that storage's class", () => {
    const [a, b] = NODE_IDS as [string, string];
    const plan = planWizardStorage(
      nodes([a, b]),
      [storageOn(a, { path: "/data/logs", storageClass: 2 })],
      { name: "brisk-otter", path: "/data/logs" },
    );

    expect(plan.storageClass).toBe(2);
    expect(plan.updates.map((u) => u.nodeId)).toEqual([b]);
    expect(plan.updates[0]!.fileStorages[0]!.storageClass).toBe(2);
  });

  test("a new path gets a class no existing storage uses, so the vault cannot land in another directory of a shared pool", () => {
    const [a, b] = NODE_IDS as [string, string];
    const plan = planWizardStorage(
      nodes([a, b]),
      [
        storageOn(a, { path: "storage/other", storageClass: DEFAULT_STORAGE_CLASS }),
        storageOn(b, { path: "/mnt/slow", storageClass: DEFAULT_STORAGE_CLASS + 1 }),
      ],
      { name: "typed", path: "storage/typed-dir" },
    );

    expect(plan.storageClass).toBe(DEFAULT_STORAGE_CLASS + 2);
    expect(plan.updates).toHaveLength(2);
    for (const u of plan.updates) {
      expect(u.fileStorages.at(-1)!.path).toBe("storage/typed-dir");
      expect(u.fileStorages.at(-1)!.storageClass).toBe(plan.storageClass);
    }
  });

  test("an existing path whose class is shared with another directory is refused", () => {
    const [a, b] = NODE_IDS as [string, string];
    expect(() =>
      planWizardStorage(
        nodes([a, b]),
        [
          storageOn(a, { path: "/data/logs", storageClass: 1 }),
          storageOn(b, { path: "/data/elsewhere", storageClass: 1 }),
        ],
        { name: "x", path: "/data/logs" },
      ),
    ).toThrow(/\/data\/logs/);
  });

  test("an existing path held in different classes on different nodes is refused", () => {
    const [a, b] = NODE_IDS as [string, string];
    expect(() =>
      planWizardStorage(
        nodes([a, b]),
        [storageOn(a, { path: "/data/logs", storageClass: 1 }), storageOn(b, { path: "/data/logs", storageClass: 2 })],
        { name: "x", path: "/data/logs" },
      ),
    ).toThrow(/\/data\/logs/);
  });

  test("path comparison ignores surrounding whitespace the operator typed", () => {
    const [a] = NODE_IDS as [string];
    const plan = planWizardStorage(nodes([a]), [storageOn(a, { path: "/data/logs", storageClass: 1 })], {
      name: "x",
      path: "  /data/logs  ",
    });
    expect(plan.updates).toEqual([]);
  });

  test("no known nodes is an error, not a vault with nowhere to live", () => {
    expect(() => planWizardStorage([], [], { name: "x", path: "storage/x" })).toThrow();
  });

  test("an empty path is an error: the caller resolves the default first", () => {
    expect(() => planWizardStorage(nodes(NODE_IDS), [], { name: "x", path: "   " })).toThrow();
  });
});
