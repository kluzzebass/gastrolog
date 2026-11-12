import type { NodeConfig } from "../../api/gen/gastrolog/v1/system_pb";
import type { FileStorage, NodeStorageConfig } from "../../api/gen/gastrolog/v1/storage_pb";
import { encode } from "../../api/glid";

// The class `gastrolog config node add-storage` assigns when none is given,
// so a storage the wizard makes pools with one made from the CLI.
export const DEFAULT_STORAGE_CLASS = 1;

/** The path a storage gets when the operator leaves it empty, as in Settings. */
export function defaultStoragePath(storageName: string): string {
  return `storage/${storageName}`;
}

export interface FileStorageInput {
  id: string;
  storageClass: number;
  name: string;
  path: string;
  memoryBudgetBytes: bigint;
  diskFreeWarn: string;
  diskFreeFloor: string;
}

export interface NodeStorageUpdate {
  nodeId: string;
  fileStorages: FileStorageInput[];
}

export interface WizardStoragePlan {
  storageClass: number;
  updates: NodeStorageUpdate[];
}

function toInput(fs: FileStorage): FileStorageInput {
  return {
    id: encode(fs.id),
    storageClass: fs.storageClass,
    name: fs.name,
    path: fs.path,
    memoryBudgetBytes: fs.memoryBudgetBytes,
    diskFreeWarn: fs.diskFreeWarn,
    diskFreeFloor: fs.diskFreeFloor,
  };
}

/**
 * Plans the file storage the setup wizard's file vault lives on: one storage
 * at `path` on every node, so placement finds an eligible node wherever the
 * vault lands. A relative path resolves on each node against its own
 * directory. Each update carries the node's full storage list, because
 * SetNodeStorageConfig replaces it. A node that already has a storage at
 * the path keeps it, and the vault takes that storage's class.
 */
export function planWizardStorage(
  nodeConfigs: NodeConfig[],
  nodeStorageConfigs: NodeStorageConfig[],
  storage: { name: string; path: string },
): WizardStoragePlan {
  const path = storage.path.trim();
  if (!path) throw new Error("A storage directory is required");
  if (nodeConfigs.length === 0) throw new Error("No cluster nodes are known yet");

  const storagesByNode = new Map(nodeStorageConfigs.map((nsc) => [encode(nsc.nodeId), nsc.fileStorages]));
  const existingAtPath = nodeStorageConfigs.flatMap((nsc) => nsc.fileStorages).find((fs) => fs.path === path);
  const storageClass = existingAtPath?.storageClass ?? DEFAULT_STORAGE_CLASS;

  const updates: NodeStorageUpdate[] = [];
  for (const nc of nodeConfigs) {
    const nodeId = encode(nc.id);
    const current = storagesByNode.get(nodeId) ?? [];
    if (current.some((fs) => fs.path === path)) continue;
    updates.push({
      nodeId,
      fileStorages: [
        ...current.map(toInput),
        {
          id: encode(crypto.getRandomValues(new Uint8Array(16))),
          storageClass,
          name: storage.name,
          path,
          memoryBudgetBytes: BigInt(0),
          diskFreeWarn: "",
          diskFreeFloor: "",
        },
      ],
    });
  }
  return { storageClass, updates };
}
