import type { NodeConfig } from "../../api/gen/gastrolog/v1/system_pb";
import type { FileStorage, NodeStorageConfig } from "../../api/gen/gastrolog/v1/storage_pb";
import { encode } from "../../api/glid";

// The class `gastrolog config node add-storage` assigns when none is given;
// the wizard's storage takes it whenever no other storage holds it.
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
 * The class that confines a vault to `path`. Storages sharing a class form a
 * pool placement may pick any member of, so the class must hold no storage
 * at another path: an existing path keeps its class when that holds, and a
 * new path takes the lowest class no storage uses.
 */
function classForPath(storages: FileStorage[], path: string): number {
  const classesAtPath = new Set(storages.filter((fs) => fs.path === path).map((fs) => fs.storageClass));
  if (classesAtPath.size === 0) {
    const used = new Set(storages.map((fs) => fs.storageClass));
    let cls = DEFAULT_STORAGE_CLASS;
    while (used.has(cls)) cls++;
    return cls;
  }
  const [cls] = classesAtPath;
  const sharedElsewhere = storages.some((fs) => fs.storageClass === cls && fs.path !== path);
  if (classesAtPath.size > 1 || sharedElsewhere) {
    throw new Error(
      `The existing storage at ${path} shares its storage class with other directories, so a vault in that class could be placed elsewhere. Set up this vault's storage in Settings instead.`,
    );
  }
  return cls!;
}

/**
 * Plans the file storage the setup wizard's file vault lives on: one storage
 * at `path` on every node, so placement finds an eligible node wherever the
 * vault lands, in a class no other directory uses. A relative path resolves
 * on each node against its own directory. Each update carries the node's
 * full storage list, because SetNodeStorageConfig replaces it. A node that
 * already has a storage at the path keeps it.
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
  const storageClass = classForPath(nodeStorageConfigs.flatMap((nsc) => nsc.fileStorages), path);

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
