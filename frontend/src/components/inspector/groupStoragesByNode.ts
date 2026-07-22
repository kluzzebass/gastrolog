import { type EntityID, EMPTY_ID } from "../../api/model/id";
import type { Storage } from "../../api/model/storage";
import { groupByNode } from "./groupByNode";

export interface StorageNodeGroup {
  nodeId: EntityID;
  nodeName: string;
  storages: Storage[];
}

/**
 * Groups storages by their owning node for the entity list's multi-node
 * view (mirrors JobsList's per-node grouping — a storage is a single
 * physical volume, owned by exactly one node, unlike a vault which can
 * span several via placement).
 *
 * StorageState carries no raw node ID on the wire, only the pre-resolved
 * node_name display string (gastrolog-3cobq4), so this resolves nodeName
 * -> EntityID against a name map built from the node registry before
 * delegating to the shared groupByNode helper. A storage whose node_name
 * doesn't resolve (a node this node's registry doesn't know about yet)
 * falls back to the local node — the same defensive behavior groupByNode
 * itself documents for an empty nodeId.
 */
export function groupStoragesByNode(
  storages: readonly Storage[],
  nodeIdByName: ReadonlyMap<string, EntityID>,
  nodeNames: ReadonlyMap<EntityID, string>,
  localNodeId: EntityID,
): StorageNodeGroup[] {
  const wrapped = storages.map((storage) => ({
    nodeId: nodeIdByName.get(storage.nodeName) ?? EMPTY_ID,
    storage,
  }));
  const groups = groupByNode(wrapped, nodeNames, localNodeId);
  return groups.map((g) => ({
    nodeId: g.nodeId,
    nodeName: g.nodeName,
    storages: g.items.map((i) => i.storage),
  }));
}
