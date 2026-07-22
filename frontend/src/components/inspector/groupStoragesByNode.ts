import type { EntityID } from "../../api/model/id";
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
 * Joins on storage.nodeId (StorageState.node_id, gastrolog-3cobq4 review)
 * — the stable key — never storage.nodeName: a name-based join collides on
 * rename races or duplicate names, silently merging two different nodes'
 * storages into one group. A storage with an empty nodeId (an old cached
 * entry from before node_id existed, or a node this registry doesn't know
 * about yet) falls back to the local node — the same defensive behavior
 * groupByNode itself documents for an empty nodeId.
 */
export function groupStoragesByNode(
  storages: readonly Storage[],
  nodeNames: ReadonlyMap<EntityID, string>,
  localNodeId: EntityID,
): StorageNodeGroup[] {
  const wrapped = storages.map((storage) => ({
    nodeId: storage.nodeId,
    storage,
  }));
  const groups = groupByNode(wrapped, nodeNames, localNodeId);
  return groups.map((g) => ({
    nodeId: g.nodeId,
    nodeName: g.nodeName,
    storages: g.items.map((i) => i.storage),
  }));
}
