import { type EntityID, isEmptyID } from "../../api/model/id";

interface NodeGroup<T> {
  nodeId: EntityID;
  nodeName: string;
  items: T[];
}

/**
 * Groups items by nodeId, resolving display names from the provided map.
 * Items with empty nodeId fall back to localNodeId (defensive; backend
 * should always populate). Groups are sorted with local node first, then
 * alphabetically by name.
 */
export function groupByNode<T extends { nodeId: EntityID }>(
  items: T[],
  nodeNames: ReadonlyMap<EntityID, string>,
  localNodeId: EntityID,
): NodeGroup<T>[] {
  const groups = new Map<EntityID, T[]>();

  for (const item of items) {
    const nodeId = isEmptyID(item.nodeId) ? localNodeId : item.nodeId;
    let group = groups.get(nodeId);
    if (!group) {
      group = [];
      groups.set(nodeId, group);
    }
    group.push(item);
  }

  const result: NodeGroup<T>[] = [];
  for (const [nodeId, groupItems] of groups) {
    result.push({
      nodeId,
      nodeName: nodeNames.get(nodeId) || nodeId,
      items: groupItems,
    });
  }

  result.sort((a, b) => {
    if (a.nodeId === localNodeId) return -1;
    if (b.nodeId === localNodeId) return 1;
    return a.nodeName.localeCompare(b.nodeName);
  });

  return result;
}
