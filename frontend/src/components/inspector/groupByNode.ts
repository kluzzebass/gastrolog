export interface NodeGroup<T> {
  nodeId: string;
  nodeName: string;
  items: T[];
}

/**
 * Groups items by nodeId, resolving display names from the provided map.
 * Items with empty nodeId fall back to localNodeId (defensive; backend should always populate).
 * Groups are sorted with local node first, then alphabetically by name.
 */
export function groupByNode<T extends { nodeId: string }>(
  items: T[],
  nodeNames: Map<string, string>,
  localNodeId: string,
): NodeGroup<T>[] {
  const groups = new Map<string, T[]>();

  for (const item of items) {
    const nodeId = item.nodeId || localNodeId;
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
