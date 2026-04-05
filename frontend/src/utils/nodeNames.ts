/** Build a Map from node ID to display name (falls back to ID). */
export function buildNodeNameMap(nodeConfigs: readonly { id: string; name: string }[]): Map<string, string> {
  return new Map(nodeConfigs.map((n) => [n.id, n.name || n.id]));
}

/** Resolve a node ID to its display name using the map, falling back to the raw ID. */
export function resolveNodeName(map: Map<string, string>, nodeId: string): string {
  return map.get(nodeId) || nodeId;
}
