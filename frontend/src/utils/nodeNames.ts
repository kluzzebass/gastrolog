import { encode } from "../api/glid";

/** Build a Map from encoded node ID to display name (falls back to encoded ID). */
export function buildNodeNameMap(nodeConfigs: readonly { id: Uint8Array | string; name: string }[]): Map<string, string> {
  return new Map(nodeConfigs.map((n) => {
    const eid = typeof n.id === "string" ? n.id : encode(n.id);
    return [eid, n.name || eid];
  }));
}

/** Resolve a node ID to its display name using the map, falling back to the raw ID. */
export function resolveNodeName(map: Map<string, string>, nodeId: string): string {
  return map.get(nodeId) || nodeId;
}
