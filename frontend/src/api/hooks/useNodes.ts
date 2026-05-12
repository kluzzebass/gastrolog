import { useConfig } from "./useSystem";
import { useClusterStatus } from "./useClusterStatus";
import { useSettings } from "./useSettings";
import { Node } from "../model/node";
import { type EntityID, idFromBytes, EMPTY_ID } from "../model/id";

/**
 * Cluster-wide node registry. Joins durable NodeConfig (from GetSystem,
 * covers every node ever known to the cluster) with the live ClusterNode
 * snapshot (from GetClusterStatus / WatchSystemStatus, covers current
 * members). Centralizes the four-place node-name lookup pattern that used
 * to live inline in NodeBadge, IngesterCard, NodeDetailPane, EntityListPane.
 */
export interface NodeRegistry {
  /** Every known node — union of cluster members and config entries. */
  readonly all: Node[];
  readonly byId: ReadonlyMap<EntityID, Node>;
  /** This API node's ID. EMPTY_ID until settings load. */
  readonly localNodeId: EntityID;
  /** Current cluster leader, or null when no leader info is available. */
  readonly leader: Node | null;
  /** True when the cluster is multi-node (raft cluster or multiple configs). */
  readonly multiNode: boolean;
  /** Convenience: display name for an ID, falling back to the ID itself. */
  nameOf(id: EntityID): string;
  /** Convenience: is this the local API node. */
  isLocal(id: EntityID): boolean;
}

export function useNodeRegistry(): NodeRegistry {
  const { data: cluster } = useClusterStatus();
  const { data: config } = useConfig();
  const { data: settings } = useSettings();

  const localNodeId: EntityID = settings?.nodeId ? idFromBytes(settings.nodeId) : EMPTY_ID;

  const byId = new Map<EntityID, Node>();

  // Seed with durable config entries first so stopped nodes still resolve.
  for (const nc of config?.nodeConfigs ?? []) {
    const id = idFromBytes(nc.id);
    byId.set(id, new Node(id, null, nc));
  }

  // Overlay with live cluster members — they always win for the cluster
  // half, but inherit the config from any earlier set.
  for (const cn of cluster?.nodes ?? []) {
    const id = idFromBytes(cn.id);
    const existing = byId.get(id);
    byId.set(id, new Node(id, cn, existing?.config ?? null));
  }

  const all = [...byId.values()];
  const leader = all.find((n) => n.isLeader) ?? null;

  const clusterEnabled = cluster?.clusterEnabled ?? false;
  const multiNode = clusterEnabled || (config?.nodeConfigs?.length ?? 0) > 1;

  return {
    all,
    byId,
    localNodeId,
    leader,
    multiNode,
    nameOf(id: EntityID): string {
      return byId.get(id)?.name ?? id;
    },
    isLocal(id: EntityID): boolean {
      return id === localNodeId && id !== EMPTY_ID;
    },
  };
}
