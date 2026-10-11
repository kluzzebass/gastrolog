import { useConfig } from "./useSystem";
import { useClusterStatus } from "./useClusterStatus";
import { useSettings } from "./useSettings";
import { Node } from "../model/node";
import { type EntityID, idFromBytes, EMPTY_ID } from "../model/id";
import { useRespondingNodeStable } from "./respondingNode";
import { parseDurationNanos } from "../../utils/units";

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
  /**
   * The node that answered the latest settings request. EMPTY_ID until
   * settings load. A usable default for "some node", not an identity to
   * label: see `respondingNodeStable`.
   */
  readonly localNodeId: EntityID;
  /**
   * True while every response this session has seen came from one node.
   * Behind a load balancer the answering node varies per request; then no
   * node is "this node" and the label is withheld rather than guessed.
   */
  readonly respondingNodeStable: boolean;
  /**
   * How long a stats broadcast stays queryable on the responding node, in
   * milliseconds; null until cluster status reports it. Stats older than
   * this are a copy the cluster itself has let go of.
   */
  readonly statsTtlMs: number | null;
  /** Current cluster leader, or null when no leader info is available. */
  readonly leader: Node | null;
  /** True when the cluster is multi-node (raft cluster or multiple configs). */
  readonly multiNode: boolean;
  /** Convenience: display name for an ID, falling back to the ID itself. */
  nameOf(id: EntityID): string;
  /** True only when id is the responding node AND that node has been stable this session. */
  isLocal(id: EntityID): boolean;
}

export function useNodeRegistry(): NodeRegistry {
  const { data: cluster } = useClusterStatus();
  const { data: config } = useConfig();
  const { data: settings } = useSettings();

  const localNodeId: EntityID = settings?.nodeId ? idFromBytes(settings.nodeId) : EMPTY_ID;
  // Two responses in the cache naming different responding nodes is the
  // same evidence as two observed over time: the answering node varies.
  const clusterLocalId: EntityID = cluster?.localNodeId?.length ? idFromBytes(cluster.localNodeId) : EMPTY_ID;
  const respondingNodeStable =
    useRespondingNodeStable() &&
    (clusterLocalId === EMPTY_ID || localNodeId === EMPTY_ID || clusterLocalId === localNodeId);
  const statsTtlNanos = cluster?.statsTtl ? parseDurationNanos(cluster.statsTtl) : BigInt(0);
  const statsTtlMs = statsTtlNanos > BigInt(0) ? Number(statsTtlNanos / BigInt(1_000_000)) : null;

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
    respondingNodeStable,
    statsTtlMs,
    leader,
    multiNode,
    nameOf(id: EntityID): string {
      return byId.get(id)?.name ?? id;
    },
    isLocal(id: EntityID): boolean {
      return respondingNodeStable && id === localNodeId && id !== EMPTY_ID;
    },
  };
}
