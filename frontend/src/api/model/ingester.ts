// Ingester domain model.
//
// Wraps `IngesterConfig` from the wire format with computed properties that
// the inspector and settings UIs need. It is the single owner of the
// AllNodes/NodeIDs eligibility logic, which no consumer may reimplement
// inline.

import type { IngesterConfig } from "../gen/gastrolog/v1/system_pb";
import { type EntityID, idFromBytes } from "./id";

/** Per-node alive map for a single ingester, keyed by encoded node ID. */
export type NodeStatusMap = { readonly [nodeID: string]: boolean };

/** Variant used for the per-ingester status badge. */
export type IngesterStatusVariant = "info" | "warn" | "error" | "muted";

export class Ingester {
  readonly id: EntityID;
  readonly name: string;
  readonly type: string;
  readonly enabled: boolean;
  readonly singleton: boolean;
  readonly allNodes: boolean;
  /** Eligible-node pin list. Honored only when `allNodes` is false. */
  readonly pinnedNodeIds: readonly EntityID[];

  constructor(cfg: IngesterConfig) {
    this.id = idFromBytes(cfg.id);
    this.name = cfg.name;
    this.type = cfg.type;
    this.enabled = cfg.enabled;
    this.singleton = cfg.singleton;
    this.allNodes = cfg.allNodes;
    this.pinnedNodeIds = cfg.nodeIds.map(idFromBytes);
  }

  /** Name if set, otherwise the encoded id. */
  get displayLabel(): string {
    return this.name || this.id;
  }

  /**
   * Whether this ingester is eligible to run on the given node. Mirrors the
   * backend's `shouldRunIngester`:
   * - `allNodes=true` → every node is eligible regardless of `pinnedNodeIds`
   * - `allNodes=false` with empty `pinnedNodeIds` → legacy "match all"
   * - otherwise → node must appear in `pinnedNodeIds`
   */
  isEligibleOn(nodeId: EntityID): boolean {
    if (this.allNodes) return true;
    if (this.pinnedNodeIds.length === 0) return true;
    return this.pinnedNodeIds.includes(nodeId);
  }

  /**
   * Number of nodes selected for this ingester. With `allNodes`, every live
   * cluster node counts; otherwise it's the pin list length (regardless of
   * liveness — dead nodes still count as selected-but-dead, surfacing as
   * "error" variant).
   */
  selectedCount(liveNodes: ReadonlySet<EntityID>): number {
    if (this.allNodes) return liveNodes.size;
    if (this.pinnedNodeIds.length === 0) return liveNodes.size;
    return this.pinnedNodeIds.length;
  }

  /**
   * Number of nodes that have reported alive=true in the FSM map AND
   * are still present in the cluster's current live-node set.
   *
   * Filtering by `liveNodes` is defense-in-depth: a stale alive flag for
   * a node removed via `cluster remove-node` would inflate the badge.
   * DeleteNode sweeps those entries out of the FSM, so steady-state this
   * filter is a no-op.
   */
  runningCount(aliveMap: ReadonlyMap<EntityID, NodeStatusMap>, liveNodes: ReadonlySet<EntityID>): number {
    const ns = aliveMap.get(this.id);
    if (!ns) return 0;
    let n = 0;
    for (const [nodeID, alive] of Object.entries(ns)) {
      if (alive && liveNodes.has(nodeID as EntityID)) n++;
    }
    return n;
  }

  /**
   * Set of node IDs to render in the per-card "Nodes" detail. With AllNodes
   * we show every live cluster node; otherwise the pin list (which may
   * include dead pins so they can be flagged).
   */
  nodesToDisplay(liveNodes: ReadonlySet<EntityID>): EntityID[] {
    if (this.allNodes) return [...liveNodes];
    return [...this.pinnedNodeIds];
  }

  /**
   * Status pill variant. Disabled → muted. No selected nodes → muted (the
   * ingester is configured but won't run anywhere). Fully running → info.
   * Partial running → warn, unless a pinned node is dead (then error).
   */
  statusVariant(
    aliveMap: ReadonlyMap<EntityID, NodeStatusMap>,
    liveNodes: ReadonlySet<EntityID>,
  ): IngesterStatusVariant {
    if (!this.enabled) return "muted";
    const selected = this.selectedCount(liveNodes);
    if (selected === 0) return "muted";
    const running = this.runningCount(aliveMap, liveNodes);
    if (running >= selected) return "info";
    const hasDeadPin = !this.allNodes && this.pinnedNodeIds.some((id) => !liveNodes.has(id));
    return hasDeadPin ? "error" : "warn";
  }
}
