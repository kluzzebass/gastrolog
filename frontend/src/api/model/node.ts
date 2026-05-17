// Node domain model.
//
// Joins the durable `NodeConfig` (from GetSystem — has a name entry even for
// nodes that aren't currently live) with the live `ClusterNode` (from
// GetClusterStatus / WatchSystemStatus — has role, suffrage, isLeader,
// stats). Either side may be missing: a freshly-joined node appears in
// `ClusterNode` before its `NodeConfig` is replicated; a stopped node
// remains in `NodeConfig` after dropping out of `ClusterNode`.

import { ClusterNodeRole, ClusterNodeSuffrage, type ClusterNode } from "../gen/gastrolog/v1/lifecycle_pb";
import type { NodeStats } from "../gen/gastrolog/v1/cluster_pb";
import type { NodeConfig } from "../gen/gastrolog/v1/system_pb";
import { NodeState } from "../gen/gastrolog/v1/system_pb";
import type { Timestamp } from "@bufbuild/protobuf";
import { type EntityID, idFromBytes } from "./id";

// Re-export proto-layer types that the model exposes through its API,
// so components can consume them without reaching into src/api/gen
// (lint rule: components import from src/api/model/ or src/api/hooks/).
export { NodeState };
export type { Timestamp };

export class Node {
  readonly id: EntityID;
  /** Live cluster-member view, or null if the node isn't currently a member. */
  readonly cluster: ClusterNode | null;
  /** Durable config-store view, or null if no config entry exists yet. */
  readonly config: NodeConfig | null;

  constructor(id: EntityID, cluster: ClusterNode | null, config: NodeConfig | null) {
    this.id = id;
    this.cluster = cluster;
    this.config = config;
  }

  /** Live cluster name → config name → id. */
  get name(): string {
    return this.cluster?.name || this.config?.name || this.id;
  }

  /** True when this node is currently a cluster member. */
  get isLive(): boolean {
    return this.cluster !== null;
  }

  get isLeader(): boolean {
    return this.cluster?.isLeader ?? false;
  }

  /** True when this node is a non-voting member of the cluster. */
  get isNonvoter(): boolean {
    return this.cluster?.suffrage === ClusterNodeSuffrage.NONVOTER;
  }

  /** True when the node is reachable (we have stats from it). */
  get isOnline(): boolean {
    return this.cluster?.stats != null;
  }

  /** Undefined when the node isn't live. */
  get role(): ClusterNodeRole | undefined {
    return this.cluster?.role;
  }

  get suffrage(): ClusterNodeSuffrage | undefined {
    return this.cluster?.suffrage;
  }

  get stats(): NodeStats | null {
    return this.cluster?.stats ?? null;
  }

  get apiAddress(): string {
    return this.cluster?.apiAddress ?? "";
  }

  get pprofAddress(): string {
    return this.cluster?.pprofAddress ?? "";
  }

  /** Lifecycle state. UNSPECIFIED is treated as LIVE per the proto contract. */
  get state(): NodeState {
    const raw = this.cluster?.state ?? NodeState.UNSPECIFIED;
    return raw === NodeState.UNSPECIFIED ? NodeState.LIVE : raw;
  }

  /** Wall-clock instant the current state was entered, or undefined. */
  get stateSince(): Timestamp | undefined {
    return this.cluster?.stateSince;
  }
}
