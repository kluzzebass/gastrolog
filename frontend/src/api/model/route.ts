// Route domain model.
//
// Joins the durable `RouteConfig` (from GetSystem) with the runtime
// `PerRouteStats` overlay (from GetRouteStats / WatchSystemStatus) so the
// inspector reads route-level matched/forwarded counts off the same
// object that carries the static config.

import type { RouteConfig, RouteStage, RouteDestination, PerRouteStats } from "../gen/gastrolog/v1/system_pb";
import { type EntityID, idFromBytes } from "./id";

export class Route {
  readonly id: EntityID;
  readonly name: string;
  readonly priority: number;
  readonly enabled: boolean;
  readonly distribution: string;
  readonly stages: readonly RouteStage[];
  readonly destinations: readonly RouteDestination[];
  /** Per-route runtime stats, if joined. */
  readonly stats: PerRouteStats | null;

  constructor(config: RouteConfig, stats: PerRouteStats | null) {
    this.id = idFromBytes(config.id);
    this.name = config.name;
    this.priority = config.priority;
    this.enabled = config.enabled;
    this.distribution = config.distribution;
    this.stages = config.stages;
    this.destinations = config.destinations;
    this.stats = stats;
  }

  get displayLabel(): string {
    return this.name || this.id;
  }

  /** Total records matched by this route (0 when no stats overlay). */
  get recordsMatched(): bigint {
    return this.stats?.recordsMatched ?? 0n;
  }

  /** Records forwarded across nodes for this route. */
  get recordsForwarded(): bigint {
    return this.stats?.recordsForwarded ?? 0n;
  }
}
