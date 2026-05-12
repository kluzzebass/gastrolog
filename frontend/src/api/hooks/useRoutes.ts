import { systemClient } from "../client";
import { useSystemMutation, useConfig } from "./useSystem";
import { useRouteStats } from "./useRouteStats";
import { decode } from "../glid";
import { RouteStage, MatchStage } from "../gen/gastrolog/v1/system_pb";
import type { PerRouteStats } from "../gen/gastrolog/v1/system_pb";
import { Route } from "../model/route";
import { type EntityID, idFromBytes } from "../model/id";

/**
 * Returns the cluster's routes as model instances, joined with their
 * runtime stats overlay. Derives from cached GetSystem config +
 * GetRouteStats; no separate cache key.
 */
export function useRoutes(): Route[] {
  const { data: config } = useConfig();
  const { data: stats } = useRouteStats();

  const statsByRouteId = new Map<EntityID, PerRouteStats>();
  for (const rs of stats?.routeStats ?? []) {
    statsByRouteId.set(idFromBytes(rs.routeId), rs);
  }

  return (config?.routes ?? []).map((cfg) => new Route(cfg, statsByRouteId.get(idFromBytes(cfg.id)) ?? null));
}

export function usePutRoute() {
  return useSystemMutation(
    async (args: {
      id: string;
      name: string;
      priority: number;
      expression: string;
      destinations: string[];
      distribution: string;
      enabled: boolean;
    }) => {
      const stages: RouteStage[] = [
        new RouteStage({
          stage: {
            case: "match",
            value: new MatchStage({ expression: args.expression }),
          },
        }),
      ];
      return systemClient.putRoute({
        config: {
          id: decode(args.id),
          name: args.name,
          priority: args.priority,
          stages,
          destinations: args.destinations.map((vaultId) => ({ vaultId: decode(vaultId) })),
          distribution: args.distribution,
          enabled: args.enabled,
        },
      });
    },
  );
}

export function useDeleteRoute() {
  return useSystemMutation(async (id: string) => {
    return systemClient.deleteRoute({ id: decode(id) });
  });
}
