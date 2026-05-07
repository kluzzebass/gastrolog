import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";
import { decode } from "../glid";
import { RouteStage, MatchStage } from "../gen/gastrolog/v1/system_pb";

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
