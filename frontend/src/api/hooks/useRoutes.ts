import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";
import { decode } from "../glid";
import { RouteSource } from "../gen/gastrolog/v1/system_pb";

export function usePutRoute() {
  return useSystemMutation(
    async (args: {
      id: string;
      name: string;
      filterId: string;
      destinations: string[];
      distribution: string;
      enabled: boolean;
      sources: RouteSource[];
      sourceVaultIds: string[];
      sourceIngesterIds: string[];
    }) => {
      return systemClient.putRoute({
        config: {
          id: decode(args.id),
          name: args.name,
          filterId: decode(args.filterId),
          destinations: args.destinations.map((vaultId) => ({ vaultId: decode(vaultId) })),
          distribution: args.distribution,
          enabled: args.enabled,
          sources: args.sources,
          sourceVaultIds: args.sourceVaultIds.map(decode),
          sourceIngesterIds: args.sourceIngesterIds.map(decode),
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
