import { configClient } from "../client";
import { useConfigMutation } from "./useConfig";

export function usePutRoute() {
  return useConfigMutation(
    async (args: {
      id: string;
      name: string;
      filterId: string;
      destinations: string[];
      distribution: string;
      enabled: boolean;
    }) => {
      return configClient.putRoute({
        config: {
          id: args.id,
          name: args.name,
          filterId: args.filterId,
          destinations: args.destinations.map((vaultId) => ({ vaultId })),
          distribution: args.distribution,
          enabled: args.enabled,
        },
      });
    },
  );
}

export function useDeleteRoute() {
  return useConfigMutation(async (id: string) => {
    return configClient.deleteRoute({ id });
  });
}
