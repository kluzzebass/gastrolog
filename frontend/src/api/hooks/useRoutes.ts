import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";
import { decode } from "../glid";

export function usePutRoute() {
  return useSystemMutation(
    async (args: {
      id: string;
      name: string;
      filterId: string;
      destinations: string[];
      distribution: string;
      enabled: boolean;
      ejectOnly: boolean;
    }) => {
      return systemClient.putRoute({
        config: {
          id: decode(args.id),
          name: args.name,
          filterId: decode(args.filterId),
          destinations: args.destinations.map((vaultId) => ({ vaultId: decode(vaultId) })),
          distribution: args.distribution,
          enabled: args.enabled,
          ejectOnly: args.ejectOnly,
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
