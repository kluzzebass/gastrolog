import { useQuery } from "@tanstack/react-query";
import { configClient } from "../client";

export type IngesterDefaults = Record<string, Record<string, string>>;

export function useIngesterDefaults() {
  return useQuery({
    queryKey: ["ingesterDefaults"],
    queryFn: async (): Promise<IngesterDefaults> => {
      const response = await configClient.getIngesterDefaults({});
      const result: IngesterDefaults = {};
      for (const [type, defaults] of Object.entries(response.types)) {
        result[type] = defaults.params;
      }
      return result;
    },
    staleTime: Infinity, // Never refetch — defaults don't change at runtime.
  });
}
