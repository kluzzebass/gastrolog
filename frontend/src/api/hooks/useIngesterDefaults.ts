import { useQuery } from "@tanstack/react-query";
import { systemClient } from "../client";
import { IngesterMode } from "../gen/gastrolog/v1/system_pb";

export type IngesterDefaults = Record<string, Record<string, string>>;
export type IngesterModes = Record<string, IngesterMode>;

export function useIngesterDefaults() {
  return useQuery({
    queryKey: ["ingesterDefaults"],
    queryFn: async (): Promise<{ defaults: IngesterDefaults; modes: IngesterModes }> => {
      const response = await systemClient.getIngesterDefaults({});
      const defaults: IngesterDefaults = {};
      const modes: IngesterModes = {};
      for (const [type, td] of Object.entries(response.types)) {
        defaults[type] = td.params;
        modes[type] = td.mode;
      }
      return { defaults, modes };
    },
    staleTime: Infinity, // Never refetch — defaults don't change at runtime.
  });
}
