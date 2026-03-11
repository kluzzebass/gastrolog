import { configClient } from "../client";
import { useConfigMutation } from "./useConfig";

export function usePutFilter() {
  return useConfigMutation(async (args: { id: string; name: string; expression: string }) => {
    return configClient.putFilter({
      config: { id: args.id, name: args.name, expression: args.expression },
    });
  });
}

export function useDeleteFilter() {
  return useConfigMutation(async (id: string) => {
    return configClient.deleteFilter({ id });
  });
}
