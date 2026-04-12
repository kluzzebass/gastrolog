import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";

export function usePutFilter() {
  return useSystemMutation(async (args: { id: string; name: string; expression: string }) => {
    return systemClient.putFilter({
      config: { id: args.id, name: args.name, expression: args.expression },
    });
  });
}

export function useDeleteFilter() {
  return useSystemMutation(async (id: string) => {
    return systemClient.deleteFilter({ id });
  });
}
