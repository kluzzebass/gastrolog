import { useMutation, useQueryClient } from "@tanstack/react-query";
import { lifecycleClient } from "../client";

export function useRemoveNode() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { nodeId: string }) => {
      await lifecycleClient.removeNode(args);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["clusterStatus"] });
      qc.invalidateQueries({ queryKey: ["system"] });
    },
  });
}
