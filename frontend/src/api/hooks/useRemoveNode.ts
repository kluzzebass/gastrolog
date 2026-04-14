import { useMutation, useQueryClient } from "@tanstack/react-query";
import { lifecycleClient } from "../client";
import { decode } from "../glid";

export function useRemoveNode() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { nodeId: string }) => {
      await lifecycleClient.removeNode({ nodeId: decode(args.nodeId) });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["clusterStatus"] });
      qc.invalidateQueries({ queryKey: ["system"] });
    },
  });
}
