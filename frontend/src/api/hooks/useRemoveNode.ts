import { useMutation, useQueryClient } from "@tanstack/react-query";
import { lifecycleClient } from "../client";
import { encodeString } from "../glid";

export function useRemoveNode() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { nodeId: string }) => {
      await lifecycleClient.removeNode({ nodeId: encodeString(args.nodeId) });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["clusterStatus"] });
      qc.invalidateQueries({ queryKey: ["system"] });
    },
  });
}
