import { useMutation, useQueryClient } from "@tanstack/react-query";
import { lifecycleClient } from "../client";

export function useSetNodeSuffrage() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async ({ nodeId, voter }: { nodeId: string; voter: boolean }) => {
      await lifecycleClient.setNodeSuffrage({ nodeId, voter });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["clusterStatus"] });
    },
  });
}
