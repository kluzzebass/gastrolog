import { useMutation, useQueryClient } from "@tanstack/react-query";
import { lifecycleClient } from "../client";
import { encodeString } from "../glid";

export function useSetNodeSuffrage() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async ({ nodeId, voter }: { nodeId: string; voter: boolean }) => {
      await lifecycleClient.setNodeSuffrage({ nodeId: encodeString(nodeId), voter });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["clusterStatus"] });
    },
  });
}
