import { useMutation, useQueryClient } from "@tanstack/react-query";
import { lifecycleClient } from "../client";

export function useJoinCluster() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { leaderAddress: string; joinToken: string }) => {
      await lifecycleClient.joinCluster(args);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["clusterStatus"] });
      qc.invalidateQueries({ queryKey: ["config"] });
      qc.invalidateQueries({ queryKey: ["settings"] });
    },
  });
}
