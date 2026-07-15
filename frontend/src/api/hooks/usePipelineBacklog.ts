import { useQuery } from "@tanstack/react-query";
import { vaultClient } from "../client";
import { VaultPipelineBacklog } from "../gen/gastrolog/v1/vault_pb";
import { protoSharing } from "./protoSharing";

export function usePipelineBacklog(vaultId: string) {
  return useQuery({
    queryKey: ["pipeline-backlog", vaultId],
    queryFn: async () => {
      const response = await vaultClient.getPipelineBacklog({ vault: vaultId });
      return response.backlog;
    },
    enabled: vaultId.length > 0,
    structuralSharing: protoSharing(VaultPipelineBacklog.equals),
    staleTime: 60_000, // push-updated via WatchSystemStatus; polling is a safety net only
  });
}
