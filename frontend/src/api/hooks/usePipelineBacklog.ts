import { useQuery, useQueryClient } from "@tanstack/react-query";
import { vaultClient } from "../client";
import { VaultPipelineBacklog, ContributionReport } from "../gen/gastrolog/v1/vault_pb";
import { protoSharing } from "./protoSharing";

export function usePipelineBacklog(vaultId: string) {
  const qc = useQueryClient();
  return useQuery({
    queryKey: ["pipeline-backlog", vaultId],
    queryFn: async () => {
      const response = await vaultClient.getPipelineBacklog({ vault: vaultId });
      // Stash the fan-out contribution report so the pipeline view can flag
      // cluster-wide segment totals that omit an unreachable node. Absent
      // report clears any stale flag — quiet-until-needed.
      qc.setQueryData<ContributionReport | null>(
        ["pipeline-backlog-contribution", vaultId],
        response.contributionReport ?? null,
      );
      return response.backlog;
    },
    enabled: vaultId.length > 0,
    structuralSharing: protoSharing(VaultPipelineBacklog.equals),
    staleTime: 60_000, // push-updated via WatchSystemStatus; polling is a safety net only
  });
}

/**
 * usePipelineBacklogContribution exposes the most recent GetPipelineBacklog
 * fan-out contribution report for a vault, or null when the last cross-node
 * segment-count merge reached every peer. Written as a side-effect of
 * usePipelineBacklog; this reader subscribes to the sibling cache key so the
 * pipeline view re-renders when the merge degrades or recovers.
 */
export function usePipelineBacklogContribution(vaultId: string): ContributionReport | null {
  const qc = useQueryClient();
  return (
    useQuery({
      queryKey: ["pipeline-backlog-contribution", vaultId],
      queryFn: () =>
        qc.getQueryData<ContributionReport | null>(["pipeline-backlog-contribution", vaultId]) ?? null,
      enabled: vaultId.length > 0,
      staleTime: Infinity,
    }).data ?? null
  );
}
