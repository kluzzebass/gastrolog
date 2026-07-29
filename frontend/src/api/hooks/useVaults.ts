import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { vaultClient, systemClient } from "../client";
import { VaultInfo, ChunkMeta, GetStatsResponse, ContributionReport } from "../gen/gastrolog/v1/vault_pb";
import { VaultConfig } from "../gen/gastrolog/v1/system_pb";
import { protoSharing, protoArraySharing } from "./protoSharing";
import { useSystemMutation, useConfig } from "./useSystem";
import { mergeChunksSnapshot } from "./useWatchChunks";
import { decode } from "../glid";
import { Vault } from "../model/vault";
import { type EntityID, idFromBytes } from "../model/id";

/**
 * Returns the cluster's vaults as model instances. Joins the runtime
 * VaultInfo overlay (from ListVaults — chunk/record counts, current
 * placement nodeId) with the durable VaultConfig (from GetSystem — typed
 * enum, cloud-service binding, retention rules). Either side may be
 * missing during transient states; the Vault constructor handles both.
 */
export function useVaults(): { data: Vault[]; isLoading: boolean } {
  const { data: infos, isLoading } = useQuery({
    queryKey: ["vaults"],
    queryFn: async () => {
      const response = await vaultClient.listVaults({});
      return response.vaults;
    },
    structuralSharing: protoArraySharing(VaultInfo.equals),
    staleTime: 60_000, // push-invalidated by WatchConfig on config changes
  });
  const { data: config } = useConfig();

  const configById = new Map<EntityID, VaultConfig>();
  for (const c of config?.vaults ?? []) {
    configById.set(idFromBytes(c.id), c);
  }

  const data = (infos ?? []).map((info) => new Vault(info, configById.get(idFromBytes(info.id)) ?? null));
  return { data, isLoading };
}

export function useVault(id: string) {
  return useQuery({
    queryKey: ["vault", id],
    queryFn: async () => {
      const response = await vaultClient.getVault({ id: decode(id) });
      return response.vault;
    },
    staleTime: 0,
    enabled: !!id,
  });
}

/**
 * useChunks returns the full chunk list for a vault. The initial fetch
 * uses ListChunks (cluster fan-out + dedup); subsequent updates arrive
 * via the WatchChunks stream as typed diffs (created / progress / sealed
 * / deleted / uploaded) and patch this cache in place via setQueryData
 * — no refetch on the steady-state path. See gastrolog-3pf9w for the
 * pre-3pf9w shape and why it was replaced.
 */
export function useChunks(vaultId: string) {
  // Initial fetch only. Subsequent updates arrive via useWatchChunks,
  // which mutates the per-vault cache directly. Refetches merge with the
  // cached watch-stamped list instead of replacing it: a fan-out round
  // that misses a slow or catching-up node must not erase residency the
  // stream already established, or the seal-pip row flaps
  // (gastrolog-68wsli; see mergeChunksSnapshot).
  const qc = useQueryClient();
  return useQuery({
    queryKey: ["chunks", vaultId],
    queryFn: async () => {
      const response = await vaultClient.listChunks({ vault: vaultId });
      // Stash the fan-out contribution report so the inspector can flag a
      // partial cross-node merge (some hosting peer timed out or failed).
      // A response with every peer contributing clears any stale flag —
      // quiet-until-needed. See gastrolog-66zrj.
      qc.setQueryData<ContributionReport | null>(
        ["chunks-contribution", vaultId],
        response.contributionReport ?? null,
      );
      return mergeChunksSnapshot(
        qc.getQueryData<ChunkMeta[]>(["chunks", vaultId]),
        response.chunks,
      );
    },
    structuralSharing: protoArraySharing(ChunkMeta.equals),
    enabled: !!vaultId,
  });
}

/**
 * useChunksContribution exposes the most recent ListChunks fan-out
 * contribution report for a vault, or null when the last merge reached
 * every hosting peer. The report is written as a side-effect of
 * useChunks; this reader subscribes to the sibling cache key so the
 * inspector re-renders when the merge degrades or recovers. Only the
 * initial ListChunks fetch carries a report — steady-state chunk updates
 * arrive via the WatchChunks stream, which does not fan out. See
 * gastrolog-66zrj.
 */
export function useChunksContribution(vaultId: string): ContributionReport | null {
  const qc = useQueryClient();
  return (
    useQuery({
      queryKey: ["chunks-contribution", vaultId],
      queryFn: () =>
        qc.getQueryData<ContributionReport | null>(["chunks-contribution", vaultId]) ?? null,
      enabled: !!vaultId,
      staleTime: Infinity,
    }).data ?? null
  );
}

export function useIndexes(vaultId: string, chunkId: string) {
  return useQuery({
    queryKey: ["indexes", vaultId, chunkId],
    queryFn: async () => {
      const response = await vaultClient.getIndexes({
        vault: vaultId,
        chunkId: decode(chunkId),
      });
      return response;
    },
    enabled: !!vaultId && !!chunkId,
  });
}

export function useStats(vaultId?: string) {
  return useQuery({
    queryKey: ["stats", vaultId ?? "all"],
    queryFn: async () => {
      const response = await vaultClient.getStats({ vault: vaultId ?? "" });
      return response;
    },
    structuralSharing: protoSharing(GetStatsResponse.equals),
    staleTime: 60_000, // push-invalidated by WatchConfig on config changes
  });
}

export function useSealVault() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (vault: string) => {
      await vaultClient.sealVault({ vault });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["vaults"] });
      qc.invalidateQueries({ queryKey: ["chunks"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

export function useReindexVault() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (vault: string) => {
      const response = await vaultClient.reindexVault({ vault });
      return response;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["vaults"] });
      qc.invalidateQueries({ queryKey: ["indexes"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

// Operator-driven recovery: reset retry backoff for chunks flagged
// unreadable in this vault so the next retention sweep retries them
// immediately. Returns the count of chunks whose backoff was reset.
// See gastrolog-25vur.
export function useRetryUnreadableChunks() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (vault: string) => {
      const response = await vaultClient.retryUnreadableChunks({ vault });
      return response;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["chunks"] });
      qc.invalidateQueries({ queryKey: ["alerts"] });
    },
  });
}

export function useValidateVault() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (vault: string) => {
      const response = await vaultClient.validateVault({ vault });
      return response;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["vaults"] });
    },
  });
}

export function usePutVault() {
  return useSystemMutation(
    async (args: {
      config: VaultConfig;
    }) => {
      return systemClient.putVault({ config: args.config });
    },
    [["vaults"], ["stats"]],
  );
}

/** Trim whitespace and strip empty values so the backend treats them as unset. */
function stripEmptyParams(params: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(params)) {
    const trimmed = v.trim();
    if (trimmed !== "") out[k] = trimmed;
  }
  return out;
}

export function useTestCloudService() {
  return useMutation({
    mutationFn: async (args: { type: string; params: Record<string, string> }) => {
      const response = await systemClient.testCloudService({
        type: args.type,
        params: stripEmptyParams(args.params),
      });
      return response;
    },
  });
}

export function useDeleteVault() {
  return useSystemMutation(
    async (args: { id: string; force?: boolean; deleteData?: boolean }) => {
      return systemClient.deleteVault({ id: decode(args.id), force: args.force ?? false, deleteData: args.deleteData ?? false });
    },
    [["vaults"], ["stats"]],
  );
}

export function useArchiveChunk() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { vaultId: string; chunkId: string; cloudStorageClass?: string }) => {
      await vaultClient.archiveChunk({
        vault: args.vaultId,
        chunkId: decode(args.chunkId),
        cloudStorageClass: args.cloudStorageClass ?? "",
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["chunks"] });
    },
  });
}

export function useRestoreChunk() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { vaultId: string; chunkId: string; restoreSpeed?: string; restoreDays?: number }) => {
      await vaultClient.restoreChunk({
        vault: args.vaultId,
        chunkId: decode(args.chunkId),
        restoreSpeed: args.restoreSpeed ?? "Standard",
        restoreDays: args.restoreDays ?? 7,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["chunks"] });
    },
  });
}

