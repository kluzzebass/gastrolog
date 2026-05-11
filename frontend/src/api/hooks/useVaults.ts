import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { vaultClient, systemClient } from "../client";
import { VaultInfo, ChunkMeta, GetStatsResponse } from "../gen/gastrolog/v1/vault_pb";
import { VaultConfig } from "../gen/gastrolog/v1/system_pb";
import { protoSharing, protoArraySharing } from "./protoSharing";
import { useSystemMutation } from "./useSystem";
import { decode } from "../glid";

export function useVaults() {
  return useQuery({
    queryKey: ["vaults"],
    queryFn: async () => {
      const response = await vaultClient.listVaults({});
      return response.vaults;
    },
    structuralSharing: protoArraySharing(VaultInfo.equals),
    staleTime: 60_000, // push-invalidated by WatchConfig on config changes
  });
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
 * uses ListChunks (cluster fan-out + dedup); subsequent lifecycle
 * updates (created / sealed / deleted / uploaded) arrive via the
 * WatchChunks stream as typed diffs and patch this cache in place via
 * setQueryData. See gastrolog-3pf9w for the pre-3pf9w shape and why it
 * was replaced.
 *
 * Active-chunk PROGRESS events fire on the bus of the node that hosts
 * the vault — but the inspector may be connected to a node that doesn't
 * host the vault (cluster RouteLocal). For that case, a slow 5-second
 * refetchInterval keeps active-chunk record counts roughly in sync
 * cross-node. On nodes that DO host the vault, the events handle
 * everything and the poll is wasted work but harmless. A future
 * improvement would push PROGRESS across nodes via the server (peer
 * stream multiplexing) — see the gastrolog-3pf9w follow-up notes.
 */
export function useChunks(vaultId: string) {
  return useQuery({
    queryKey: ["chunks", vaultId],
    queryFn: async () => {
      const response = await vaultClient.listChunks({ vault: vaultId });
      return response.chunks;
    },
    structuralSharing: protoArraySharing(ChunkMeta.equals),
    refetchInterval: 5000,
    enabled: !!vaultId,
  });
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
    mutationFn: async (args: { vaultId: string; chunkId: string; storageClass?: string }) => {
      await vaultClient.archiveChunk({
        vault: args.vaultId,
        chunkId: decode(args.chunkId),
        storageClass: args.storageClass ?? "",
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

