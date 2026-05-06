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
 * useChunks returns the full chunk list for a vault. Discrete metadata
 * changes (seal, delete, compress) arrive instantly via the WatchChunks
 * stream which invalidates the ["chunks"] cache. Active chunk stats
 * (record count, bytes) are kept fresh by a lightweight 5-second poll
 * that fetches only unsealed chunks from the local node (no fan-out)
 * and merges them into the full cache by ID replacement.
 *
 * Net effect: instant updates for operational events, 5-second lag for
 * gradual active-chunk growth, and dramatically less network traffic
 * than polling the full fan-out list every 5 seconds.
 *
 * See gastrolog-1jijm.
 */
export function useChunks(vaultId: string) {
  // Full chunk list: stream-driven invalidation, no polling.
  //
  // WatchChunks pushes notifications on lifecycle events (seal / create /
  // delete / compress / upload) AND on mid-chunk growth (a 1 Hz ticker on
  // the backend fires NotifyChunkChange() when any active chunk's record
  // count has advanced — see gastrolog-4y03v). The previous separate
  // 5-second active-chunks poll is no longer needed.
  return useQuery({
    queryKey: ["chunks", vaultId],
    queryFn: async () => {
      const response = await vaultClient.listChunks({ vault: vaultId });
      return response.chunks;
    },
    structuralSharing: protoArraySharing(ChunkMeta.equals),
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
    mutationFn: async (args: { vaultId: string; chunkId: string; restoreTier?: string; restoreDays?: number }) => {
      await vaultClient.restoreChunk({
        vault: args.vaultId,
        chunkId: decode(args.chunkId),
        restoreTier: args.restoreTier ?? "Standard",
        restoreDays: args.restoreDays ?? 7,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["chunks"] });
    },
  });
}

