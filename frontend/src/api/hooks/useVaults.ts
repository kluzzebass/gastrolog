import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { vaultClient, systemClient } from "../client";
import { VaultInfo, ChunkMeta, GetStatsResponse } from "../gen/gastrolog/v1/vault_pb";
import { protoSharing, protoArraySharing } from "./protoSharing";
import { useSystemMutation } from "./useSystem";

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
      const response = await vaultClient.getVault({ id });
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
  const qc = useQueryClient();

  // Full chunk list: stream-driven invalidation, no polling.
  const query = useQuery({
    queryKey: ["chunks", vaultId],
    queryFn: async () => {
      const response = await vaultClient.listChunks({ vault: vaultId });
      return response.chunks;
    },
    structuralSharing: protoArraySharing(ChunkMeta.equals),
    enabled: !!vaultId,
  });

  // Lightweight active-chunk poll: local-only, no fan-out. Merges
  // updated active chunks into the full cache by ID replacement so
  // the component sees a single unified array.
  useQuery({
    queryKey: ["active-chunks", vaultId],
    queryFn: async () => {
      const resp = await vaultClient.listChunks({
        vault: vaultId,
        activeOnly: true,
      });
      qc.setQueryData(
        ["chunks", vaultId],
        (old: ChunkMeta[] | undefined) => {
          if (!old) return resp.chunks;
          const freshById = new Map(resp.chunks.map((c) => [c.id, c]));
          // Merge: for chunks in both old and poll, update only the
          // fields the active-only poll is authoritative for (growing
          // stats). Preserve everything else (replica_count, compressed,
          // cloud_backed, etc.) from the full-refetch cache entry,
          // since those require the fan-out/dedup path to be accurate.
          const merged = old.map((c) => {
            const fresh = freshById.get(c.id);
            if (!fresh) return c;
            freshById.delete(c.id);
            const patched = c.clone();
            patched.recordCount = fresh.recordCount;
            patched.bytes = fresh.bytes;
            patched.diskBytes = fresh.diskBytes;
            return patched;
          });
          // Append any active chunks not in the old list (new chunk
          // created since last full refetch).
          for (const c of freshById.values()) {
            merged.push(c);
          }
          return merged;
        },
      );
      return resp.chunks;
    },
    enabled: !!vaultId,
    refetchInterval: 5_000,
  });

  return query;
}

export function useIndexes(vaultId: string, chunkId: string) {
  return useQuery({
    queryKey: ["indexes", vaultId, chunkId],
    queryFn: async () => {
      const response = await vaultClient.getIndexes({
        vault: vaultId,
        chunkId,
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

export function useMigrateVault() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      source: string;
      destination: string;
      destinationType?: string;
      destinationParams?: Record<string, string>;
    }) => {
      const response = await vaultClient.migrateVault({
        source: args.source,
        destination: args.destination,
        destinationType: args.destinationType ?? "",
        destinationParams: args.destinationParams ?? {},
      });
      return response;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["vaults"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
      qc.invalidateQueries({ queryKey: ["system"] });
    },
  });
}

export function useMergeVaults() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { source: string; destination: string }) => {
      const response = await vaultClient.mergeVaults({
        source: args.source,
        destination: args.destination,
      });
      return response;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["vaults"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
      qc.invalidateQueries({ queryKey: ["system"] });
    },
  });
}

export function usePutVault() {
  return useSystemMutation(
    async (args: {
      id: string;
      name: string;
      enabled?: boolean;
    }) => {
      return systemClient.putVault({
        config: {
          id: args.id,
          name: args.name,
          enabled: args.enabled ?? true,
        },
      });
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
      return systemClient.deleteVault({ id: args.id, force: args.force ?? false, deleteData: args.deleteData ?? false });
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
        chunkId: args.chunkId,
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
        chunkId: args.chunkId,
        restoreTier: args.restoreTier ?? "Standard",
        restoreDays: args.restoreDays ?? 7,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["chunks"] });
    },
  });
}

