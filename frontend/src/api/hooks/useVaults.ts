import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { vaultClient, configClient } from "../client";
import type { RetentionRule } from "../gen/gastrolog/v1/config_pb";
import { VaultInfo, ChunkMeta, GetStatsResponse } from "../gen/gastrolog/v1/vault_pb";
import { protoSharing, protoArraySharing } from "./protoSharing";
import { useConfigMutation } from "./useConfig";

export function useVaults() {
  return useQuery({
    queryKey: ["vaults"],
    queryFn: async () => {
      const response = await vaultClient.listVaults({});
      return response.vaults;
    },
    structuralSharing: protoArraySharing(VaultInfo.equals),
    refetchInterval: 10_000,
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

export function useChunks(vaultId: string) {
  return useQuery({
    queryKey: ["chunks", vaultId],
    queryFn: async () => {
      const response = await vaultClient.listChunks({ vault: vaultId });
      return response.chunks;
    },
    structuralSharing: protoArraySharing(ChunkMeta.equals),
    enabled: !!vaultId,
    refetchInterval: 10_000,
  });
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
    refetchInterval: 10_000,
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
      qc.invalidateQueries({ queryKey: ["config"] });
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
      qc.invalidateQueries({ queryKey: ["config"] });
    },
  });
}

export function usePutVault() {
  return useConfigMutation(
    async (args: {
      id: string;
      name: string;
      type: string;
      policy: string;
      retentionRules: Partial<RetentionRule>[];
      params: Record<string, string>;
      enabled?: boolean;
      nodeId?: string;
    }) => {
      return configClient.putVault({
        config: {
          id: args.id,
          name: args.name,
          type: args.type,
          policy: args.policy,
          retentionRules: args.retentionRules,
          params: args.params,
          enabled: args.enabled ?? true,
          nodeId: args.nodeId ?? "",
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

export function useTestVault() {
  return useMutation({
    mutationFn: async (args: { type: string; params: Record<string, string> }) => {
      const response = await configClient.testVault({
        type: args.type,
        params: stripEmptyParams(args.params),
      });
      return response;
    },
  });
}

export function useDeleteVault() {
  return useConfigMutation(
    async (args: { id: string; force?: boolean; deleteData?: boolean }) => {
      return configClient.deleteVault({ id: args.id, force: args.force ?? false, deleteData: args.deleteData ?? false });
    },
    [["vaults"], ["stats"]],
  );
}

