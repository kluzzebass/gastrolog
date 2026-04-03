import { configClient } from "../client";
import { useConfigMutation } from "./useConfig";
import type { CloudService } from "../gen/gastrolog/v1/storage_pb";
import type { NodeStorageConfig } from "../gen/gastrolog/v1/storage_pb";
import type { TierConfig } from "../gen/gastrolog/v1/config_pb";

export function usePutCloudService() {
  return useConfigMutation(
    async (args: {
      id: string;
      name: string;
      provider: string;
      bucket: string;
      region: string;
      endpoint: string;
      accessKey: string;
      secretKey: string;
      container: string;
      connectionString: string;
      credentialsJson: string;
      archivalMode?: string;
      transitions?: Array<{ after: string; storageClass: string }>;
      restoreTier?: string;
      restoreDays?: number;
      suspectGraceDays?: number;
      reconcileSchedule?: string;
    }) => {
      return configClient.putCloudService({
        config: {
          id: args.id,
          name: args.name,
          provider: args.provider,
          bucket: args.bucket,
          region: args.region,
          endpoint: args.endpoint,
          accessKey: args.accessKey,
          secretKey: args.secretKey,
          container: args.container,
          connectionString: args.connectionString,
          credentialsJson: args.credentialsJson,
          archivalMode: args.archivalMode ?? "",
          transitions: (args.transitions ?? []).map((t) => ({
            after: t.after,
            storageClass: t.storageClass,
          })),
          restoreTier: args.restoreTier ?? "",
          restoreDays: args.restoreDays ?? 0,
          suspectGraceDays: args.suspectGraceDays ?? 0,
          reconcileSchedule: args.reconcileSchedule ?? "",
        } as CloudService,
      });
    },
    [],
  );
}

export function useDeleteCloudService() {
  return useConfigMutation(
    async (args: { id: string }) => {
      return configClient.deleteCloudService({ id: args.id });
    },
    [],
  );
}

export function useSetNodeStorageConfig() {
  return useConfigMutation(
    async (args: {
      nodeId: string;
      fileStorages: {
        id: string;
        storageClass: number;
        name: string;
        path: string;
        memoryBudgetBytes: bigint;
      }[];
    }) => {
      return configClient.setNodeStorageConfig({
        config: {
          nodeId: args.nodeId,
          fileStorages: args.fileStorages,
        } as NodeStorageConfig,
      });
    },
    [],
  );
}

export function usePutTier() {
  return useConfigMutation(
    async (args: {
      config: TierConfig;
    }) => {
      return configClient.putTier({ config: args.config });
    },
    [["vaults"], ["stats"]],
  );
}

export function useDeleteTier() {
  return useConfigMutation(
    async (args: { id: string; drain?: boolean }) => {
      return configClient.deleteTier({ id: args.id, drain: args.drain });
    },
    [["vaults"], ["stats"]],
  );
}
