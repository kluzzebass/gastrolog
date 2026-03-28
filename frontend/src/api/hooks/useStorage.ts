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
      storageClass: string;
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
          storageClass: args.storageClass,
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
      areas: {
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
          areas: args.areas,
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
    async (args: { id: string }) => {
      return configClient.deleteTier({ id: args.id });
    },
    [["vaults"], ["stats"]],
  );
}
