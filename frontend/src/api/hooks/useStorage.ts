import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";
import type { CloudService } from "../gen/gastrolog/v1/storage_pb";
import type { NodeStorageConfig } from "../gen/gastrolog/v1/storage_pb";
import type { TierConfig } from "../gen/gastrolog/v1/system_pb";
import { decode } from "../glid";

export function usePutCloudService() {
  return useSystemMutation(
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
      return systemClient.putCloudService({
        config: {
          id: decode(args.id),
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
  return useSystemMutation(
    async (args: { id: string }) => {
      return systemClient.deleteCloudService({ id: decode(args.id) });
    },
    [],
  );
}

export function useSetNodeStorageConfig() {
  return useSystemMutation(
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
      return systemClient.setNodeStorageConfig({
        config: {
          nodeId: decode(args.nodeId),
          fileStorages: args.fileStorages.map((fs) => ({
            ...fs,
            id: decode(fs.id),
          })),
        } as NodeStorageConfig,
      });
    },
    [],
  );
}

export function usePutTier() {
  return useSystemMutation(
    async (args: {
      config: TierConfig;
    }) => {
      return systemClient.putTier({ config: args.config });
    },
    [["vaults"], ["stats"]],
  );
}

export function useDeleteTier() {
  return useSystemMutation(
    async (args: { id: string; drain?: boolean }) => {
      return systemClient.deleteTier({ id: decode(args.id), drain: args.drain });
    },
    [["vaults"], ["stats"]],
  );
}
