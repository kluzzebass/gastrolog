import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";
import type { CloudService } from "../gen/gastrolog/v1/storage_pb";
import type { NodeStorageConfig } from "../gen/gastrolog/v1/storage_pb";
import { decode, encodeString } from "../glid";

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
      transitions?: Array<{ after: string; cloudStorageClass: string }>;
      restoreSpeed?: string;
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
            cloudStorageClass: t.cloudStorageClass,
          })),
          restoreSpeed: args.restoreSpeed ?? "",
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
        diskFreeWarn: string;
        diskFreeFloor: string;
      }[];
    }) => {
      return systemClient.setNodeStorageConfig({
        config: {
          nodeId: encodeString(args.nodeId),
          fileStorages: args.fileStorages.map((fs) => ({
            ...fs,
            id: decode(fs.id),
          })),
        } as NodeStorageConfig,
      });
    },
    // Invalidate the storage entity list (ListStorages, keyed ["storages"])
    // — SetNodeStorageConfig can add, edit, or remove file storages, and
    // without this the inspector's storage cards only refresh on the next
    // WatchSystemStatus push, leaving a just-deleted storage's card
    // stranded until then (gastrolog-3cobq4 review).
    [["storages"]],
  );
}

