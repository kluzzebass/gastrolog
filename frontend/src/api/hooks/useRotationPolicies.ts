import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";

export function usePutRotationPolicy() {
  return useSystemMutation(
    async (args: {
      id: string;
      name: string;
      maxBytes: bigint;
      maxRecords: bigint;
      maxAgeSeconds: bigint;
      cron: string;
    }) => {
      return systemClient.putRotationPolicy({
        config: {
          id: args.id,
          name: args.name,
          maxBytes: args.maxBytes,
          maxRecords: args.maxRecords,
          maxAgeSeconds: args.maxAgeSeconds,
          cron: args.cron,
        },
      });
    },
  );
}

export function useDeleteRotationPolicy() {
  return useSystemMutation(async (id: string) => {
    return systemClient.deleteRotationPolicy({ id });
  });
}
