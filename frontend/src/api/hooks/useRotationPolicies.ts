import { configClient } from "../client";
import { useConfigMutation } from "./useConfig";

export function usePutRotationPolicy() {
  return useConfigMutation(
    async (args: {
      id: string;
      name: string;
      maxBytes: bigint;
      maxRecords: bigint;
      maxAgeSeconds: bigint;
      cron: string;
    }) => {
      await configClient.putRotationPolicy({
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
  return useConfigMutation(async (id: string) => {
    await configClient.deleteRotationPolicy({ id });
  });
}
