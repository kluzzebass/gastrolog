import { configClient } from "../client";
import { useConfigMutation } from "./useConfig";

export function usePutRetentionPolicy() {
  return useConfigMutation(
    async (args: {
      id: string;
      name: string;
      maxAgeSeconds: bigint;
      maxBytes: bigint;
      maxChunks: bigint;
    }) => {
      await configClient.putRetentionPolicy({
        config: {
          id: args.id,
          name: args.name,
          maxAgeSeconds: args.maxAgeSeconds,
          maxBytes: args.maxBytes,
          maxChunks: args.maxChunks,
        },
      });
    },
  );
}

export function useDeleteRetentionPolicy() {
  return useConfigMutation(async (id: string) => {
    await configClient.deleteRetentionPolicy({ id });
  });
}
