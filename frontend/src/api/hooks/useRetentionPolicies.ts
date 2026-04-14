import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";
import { decode } from "../glid";

export function usePutRetentionPolicy() {
  return useSystemMutation(
    async (args: {
      id: string;
      name: string;
      maxAgeSeconds: bigint;
      maxBytes: bigint;
      maxChunks: bigint;
    }) => {
      return systemClient.putRetentionPolicy({
        config: {
          id: decode(args.id),
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
  return useSystemMutation(async (id: string) => {
    return systemClient.deleteRetentionPolicy({ id: decode(id) });
  });
}
