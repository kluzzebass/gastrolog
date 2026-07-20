import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";
import { decode } from "../glid";

export function usePutRetentionPolicy() {
  return useSystemMutation(
    async (args: {
      id: string;
      name: string;
      maxAge: string;
      maxSize: string;
      maxChunks: bigint;
      refuse: boolean;
    }) => {
      return systemClient.putRetentionPolicy({
        config: {
          id: decode(args.id),
          name: args.name,
          maxAge: args.maxAge,
          maxSize: args.maxSize,
          maxChunks: args.maxChunks,
          refuse: args.refuse,
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
