import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { configClient } from "../client";

export function useConfig() {
  return useQuery({
    queryKey: ["config"],
    queryFn: async () => {
      const response = await configClient.getConfig({});
      return response;
    },
    refetchInterval: 10_000,
  });
}

export function usePutFilter() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { id: string; expression: string }) => {
      await configClient.putFilter({
        id: args.id,
        config: { expression: args.expression },
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function useDeleteFilter() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.deleteFilter({ id });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function usePutRotationPolicy() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      id: string;
      maxBytes: bigint;
      maxRecords: bigint;
      maxAgeSeconds: bigint;
      cron: string;
    }) => {
      await configClient.putRotationPolicy({
        id: args.id,
        config: {
          maxBytes: args.maxBytes,
          maxRecords: args.maxRecords,
          maxAgeSeconds: args.maxAgeSeconds,
          cron: args.cron,
        },
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function useDeleteRotationPolicy() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.deleteRotationPolicy({ id });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function usePutRetentionPolicy() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      id: string;
      maxAgeSeconds: bigint;
      maxBytes: bigint;
      maxChunks: bigint;
    }) => {
      await configClient.putRetentionPolicy({
        id: args.id,
        config: {
          maxAgeSeconds: args.maxAgeSeconds,
          maxBytes: args.maxBytes,
          maxChunks: args.maxChunks,
        },
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function useDeleteRetentionPolicy() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.deleteRetentionPolicy({ id });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function usePutStore() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      id: string;
      type: string;
      filter: string;
      policy: string;
      retention: string;
      params: Record<string, string>;
    }) => {
      await configClient.putStore({
        config: {
          id: args.id,
          type: args.type,
          filter: args.filter,
          policy: args.policy,
          retention: args.retention,
          params: args.params,
        },
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["config"] });
      qc.invalidateQueries({ queryKey: ["stores"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

export function useDeleteStore() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.deleteStore({ id });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["config"] });
      qc.invalidateQueries({ queryKey: ["stores"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

export function usePutIngester() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      id: string;
      type: string;
      params: Record<string, string>;
    }) => {
      await configClient.putIngester({
        config: {
          id: args.id,
          type: args.type,
          params: args.params,
        },
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function useDeleteIngester() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.deleteIngester({ id });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}
