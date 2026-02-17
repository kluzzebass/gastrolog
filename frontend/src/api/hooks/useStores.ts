import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Timestamp } from "@bufbuild/protobuf";
import { storeClient, configClient } from "../client";

export function useStores() {
  return useQuery({
    queryKey: ["stores"],
    queryFn: async () => {
      const response = await storeClient.listStores({});
      return response.stores;
    },
    staleTime: 0,
    refetchInterval: 10_000,
  });
}

export function useStore(id: string) {
  return useQuery({
    queryKey: ["store", id],
    queryFn: async () => {
      const response = await storeClient.getStore({ id });
      return response.store;
    },
    staleTime: 0,
    enabled: !!id,
  });
}

export function useChunks(storeId: string) {
  return useQuery({
    queryKey: ["chunks", storeId],
    queryFn: async () => {
      const response = await storeClient.listChunks({ store: storeId });
      return response.chunks;
    },
    staleTime: 0,
    enabled: !!storeId,
    refetchInterval: 10_000,
  });
}

export function useIndexes(storeId: string, chunkId: string) {
  return useQuery({
    queryKey: ["indexes", storeId, chunkId],
    queryFn: async () => {
      const response = await storeClient.getIndexes({
        store: storeId,
        chunkId,
      });
      return response;
    },
    enabled: !!storeId && !!chunkId,
  });
}

export function useStats(storeId?: string) {
  return useQuery({
    queryKey: ["stats", storeId ?? "all"],
    queryFn: async () => {
      const response = await storeClient.getStats({ store: storeId ?? "" });
      return response;
    },
    staleTime: 0,
    refetchInterval: 10_000,
  });
}

export function useSealStore() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (store: string) => {
      await storeClient.sealStore({ store });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["stores"] });
      qc.invalidateQueries({ queryKey: ["chunks"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

export function useReindexStore() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (store: string) => {
      const response = await storeClient.reindexStore({ store });
      return response;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["stores"] });
      qc.invalidateQueries({ queryKey: ["indexes"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

export function useValidateStore() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (store: string) => {
      const response = await storeClient.validateStore({ store });
      return response;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["stores"] });
    },
  });
}

export function useMigrateStore() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      source: string;
      destination: string;
      destinationType?: string;
      destinationParams?: Record<string, string>;
    }) => {
      const response = await storeClient.migrateStore({
        source: args.source,
        destination: args.destination,
        destinationType: args.destinationType ?? "",
        destinationParams: args.destinationParams ?? {},
      });
      return response;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["stores"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
      qc.invalidateQueries({ queryKey: ["config"] });
    },
  });
}

export function useMergeStores() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { source: string; destination: string }) => {
      const response = await storeClient.mergeStores({
        source: args.source,
        destination: args.destination,
      });
      return response;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["stores"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
      qc.invalidateQueries({ queryKey: ["config"] });
    },
  });
}

export function useImportRecords() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      store: string;
      records: Array<{
        sourceTs?: Date;
        ingestTs?: Date;
        attrs?: Record<string, string>;
        raw: Uint8Array;
      }>;
    }) => {
      const response = await storeClient.importRecords({
        store: args.store,
        records: args.records.map((r) => ({
          raw: r.raw as Uint8Array<ArrayBuffer>,
          attrs: r.attrs,
          sourceTs: r.sourceTs ? Timestamp.fromDate(r.sourceTs) : undefined,
          ingestTs: r.ingestTs ? Timestamp.fromDate(r.ingestTs) : undefined,
        })),
      });
      return response;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["stores"] });
      qc.invalidateQueries({ queryKey: ["chunks"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

export function usePutStore() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      id: string;
      name: string;
      type: string;
      filter: string;
      policy: string;
      retention: string;
      params: Record<string, string>;
      enabled?: boolean;
    }) => {
      await configClient.putStore({
        config: {
          id: args.id,
          name: args.name,
          type: args.type,
          filter: args.filter,
          policy: args.policy,
          retention: args.retention,
          params: args.params,
          enabled: args.enabled ?? true,
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
    mutationFn: async (args: { id: string; force?: boolean }) => {
      await configClient.deleteStore({ id: args.id, force: args.force ?? false });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["config"] });
      qc.invalidateQueries({ queryKey: ["stores"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

export function usePauseStore() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.pauseStore({ id });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["config"] });
      qc.invalidateQueries({ queryKey: ["stores"] });
    },
  });
}

export function useResumeStore() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.resumeStore({ id });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["config"] });
      qc.invalidateQueries({ queryKey: ["stores"] });
    },
  });
}
