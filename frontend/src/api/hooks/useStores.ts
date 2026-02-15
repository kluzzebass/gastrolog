import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { storeClient } from "../client";

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
      const response = await storeClient.importRecords(args);
      return response;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["stores"] });
      qc.invalidateQueries({ queryKey: ["chunks"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}
