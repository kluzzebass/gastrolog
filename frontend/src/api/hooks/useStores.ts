import { useQuery } from "@tanstack/react-query";
import { storeClient } from "../client";

export function useStores() {
  return useQuery({
    queryKey: ["stores"],
    queryFn: async () => {
      const response = await storeClient.listStores({});
      return response.stores;
    },
  });
}

export function useStore(id: string) {
  return useQuery({
    queryKey: ["store", id],
    queryFn: async () => {
      const response = await storeClient.getStore({ id });
      return response.store;
    },
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
    enabled: !!storeId,
  });
}

export function useStats(storeId?: string) {
  return useQuery({
    queryKey: ["stats", storeId ?? "all"],
    queryFn: async () => {
      const response = await storeClient.getStats({ store: storeId ?? "" });
      return response;
    },
    refetchInterval: 10_000,
  });
}
