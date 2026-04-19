import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { systemClient } from "../client";

export function useSavedQueries() {
  return useQuery({
    queryKey: ["savedQueries"],
    queryFn: async () => {
      const response = await systemClient.getSavedQueries({});
      return response.queries;
    },
  });
}

export function usePutSavedQuery() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { name: string; query: string }) => {
      return systemClient.putSavedQuery({
        query: { name: args.name, query: args.query },
      });
    },
    onSuccess: (res) => {
      qc.setQueryData(["savedQueries"], res.savedQueries?.queries ?? []);
    },
  });
}

export function useDeleteSavedQuery() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (name: string) => {
      return systemClient.deleteSavedQuery({ name });
    },
    onSuccess: (res) => {
      qc.setQueryData(["savedQueries"], res.savedQueries?.queries ?? []);
    },
  });
}
