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
      await systemClient.putSavedQuery({
        query: { name: args.name, query: args.query },
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["savedQueries"] }),
  });
}

export function useDeleteSavedQuery() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (name: string) => {
      await systemClient.deleteSavedQuery({ name });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["savedQueries"] }),
  });
}
