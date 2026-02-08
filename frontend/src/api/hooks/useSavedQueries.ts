import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { configClient } from "../client";

export function useSavedQueries() {
  return useQuery({
    queryKey: ["savedQueries"],
    queryFn: async () => {
      const response = await configClient.getSavedQueries({});
      return response.queries;
    },
  });
}

export function usePutSavedQuery() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { name: string; query: string }) => {
      await configClient.putSavedQuery({
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
      await configClient.deleteSavedQuery({ name });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["savedQueries"] }),
  });
}
