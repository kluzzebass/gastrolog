import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { configClient } from "../client";

export function usePreferences() {
  return useQuery({
    queryKey: ["preferences"],
    queryFn: async () => {
      const response = await configClient.getPreferences({});
      return response;
    },
  });
}

export function usePutPreferences() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { theme: string }) => {
      await configClient.putPreferences({ theme: args.theme });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["preferences"] }),
  });
}
