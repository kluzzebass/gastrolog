import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { configClient, getToken } from "../client";

export function usePreferences() {
  return useQuery({
    queryKey: ["preferences"],
    queryFn: async () => {
      const response = await configClient.getPreferences({});
      return response;
    },
    enabled: !!getToken(),
  });
}

export function usePutPreferences() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { theme: string; syntaxHighlight: string; palette: string }) => {
      await configClient.putPreferences({ theme: args.theme, syntaxHighlight: args.syntaxHighlight, palette: args.palette });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["preferences"] }),
  });
}
