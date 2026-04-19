import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { systemClient, getToken } from "../client";

export function usePreferences() {
  return useQuery({
    queryKey: ["preferences"],
    queryFn: async () => {
      const response = await systemClient.getPreferences({});
      return response;
    },
    enabled: !!getToken(),
  });
}

export function usePutPreferences() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { theme: string; syntaxHighlight: string; palette: string }) => {
      return systemClient.putPreferences({
        theme: args.theme,
        syntaxHighlight: args.syntaxHighlight,
        palette: args.palette,
      });
    },
    onSuccess: (res) => {
      if (res.preferences) qc.setQueryData(["preferences"], res.preferences);
    },
  });
}
