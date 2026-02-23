import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { configClient } from "../client";

export function useIngesters() {
  return useQuery({
    queryKey: ["ingesters"],
    queryFn: async () => {
      const response = await configClient.listIngesters({});
      return response.ingesters;
    },
    refetchInterval: 10_000,
  });
}

export function useIngesterStatus(id: string) {
  return useQuery({
    queryKey: ["ingester-status", id],
    queryFn: async () => {
      const response = await configClient.getIngesterStatus({ id });
      return response;
    },
    enabled: !!id,
    refetchInterval: 5_000,
  });
}

/** Strip empty-string values from params so the backend treats them as unset. */
function stripEmptyParams(params: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(params)) {
    if (v !== "") out[k] = v;
  }
  return out;
}

export function usePutIngester() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      id: string;
      name: string;
      type: string;
      enabled: boolean;
      params: Record<string, string>;
    }) => {
      await configClient.putIngester({
        config: {
          id: args.id,
          name: args.name,
          type: args.type,
          enabled: args.enabled,
          params: stripEmptyParams(args.params),
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

export function useTestIngester() {
  return useMutation({
    mutationFn: async (args: { type: string; params: Record<string, string> }) => {
      const response = await configClient.testIngester({
        type: args.type,
        params: stripEmptyParams(args.params),
      });
      return response;
    },
  });
}
