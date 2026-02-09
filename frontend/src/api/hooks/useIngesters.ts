import { useQuery } from "@tanstack/react-query";
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
