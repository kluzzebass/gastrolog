import { useQuery } from "@tanstack/react-query";
import { configClient } from "../client";
import { GetRouteStatsResponse } from "../gen/gastrolog/v1/config_pb";
import { protoSharing } from "./protoSharing";

export function useRouteStats() {
  return useQuery({
    queryKey: ["route-stats"],
    queryFn: async () => {
      const response = await configClient.getRouteStats({});
      return response;
    },
    structuralSharing: protoSharing(GetRouteStatsResponse.equals),
    refetchInterval: 3_000,
  });
}
