import { useQuery } from "@tanstack/react-query";
import { systemClient } from "../client";
import { GetRouteStatsResponse } from "../gen/gastrolog/v1/system_pb";
import { protoSharing } from "./protoSharing";
export function useRouteStats() {
  return useQuery({
    queryKey: ["route-stats"],
    queryFn: async () => {
      const response = await systemClient.getRouteStats({});
      return response;
    },
    structuralSharing: protoSharing(GetRouteStatsResponse.equals),
    staleTime: 60_000, // push-updated via WatchSystemStatus; polling is a safety net only
  });
}
