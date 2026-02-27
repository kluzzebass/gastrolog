import { useQuery } from "@tanstack/react-query";
import { lifecycleClient } from "../client";
import { GetClusterStatusResponse } from "../gen/gastrolog/v1/lifecycle_pb";
import { protoSharing } from "./protoSharing";

export function useClusterStatus() {
  return useQuery({
    queryKey: ["clusterStatus"],
    queryFn: async () => {
      const response = await lifecycleClient.getClusterStatus({});
      return response;
    },
    structuralSharing: protoSharing(GetClusterStatusResponse.equals),
    refetchInterval: 5_000,
  });
}
