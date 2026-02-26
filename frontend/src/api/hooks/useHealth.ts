import { useQuery } from "@tanstack/react-query";
import { lifecycleClient } from "../client";
import { HealthResponse } from "../gen/gastrolog/v1/lifecycle_pb";
import { protoSharing } from "./protoSharing";

export function useHealth() {
  return useQuery({
    queryKey: ["health"],
    queryFn: async () => {
      const response = await lifecycleClient.health({});
      return response;
    },
    structuralSharing: protoSharing(HealthResponse.equals),
    refetchInterval: 5_000,
  });
}
