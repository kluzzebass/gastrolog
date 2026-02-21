import { useQuery } from "@tanstack/react-query";
import { lifecycleClient } from "../client";

export function useHealth() {
  return useQuery({
    queryKey: ["health"],
    queryFn: async () => {
      const response = await lifecycleClient.health({});
      return response;
    },
    refetchInterval: 5_000,
  });
}
