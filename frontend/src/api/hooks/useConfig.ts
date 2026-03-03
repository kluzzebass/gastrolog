import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { configClient } from "../client";
import { GetConfigResponse } from "../gen/gastrolog/v1/config_pb";
import { protoSharing } from "./protoSharing";

/** Factory that eliminates the useQueryClient + onSuccess boilerplate for config mutations. */
export function useConfigMutation<TArgs, TResult = void>(
  fn: (args: TArgs) => Promise<TResult>,
  invalidateKeys: string[][] = [["config"]],
) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: fn,
    onSuccess: () => {
      for (const key of invalidateKeys) {
        qc.invalidateQueries({ queryKey: key });
      }
    },
  });
}

export function useConfig() {
  return useQuery({
    queryKey: ["config"],
    queryFn: async () => {
      const response = await configClient.getConfig({});
      return response;
    },
    structuralSharing: protoSharing(GetConfigResponse.equals),
    staleTime: 60_000, // safety net; WatchConfig stream invalidation is primary
  });
}

export function usePutNodeConfig() {
  return useConfigMutation(
    async (args: { id: string; name: string }) => {
      await configClient.putNodeConfig({ config: { id: args.id, name: args.name } });
    },
    [["settings"], ["config"]],
  );
}

export function useGenerateName() {
  return useMutation({
    mutationFn: async () => {
      const response = await configClient.generateName({});
      return response.name;
    },
  });
}
