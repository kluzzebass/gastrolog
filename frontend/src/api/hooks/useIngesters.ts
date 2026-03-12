import { useQuery, useMutation } from "@tanstack/react-query";
import { configClient } from "../client";
import { IngesterConfig, GetIngesterStatusResponse } from "../gen/gastrolog/v1/config_pb";
import { protoSharing, protoArraySharing } from "./protoSharing";
import { useConfigMutation } from "./useConfig";

export function useIngesters() {
  return useQuery({
    queryKey: ["ingesters"],
    queryFn: async () => {
      const response = await configClient.listIngesters({});
      return response.ingesters;
    },
    structuralSharing: protoArraySharing(IngesterConfig.equals),
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
    structuralSharing: protoSharing(GetIngesterStatusResponse.equals),
    enabled: !!id,
    refetchInterval: 5_000,
  });
}

/** Trim whitespace and strip empty values so the backend treats them as unset. */
function stripEmptyParams(params: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(params)) {
    const trimmed = v.trim();
    if (trimmed !== "") out[k] = trimmed;
  }
  return out;
}

export function usePutIngester() {
  return useConfigMutation(
    async (args: {
      id: string;
      name: string;
      type: string;
      enabled: boolean;
      params: Record<string, string>;
      nodeId?: string;
    }) => {
      return configClient.putIngester({
        config: {
          id: args.id,
          name: args.name,
          type: args.type,
          enabled: args.enabled,
          params: stripEmptyParams(args.params),
          nodeId: args.nodeId ?? "",
        },
      });
    },
  );
}

export function useDeleteIngester() {
  return useConfigMutation(
    async (id: string) => {
      return configClient.deleteIngester({ id });
    },
  );
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
