import { useQuery, useMutation } from "@tanstack/react-query";
import { systemClient } from "../client";
import { IngesterConfig, GetIngesterStatusResponse } from "../gen/gastrolog/v1/system_pb";
import { protoSharing, protoArraySharing } from "./protoSharing";
import { useSystemMutation } from "./useSystem";
import { decode, encodeString } from "../glid";

export function useIngesters() {
  return useQuery({
    queryKey: ["ingesters"],
    queryFn: async () => {
      const response = await systemClient.listIngesters({});
      return response.ingesters;
    },
    structuralSharing: protoArraySharing(IngesterConfig.equals),
    staleTime: 60_000, // push-invalidated by WatchConfig on config changes
  });
}

export function useIngesterStatus(id: string) {
  return useQuery({
    queryKey: ["ingester-status", id],
    queryFn: async () => {
      const response = await systemClient.getIngesterStatus({ id: decode(id) });
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
  return useSystemMutation(
    async (args: {
      id: string;
      name: string;
      type: string;
      enabled: boolean;
      params: Record<string, string>;
      nodeIds?: string[];
      singleton?: boolean;
    }) => {
      return systemClient.putIngester({
        config: {
          id: decode(args.id),
          name: args.name,
          type: args.type,
          enabled: args.enabled,
          params: stripEmptyParams(args.params),
          nodeIds: (args.nodeIds ?? []).map(encodeString),
          singleton: args.singleton ?? false,
        },
      });
    },
    [["ingesters"]],
  );
}

export function useDeleteIngester() {
  return useSystemMutation(
    async (id: string) => {
      return systemClient.deleteIngester({ id: decode(id) });
    },
    [["ingesters"]],
  );
}

export function useTestIngester() {
  return useMutation({
    mutationFn: async (args: { type: string; params: Record<string, string>; id?: string }) => {
      const response = await systemClient.testIngester({
        type: args.type,
        params: stripEmptyParams(args.params),
        id: args.id ? decode(args.id) : new Uint8Array(16),
      });
      return response;
    },
  });
}

/**
 * Auto-checks listen address availability for listener ingesters.
 * Calls TestIngester on the server with debounce whenever params change.
 */
export function useCheckListenAddrs(type: string, params: Record<string, string>, id: string) {
  const LISTENER_TYPES = new Set(["syslog", "http", "fluentfwd", "otlp", "relp"]);
  const isListener = LISTENER_TYPES.has(type);

  // Build a stable key from the address-relevant params.
  const stripped = stripEmptyParams(params);
  const paramKey = isListener ? JSON.stringify(stripped) : "";

  return useQuery({
    queryKey: ["checkListenAddrs", type, paramKey, id],
    queryFn: async () => {
      const response = await systemClient.testIngester({
        type,
        params: stripped,
        id: id ? decode(id) : new Uint8Array(16),
      });
      return response;
    },
    enabled: isListener && paramKey !== "{}",
    staleTime: 5_000,
    gcTime: 10_000,
  });
}
