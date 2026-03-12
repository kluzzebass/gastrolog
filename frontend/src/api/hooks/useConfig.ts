import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { configClient } from "../client";
import { GetConfigResponse } from "../gen/gastrolog/v1/config_pb";
import { protoSharing } from "./protoSharing";

/**
 * Mutation-based config writes set data directly into the query cache via
 * setQueryData. The WatchConfig stream must NOT invalidate the config query
 * while a mutation's authoritative data is still fresh — otherwise a refetch
 * can hit a stale Raft follower and overwrite the correct value.
 *
 * suppressConfigInvalidation is a simple counter: incremented before
 * setQueryData, decremented after a delay. WatchConfig skips config
 * invalidation while the counter is >0.
 */
let suppressConfigInvalidation = 0;

/** Called by useConfigMutation before setting query data. */
export function beginConfigSuppression() {
  suppressConfigInvalidation++;
  // Clear after a generous window — follower lag should be well under 10s.
  setTimeout(() => { suppressConfigInvalidation--; }, 10_000);
}

/** Called by useWatchConfig to check whether to skip config invalidation. */
export function isConfigSuppressed(): boolean {
  return suppressConfigInvalidation > 0;
}

/**
 * Factory that eliminates the useQueryClient + onSuccess boilerplate for config mutations.
 *
 * When the mutation response carries a `config` field (all Put/Delete RPCs now do),
 * we write it directly into the query cache — bypassing the Raft follower-lag race
 * that caused stale reads with invalidateQueries.
 *
 * Extra invalidateKeys (e.g. ["settings"], ["certificates"]) are still fired
 * for non-config caches that need refreshing.
 */
export function useConfigMutation<TArgs, TResult>(
  fn: (args: TArgs) => Promise<TResult>,
  extraInvalidateKeys: string[][] = [],
) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: fn,
    onMutate: () => {
      // Suppress BEFORE the request fires — WatchConfig can deliver the
      // Raft-committed notification before the HTTP response arrives, and
      // its refetch would hit a stale follower, overwriting setQueryData.
      beginConfigSuppression();
    },
    onSuccess: (result: TResult) => {
      const cfg = result != null && typeof result === "object" && "config" in result
        ? (result as { config?: GetConfigResponse }).config
        : undefined;
      if (cfg) {
        qc.cancelQueries({ queryKey: ["config"] });
        qc.setQueryData(["config"], cfg);
      } else {
        qc.invalidateQueries({ queryKey: ["config"] });
      }
      for (const key of extraInvalidateKeys) {
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
    staleTime: 5_000, // short safety net; mutations now set data directly
  });
}

export function usePutNodeConfig() {
  return useConfigMutation(
    async (args: { id: string; name: string }) => {
      return configClient.putNodeConfig({ config: { id: args.id, name: args.name } });
    },
    [["settings"]],
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
