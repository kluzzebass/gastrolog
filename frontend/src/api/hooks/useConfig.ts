import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { configClient } from "../client";
import { GetConfigResponse } from "../gen/gastrolog/v1/config_pb";
import { protoSharing } from "./protoSharing";

/**
 * Config version tracking for cache coherence.
 *
 * Every config response (GetConfig, mutations, WatchConfig) carries a
 * monotonically increasing config_version (Raft log index). The frontend
 * tracks the highest version it has seen from authoritative sources
 * (mutation responses and setQueryData). WatchConfig only invalidates
 * the config cache when its version exceeds the cached version — no
 * timers, no races.
 */
let cachedConfigVersion = 0n;

/** Update the cached version. Only advances forward (max wins). */
export function setConfigVersion(v: bigint) {
  if (v > cachedConfigVersion) cachedConfigVersion = v;
}

/** Read the current cached version for comparison by WatchConfig. */
export function getConfigVersion(): bigint {
  return cachedConfigVersion;
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
    onSuccess: (result: TResult) => {
      const cfg = result != null && typeof result === "object" && "config" in result
        ? (result as { config?: GetConfigResponse }).config
        : undefined;
      if (cfg) {
        setConfigVersion(cfg.configVersion);
        qc.cancelQueries({ queryKey: ["config"] });
        qc.setQueryData(["config"], cfg);
      }
      // Invalidate all data-dependent caches. Any mutation can affect
      // config, vault stats, chunk lists, and settings.
      qc.invalidateQueries({ queryKey: ["config"] });
      qc.invalidateQueries({ queryKey: ["vaults"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
      qc.invalidateQueries({ queryKey: ["chunks"] });
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
      setConfigVersion(response.configVersion);
      return response;
    },
    structuralSharing: protoSharing(GetConfigResponse.equals),
    staleTime: 60_000, // safety net only; mutations set data directly, WatchConfig handles push
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
