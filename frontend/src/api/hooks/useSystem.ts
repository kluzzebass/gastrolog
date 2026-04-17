import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { systemClient } from "../client";
import { GetSystemResponse } from "../gen/gastrolog/v1/system_pb";
import { protoSharing } from "./protoSharing";
import { decode } from "../glid";

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
let cachedSystemVersion = 0n;

/** Update the cached version. Only advances forward (max wins). */
export function setSystemVersion(v: bigint) {
  if (v > cachedSystemVersion) cachedSystemVersion = v;
}

/** Read the current cached version for comparison by WatchConfig. */
export function getSystemVersion(): bigint {
  return cachedSystemVersion;
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
export function useSystemMutation<TArgs, TResult>(
  fn: (args: TArgs) => Promise<TResult>,
  extraInvalidateKeys: string[][] = [],
) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: fn,
    onSuccess: (result: TResult) => {
      const cfg = result != null && typeof result === "object" && "system" in result
        ? (result as { system?: GetSystemResponse }).system
        : undefined;
      if (cfg) {
        setSystemVersion(cfg.systemVersion);
        qc.cancelQueries({ queryKey: ["system"] });
        qc.setQueryData(["system"], cfg);
      } else {
        // Only invalidate config when the response didn't carry the full
        // config — otherwise the refetch can hit a stale Raft follower and
        // overwrite the correct setQueryData with old data.
        qc.invalidateQueries({ queryKey: ["system"] });
      }
      for (const key of extraInvalidateKeys) {
        qc.invalidateQueries({ queryKey: key });
      }
    },
  });
}

export function useConfig() {
  const qc = useQueryClient();
  return useQuery({
    queryKey: ["system"],
    queryFn: async () => {
      const response = await systemClient.getSystem({});
      // Reject stale refetches: if a Raft follower returns an older version
      // than what's already in the cache (from a mutation or earlier fetch),
      // keep the cached data instead of regressing.
      const cached = qc.getQueryData<GetSystemResponse>(["system"]);
      if (cached && response.systemVersion < cached.systemVersion) {
        return cached;
      }
      setSystemVersion(response.systemVersion);
      return response;
    },
    structuralSharing: protoSharing(GetSystemResponse.equals),
    staleTime: 60_000, // safety net only; mutations set data directly, WatchConfig handles push
  });
}

export function usePutNodeConfig() {
  return useSystemMutation(
    async (args: { id: string; name: string }) => {
      return systemClient.putNodeConfig({ config: { id: decode(args.id), name: args.name } });
    },
    [["settings"]],
  );
}

export function useGenerateName() {
  return useMutation({
    mutationFn: async () => {
      const response = await systemClient.generateName({});
      return response.name;
    },
  });
}
