import type { PlainMessage } from "@bufbuild/protobuf";
import type { QueryClient } from "@tanstack/react-query";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { systemClient } from "../client";
import { GetSystemResponse, SettingsMutationEcho } from "../gen/gastrolog/v1/system_pb";
import { protoSharing } from "./protoSharing";
import { decode } from "../glid";

/**
 * Tracks the highest cluster-ctl Raft log index seen from authoritative sources
 * (GetSystem, mutation responses, setQueryData). useWatchSystem compares the
 * stream against this to avoid redundant invalidation.
 */
let cachedClusterCtlRaftIndex = 0n;

/** Update the cached index. Only advances forward (max wins). */
function setClusterCtlRaftIndex(v: unknown) {
  const n = clusterCtlRaftIndexScalarToBigInt(v);
  if (n > cachedClusterCtlRaftIndex) cachedClusterCtlRaftIndex = n;
}

/** Read the cached cluster-ctl raft index for comparison by useWatchSystem. */
export function getClusterCtlRaftIndex(): bigint {
  return cachedClusterCtlRaftIndex;
}

/** Coerce protobuf uint64 scalars to bigint for index comparisons. */
export function clusterCtlRaftIndexScalarToBigInt(v: unknown): bigint {
  try {
    if (typeof v === "bigint") return v;
    if (typeof v === "string") return BigInt(v);
    if (typeof v === "number") return BigInt(v);
    return 0n;
  } catch {
    return 0n;
  }
}

/**
 * After server-settings mutations, mirror GetSettings and advance cluster_ctl_raft_index without
 * invalidating (avoids follower-lag refetch races). Patches cached GetSystem when the echo
 * index is newer than what is already in the React Query cache.
 */
export function applySettingsMutationEcho(
  qc: QueryClient,
  echo?: PlainMessage<SettingsMutationEcho> | SettingsMutationEcho,
) {
  if (!echo?.settings) return;
  qc.setQueryData(["settings"], echo.settings);
  const ver = clusterCtlRaftIndexScalarToBigInt(echo.clusterCtlRaftIndex);
  if (ver === 0n) return;
  setClusterCtlRaftIndex(ver);
  const cached = qc.getQueryData<GetSystemResponse>(["system"]);
  if (!cached) return;
  if (ver <= clusterCtlRaftIndexScalarToBigInt(cached.clusterCtlRaftIndex)) return;
  const next = cached.clone();
  next.clusterCtlRaftIndex = echo.clusterCtlRaftIndex;
  qc.cancelQueries({ queryKey: ["system"] });
  qc.setQueryData(["system"], next);
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
        const prev = qc.getQueryData<GetSystemResponse>(["system"]);
        const prevBig = prev
          ? clusterCtlRaftIndexScalarToBigInt(prev.clusterCtlRaftIndex)
          : -1n;
        const nextBig = clusterCtlRaftIndexScalarToBigInt(cfg.clusterCtlRaftIndex);
        // Ignore stale/equal mutation payloads to avoid UI regressing to an
        // older snapshot and then jumping forward again when WatchSystem refetches.
        if (nextBig > prevBig) {
          setClusterCtlRaftIndex(cfg.clusterCtlRaftIndex);
          qc.cancelQueries({ queryKey: ["system"] });
          qc.setQueryData(["system"], cfg);
        }
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
      // Reject stale refetches: if a Raft follower returns an older index
      // than what's already in the cache (from a mutation or earlier fetch),
      // keep the cached data instead of regressing.
      const cached = qc.getQueryData<GetSystemResponse>(["system"]);
      const respBig = clusterCtlRaftIndexScalarToBigInt(response.clusterCtlRaftIndex);
      const cacheBig = cached
        ? clusterCtlRaftIndexScalarToBigInt(cached.clusterCtlRaftIndex)
        : 0n;
      if (cached && respBig <= cacheBig) {
        return cached;
      }
      setClusterCtlRaftIndex(response.clusterCtlRaftIndex);
      return response;
    },
    structuralSharing: protoSharing(GetSystemResponse.equals),
    staleTime: 60_000, // safety net only; mutations set data directly, WatchSystem gates invalidation
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
