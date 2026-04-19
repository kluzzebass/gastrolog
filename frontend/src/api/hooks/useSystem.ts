import type { PlainMessage } from "@bufbuild/protobuf";
import type { QueryClient } from "@tanstack/react-query";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { systemClient } from "../client";
import { GetSystemResponse, SettingsMutationEcho } from "../gen/gastrolog/v1/system_pb";
import { protoSharing } from "./protoSharing";
import { decode } from "../glid";

/**
 * Tracks the highest system Raft log index seen from authoritative sources
 * (GetSystem, mutation responses, setQueryData). useWatchSystem compares the
 * stream against this to avoid redundant invalidation.
 */
let cachedSystemRaftIndex = 0n;

/** Update the cached index. Only advances forward (max wins). */
export function setSystemRaftIndex(v: unknown) {
  const n = systemRaftIndexScalarToBigInt(v);
  if (n > cachedSystemRaftIndex) cachedSystemRaftIndex = n;
}

/** Read the cached system raft index for comparison by useWatchSystem. */
export function getSystemRaftIndex(): bigint {
  return cachedSystemRaftIndex;
}

/** Coerce protobuf uint64 scalars to bigint for index comparisons. */
export function systemRaftIndexScalarToBigInt(v: unknown): bigint {
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
 * After server-settings mutations, mirror GetSettings and advance system_raft_index without
 * invalidating (avoids follower-lag refetch races). Patches cached GetSystem when the echo
 * index is newer than what is already in the React Query cache.
 */
export function applySettingsMutationEcho(
  qc: QueryClient,
  echo?: PlainMessage<SettingsMutationEcho> | SettingsMutationEcho,
) {
  if (!echo?.settings) return;
  qc.setQueryData(["settings"], echo.settings);
  const ver = systemRaftIndexScalarToBigInt(echo.systemRaftIndex);
  if (ver === 0n) return;
  setSystemRaftIndex(ver);
  const cached = qc.getQueryData<GetSystemResponse>(["system"]);
  if (!cached) return;
  if (ver <= systemRaftIndexScalarToBigInt(cached.systemRaftIndex)) return;
  const next = cached.clone();
  next.systemRaftIndex = echo.systemRaftIndex;
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
          ? systemRaftIndexScalarToBigInt(prev.systemRaftIndex)
          : -1n;
        const nextBig = systemRaftIndexScalarToBigInt(cfg.systemRaftIndex);
        // Ignore stale/equal mutation payloads to avoid UI regressing to an
        // older snapshot and then jumping forward again when WatchSystem refetches.
        if (nextBig > prevBig) {
          setSystemRaftIndex(cfg.systemRaftIndex);
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
      const respBig = systemRaftIndexScalarToBigInt(response.systemRaftIndex);
      const cacheBig = cached
        ? systemRaftIndexScalarToBigInt(cached.systemRaftIndex)
        : 0n;
      if (cached && respBig <= cacheBig) {
        return cached;
      }
      setSystemRaftIndex(response.systemRaftIndex);
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
