import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Code, ConnectError } from "@connectrpc/connect";
import { systemClient, refreshAuth } from "../client";
import { GetSystemResponse } from "../gen/gastrolog/v1/system_pb";
import { getClusterCtlRaftIndex, clusterCtlRaftIndexScalarToBigInt } from "./useSystem";

export function useWatchSystem() {
  const qc = useQueryClient();

  useEffect(() => {
    const abort = new AbortController();
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

    async function connect(backoff = 0) {
      let nextBackoff = backoff;
      try {
        for await (const msg of systemClient.watchSystem(
          {},
          { signal: abort.signal },
        )) {
          const streamVer = clusterCtlRaftIndexScalarToBigInt(msg.clusterCtlRaftIndex);
          const memVer = getClusterCtlRaftIndex();
          const cached = qc.getQueryData<GetSystemResponse>(["system"]);
          const dataVer = cached
            ? clusterCtlRaftIndexScalarToBigInt(cached.clusterCtlRaftIndex)
            : 0n;
          const known = (() => {
            if (memVer > dataVer) return memVer;
            return dataVer;
          })();

          // Only invalidate if the stream's cluster-ctl raft index is newer than
          // what we already hold from a mutation response or prior fetch.
          // Compare against both the module global and the React Query cache so
          // we do not re-fire when Put* already applied GetSystem via setQueryData
          // but Watch delivered in the same tick ordering as the mutation.
          if (streamVer > known) {
            // Don't advance the global index here — only mutation responses
            // should do that. The queryFn compares against the cached data's
            // clusterCtlRaftIndex, so a refetch that returns the new index will be accepted.
            qc.invalidateQueries({ queryKey: ["system"] });
            // Settings are not on GetSystem; refresh when the raft index advances.
            qc.invalidateQueries({ queryKey: ["settings"] });
            // Log component levels resolve against the current LogLevelConfig
            // — any config commit may shift effective levels.
            qc.invalidateQueries({ queryKey: ["log-components"] });
            // Chunks are driven by WatchChunks; not invalidating here so
            // event-applied replica info (additions from catchup) survives
            // unrelated cluster-ctl raft commits like ingester checkpoints.
          }
          nextBackoff = 0; // reset backoff on successful message
        }
      } catch (err) {
        if (abort.signal.aborted) return;
        if (
          err instanceof ConnectError &&
          err.code === Code.Unauthenticated
        ) {
          await refreshAuth();
        }
        // Exponential backoff: 1s, 2s, 4s, ... 30s max.
        const delay = Math.min(1000 * 2 ** nextBackoff, 30_000);
        reconnectTimer = setTimeout(() => connect(nextBackoff + 1), delay);
      }
    }

    connect();

    return () => {
      abort.abort();
      if (reconnectTimer) clearTimeout(reconnectTimer);
    };
  }, [qc]);
}
