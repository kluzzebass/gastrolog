import { useEffect } from "react";
import { type QueryClient, useQueryClient } from "@tanstack/react-query";
import { Code, ConnectError } from "@connectrpc/connect";
import { lifecycleClient, refreshAuth } from "../client";
import type { WatchSystemStatusResponse } from "../gen/gastrolog/v1/lifecycle_pb";

/** Apply a single WatchSystemStatus message to the query cache. */
function applyStatusMessage(qc: QueryClient, msg: WatchSystemStatusResponse) {
  if (msg.cluster) {
    qc.setQueryData(["clusterStatus"], msg.cluster);
  }
  if (msg.health) {
    qc.setQueryData(["health"], msg.health);
  }
  // routeStats is always present on the message proto — write unconditionally.
  qc.setQueryData(["route-stats"], msg.routeStats);
  if (msg.vaults.length > 0) {
    qc.setQueryData(["vaults"], msg.vaults);
  }
  if (msg.stats) {
    qc.setQueryData(["stats", "all"], msg.stats);
  }
}

/**
 * Subscribes to the WatchSystemStatus server stream and updates query caches
 * when system status changes. Replaces polling for cluster status, health,
 * and route stats.
 *
 * Uses the same reconnection pattern as useWatchConfig: exponential backoff
 * on errors, auth refresh on Unauthenticated.
 */
export function useWatchSystemStatus() {
  const qc = useQueryClient();

  useEffect(() => {
    const abort = new AbortController();
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

    async function connect(backoff = 0) {
      let nextBackoff = backoff;
      try {
        for await (const msg of lifecycleClient.watchSystemStatus(
          {},
          { signal: abort.signal },
        )) {
          applyStatusMessage(qc, msg);
          nextBackoff = 0;
        }
      } catch (err) {
        if (abort.signal.aborted) return;
        if (
          err instanceof ConnectError &&
          err.code === Code.Unauthenticated
        ) {
          await refreshAuth();
        }
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
