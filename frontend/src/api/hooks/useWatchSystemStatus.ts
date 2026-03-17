import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Code, ConnectError } from "@connectrpc/connect";
import { lifecycleClient, refreshAuth } from "../client";

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
          // Write cluster status directly into cache.
          if (msg.cluster) {
            qc.setQueryData(["clusterStatus"], msg.cluster);
          }
          // Write health directly into cache.
          if (msg.health) {
            qc.setQueryData(["health"], msg.health);
          }
          // Write route stats directly into cache.
          if (msg.routeStats) {
            qc.setQueryData(["route-stats"], msg.routeStats);
          }
          // Write vault list and stats directly — no HTTP refetch needed.
          if (msg.vaults) {
            qc.setQueryData(["vaults"], msg.vaults);
          }
          if (msg.stats) {
            qc.setQueryData(["stats", "all"], msg.stats);
          }
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
