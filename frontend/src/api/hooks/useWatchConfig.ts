import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Code, ConnectError } from "@connectrpc/connect";
import { configClient, refreshAuth } from "../client";

export function useWatchConfig() {
  const qc = useQueryClient();

  useEffect(() => {
    const abort = new AbortController();
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

    async function connect(backoff = 0) {
      let nextBackoff = backoff;
      try {
        for await (const _ of configClient.watchConfig(
          {},
          { signal: abort.signal },
        )) {
          // Each message = "config changed". Invalidate the query cache.
          // Skip config invalidation if a mutation recently set the data
          // directly — refetching now would hit a stale follower and
          // briefly overwrite the correct data with old values.
          const state = qc.getQueryState(["config"]);
          if (!state || Date.now() - state.dataUpdatedAt > 2000) {
            qc.invalidateQueries({ queryKey: ["config"] });
          }
          qc.invalidateQueries({ queryKey: ["settings"] });
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
