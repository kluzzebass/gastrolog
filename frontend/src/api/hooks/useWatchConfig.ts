import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Code, ConnectError } from "@connectrpc/connect";
import { configClient, refreshAuth } from "../client";
import { getConfigVersion, setConfigVersion } from "./useConfig";

export function useWatchConfig() {
  const qc = useQueryClient();

  useEffect(() => {
    const abort = new AbortController();
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

    async function connect(backoff = 0) {
      let nextBackoff = backoff;
      try {
        for await (const msg of configClient.watchConfig(
          {},
          { signal: abort.signal },
        )) {
          const streamVersion = msg.configVersion;

          // Only invalidate config if the stream's version is newer than
          // what we already hold from a mutation response or prior fetch.
          // This replaces the old timer-based suppression — zero races.
          if (streamVersion > getConfigVersion()) {
            setConfigVersion(streamVersion);
            qc.invalidateQueries({ queryKey: ["config"] });
          }

          // Non-config caches are always invalidated — they don't carry
          // version info and are cheap to refetch.
          qc.invalidateQueries({ queryKey: ["settings"] });
          qc.invalidateQueries({ queryKey: ["vaults"] });
          qc.invalidateQueries({ queryKey: ["stats"] });
          qc.invalidateQueries({ queryKey: ["chunks"] });
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
