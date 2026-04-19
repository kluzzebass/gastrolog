import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Code, ConnectError } from "@connectrpc/connect";
import { systemClient, refreshAuth } from "../client";
import { getSystemVersion } from "./useSystem";

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
          const streamVersion = msg.systemVersion;

          // Only invalidate config if the stream's version is newer than
          // what we already hold from a mutation response or prior fetch.
          // This replaces the old timer-based suppression — zero races.
          if (streamVersion > getSystemVersion()) {
            // Don't advance the global version here — only mutation responses
            // should do that. The queryFn compares against the cached data's
            // version, so a refetch that returns the new version will be accepted.
            qc.invalidateQueries({ queryKey: ["system"] });
            // Keep dependent caches in the same gate as ["system"]: the stream
            // only carries a version bump, not which subsystem changed. Running
            // these on every message (even when the version did not advance)
            // duplicated refetches after mutations that already invalidated.
            qc.invalidateQueries({ queryKey: ["settings"] });
            qc.invalidateQueries({ queryKey: ["vaults"] });
            qc.invalidateQueries({ queryKey: ["stats"] });
            qc.invalidateQueries({ queryKey: ["chunks"] });
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
