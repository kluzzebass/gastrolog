import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Code, ConnectError } from "@connectrpc/connect";
import { vaultClient, refreshAuth } from "../client";

/**
 * useWatchChunks opens a server-streaming subscription to WatchChunks,
 * which pushes a notification every time chunk metadata changes on the
 * connected node (seal, delete, create, compress, cloud upload). On
 * each notification the ["chunks"] React Query cache is invalidated,
 * triggering a refetch via ListChunks for any expanded vault card.
 *
 * Replaces the previous 5-second polling interval on useChunks. The
 * stream carries only a monotonic version counter — no chunk data —
 * so the bandwidth is negligible. Same reconnection pattern as
 * useWatchSystem: exponential backoff on error, auth refresh on 401.
 *
 * See gastrolog-1jijm.
 */
export function useWatchChunks() {
  const qc = useQueryClient();

  useEffect(() => {
    const abort = new AbortController();
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

    async function connect(backoff = 0) {
      let nextBackoff = backoff;
      try {
        for await (const _msg of vaultClient.watchChunks(
          {},
          { signal: abort.signal },
        )) {
          // Invalidate all chunk caches unconditionally. Unlike config
          // (which has a version-gating scheme), chunks don't carry a
          // client-side version counter — every notification is
          // actionable. ListChunks re-fetches the authoritative state.
          qc.invalidateQueries({ queryKey: ["chunks"] });

          // Delayed second invalidation to catch replication convergence.
          // When a new active chunk is created, the immediate refetch runs
          // before follower replication completes — so the new chunk shows
          // replica_count=1 even in a 3-node cluster. The delayed refetch
          // re-runs the fan-out after replication has had time to finish,
          // correcting the replica count without a recurring poll.
          setTimeout(() => {
            qc.invalidateQueries({ queryKey: ["chunks"] });
          }, 3000);

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
