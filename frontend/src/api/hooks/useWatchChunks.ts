import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Code, ConnectError } from "@connectrpc/connect";
import { vaultClient, refreshAuth } from "../client";
import {
  ChunkChangeOp,
  ChunkMeta,
  WatchChunksResponse,
} from "../gen/gastrolog/v1/vault_pb";
import { encode } from "../glid";

/**
 * useWatchChunks opens a server-streaming subscription to WatchChunks. Each
 * message carries a typed chunk-state change (created, progress, sealed,
 * deleted, uploaded) plus enough payload for the client to mutate its
 * per-vault chunk cache via setQueryData — no ListChunks refetch on the
 * happy path.
 *
 * Replaces the pre-gastrolog-3pf9w shape, where the stream carried only a
 * bare wake-up counter that forced the client to invalidate all
 * `["chunks"]` entries and refetch via ListChunks fan-out on every
 * notification. Under steady-state ingest that produced O(visible vaults
 * × notifications/sec) RPCs; the typed-event shape eliminates the fan-out
 * entirely.
 *
 * Resync semantics: each event carries a monotonic per-node version. The
 * subscriber tracks the last version seen and detects dropped events
 * (subscriber-channel-full on the backend bus) by checking for gaps.
 * When a gap is detected — or on the first reconnect after disconnect —
 * the subscriber invalidates the affected `["chunks", vaultId]` cache
 * entries to force a cold-start refetch and resumes consuming events from
 * the new high-watermark.
 *
 * Routing: RouteLocal per node. For multi-node clusters where the local
 * node doesn't host the inspected vault, lifecycle events still propagate
 * via vault-ctl Raft FSM apply callbacks on every node that participates
 * in the group, so every node's WatchChunks stream emits events for
 * vaults whose chunk metadata it sees.
 */
export function useWatchChunks() {
  const qc = useQueryClient();

  useEffect(() => {
    const abort = new AbortController();
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    // Tracks the last per-node version we've seen. On reconnect the
    // server sends a heartbeat (op = UNSPECIFIED) with the current
    // high-watermark; we compare against this to detect drops.
    let lastVersion = 0n;

    async function connect(backoff = 0) {
      let nextBackoff = backoff;
      try {
        for await (const msg of vaultClient.watchChunks(
          {},
          { signal: abort.signal },
        )) {
          handleEvent(qc, msg, lastVersion);
          lastVersion = msg.version;
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
        // Reconnect resets the per-node baseline; force a cold-start
        // refetch of all per-vault caches so any events dropped during
        // the disconnect window get reconciled.
        lastVersion = 0n;
        qc.invalidateQueries({ queryKey: ["chunks"] });
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

type ChunksCache = ChunkMeta[] | undefined;

/**
 * handleEvent applies a single WatchChunksResponse to the React Query
 * cache. Each op patches the per-vault chunk list via setQueryData;
 * dropped events (version gap) trigger an invalidate of the affected
 * vault key so the next render triggers a cold-start ListChunks.
 */
function handleEvent(
  qc: ReturnType<typeof useQueryClient>,
  msg: WatchChunksResponse,
  lastVersion: bigint,
) {
  // Heartbeat at stream start — no payload, just a version baseline.
  if (msg.op === ChunkChangeOp.UNSPECIFIED) return;

  // Version-gap drop detection: any non-contiguous version step means
  // the backend bus dropped events to this subscriber. Cold-start the
  // affected vault so we don't trust our local projection.
  if (lastVersion > 0n && msg.version > lastVersion + 1n) {
    const vaultId = encode(msg.vaultId);
    qc.invalidateQueries({ queryKey: ["chunks", vaultId] });
    return;
  }

  const vaultId = encode(msg.vaultId);
  qc.setQueryData<ChunksCache>(["chunks", vaultId], (prev) =>
    mutateCache(prev, msg),
  );
}

/**
 * mutateCache applies one event to the in-memory chunk list for a vault.
 * Pure function (no side effects on prev — returns a new array when the
 * shape changes, or prev when the event is a no-op).
 */
function mutateCache(prev: ChunksCache, msg: WatchChunksResponse): ChunksCache {
  const next = prev ? prev.slice() : [];
  const chunkIdKey = bytesToHex(msg.chunkId);

  const findIdx = () =>
    next.findIndex((c) => bytesToHex(c.id) === chunkIdKey);

  switch (msg.op) {
    case ChunkChangeOp.CREATED: {
      if (!msg.meta) return prev;
      const idx = findIdx();
      if (idx >= 0) next[idx] = msg.meta;
      else next.push(msg.meta);
      return next;
    }
    case ChunkChangeOp.SEALED:
    case ChunkChangeOp.UPLOADED: {
      if (!msg.meta) return prev;
      const idx = findIdx();
      if (idx >= 0) next[idx] = msg.meta;
      else next.push(msg.meta);
      return next;
    }
    case ChunkChangeOp.PROGRESS: {
      // Patch the active chunk's record count in place; no allocation
      // unless we actually find the entry.
      const idx = findIdx();
      if (idx < 0) return prev;
      const existing = next[idx];
      if (!existing || existing.recordCount === msg.recordCount) return prev;
      const patched = existing.clone();
      patched.recordCount = msg.recordCount;
      next[idx] = patched;
      return next;
    }
    case ChunkChangeOp.DELETED: {
      const idx = findIdx();
      if (idx < 0) return prev;
      next.splice(idx, 1);
      return next;
    }
    default:
      return prev;
  }
}

/** Convert bytes to a lowercase hex string for stable map keys. */
function bytesToHex(bytes: Uint8Array): string {
  let hex = "";
  for (const b of bytes) {
    hex += b.toString(16).padStart(2, "0");
  }
  return hex;
}
