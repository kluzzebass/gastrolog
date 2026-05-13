import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Code, ConnectError } from "@connectrpc/connect";
import { vaultClient, refreshAuth } from "../client";
import { Timestamp } from "@bufbuild/protobuf";
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
    // Tracks the last version seen per producing node. The API node
    // multiplexes its own ChunkBus events plus peer-node events into
    // one stream; each event carries a node_id (empty for local) and a
    // version that's monotonic per producing node. A gap in any
    // node's version sequence means events were dropped between that
    // node's bus and us, and we resync via invalidateQueries on the
    // affected vault.
    const lastVersionByNode = new Map<string, bigint>();

    async function connect(backoff = 0) {
      let nextBackoff = backoff;
      try {
        for await (const msg of vaultClient.watchChunks(
          {},
          { signal: abort.signal },
        )) {
          handleEvent(qc, msg, lastVersionByNode);
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
        // Reconnect resets every per-node baseline; force a cold-start
        // refetch of all per-vault caches so any events dropped during
        // the disconnect window get reconciled.
        lastVersionByNode.clear();
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

export type ChunksCache = ChunkMeta[] | undefined;

/**
 * handleEvent applies a single WatchChunksResponse to the React Query
 * cache. Each op patches the per-vault chunk list via setQueryData;
 * dropped events (per-node version gap) trigger an invalidate of the
 * affected vault key so the next render triggers a cold-start
 * ListChunks. Per-node versions are tracked separately because the
 * API node multiplexes events from every cluster node's bus — each bus
 * has its own monotonic version space.
 */
function handleEvent(
  qc: ReturnType<typeof useQueryClient>,
  msg: WatchChunksResponse,
  lastVersionByNode: Map<string, bigint>,
) {
  // Heartbeat at stream start (and on every fresh peer subscription on
  // the API node) — no payload, just a version baseline. Record the
  // baseline so subsequent events for the same node can detect gaps.
  if (msg.op === ChunkChangeOp.UNSPECIFIED) {
    lastVersionByNode.set(nodeKey(msg.nodeId), msg.version);
    return;
  }

  // Version-gap drop detection per producing node: any non-contiguous
  // version step means the backend bus dropped events to this
  // subscriber for that node. Cold-start the affected vault so we
  // don't trust our local projection.
  const key = nodeKey(msg.nodeId);
  const prevVer = lastVersionByNode.get(key) ?? 0n;
  if (prevVer > 0n && msg.version > prevVer + 1n) {
    const vaultId = encode(msg.vaultId);
    qc.invalidateQueries({ queryKey: ["chunks", vaultId] });
    lastVersionByNode.set(key, msg.version);
    return;
  }
  lastVersionByNode.set(key, msg.version);

  const vaultId = encode(msg.vaultId);
  qc.setQueryData<ChunksCache>(["chunks", vaultId], (prev) =>
    mutateCache(prev, msg),
  );

  // Indexes for a chunk are built by a post-seal background job on the
  // backend and surfaced via the separate GetIndexes RPC (sibling query
  // key ["indexes", vaultId, chunkId]). A user who expands the chunk
  // detail panel while the chunk is still active gets an "all indexes
  // missing" snapshot; without explicit invalidation the SEALED event
  // refreshes the chunk meta but leaves that stale snapshot in place
  // forever. Invalidate on every seal/upload transition so the detail
  // pane refetches once the indexes actually exist. See gastrolog-4zy8a.
  if (msg.op === ChunkChangeOp.SEALED || msg.op === ChunkChangeOp.UPLOADED) {
    const chunkIdStr = encode(msg.chunkId);
    qc.invalidateQueries({ queryKey: ["indexes", vaultId, chunkIdStr] });
  }
}

/** nodeKey converts the producing-node id bytes to a stable map key.
 * Empty bytes (events from the connected node itself) map to "local".
 */
function nodeKey(nodeId: Uint8Array): string {
  if (nodeId.length === 0) return "local";
  return encode(nodeId);
}

/**
 * mutateCache applies one event to the in-memory chunk list for a vault.
 * Pure function (no side effects on prev — returns a new array when the
 * shape changes, or prev when the event is a no-op).
 *
 * Replica tracking model (gastrolog-66vmg): the server stamps
 * authoritative `replica_count` + `replica_node_ids` on every event,
 * computed from the vault-ctl FSM (placement set minus in-flight
 * delete-acks). The client trusts that overlay verbatim. The previous
 * per-node-attribution accumulator drifted on leadership transfer and
 * during active-chunk catchup because it only grew — that whole
 * approach is replaced by server-authoritative push.
 *
 * If the server omits replica fields (zero count) — e.g. memory-mode
 * vault, single-node, or the chunk has been finalized — mergeMeta
 * preserves the existing cache value, so the cold-start ListChunks
 * snapshot remains visible until something authoritative supersedes it.
 */
export function mutateCache(
  prev: ChunksCache,
  msg: WatchChunksResponse,
): ChunksCache {
  const next = prev ? prev.slice() : [];
  const chunkIdKey = bytesToHex(msg.chunkId);

  const findIdx = () =>
    next.findIndex((c) => bytesToHex(c.id) === chunkIdKey);

  switch (msg.op) {
    case ChunkChangeOp.CREATED:
    case ChunkChangeOp.SEALED:
    case ChunkChangeOp.UPLOADED: {
      if (!msg.meta) return prev;
      const idx = findIdx();
      const merged = idx >= 0 ? mergeMeta(next[idx], msg.meta) : msg.meta;
      if (idx >= 0) next[idx] = merged;
      else next.push(merged);
      return next;
    }
    case ChunkChangeOp.PROGRESS: {
      // PROGRESS carries the full active-chunk meta so live updates to
      // WriteEnd / IngestEnd / Bytes flow through to the inspector, not
      // just record_count.
      const idx = findIdx();
      if (idx < 0) return prev;
      if (msg.meta) {
        next[idx] = mergeMeta(next[idx], msg.meta);
        return next;
      }
      // Backward-compat for events without an inline meta — record_count
      // update only.
      const existing = next[idx];
      if (!existing) return prev;
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

/**
 * mergeMeta returns a ChunkMeta with the event's authoritative fields
 * (Sealed, RecordCount, Bytes, DiskBytes, CloudBacked, etc. — every
 * field ChunkMetaToProto knows about on the backend) overlaid on top
 * of the existing cache entry. The merge preserves fields that the
 * event does NOT carry — replica_count, replica_node_ids, retention_
 * pending, pending_ack_node_ids — which are populated by ListChunks'
 * cluster-side dedup pass, not by the chunk manager's per-chunk
 * snapshot.
 *
 * Without this merge, the first event for an existing chunk would
 * zero out those server-computed fields, hiding important operator
 * signal (replica count, retention-pending) from the inspector until
 * the next ListChunks refetch.
 *
 * If there's no existing entry to merge against, returns the event
 * meta unchanged — fresh chunks won't have replica info yet anyway.
 */
export function mergeMeta(existing: ChunkMeta | undefined, incoming: ChunkMeta): ChunkMeta {
  if (!existing) return incoming;
  const merged = existing.clone();
  // Identity fields are always authoritative from the event.
  merged.id = incoming.id;
  merged.vaultId = incoming.vaultId;
  merged.vaultType = incoming.vaultType;
  // Lifecycle flags: incoming wins only when it's "more advanced." Each
  // flag transitions once and never goes backwards (active → sealed,
  // local → cloud-backed, etc.). Critically, this means CREATED events
  // from delayed peer FSM applies that arrive AFTER the chunk's
  // SEALED event don't roll the chunk back to active.
  if (incoming.sealed) merged.sealed = true;
  if (incoming.cloudBacked) merged.cloudBacked = true;
  if (incoming.archived) merged.archived = true;
  merged.compressed = incoming.compressed;
  merged.storageClass = incoming.storageClass;
  // Monotone time-end fields: take max(existing, incoming). CREATED
  // events have zero-value WriteEnd/IngestEnd (chunk has no records
  // yet); if such an event arrives out of order after a SEALED event,
  // overlaying the zero would erase the seal's final timestamps and
  // show "January 1 1970" in the inspector.
  merged.writeStart = pickStart(existing.writeStart, incoming.writeStart);
  merged.writeEnd = pickEnd(existing.writeEnd, incoming.writeEnd);
  merged.ingestStart = pickStart(existing.ingestStart, incoming.ingestStart);
  merged.ingestEnd = pickEnd(existing.ingestEnd, incoming.ingestEnd);
  // Monotone size fields: take max(existing, incoming) so a later
  // event with a stale or zero value can't undo earlier authoritative
  // data. Bytes and recordCount only grow during a chunk's lifecycle
  // (and stay frozen post-seal). The post-seal pipeline can emit
  // SEALED events derived from cm.Meta after local files are removed,
  // where the local manager's view temporarily reports lower values —
  // ignore those.
  if (incoming.bytes > merged.bytes) merged.bytes = incoming.bytes;
  if (incoming.recordCount > merged.recordCount) merged.recordCount = incoming.recordCount;
  if (incoming.diskBytes > merged.diskBytes) merged.diskBytes = incoming.diskBytes;
  // Replica info is authoritative when present on the event: the
  // backend stamps cluster-wide residency from the vault-ctl FSM
  // (placement set minus in-flight delete-acks) on every CREATED /
  // SEALED / UPLOADED / PROGRESS event. A zero replicaCount means the
  // server didn't stamp anything (memory-mode vault, chunk finalized,
  // or a transient FSM gap) — preserve the existing cached value so
  // we don't flicker to zero between an authoritative snapshot and
  // the next one. See gastrolog-66vmg.
  if (incoming.replicaCount > 0) {
    merged.replicaCount = incoming.replicaCount;
    merged.replicaNodeIds = incoming.replicaNodeIds;
  }
  // Fields NOT carried by the event — preserved from existing:
  // - retentionPending (set by retention runner via FSM apply)
  // - pendingAckNodeIds (set by retention runner during fan-out)
  return merged;
}

/** Take the earliest non-zero start time so an event with a zero or
 * later timestamp doesn't erase the original chunk-open time. */
function pickStart(a: Timestamp | undefined, b: Timestamp | undefined): Timestamp | undefined {
  if (!a || tsZero(a)) return b;
  if (!b || tsZero(b)) return a;
  return tsLess(a, b) ? a : b;
}

/** Take the latest end time so an event with a zero or earlier
 * timestamp doesn't roll back the chunk-close time. */
function pickEnd(a: Timestamp | undefined, b: Timestamp | undefined): Timestamp | undefined {
  if (!a || tsZero(a)) return b;
  if (!b || tsZero(b)) return a;
  return tsLess(a, b) ? b : a;
}

function tsZero(t: Timestamp): boolean {
  return t.seconds === 0n && t.nanos === 0;
}

function tsLess(a: Timestamp, b: Timestamp): boolean {
  if (a.seconds !== b.seconds) return a.seconds < b.seconds;
  return a.nanos < b.nanos;
}
