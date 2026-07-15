import { describe, test, expect } from "bun:test";
import { Timestamp } from "@bufbuild/protobuf";
import {
  ChunkChangeOp,
  ChunkMeta,
  ChunkState,
  WatchChunksResponse,
} from "../gen/gastrolog/v1/vault_pb";
import {
  mergeChunksSnapshot,
  mergeMeta,
  mutateCache,
  shouldRefetchChunksAfterDelete,
} from "./useWatchChunks";

function bytes(b: number): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(new ArrayBuffer(16));
  out[0] = b;
  return out;
}

const ts = (sec: number): Timestamp => new Timestamp({ seconds: BigInt(sec), nanos: 0 });

// gastrolog-66vmg: the server stamps authoritative replicaCount /
// replicaNodeIds on every event. mergeMeta must trust those values
// when present so the inspector's badge converges to the cluster-wide
// truth without operator action. Pre-fix, mergeMeta preserved the
// existing cached value and the client tried to reconstruct it from
// per-node event evidence — which drifted on leadership transfer and
// during the active-chunk catchup window.
describe("mergeMeta — replica info trust model (gastrolog-66vmg)", () => {
  test("incoming replicaCount > 0 overrides existing", () => {
    const existing = new ChunkMeta({
      id: bytes(1),
      replicaCount: 2,
      replicaNodeIds: ["node-a", "node-b"],
      bytes: BigInt(100),
      sealed: true,
    });
    const incoming = new ChunkMeta({
      id: bytes(1),
      replicaCount: 3,
      replicaNodeIds: ["node-a", "node-b", "node-c"],
      bytes: BigInt(100),
      sealed: true,
    });
    const merged = mergeMeta(existing, incoming);
    expect(merged.replicaCount).toBe(3);
    expect(merged.replicaNodeIds).toEqual(["node-a", "node-b", "node-c"]);
  });

  test("incoming replicaCount=0 preserves existing (no authoritative stamp)", () => {
    // The server omits replica info when the FSM doesn't have an
    // authoritative answer (memory-mode vault, chunk finalized, transient
    // FSM gap). The client must keep its prior cached value rather than
    // flickering to zero.
    const existing = new ChunkMeta({
      id: bytes(2),
      replicaCount: 3,
      replicaNodeIds: ["node-a", "node-b", "node-c"],
      sealed: true,
    });
    const incoming = new ChunkMeta({
      id: bytes(2),
      replicaCount: 0,
      replicaNodeIds: [],
      sealed: true,
    });
    const merged = mergeMeta(existing, incoming);
    expect(merged.replicaCount).toBe(3);
    expect(merged.replicaNodeIds).toEqual(["node-a", "node-b", "node-c"]);
  });

  test("incoming retentionPending stamps the ret badge live", () => {
    const existing = new ChunkMeta({
      id: bytes(20),
      sealed: true,
      retentionPending: false,
    });
    const incoming = new ChunkMeta({
      id: bytes(20),
      sealed: true,
      retentionPending: true,
    });
    const merged = mergeMeta(existing, incoming);
    expect(merged.retentionPending).toBe(true);
  });

  test("incoming replicaCount can shrink the existing value (e.g. after RF decrease)", () => {
    // Pre-fix the only way to shrink replicaCount was a cold-start
    // ListChunks refetch. Now the server's authoritative stamp shrinks
    // it on the next event, no refresh needed.
    const existing = new ChunkMeta({
      id: bytes(3),
      replicaCount: 3,
      replicaNodeIds: ["node-a", "node-b", "node-c"],
    });
    const incoming = new ChunkMeta({
      id: bytes(3),
      replicaCount: 2,
      replicaNodeIds: ["node-a", "node-b"],
    });
    const merged = mergeMeta(existing, incoming);
    expect(merged.replicaCount).toBe(2);
    expect(merged.replicaNodeIds).toEqual(["node-a", "node-b"]);
  });

  test("advances chunk state active → sealing and never rolls back", () => {
    const existing = new ChunkMeta({
      id: bytes(5),
      state: ChunkState.ACTIVE,
    });
    const sealing = new ChunkMeta({
      id: bytes(5),
      state: ChunkState.SEALING,
    });
    expect(mergeMeta(existing, sealing).state).toBe(ChunkState.SEALING);

    const staleActive = new ChunkMeta({
      id: bytes(5),
      state: ChunkState.ACTIVE,
    });
    expect(mergeMeta(sealing, staleActive).state).toBe(ChunkState.SEALING);
  });

  test("sealing → sealed when SEALED event carries sealed=true", () => {
    const sealing = new ChunkMeta({
      id: bytes(6),
      state: ChunkState.SEALING,
    });
    const sealedPayload = new ChunkMeta({
      id: bytes(6),
      state: ChunkState.SEALING, // backend may still carry sealing enum
      sealed: true,
    });
    const merged = mergeMeta(sealing, sealedPayload);
    expect(merged.sealed).toBe(true);
    expect(merged.state).toBe(ChunkState.SEALED);
  });

  test("preserves monotonic fields (writeEnd, recordCount) from existing on stale incoming", () => {
    const existing = new ChunkMeta({
      id: bytes(4),
      writeEnd: ts(2000),
      recordCount: BigInt(500),
      replicaCount: 3,
    });
    const incoming = new ChunkMeta({
      id: bytes(4),
      writeEnd: ts(1000), // earlier — must not roll back
      recordCount: BigInt(100), // smaller — must not shrink
      replicaCount: 3,
    });
    const merged = mergeMeta(existing, incoming);
    expect(merged.writeEnd?.seconds).toBe(BigInt(2000));
    expect(merged.recordCount).toBe(BigInt(500));
  });
});

// gastrolog-66vmg: mutateCache no longer accumulates replica evidence
// from per-event nodeId attribution — that was the source of drift.
// Events from any node with the same authoritative replicaCount stamp
// produce the same end state.
describe("mutateCache — server-authoritative replica info", () => {
  test("CREATED event uses meta's authoritative replicaCount", () => {
    const id = bytes(10);
    const meta = new ChunkMeta({
      id,
      vaultId: bytes(99),
      replicaCount: 3,
      replicaNodeIds: ["node-a", "node-b", "node-c"],
    });
    const msg = new WatchChunksResponse({
      vaultId: bytes(99),
      chunkId: id,
      op: ChunkChangeOp.CREATED,
      meta,
      nodeId: new Uint8Array(), // empty — local event
    });
    const next = mutateCache(undefined, msg);
    const got = next?.[0];
    if (!got) throw new Error("expected entry");
    expect(got.replicaCount).toBe(3);
    expect(got.replicaNodeIds).toEqual(["node-a", "node-b", "node-c"]);
  });

  test("event nodeId is NOT auto-merged into replicaNodeIds (server is authoritative)", () => {
    // Pre-fix: an event from a previously-unseen node would have
    // added that node to replicaNodeIds and bumped replicaCount,
    // producing client-side drift. Post-fix: only the server's
    // explicit stamp on meta matters.
    const id = bytes(11);
    const existing = new ChunkMeta({
      id,
      vaultId: bytes(99),
      replicaCount: 2,
      replicaNodeIds: ["node-a", "node-b"],
    });
    const incoming = new ChunkMeta({
      id,
      vaultId: bytes(99),
      replicaCount: 2,
      replicaNodeIds: ["node-a", "node-b"],
    });
    const msg = new WatchChunksResponse({
      vaultId: bytes(99),
      chunkId: id,
      op: ChunkChangeOp.SEALED,
      meta: incoming,
      // Event came from a node not in the replicaNodeIds set; pre-fix
      // this would have grown the count to 3. Post-fix: server's stamp
      // (replicaCount=2) wins.
      nodeId: new TextEncoder().encode("node-stranger"),
    });
    const next = mutateCache([existing], msg);
    const got = next?.[0];
    if (!got) throw new Error("expected entry");
    expect(got.replicaCount).toBe(2);
    expect(got.replicaNodeIds).toEqual(["node-a", "node-b"]);
  });
});

// gastrolog-68wsli: ListChunks refetches derive replica_node_ids from
// which nodes REPORTED the chunk in that fan-out round (reachability
// evidence), while WatchChunks stamps residency from the vault-ctl FSM
// (bytes truth). During a node rejoin the two ping-ponged the cache —
// the seal-pip row count flapped as the rejoining node vanished from
// one snapshot and reappeared on the next stamp. A snapshot may only
// GROW the cached replica set; real shrink (delete acks, holder
// revokes) still arrives via watch stamps, which replace it wholesale.
describe("mergeChunksSnapshot — snapshots cannot shrink residency (gastrolog-68wsli)", () => {
  test("snapshot missing a node keeps the watch-established replica set", () => {
    const cached = [
      new ChunkMeta({
        id: bytes(30),
        sealed: true,
        replicaCount: 4,
        replicaNodeIds: ["node-a", "node-b", "node-c", "node-d"],
      }),
    ];
    const fresh = [
      new ChunkMeta({
        id: bytes(30),
        sealed: true,
        replicaCount: 3,
        // node-d timed out of this fan-out round — must not vanish.
        replicaNodeIds: ["node-a", "node-b", "node-c"],
      }),
    ];
    const merged = mergeChunksSnapshot(cached, fresh);
    expect(merged[0]?.replicaNodeIds).toEqual(["node-a", "node-b", "node-c", "node-d"]);
    expect(merged[0]?.replicaCount).toBe(4);
  });

  test("snapshot can grow the replica set (new copy pulled during catch-up)", () => {
    const cached = [
      new ChunkMeta({
        id: bytes(31),
        sealed: true,
        replicaCount: 3,
        replicaNodeIds: ["node-a", "node-b", "node-c"],
      }),
    ];
    const fresh = [
      new ChunkMeta({
        id: bytes(31),
        sealed: true,
        replicaCount: 4,
        replicaNodeIds: ["node-a", "node-b", "node-c", "node-d"],
      }),
    ];
    const merged = mergeChunksSnapshot(cached, fresh);
    expect(merged[0]?.replicaNodeIds).toEqual(["node-a", "node-b", "node-c", "node-d"]);
    expect(merged[0]?.replicaCount).toBe(4);
  });

  test("disjoint sets union in sorted order", () => {
    const cached = [
      new ChunkMeta({ id: bytes(32), replicaCount: 1, replicaNodeIds: ["node-d"] }),
    ];
    const fresh = [
      new ChunkMeta({ id: bytes(32), replicaCount: 2, replicaNodeIds: ["node-a", "node-b"] }),
    ];
    const merged = mergeChunksSnapshot(cached, fresh);
    expect(merged[0]?.replicaNodeIds).toEqual(["node-a", "node-b", "node-d"]);
    expect(merged[0]?.replicaCount).toBe(3);
  });

  test("snapshot is authoritative for which chunks exist", () => {
    // A chunk deleted while the watch stream was down must drop when the
    // resync snapshot lands — the union rule applies to replica sets, not
    // to chunk existence.
    const cached = [
      new ChunkMeta({ id: bytes(33), replicaCount: 3 }),
      new ChunkMeta({ id: bytes(34), replicaCount: 3 }),
    ];
    const fresh = [new ChunkMeta({ id: bytes(33), replicaCount: 3 })];
    const merged = mergeChunksSnapshot(cached, fresh);
    expect(merged.map((c) => c.id[0])).toEqual([33]);
  });

  test("cold start (no cache) passes the snapshot through", () => {
    const fresh = [new ChunkMeta({ id: bytes(35), replicaCount: 2 })];
    expect(mergeChunksSnapshot(undefined, fresh)).toBe(fresh);
    expect(mergeChunksSnapshot([], fresh)).toBe(fresh);
  });

  test("monotone lifecycle fields survive a stale reporter's snapshot", () => {
    // The fan-out can elect a lagging node's view of the chunk meta;
    // mergeMeta's monotone rules apply to snapshots the same way they
    // apply to watch events.
    const cached = [
      new ChunkMeta({
        id: bytes(36),
        sealed: true,
        recordCount: BigInt(500),
        replicaCount: 3,
        replicaNodeIds: ["node-a", "node-b", "node-c"],
      }),
    ];
    const fresh = [
      new ChunkMeta({
        id: bytes(36),
        sealed: false,
        recordCount: BigInt(100),
        replicaCount: 3,
        replicaNodeIds: ["node-a", "node-b", "node-c"],
      }),
    ];
    const merged = mergeChunksSnapshot(cached, fresh);
    expect(merged[0]?.sealed).toBe(true);
    expect(merged[0]?.recordCount).toBe(BigInt(500));
  });
});

describe("shouldRefetchChunksAfterDelete", () => {
  test("true when bulk delete drains a non-empty cache", () => {
    const prev = [new ChunkMeta({ id: bytes(1) })];
    expect(shouldRefetchChunksAfterDelete(prev, [])).toBe(true);
  });

  test("false when cache was already empty", () => {
    expect(shouldRefetchChunksAfterDelete([], [])).toBe(false);
    expect(shouldRefetchChunksAfterDelete(undefined, [])).toBe(false);
  });

  test("false when chunks remain", () => {
    const prev = [new ChunkMeta({ id: bytes(1) }), new ChunkMeta({ id: bytes(2) })];
    expect(shouldRefetchChunksAfterDelete(prev, [prev[0]!])).toBe(false);
  });
});
