# Pull-by-EventID Design

**Status:** Design for [gastrolog-4t3y4](dcat://gastrolog-4t3y4) (child of [gastrolog-37k2b](dcat://gastrolog-37k2b), under epic [gastrolog-2ujjh](dcat://gastrolog-2ujjh)). Not yet implemented. This doc captures the wire contract + receiver-side semantics so subsequent implementation PRs share the same target.

## Why

Set-diff reconcile identifies cross-replica EventID divergence; drain pulls a node's records to peers before removal; receiver-driven catchup fetches missed records on rejoin; retention-transfer-chunks moves chunks across vaults preserving identity. Today none of these have a primitive to call — the existing `AppendRecords` pushes records to the receiver's *active* chunk only, and `ImportSealedChunk` rebuilds the entire chunk from scratch.

This issue introduces the missing primitive: **pull a specific set of EventIDs from a peer into a specific local chunk**, regardless of the chunk's lifecycle state. Every relocation-shaped operation in the fan-out architecture is structurally a series of pulls.

## Wire contract

### Two new RPCs (additions to `ClusterService`)

```protobuf
// Initiator (P, the puller) → Source (S). Unary.
// S responds with a scheduled count; the actual record transfer
// happens asynchronously via S → P FillRecords frames over the
// existing per-vault chunk-replication stream.
//
// Same flow shape as RequestReplicaCatchup, but at EventID
// granularity instead of chunk granularity.
message PullRecordsRequest {
  bytes vault_id = 1;
  bytes chunk_id = 2;
  repeated bytes event_ids = 3;  // 32-byte EventIDs (chunk.EventID encoding)
  bytes requester_node_id = 4;   // utf-8 node ID; S validates auth and picks the
                                 // appropriate outbound stream to push fills on
}

message PullRecordsResponse {
  uint32 scheduled = 1;          // EventIDs S has locally and will push
  uint32 missing = 2;            // EventIDs S does NOT have locally (P should
                                 // try another peer for these)
}
```

### One new ChunkReplicationCommand variant

```protobuf
// Push records into a NAMED chunk (which may be active, sealing, or
// sealed-not-reconciled), not the receiver's current active chunk.
// Sent S → P over the same per-vault stream as Append/Seal/Import frames.
// Receiver dispatches based on the local chunk's lifecycle state:
//   - Active/Sealing locally → record-by-record append via the standard path
//   - Sealed-not-reconciled locally → new pull-into-sealed write path (37k2b-c phase 2)
//   - Sealed-and-reconciled locally → reject (already converged)
//   - Not present locally → reject (S should not be sending fills for chunks P doesn't know)
message ChunkReplicationFillRecords {
  bytes chunk_id = 1;
  repeated ExportRecord records = 2;
  bool last_batch = 3;           // S sets true on the final frame for this PullRequest
                                 // so P can fire CmdAckPull when last_batch=true
                                 // AND all expected records have landed
}

// Extension of the existing oneof:
oneof command {
  // ... existing variants ...
  ChunkReplicationFillRecords fill_records = 16;
}
```

### One new ack-style command variant (defensive)

```protobuf
// Sent S → P at the end of a fill sequence when S found NO local records
// matching the requested EventIDs. Without this signal, P can't tell
// "S is still working on it" from "S is done and had nothing." Avoids
// indefinite waits when a node is asked for records it doesn't have.
//
// Implicit in the (scheduled=0, missing=N) PullRecordsResponse for the
// initial request, but also needed if S discovers mid-stream that its
// local copy diverged after the request.
message ChunkReplicationFillComplete {
  bytes chunk_id = 1;
  uint32 records_sent = 2;       // Total ExportRecord frames S sent in this fill sequence
  string error = 3;              // non-empty if S aborted mid-stream
}
```

## Semantics

### Sender (S) side

When S receives `PullRecordsRequest(vault_id=V, chunk_id=C, event_ids=E)`:

1. Validate: V exists locally, C exists locally, requester is authorized for V.
2. Open a cursor on the local copy of (V, C) — works regardless of lifecycle state because cursors operate on any chunk.
3. Filter: scan the cursor, collect records whose EventID is in E. Use a HashSet for O(1) E membership check.
4. Respond with `PullRecordsResponse(scheduled=|local ∩ E|, missing=|E \ local|)` so the requester knows what to expect.
5. Asynchronously: open / reuse the per-vault chunk-replication stream to the requester. Push matched records in `ChunkReplicationFillRecords` frames, capped by the existing `importRecordsMaxBytes` / `importRecordsMaxRecords` limits. Set `last_batch=true` on the final frame.
6. If S encounters an error mid-stream (e.g., cursor read fails, local chunk got deleted), close the sequence with `ChunkReplicationFillComplete{error: "..."}`.

The async push reuses the existing chunk-replication stream infrastructure — no new transport layer. The send model is the same as `AppendRecords` and `ImportRecords`.

### Receiver (P) side

When P receives `ChunkReplicationFillRecords` on the per-vault stream:

1. Look up local chunk state for the named chunk_id.
2. Dispatch:
   - **Active locally**: route each record through the standard active-chunk Append path. The append uses the record's EventID for idempotency (already-present EventIDs are silent no-ops).
   - **Sealing locally**: same as Active — Sealing accepts new records until seal completion. Pulled records integrate with the seal-time set-hash exchange.
   - **Sealed-not-reconciled locally** (FinalSetHash populated AND this node is in PendingSealReconcile): use the new pull-into-sealed write path. Opens the chunk's idx.log / raw.log / attr.log for append; writes each record; updates the local recordCount. After all records land, recomputes local set-hash and compares against placement.FinalSetHash; on match, fires `CmdAckPull(chunkID, fromNode=S, toNode=P)` to clear the PendingSealReconcile entry.
   - **Sealed-and-reconciled locally**: reject the fill with an ack carrying error="chunk already reconciled". The sender should never have sent these in this case — likely a stale request.
   - **Not present locally**: reject with error="chunk unknown". Probably indicates a placement-state divergence between sender and receiver.

When P receives `ChunkReplicationFillRecords{last_batch=true}` OR `ChunkReplicationFillComplete`:

- If all expected records (per the earlier `PullRecordsResponse.scheduled` count) have landed: declare the pull successful.
- If fewer records landed than expected: log the discrepancy + retry against another peer for the missing EventIDs.
- Either way, the caller (reconcile loop, drain, etc.) gets a per-pull success/failure signal it can use to drive its own state machine.

### Idempotency

- Sender-side: each `PullRecordsRequest` is independent. Asking twice with the same arguments produces two independent fill sequences. Receiver dedup handles the duplicate records.
- Receiver-side: pulling an EventID that's already locally present is a silent no-op. The chunk manager's append path uses EventID as a dedup key when the receiver supports it (verify: this may need explicit support in some receiver-side paths).
- Connection drop mid-fill: the sequence is interrupted; receiver knows it didn't get `last_batch=true` or `FillComplete`. The caller's reconcile loop retries the pull.

## The "pulls into sealed chunks" subdesign

The trickiest semantic question: can the chunk manager write to a sealed-on-disk chunk?

Today the chunk manager's Append path only writes to the active chunk. The active chunk holds open file handles; sealed chunks have closed handles and immutable files (eventually compressed into a single GLCB blob via PostSealProcess). Pulling into a sealed chunk means reopening its files for write, which breaks the "sealed = immutable" invariant.

**Resolution**: introduce a `SealedRepairer` interface on the chunk manager — narrowly scoped, only used by the pull-into-sealed receiver path. The interface:

```go
type SealedRepairer interface {
  // FillSealed appends records to a sealed-but-not-reconciled chunk.
  // Returns an error if:
  //   - chunk is not locally Sealed
  //   - chunk's FSM state says reconciled (PendingSealReconcile empty for self)
  //   - chunk has been compressed (post-PostSealProcess GLCB exists; the
  //     file-format machinery for compressed chunks is too constrained to
  //     accept appends — see Coordination below)
  //   - any underlying I/O fails
  FillSealed(chunkID ChunkID, records []Record) error
}
```

**Coordination with PostSealProcess.** The seal coordinator ([gastrolog-4zbxk](dcat://gastrolog-4zbxk), 37k2b-a) writes FinalSetHash when ≥W replicas agree at seal time. Replicas in PendingSealReconcile have NOT agreed; their local copy is divergent. PostSealProcess (which compresses the chunk into a GLCB blob) must NOT run on a divergent replica until that replica has caught up. The pre-condition for PostSealProcess becomes: `placement.PendingSealReconcile[selfNodeID] == false`.

This means a divergent replica's chunk stays in its uncompressed multi-file representation (idx.log/raw.log/attr.log/dict.log) until the pull-into-sealed catch-up completes. After completion, the local set-hash matches FinalSetHash; the replica fires CmdAckPull; PostSealProcess fires immediately after; the chunk converges to its compressed canonical form.

**Implication for cloud-backed chunks.** A divergent replica MUST NOT mark a chunk as cloud-backed until reconcile completes locally. The UploadGate ([gastrolog-4t3rs](dcat://gastrolog-4t3rs)) already gates cloud upload on vault-ctl Raft leadership; this gating extends to "local replica is not in PendingSealReconcile."

## Acceptance

- **Single-node tests** (sender + receiver on same process):
  - Pull single EventID from a peer-process active chunk; receiver appends it.
  - Pull 10K EventIDs in a stream; backpressure works (no OOM under slow consumer).
  - Pull EventIDs the sender doesn't have; `PullRecordsResponse.missing` accurately reports the gap.
  - Pull during concurrent appends to the same active chunk; no record loss, no duplicates.
  - Pull from a Sealing chunk; records integrate before seal-time hash exchange.
  - Pull from a Sealed-not-reconciled chunk; records land in the receiver's local copy; set-hash matches FinalSetHash post-fill; CmdAckPull fires.
- **Multi-node tests**:
  - 3-node cluster, partition one node during writes, reconnect, pull missed records from peers, verify convergence.
  - Concurrent pulls from multiple peers for the same chunk; idempotency holds.
  - Pull during topology change (node added to Receiving mid-pull); no protocol violation.
- **Failure modes**:
  - Source node unreachable during pull: stream errors; caller retries against alternate peer.
  - Source has the chunk but a subset of requested EventIDs: PullRecordsResponse reports accurately; caller knows to ask elsewhere for the missing set.
  - Source's local copy gets deleted mid-pull: FillComplete with error; caller knows to retry.
  - Receiver crashes mid-pull: on restart, receiver re-fires the pull request.

## Out of scope for 4t3y4

- **Triggering policy**: who decides when to pull, against which peer, with what backoff. That's reconcile (4zbxk + c86lj + 2t4f8 + 1gl8p), drain (68cfq), and retention-transfer (2l918) — each integrates pull into its own policy.
- **The Merkle protocol that identifies WHICH EventIDs to pull**: that's [gastrolog-c86lj](dcat://gastrolog-c86lj) (37k2b-b).
- **CLI-driven pulls**: the operator-on-demand reconcile CLI is [gastrolog-frced](dcat://gastrolog-frced) (37k2b-f).

## Implementation order

1. Proto additions: `PullRecordsRequest` / `PullRecordsResponse` / `ChunkReplicationFillRecords` / `ChunkReplicationFillComplete`. Run `buf generate`.
2. Cluster server dispatch: add PullRecords to the `FullMethod` switch in `internal/cluster/forward.go`; add FillRecords / FillComplete frame handling in the per-vault chunk-replication stream's command dispatcher.
3. ChunkReplicator client wrapper: `(tr *ChunkReplicator) PullRecords(...)` + sender-side fill push logic.
4. Chunk manager `SealedRepairer` interface + file.Manager implementation. Includes the PostSealProcess coordination (chunk-doesn't-compress-until-reconciled gate).
5. Single-node integration tests covering active / sealing / sealed-not-reconciled receiver paths.
6. Multi-node integration tests covering the partition + reconnect + pull scenario.

Each numbered step is roughly a focused commit; the full 4t3y4 is approximately 6 commits.
