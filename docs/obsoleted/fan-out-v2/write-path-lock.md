# Fan-Out V2 Write Path Lock

Status: **locked** (2026-05-26). Supersedes conflicting ingest-path assumptions in earlier v2 drafts.

This document is the authoritative write-path contract. If another v2 doc disagrees with this one on ingest, spool accept, or sequence assignment, **this document wins**.

Related:

- [architecture-overview.md](architecture-overview.md)
- [router-delivery-queue.md](router-delivery-queue.md) — persistent pre-vault buffer (**upstream** of this contract; ingest durability prerequisite)
- [high-watermark-contract.md](high-watermark-contract.md) (seq/fence/watermark semantics; ingest sections superseded here)
- [spool-state-machine.md](spool-state-machine.md)
- [phase-rework-map.md](phase-rework-map.md)

## Pre-vault delivery (ingest durability)

**System contract:** nothing ingested is lost once a route destination captures the record. Unrouted records may still be dropped silently.

Routed-but-undelivered ingest must persist in a **router delivery queue** (node-local disk, not vault spool) until delivery into the locked path below succeeds. See [router-delivery-queue.md](router-delivery-queue.md). This document governs accept **after** delivery, not the queue itself.

## Premise (unchanged product story)

GastroLog records carry a strong **`EventID`**. Identity dedup is applied at **multiple choke points** (materialize, search, and others) — not exclusively on ingest.

**`VaultSeq`** (`seq` in older text) is a vault-local **accept label** used for:

- deterministic fence membership (`prev < seq <= curr`),
- spool addressing,
- materialization batch boundaries.

`VaultSeq` is **not** identity. Duplicate accepts (same `EventID`, different `VaultSeq`) are allowed on the hot path.

**Ingest idempotency is not a requirement.** Client retries may consume additional seq values. Search and materialize collapse duplicates for operators and sealed output.

## Write path (locked)

For each source record on the **ingesting node**:

```text
1. Route fan-out: compute destination vault set (existing router).

2. For each destination vault V:
   a. Ensure this node holds a seq swath for V
      (request from vault-ctl allocator leader when low/empty).
   b. VaultSeq = next value from local swath (no EventID lookup).
   c. Attach (vault_id, EventID, VaultSeq, payload).
   d. Replica fan-out in parallel to ALL replicas of V.

3. Each replica:
   → durable spool slot write keyed by VaultSeq (fsync).
   → return ack.

4. W-of-N acceptance on the ingesting router decides write success/failure.
   Do not block slot write waiting for lower seq numbers.
```

### Two fan-outs (still separate)

| Layer | Meaning |
|---|---|
| **Route fan-out** | One source record → many destination vaults. Each vault gets its **own** `VaultSeq` from **that vault's** swath on this router. |
| **Replica fan-out** | One destination-vault write → all replicas of that vault. **Same** `VaultSeq` on every replica for that write. |

### What we explicitly do **not** do on the sequenced path

- Full-record relay through placement residency for assignment.
- Per-record vault-ctl Raft entries.
- Per-record assign RPC to a central node (swath refill only).
- Assign-time `EventID` dedup or ingest idempotency.
- RAM reorder buffers waiting for missing predecessor seqs.
- Chunk-append landing for cross-node sequenced vault writes.

## Sequence swaths (allocator)

### Authority

- **Vault-ctl Raft leader** for vault `V` owns global `next_seq`, epoch, and grant metadata.
- **No per-record Raft logging.**

### Grant model (multi-holder)

Each ingesting node requests **non-overlapping swaths** for vault `V`:

```text
ReserveSeqSwath(vault=V, holder=node_id, count=K)
  → grant [start..end] inclusive
```

Rules:

- Grants are disjoint and monotonic in global seq space.
- Multiple nodes may hold **different** active swaths concurrently (e.g. node A `[1..256]`, node B `[257..512]`).
- A node consumes locally: `next++` until `next > end`, then requests a new swath.
- Abandoned swath tails are **burned** → **unassigned gaps** (existing epoch/burn semantics).

Default `K` is 256 unless configured otherwise. Hot nodes renew more often; slow nodes renew rarely. This is expected.

### Router-local state (per vault, per node)

- `swath_start`, `swath_end`, `swath_next`, `epoch`
- Refill via allocator when exhausted
- **No** `EventID → VaultSeq` map required on the hot path

## Spool (locked storage model)

**Spool** is the pre-chunk durable store on each replica. **Slots** are how spool stores accepts — not a separate layer.

### Sequence window (on disk)

A **sequence window** is one allocator swath range `[start..end]` materialized as a spool storage unit on a replica, e.g.:

```text
spool/windows/w-0000000257-0512/
  idx   # VaultSeq → payload offset (sparse within window)
  raw / attr
```

- One **slot** = one `(VaultSeq, record)` within a window.
- Windows align with **allocator grant ranges**, not with ingesting node identity.
- All replicas receiving fan-out for a vault maintain the same window set (same seq labels).

### Accept rule

On replica RPC `PutSpoolSlot(vault, VaultSeq, record)`:

1. Resolve window from `VaultSeq`.
2. Write slot durably (data before index commit marker — same crash model as chunks).
3. Ack. **Do not** wait for `VaultSeq-1`.

Out-of-order arrival within and across windows is normal. Correctness is:

- same `VaultSeq` + same payload on all replicas after fan-out, and
- fence/materialize reads by `VaultSeq`, not by append order.

### Materialize dedup (canonicalization choke point)

When sealing a fence range, materializer:

1. Iterates `seq` in `(prev_fence, curr_fence]`.
2. Reads each slot via `ReadByVaultSeq`.
3. Emits chunk records with **dedup by `EventID`** (policy: lowest seq wins unless otherwise specified).
4. Missing assigned seq → assigned-missing; never-assigned seq → unassigned gap.

Search continues to dedup on read for presentation.

## Fences and watermarks (revised semantics)

| Signal | Meaning |
|---|---|
| **`VaultSeq`** | Global-vault accept label from swaths |
| **Fence `F_n`** | Upper bound `curr`; inclusion `prev < VaultSeq <= curr` |
| **`H` (optional operator signal)** | Highest `VaultSeq` accepted under local policy — **not** a contiguous-prefix guarantee |
| **`M_r`** | Highest `VaultSeq` materialized locally |
| **`C_r`** | Highest fence upper bound converge-sealed locally |

Record-count rotation triggers on **`H` or fence policy** mean **seq accepts**, not unique `EventID` cardinality, unless materialize policy says otherwise.

## Validation gate (P0, before more feature work)

Required **4+ node** test:

```text
- vault V, RF≥3, writeModel=sequenced
- ingesters on nodes A and B ( asymmetric rates OK )
- each router holds swaths; assigns locally; fans out directly to replicas
- assert: same VaultSeq on all replicas per write
- assert: spool slot present (not chunk append)
- assert: cross-node routed ingest uses sequenced fan-out, not SetRecordAppender→chunk Append
```

## Supersedes

The following assumptions in earlier drafts are **withdrawn**:

- Destination write pipeline assigns only on placement residency node via record relay.
- Single global `ActiveLease` holder as the only seq consumer.
- Assign-time `EventID` dedup / ingest idempotency before `H`.
- Append-only spool tail monotonicity as an accept requirement.
- “Out-of-order arrival is fine” without slot storage.

Implementation must follow [phase-rework-map.md](phase-rework-map.md) stack order.
