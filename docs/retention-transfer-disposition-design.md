# Transfer Retention Disposition

Status: implemented (`backend/internal/orchestrator/retention_transfer.go`,
`glcb_catchup.go`, `vault_lifecycle_reconciler.go`,
`backend/internal/server/system_vaults.go`,
`backend/internal/vaultraft/vaultctlfsm/fsm.go`).

## What

`transfer` is the third retention disposition, alongside `delete` and
`route`: when a retention event fires, the sealed chunk is re-homed to a
target vault UNCHANGED — no record decode, no routing, no re-ingest. Where
`route` filters, re-tags and fans out and `delete` drops, `transfer` moves.
It is the common archive pattern: aging chunks into another vault without
touching them.

## Configuration

Disposition and target are per-vault: `VaultConfig.RetentionDisposition =
"transfer"` and `VaultConfig.RetentionTransferTargetVaultID`. Retention
policies say *when* a chunk leaves; the vault says *what happens*.
`PutVault` validates (`validateRetentionTransferDisposition`,
`detectTransferCycle`):

- a target is required when, and only valid when, the disposition is
  `transfer`;
- the target must exist and must not be the source vault (self-transfer is
  a retention cascade);
- source and target must both be transfer-eligible
  (`VaultConfig.TransferEligible`): file or memory vaults that are not
  cloud-backed. Transfer moves a sealed GLCB between two local chunk roots;
  a cloud-backed vault keeps sealed chunks in object storage with its own
  upload and cache lifecycle. The rejection is a config error naming the
  constraint, not a runtime surprise;
- the transfer-target graph must stay acyclic. `PutVault` walks the
  incoming config plus every other vault's stored target and refuses a
  config that would close a cycle (`A→B→A`, `A→B→C→A`). A cycle would never
  come to rest: each vault's retention would hand the chunk to the next on
  every retention cycle.

Surfaces: the vault form's Retention Disposition selector offers Transfer
and shows a Transfer Target picker only for it (ineligible vaults are
listed disabled with the reason); CLI `config vault …
--retention-disposition transfer --retention-transfer-target <vault>`;
help `vaults-config.md` (Retention Disposition, Transfer Target).

## Protocol

The retention runner's disposition gate
(`applyRetentionDispositionToChunk`) runs `fireTransferEvent` for a
`transfer` vault. Per chunk:

1. **Pre-checks.** Defer when the drain gate is engaged
   (`diskDeferWrites()` — transfer is drain work like route fan-out; its
   transient cost is the destination's disk, the source only frees), when
   the target stalled earlier in this sweep (circuit breaker, below), when
   the target is missing, disabled or no longer eligible at sweep time
   (config drifted after validation), or when the destination's admission
   gate (`vaultAdmissionGate` — storage disk protect, max-size, age/count
   bounds, backlog budget, local and peer) refuses. A capped destination
   defers the transfer; it does not overfill.
2. **Announce-import.** The source proposes the chunk's manifest entry into
   the destination vault-ctl FSM, reusing `CmdRepatriateChunk`: a copy of
   the source's own entry with a fresh `SealedAt`, `TransferSourceVaultID`
   set to the source vault, and no holders. Destination homes learn the
   chunk and pull the GLCB through GLCB catch-up — the same
   verify-before-promote pull same-vault replication uses, addressed at the
   source vault's placement because `TransferSourceVaultID` is set. When
   the sweeping node is itself a destination home, it copies and
   verify-before-promotes the GLCB locally at once
   (`tryLocalTransferCopy`); on a single node that is the only path.
3. **Receipts wait.** The source waits for chunk holder receipts from
   destination homes in the destination's current placement to reach the
   destination's replication factor. The wait is bounded like the route
   fan-out watchdog: `transferReceiptsMaxStallTicks` polls at
   `transferReceiptsPollInterval` (2 minutes) with no new holder defers the
   chunk.
4. **Completion.** With destination RF met, the source proposes
   `CmdClearTransferSource` against the destination's FSM, then returns
   success; the caller marks retention-pending and expires the source chunk
   (`expireChunk`) exactly as for the other dispositions — a pure local-copy
   delete, since the data lives on at the destination.

No loss window: nothing is marked on the source until the destination
confirms RF, and a deferred or stalled transfer retains the chunk with the
retention one-shot unconsumed. Source RF is irrelevant after hand-off; the
source's copies all delete together through the source FSM delete.

Every deferral feeds the same streak as route fan-out
(`noteRetentionDeferral`); a completed transfer is progress
(`noteRetentionProgress`). Three consecutive deferred sweeps raise
`retention-deferred:<vault>`, whose detail names the disposition and the
cause (target missing, disabled, ineligible, refusing admission,
tombstoned, corrupt, receipts stalled, …). See
`docs/retention-disk-protect-design.md`.

### Per-sweep stall circuit breaker

Once one chunk's receipts wait stalls against a target vault, every other
chunk targeting that vault defers immediately for the rest of that sweep,
instead of each re-burning the full stall window against a destination that
has already shown it is not answering. The breaker resets at the start of
the next sweep, so a target gets a clean retry every cycle.

### Retention anchoring at the destination

The transferred chunk's destination-side retention clock starts at
transfer: the announced entry carries a fresh `SealedAt`, so a shorter
destination TTL does not re-fire retention the moment the chunk lands. The
chunk's record timestamps and identity are untouched — only the lifecycle
anchor is fresh.

### Identity, idempotence and corruption

Chunk IDs are GLIDs, globally unique. A destination already holding the
chunk ID means an earlier attempt reached announce-import: when its record
count matches the source's, the transfer proceeds straight to the receipts
wait (idempotent retry); a mismatch is corruption — the transfer defers and
the alarm cause names it. The destination FSM is re-read after the apply,
so a concurrent or earlier identical announce counts as success whatever
the apply error said.

## Cycles and tombstones

Transfer-target cycles are rejected at config time (see Configuration), so
a chunk always comes to rest.

If the destination has tombstoned the chunk ID — a prior transfer attempt
to the same destination was retracted (abandoned-announce GC, below) or an
operator deleted it — the announce is refused. That is a named defer cause
(`tombstoned`), not corruption: the transfer waits until the destination's
chunk tombstone is pruned — once every vault-ctl member has confirmed it
holds no copy (`PruneChunkTombstonesCommand`) — and then proceeds like a new
transfer. The source chunk is untouched and retried every sweep.

## The announce-before-bytes window

Announce-import registers the destination's manifest entry before any bytes
have necessarily landed on a destination home — the entry exists with zero
holders until the first home's pull completes. Two consequences:

- The destination's own retention excludes a transfer-introduced entry with
  zero holders from candidate selection; otherwise a short destination TTL
  could tombstone the placeholder out from under the transfer before
  receipts land.
- If the source abandons the transfer for good (disposition changed away
  from transfer, target changed, a corruption mismatch) while the announce
  still sits at zero holders, nothing tells the destination to give up. The
  destination's reconciler retracts a transfer-introduced entry that has
  sat at zero holders past `abandonedTransferAnnounceGCAge` (24 hours)
  through the same receipt-based delete every other retirement path uses;
  it also runs on vault-ctl lead gain. A transfer still retrying against a
  long-unreachable destination looks identical from there; at that age a
  false positive is rare, and a source that really is still trying is
  deferred by the fresh tombstone until it prunes, then proceeds like new.

## Replica repair after completion

`TransferSourceVaultID` directs the destination's GLCB catch-up to pull from
the source vault's placement. It must not survive completion: left set,
every future replica-repair pull for the chunk — a destination home losing
its copy weeks later — would address the source vault, which has deleted
its copies. Two defenses, either sufficient alone:

1. **Clear on completion.** The source proposes `CmdClearTransferSource`
   against the destination's FSM once receipts confirm RF, immediately
   before its own expire. Afterwards the entry is an ordinary same-vault
   sealed chunk to the destination's catch-up.
2. **Holder-set fallback.** If the clear is missed (a crash or apply
   failure between receipts-met and the clear), `runGLCBPull` tries the
   source vault's placement first and, finding nothing, falls back to the
   destination vault's own confirmed holders.

## Transient dual-vault query visibility

During a healthy hand-off there is a window — bounded by the receipts wait,
not the sweep interval — where the same chunk (same ID, same records) can be
queryable from both vaults: the destination has registered the entry and
homes may have pulled the bytes, while the source keeps its copy until
receipts confirm. A query to the source vault sees the chunk there; a query
to the destination sees it there too, with a fresh `SealedAt` but the same
records and chunk ID. Duplicate-visible-briefly, never a gap where neither
vault answers for the chunk; it resolves within the receipts window and
needs no operator action.

Vocabulary: **Transfer (retention disposition)** and **Retention deferral**
in `docs/ubiquitous_language.md`.
