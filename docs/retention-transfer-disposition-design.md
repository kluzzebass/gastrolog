# Transfer Retention Disposition

Issue: gastrolog-2l918. Status: approved design (operator direction
2026-07-19: transfer is expected to be the COMMON retention configuration),
pre-implementation.

## What

A third retention disposition, `transfer`: when a retention event fires,
the sealed chunk is re-homed to a target vault UNCHANGED — no record
decode, no routing, no re-ingest. Where `route` filters/re-tags/fans out
and `delete` drops, `transfer` moves. Most operators aging chunks into an
archive vault want exactly this.

## Settled decisions

- Disposition and target stay PER-VAULT (gastrolog-2l918-c4):
  `VaultConfig.retention_disposition` gains `transfer`;
  `VaultConfig.retention_transfer_target` (vault ID) is required when — and
  only valid when — disposition is `transfer`. The target must not be the
  source vault (self-transfer is the cascade footgun, rejected at Put).
- Mechanism and atomicity (gastrolog-2l918-c2 sketch, confirmed): the
  destination's homes pull the sealed GLCB via the existing
  PullChunkGLCB verify-before-promote machinery, pointed at the
  destination vault's chunk root; the destination vault-ctl FSM registers
  the chunk (the ImportSealedChunk / registerPipelineGLCB seams); chunk
  holder receipts confirm the destination holds ≥ its replication factor;
  ONLY THEN does the source propose its delete (CmdRequestDelete-style
  receipt protocol). No loss window: the source keeps its copies until the
  destination's RF is met. Duplicate-on-retry at the destination is
  impossible by chunk identity (below); duplicate-transfer-attempt is
  idempotent.
- RF mismatch (c2): destination homes pull independently (replica catch-up
  fan-in); receipts gate on DESTINATION RF. Source RF is irrelevant after
  hand-off; source copies all delete together via the source FSM delete.

## Decisions settled by this spec (previously open)

- **#4 Type compatibility: file → file only.** Transfer moves an on-disk
  GLCB between vault chunk roots; cloud-backed and memory vaults have
  different at-rest forms and lifecycle machinery. `PutVault` validation:
  disposition `transfer` requires source and target both file vaults.
  Anything else is an explicit config error, not a runtime surprise.
  (Cloud-target transfer is a future extension: land in the file vault,
  let ITS cloud binding offload — already expressible by transferring to a
  cloud-backed file vault? NO — cloud vaults are their own type; not in
  scope. The validation message names the constraint.)
- **#5 Destination state: defer, never drop.** Target missing, disabled,
  type-invalid at sweep time (config drifted after validation), or its
  pull/receipt cycle not completing: the chunk is RETAINED and the sweep
  records a retention deferral — the same streak/alarm machinery route
  fan-out uses. The alarm type generalizes: `retention-route-deferred`
  renames to **`retention-deferred`** (it now covers route fan-out AND
  transfer; rename through the stack: constant, catalog, doc table, UL
  entry, tests — the alarm is days old and nothing is deployed).
  Cause text names the disposition and the target state.
- **#6 Retention anchoring at the destination: fresh anchor.** The
  transferred chunk's destination-side retention clock starts at transfer
  (SealedAt-equivalent stamped on arrival), so a shorter destination TTL
  does not re-fire retention the moment the chunk lands. The chunk's own
  record timestamps and identity are untouched — only the lifecycle anchor
  is fresh. Documented in UL under Retention event/anchor.
- **#7 ID collision: idempotent success.** Chunk IDs are GLIDs, globally
  unique; the destination already holding this chunk ID means a previous
  transfer attempt completed the pull. Verify-before-promote already
  compares seal metadata + record count; a match short-circuits to
  success (proceed to receipts/source delete), a mismatch is corruption —
  error, defer, alarm cause names it.

## Cycles and tombstones

Two config-time and two runtime edge cases the mechanism above must not
silently mishandle (gastrolog-2l918 review):

- **Cycles are rejected at config, not discovered at sweep time.**
  `PutVault` walks the transfer-target graph (the incoming config plus
  every other vault's stored target) and refuses to write a config that
  would close a cycle — `A→B→A`, or a longer `A→B→C→A` chain. The graph
  is tiny (one edge per vault), so the check is a plain walk with a
  seen-set, run after the existing self-transfer / target-exists /
  both-file-vaults checks. Without this, a cycle would sweep forever:
  each vault's retention would announce-import into the next, receipts
  would converge, and the chunk would ping-pong around the ring on
  every TTL cycle rather than ever coming to rest.
- **A tombstone-refused announce is a NAMED defer, not corruption.** If
  the destination has tombstoned this chunk ID — because a prior
  transfer attempt to this same destination was retracted (see
  abandoned-announce GC below) or an operator deleted it — the
  announce-import is refused. That refusal is reported as a distinct
  defer cause (`tombstoned`), not folded into the corruption path: the
  right response is to wait for `PruneTombstones` to drop the tombstone
  and retry, not to alarm as if the destination held conflicting data.
  The source's chunk is untouched and retried every sweep like any
  other deferred transfer.
- **The announce-before-bytes window is real and self-heals.**
  Announce-import registers the destination's manifest entry (identity,
  fresh `SealedAt` anchor, `TransferSourceVaultID`) before any bytes
  have necessarily landed on any destination home — the entry exists
  with zero holders until the first home's pull completes. Two
  consequences, both handled: (1) the destination's OWN retention rules
  must not treat that zero-holder, transfer-introduced entry as a
  candidate (a short destination TTL firing on it would tombstone the
  transfer's own placeholder out from under itself before receipts ever
  land) — excluded explicitly from destination retention candidate
  selection. (2) If the SOURCE later abandons the transfer for good
  (disposition changed away from transfer, target changed, a corruption
  mismatch) while the announce is still sitting at zero holders, nothing
  in the protocol tells the destination to give up — there is no
  retraction message. The destination self-heals this with a GC pass:
  a transfer-introduced entry that has sat at zero holders past a
  generous age (a day) is retracted via the same receipt-based delete
  every other retirement path in the reconciler uses. This is
  deliberately imprecise — a genuinely still-retrying transfer against a
  long-unreachable destination looks identical from here — but false
  positives are rare at that age, and if the source really is still
  trying, its next announce is deferred by the fresh tombstone (case
  above) until it prunes, then proceeds like new.

## Execution shape

The retention runner's disposition gate
(`applyRetentionDispositionToChunk`) grows a third arm: for `transfer`,
run the per-chunk transfer protocol instead of the fan-out —
1. resolve target vault + validate current state (else defer);
2. propose/ensure the destination FSM entry (announce-import) so
   destination homes learn the chunk and pull it (reusing the replica
   catch-up pull path against the destination's group);
3. wait bounded (watchdog pattern; a stalled transfer defers like a
   stalled fan-out, one-shot NOT consumed — the 5034va ordering applies:
   nothing is marked on the source until the destination confirms);
4. on destination receipts ≥ RF: clear the destination entry's
   `TransferSourceVaultID` (`CmdClearTransferSource`, applied by the
   source against the destination's FSM right before its own expire —
   see "Replica repair after completion" below), then mark
   retention-pending and expire the source chunk exactly as today
   (`expireChunk`), which is now a pure local-copy delete — the data
   lives on at the destination.
Progress notes feed the deferral streak (`noteFanOutProgress` renames or
gains a sibling — progress is progress regardless of disposition, per the
33ul6h fix).

Drain-gate interaction: transfer is drain work like the fan-out — same
`diskDeferWrites()` pre-check (its transient cost is the destination's
disk, plus receipts; the source only frees). Destination admission: the
destination's disk guard / size budget must gate transfer intake the same
way it gates routed records — a capped destination vault DEFERS the
transfer (cause named), it does not overfill.

Per-sweep stall circuit breaker: a single stalled destination must not
freeze every other chunk's retention on the source vault behind it.
Once one chunk's receipts wait stalls against a target vault, every
OTHER chunk targeting that same vault defers immediately for the REST
OF THAT SWEEP — no second (or third, or Nth) chunk re-burns the full
receipts stall window against a destination that has already proven,
this sweep, that it is not answering. The breaker resets at the start
of the next sweep, so a target gets a clean retry every cycle.

### Replica repair after completion

The destination's manifest entry carries `TransferSourceVaultID` so its
replica-catch-up sweep knows to pull bytes from the SOURCE vault's
placement rather than its own (the source and destination are different
vaults with different placements). That field must not survive past
completion: left set, every FUTURE replica-repair pull for this chunk —
a destination home losing its copy to disk corruption, operator error,
weeks after the transfer finished — would still address itself at the
source vault, which by then has deleted its copies. Two defenses, either
sufficient alone:

1. **Clear on completion.** The source proposes `CmdClearTransferSource`
   against the destination's FSM once receipts confirm RF, immediately
   before its own expire. After this, the entry looks exactly like any
   other same-vault sealed chunk to the destination's own replica
   catch-up.
2. **Holder-set fallback.** If the clear is ever missed (a crash or apply
   failure in the narrow window between receipts-met and the clear), the
   destination's replica-repair pull tries the source vault's placement
   first (as `TransferSourceVaultID` still directs) and, finding nothing
   there, falls back to the DESTINATION vault's own confirmed holders —
   the same peers ordinary same-vault catch-up would have used all
   along.

### Transient dual-vault query visibility

During a healthy hand-off there is a window — bounded by the receipts
wait, not the sweep interval — where the SAME chunk (same ID, same
records) is potentially queryable from both vaults at once: announce-
import has registered the destination's manifest entry and homes may
already have pulled and registered the bytes locally, while the source
has not yet expired its own copy (by design — the 5034va ordering keeps
the source's copy live until destination receipts confirm). A query
issued to the SOURCE vault during this window sees the chunk there; a
query issued to the DESTINATION vault sees it there too, under a fresh
`SealedAt` but the same record content and chunk ID. This is the safe
direction for the ambiguity to resolve — duplicate-visible-briefly, never
a gap where neither vault answers for the chunk — and it self-resolves
within the receipts window; no operator action is needed and no record
is ever double-counted by anything that keys on chunk/record identity.

## Surfaces

proto: `RetentionDisposition` enum value + `retention_transfer_target`
field on VaultConfig (+ fsm command copy via embed), `just gen` both
sides. Validation in PutVault. UI: disposition selector gains "Transfer",
with a vault dropdown (existing vault-select component if one exists)
shown only for transfer; help topics updated (vaults-config.md disposition
entry; policy-retention.md cross-ref). CLI: `config vault` disposition
flag accepts transfer + `--retention-transfer-target`. UL: **Transfer
(retention disposition)** entry; `retention-deferred` rename. Alarm
catalog + doc table row updated for the rename and new causes.

## Testing

- Unit: disposition gate arm (transfer defers when target missing/invalid/
  capped; proceeds when healthy); anchoring (fresh anchor stamped);
  idempotent re-transfer (destination already holds → success path);
  validation matrix (self-transfer rejected, non-file rejected, target
  required); rename completeness (no `retention-route-deferred` string
  anywhere).
- Integration (orchestrator-level, file-backed fixtures): full transfer of
  a sealed chunk between two file vaults on one node — GLCB lands in the
  destination chunk root, destination FSM registers, source expires only
  after receipts; crash-window honesty test: source survives when
  receipts never arrive (defer + alarm, chunk retained).
- Multi-node: the server harness is memory-vault-only (33ul6h finding) —
  transfer REQUIRES file vaults, so multi-node coverage uses the
  orchestrator multi-node fixtures that the chunk-replication/catch-up
  tests use (find them: replicateToFollower / PullChunkGLCB tests);
  destination RF > 1: receipts gate holds until every destination home
  holds the chunk. If those fixtures cannot express it, the gap is named
  in the report and covered at epic validation on the real cluster —
  not silently dropped.
- `testing.Short()` for slow cases; `just test` + full gate before done.
