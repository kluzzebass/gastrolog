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
4. on destination receipts ≥ RF: mark retention-pending and expire the
   source chunk exactly as today (`expireChunk`), which is now a pure
   local-copy delete — the data lives on at the destination.
Progress notes feed the deferral streak (`noteFanOutProgress` renames or
gains a sibling — progress is progress regardless of disposition, per the
33ul6h fix).

Drain-gate interaction: transfer is drain work like the fan-out — same
`diskDeferWrites()` pre-check (its transient cost is the destination's
disk, plus receipts; the source only frees). Destination admission: the
destination's disk guard / size budget must gate transfer intake the same
way it gates routed records — a capped destination vault DEFERS the
transfer (cause named), it does not overfill.

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
