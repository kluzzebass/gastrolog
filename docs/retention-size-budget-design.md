# Size Budget Folds Into Retention Policy

Issue: gastrolog-33ul6h. Status: approved design (operator decisions
2026-07-19), pre-implementation.

## Problem

The vault's size story is split across two config surfaces that are only
correct in combination, with nothing enforcing the combination — and they
do not measure the same thing.

- `VaultConfig.max_size` drives the disk guard's cap-and-refuse: at the
  budget, admission for the vault is refused cluster-wide "until retention
  drains it". It drains nothing itself; on a vault with no size-triggered
  retention the promise is unfulfillable.
- `RetentionPolicyConfig.max_size` is the drain trigger: destroy/route the
  oldest sealed chunks past the bound. It has no backstop when retention
  wedges (gastrolog-5ct2av, live).
- The drain trigger sums LOGICAL record bytes of sealed chunks only; the
  guard budget measures the actual local disk claim (GLCB bytes + indexes +
  segment backlog, cached cloud chunks included). Under compression they
  diverge 3-4×; a careful drain-below-refuse pairing does not compose.

## Operator decisions (2026-07-19)

1. Max size is not a separate vault-level thing — it is a component of the
   retention policy. A policy is the vault's complete size/age lifecycle:
   drain triggers plus an optional hard refuse bound.
2. The refuse bound lives ON THE POLICY, min-wins across a vault's attached
   policies, creation default as the floor when none carries a bound.
3. ONE meaning of size: the local disk claim — and it includes the warm
   cache of cloud-backed chunks ("a cached GLCB file takes up just as much
   space as a GLCB file in a regular vault").
4. Disposition and transfer target stay per-vault (gastrolog-2l918-c4);
   policies say WHEN, the vault says WHAT HAPPENS.

## Shape

### Config

- `RetentionPolicyConfig` gains `optional string size_budget` — the
  per-node disk-claim budget, in the same size vocabulary as every other
  quantity ("50GB"). Named after the guard's established term ("size
  budget" is what the alarms already call it); deliberately NOT `max_size`,
  which remains the drain trigger.
- `VaultConfig.max_size` (field 19) is REMOVED; remaining tags renumber.
  Same for the `PutVaultCommand` copy via the shared VaultConfig embed.
  No reserved tags (house rule; wire-format break accepted per
  formats-stay-V1, operator reinitializes).
- Effective budget resolution (config→runtime boundary,
  `refreshVaultDiskGuards`): for each file vault, resolve every attached
  retention rule's policy; effective budget = min over the policies'
  parsed `size_budget` values; if no attached policy carries one, the
  creation default (`system.DefaultVaultMaxSize`, today "1GiB") applies.
  A file vault therefore remains bounded with zero retention rules, zero
  policies, or only trigger-less policies — the product-defaults invariant
  survives with no operator diligence required.
- Validation moves with the field: `PutRetentionPolicy` parse-checks
  `size_budget` (must parse, must be > 0 when set) the way `PutVault`
  checked `max_size`; `PutVault` loses that validation.

### Measurement

- ONE unit: local disk claim. A chunk's claim is `DiskBytes` when recorded
  (the on-disk GLCB, which is also what cached cloud-backed chunks report)
  else `Bytes` plus index sizes — exactly `localVaultChunkBytes` semantics,
  extracted into a shared per-chunk helper both consumers use.
- The REFUSE bound (guard footprint) keeps measuring the whole local claim:
  chunk store + pipeline segment backlog. Unchanged machinery
  (`localVaultFootprintBytes`), new config source only.
- The DRAIN trigger (`SizeRetentionPolicy`) switches from `meta.Bytes` to
  the same per-chunk disk-claim helper. Scope difference is inherent and
  documented, not hidden: the trigger measures the retained chunk store
  (what draining can reclaim); the bound measures everything the vault
  holds (what refusal protects). Same currency, different scope — the
  drain-below-budget pairing now composes.
- Cloud-backed chunks under the drain trigger: they count their LOCAL
  claim (cache bytes; ~zero when evicted). Consequence, intended: the size
  trigger only destroys chunks whose destruction frees local disk. A
  fully-evicted cloud chunk is never destroyed by a size trigger — its
  local cost is cache, and cache eviction (the existing LRU/TTL job) is
  the mechanism that manages cache, not retention. Age/count triggers are
  unaffected and still see cloud-backed chunks.

### Enforcement (unchanged)

Cap-and-refuse stays in the disk guard exactly as today: per-node
evaluation, `vaultAdmissionGate` local + peer via NodeStats, the
`vault-max-size-capped` / `vault-max-size-approaching` alarms. Only the
config SOURCE of `maxSizeBytes` changes. The capped alarm's "until
retention drains it" phrasing becomes structurally honest: a bound now
implies a policy attachment point, and the UI presents budget and drain
threshold side by side on the policy.

### Surfaces swept (rename-through-the-stack)

proto (`system.proto`, `fsm.proto`) + `just gen` both sides; convert layer;
`system/vault.go` creation defaulting (moves to resolver) and
`system/policy` validation; `refreshVaultDiskGuards`; CLI (`config vault`
loses max-size flags, `config retention-policy` gains `--size-budget`);
UI (`VaultsSettings.tsx` Max Size field removed; retention-policy settings
gain the budget field with placeholder = none/unbounded-per-policy and help
explaining min-wins + the creation default); help topics
(`vaults-config.md`, retention policy help); `ubiquitous_language.md`
(**size budget** entry; drain trigger vs refuse bound distinction);
`docs/alarm-management-design.md` only if alarm text changes.

## Testing

- Unit: min-wins resolution across multiple attached policies; default
  floor with no rules / trigger-less policies; validation (parse, >0);
  drain trigger measuring disk claim (DiskBytes preferred, fallback
  Bytes+indexes; cloud-backed cached counts, evicted ~zero).
- Guard integration: capped state driven from a policy budget; budget
  raised on the policy → admission resumes; policy detached → default
  floor applies (not unbounded).
- Multi-node (4+ nodes, file vaults): remote placement capped from a
  policy budget refuses cluster-wide (peer broadcast path unchanged);
  sweep-node independence.
- Unhappy: retention wedged → bound holds (backstop); restart survival of
  resolved budgets; UI round-trip (policy create/edit with budget).
- `testing.Short()` skips for slow multi-node cases; `just test` +
  `just backend test-full` gate.
