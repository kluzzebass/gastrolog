# Vault Size Bound on the Retention Policy

Status: implemented (`backend/internal/system/policies.go`,
`backend/internal/orchestrator/disk_guard.go`, `retention.go`,
`storage_bytes.go`, `backend/internal/chunk/claim.go`,
`backend/internal/server/system_policies.go`).

A vault's size bound is not a vault field. It is `MaxSize` on a retention
policy the vault attaches, measured in one currency — the local disk claim
— and it governs both draining and, when the policy opts in, refusing.

## Shape

A retention policy (`RetentionPolicyConfig`) is the vault's complete
size/age/count lifecycle: `MaxAge`, `MaxSize`, `MaxChunks`, and the
`Refuse` flag. Policies say **when** chunks leave; the vault's retention
disposition and transfer target say **what happens** to them, and stay on
`VaultConfig`.

`MaxSize` is a size expression (`"50GB"`, `"1GiB"`), stored verbatim and
resolved at use. It means two things at once:

- **Drain.** Every stated `MaxSize` drains, regardless of `Refuse`: once the
  vault's sealed-chunk store exceeds the bound, the oldest sealed chunks
  fire retention events per the vault's disposition
  (`SizeRetentionPolicy`). Each attached policy drains on its own bound, so
  the tightest stated bound drives draining.
- **Refuse.** Only when the stating policy has `Refuse` set (default off):
  while the vault's whole local footprint is at or over the bound,
  admission for the vault refuses cluster-wide — the backstop while drain
  catches up or is deferred. The effective refuse bound is the minimum
  `MaxSize` over the refuse-enabled attached policies
  (`attachedSizeBound` → `resolveVaultSizeBound`, resolved at the
  config→runtime boundary in `refreshVaultDiskGuards`).

There is no per-vault size default. A vault whose attached policies state
no refuse-enabled `MaxSize` has no per-vault refuse bound; the storage
free-space thresholds it is placed on (`FileStorage.DiskFreeWarn` /
`DiskFreeFloor`, defaults `"10%"` / `"3%"`) are its backstop. Those are
percentages of the volume, so they compose across every vault sharing it,
where a per-vault default would not — and a refuse-only default would
refuse admission without ever draining, which no recovery path could
clear.

"Bound-only" is not a concept: a policy that sets only `MaxSize` is a
drain policy that also bounds the vault.

`Refuse` generalizes to every bound the policy states: on a refuse-enabled
policy, a `MaxAge` or `MaxChunks` violation that a retention sweep fails to
clear also refuses admission (`checkBoundViolations`). `MaxSize`'s refuse
check is instantaneous (every disk-guard tick); age and count are
sweep verdicts.

## Measurement

One unit, the local disk claim of a chunk (`chunk.DiskClaim`):

- `DiskBytes` when recorded — the on-disk GLCB, which is also what a cached
  cloud-backed chunk reports, since a cached GLCB takes as much local disk
  as a GLCB in any other vault;
- 0 for a cloud-backed chunk with no local copy — destroying it frees
  nothing locally;
- otherwise logical `Bytes` plus index file sizes.

The two halves share the per-chunk arithmetic and differ in scope:

- The **drain** trigger measures the retained sealed-chunk store — what
  draining can reclaim.
- The **refuse** bound measures everything the vault holds on the node
  (`localVaultFootprintBytes`): the chunk store plus the pipeline segment
  backlog — what refusal protects.

Consequence for cloud-backed vaults: the size drain only selects chunks
whose destruction frees local disk. A fully evicted cloud-backed chunk is
never destroyed by a size trigger; its local cost is cache, and cache
eviction (LRU or TTL against `CacheBudget` / `CacheTTL`) manages cache.
Age and count triggers still see cloud-backed chunks.

## Enforcement

Cap-and-refuse lives in the disk guard: per-node evaluation, with
`vaultAdmissionGate` merging the local verdict and every peer's via the
NodeStats broadcast, so a vault capped on any node refuses cluster-wide.
Alarms:

- `vault-max-size-approaching:<vault>` (Low) at 90% of the refuse bound;
- `vault-max-size-capped:<vault>` (High) at the bound — new records for
  that vault are refused, others are unaffected;
- `vault-bound-capped:<vault>/age` | `/count` (High) for a refuse-enabled
  age or count bound still violated after a sweep;
- `retention-unenforceable:<vault>` (High) when every attached policy
  resolves with no trigger, so the vault has no drain.

## Validation

`PutRetentionPolicy` parse-checks `MaxSize`: it must parse and must be
greater than zero when set (`validateRetentionMaxSize`); `"0"` is rejected
as meaning no bound. An unset `MaxSize` is stored unset — nothing is
stamped onto the policy.

## Surfaces

- CLI: `config retention-policy … --max-size <size> [--refuse]`.
- UI: the retention policy form carries Max Size and Refuse; the vault form
  has no size field.
- Help: `policy-retention.md`.
- Vocabulary: **Max size (retention policy)** and **Refuse** in
  `docs/ubiquitous_language.md`.
