# Defaults audit

**Epic:** gastrolog-4j1e6y — *Product defaults policy*
**Task:** gastrolog-364b57
**Policy:** [../product-defaults-policy-design.md](../product-defaults-policy-design.md)

Read-only classification of every configurable knob against the four
criteria. Separate from the gastrolog-2p313 backend audit (which owns
`coverage.md` / `README.md` in this directory); this file is self-contained.

## Criteria

1. **Unbounded-when-unset?** Does an unset value express an unbounded claim on
   a finite shared resource (disk, cache, memory)? A *yes* is a finding.
2. **Mechanism or limit?** Mechanisms carry opinionated defaults legitimately;
   limits are the focus.
3. **Derivable or operator-knowledge?** Decides which kind of default a knob
   needs: a computed value, or a conservative constant/minimum.
4. **Honest surface?** Placeholder / help / field comment / export all state
   the *applied* default.

Verdict legend: **PASS** (unset is safe), **FIX** (finding, needs
remediation), **OK-mech** (mechanism default, considered), **N/A** (identity
or required field, not a default question).

## The pattern, stated once

The good pattern and the bad pattern sit **in the same `vault create`
command**. `--disk-free-warn` and `--disk-free-floor` both document *"empty
inherits the node default"* — unset resolves to a computed, bounded value.
`--max-size`, `--cache-budget`, `--memory-budget` do the opposite: unset
means unlimited. This is the `disk_guard.go` `SetVaultGuard` sentence made
visible at the CLI — *"warn/floor of 0 inherit the node defaults … maxSizeBytes
of 0 means no budget."* The remediation is to make the finite-resource limits
follow the pattern their neighbours already set.

**Update (gastrolog-etcjdx):** the remediation went further than aligning
defaults — all config quantities are now stored as the operator's expression
(`"5GiB"`, `"3m"`) and resolved at use, empty = unset. So the "warn/floor of
0 = inherit" vs "max-size 0 = no budget" split is gone entirely: every field
reads empty = unset (defaulted or inherited per its rule), `"0"` = explicit
zero (rejected where meaningless). See the design doc's *Representation*
section.

## Matrix — vault limits (`config vault create`)

| Knob | Unset | Crit 1 | Crit 2 | Crit 3 | Crit 4 | Verdict | Follow-up |
|---|---|---|---|---|---|---|---|
| `replication-factor` | `1` | bounded (1 copy = structural min) | limit | operator-knowledge | honest | **PASS** — the exemplar | — |
| `max-size` | `""` = unlimited | **unbounded** | limit | operator-knowledge (constant) | help says "empty means unlimited" (accurate, but the behaviour is the bug) | **FIX** | gastrolog-1epfgb (product) + gastrolog-2b2yyy (script) |
| `cache-budget` | `""` = **no cap** | **unbounded** | limit | derivable/op | **DISHONEST** — field comment says `default: "1GiB"`, code never applies it | **FIX** (crit 1 **and** 4) | gastrolog-338j51 |
| `memory-budget` | `0` = no budget | **unbounded** | limit | operator-knowledge | help silent on unbounded | **FIX** | gastrolog-1qd5wz |
| `disk-free-warn` | `""` = inherit node default `"10%"` | bounded | limit | derivable **and typeable** (`%` grammar) | **was vague, then untypeable** — see correction below | **PASS** (crit 1) + crit-4 fix | fixed in this epic |
| `disk-free-floor` | `""` = inherit node default `"3%"` | bounded | limit | derivable **and typeable** (`%` grammar) | **was vague, then untypeable** — see correction below | **PASS** (crit 1) + crit-4 fix | fixed in this epic |
| `cache-ttl` | `""` | n/a (only used in `ttl` eviction mode) | mechanism | operator | honest | **OK-mech** | — |

### Criterion-4 correction: `disk-free-warn` / `disk-free-floor` (found in the field)

The original verdict rated these two "honest" by reading the **CLI flag help
only** ("empty inherits the node default") and never checked the UI: the
vault form rendered `placeholder=""` with the same vague sentence, so an
operator staring at a live disk-space alarm and a blank threshold field had
no way to learn what was being enforced. "Inherits the node default" without
stating the default is not honest — it is a pointer to nowhere. The first
fix stated the then-current formula on every surface — and thereby exposed
the deeper defect: the formula (`max(10% of volume, 10 GiB)` capped at 25%;
floor `max(3%, 3 GiB)` capped at 10%) was **not a value an operator could
type into the field**, so it violated the typeable-defaults principle
(policy decision 6, operator directive). The formula is GONE. The fields now
accept a size (`"10GB"`) or a percentage of the volume (`"10%"`), and the
node defaults ARE the typeable expressions `"10%"` (warn) and `"3%"`
(floor), stated as placeholder and help on every surface. Two lessons stand:
criterion 4 must be checked on **every** surface a knob has, and a default a
surface cannot state as a typeable value is itself a finding.

### The `cache-budget` finding (crit 4, proven)

`internal/chunk/file/factory.go:102` sets the budget only `if v != ""`.
`internal/chunk/file/manager.go:3561` `lruRule(0)` returns `nil` — no
eviction rule. So an unset `cache-budget` yields an **unbounded warm cache**,
while `internal/system/storage.go` (`CacheBudget` field) comments
*"default: 1GiB"*. Documented, never applied. This is the worst pairing:
a surface that claims to be safe and is not. gastrolog-338j51 must apply the
default or correct the comment — the design says apply it.

## Matrix — vault policy/identity knobs (not limits)

| Knob | Unset | Verdict | Note |
|---|---|---|---|
| `name` | `""` (required) | **N/A** | required field |
| `type` | `file` | **OK-mech** | |
| `enabled` | `true` | **OK-mech** | |
| `storage-class` | `1` | **OK-mech** | |
| `cache-eviction` | `lru` | **OK-mech** | |
| `retention-disposition` | `delete` | **OK-mech** | safe default: drop, don't route — the conservative choice |
| `rotation-policy` | `""` = no rotation | **PASS** (operator-confirmed) | Working as intended: size bounds the vault (`max-size`), so rotation may legitimately be unset. Not a defaults defect |
| `retention-policy` | `""` = keep forever | **PASS** (operator-confirmed) | Working as intended (`retention.go:529` skips when no rules): age is unbounded by design, size is the bound. Not a defaults defect |
| `cloud-service` | `""` | **N/A** | identity |
| `path` | `""` | **N/A** | jsonl sinks only |

## Matrix — ingester params (`ParamDefaults`, ten implementations)

All are **mechanism** defaults (poll intervals, listen addresses, log levels,
client identifiers). None claims a finite shared resource, so criterion 1 does
not apply. They exist and are populated — the project's placeholder-honesty
rule (placeholder text == `ParamDefaults`) is the criterion-4 check.

| Ingester | Defaults | Crit 4 |
|---|---|---|
| chatterbox | min/max interval, host/service count | spot-check UI |
| docker | poll 30s, stdout/stderr true | spot-check UI |
| fluentfwd | addr `:24224` | — |
| http | addr `:3100` | — |
| mqtt | version 3, clean_session | — |
| self | min_level warn | — |
| kafka | group `gastrolog` | — |
| metrics | interval, vault_interval | — |
| otlp | http `:4318`, grpc `:4317` | — |
| tail | poll 30s | — |
| syslog | (read) | — |
| relp | (read) | — |

**Verdict:** **OK-mech** across the board, and the placeholder-honesty rule
(criterion 4) is satisfied **by construction**: the UI does not hardcode
placeholders. `useIngesterDefaults` fetches them from the `GetIngesterDefaults`
RPC, which serves the backend `ParamDefaults`; the param forms read
`placeholder={d["<key>"]}`. A grep confirms no form supplies a literal
placeholder that could drift. There is nothing to keep in sync — the
placeholder *is* `ParamDefaults`, at runtime.

## Matrix — mechanism defaults (schedules, sizes, thresholds)

| Knob | Default | Verdict | Note |
|---|---|---|---|
| segment-complete max bytes | 8 MiB | **OK-mech** | `orchestrator.go` |
| segment-complete max age | 10 s | **OK-mech** | |
| retention sweep cadence | every minute | **OK-mech** | |
| reconcile schedule | daily 03:00 | **OK-mech** | |
| suspect grace | 7 days | **OK-mech** | |
| disk-guard warn default | `"10%"` | **REFERENCE** | typeable expression, resolved per node |
| disk-guard floor default | `"3%"` | **REFERENCE** | typeable expression, resolved per node |

### The disk guard is the reference implementation — for *typeable* defaults

An earlier revision of this audit celebrated the guard's
`min( max(fraction·total, floorBytes), share·total )` curve as the reference
derived default. That verdict is **superseded** by the typeable-defaults
principle (policy decision 6, operator directive): no operator can type
`max(fraction·volume, hardBytes)` into a config field, so the formula was
never a legitimate default — the six constants and the max()/clamp machinery
were deleted. The reference pattern now is: give the field a grammar that can
express the volume-scaled value (`ParseSizeOrPercent` — size or percentage),
then make the default a constant **expression in that grammar** (`"10%"` /
`"3%"`), resolved through the exact resolver an explicit value uses.
Criterion 3's *derivable* now requires typeability. `max-size` deliberately
does **not** take a percentage (a per-vault share doesn't compose — see the
design doc); future per-node derived defaults follow the disk guard's
expression pattern.

## Cluster / node settings

`config cluster join` (`--leader`, `--join-token`) and `config node` are
identity/topology, not resource defaults. **N/A** for this audit. No knob
found that claims a finite shared resource when unset.

## Findings summary

Three finite-resource limits are unbounded when unset; all three already have
remediation children:

- `max-size` → gastrolog-1epfgb (+ script gastrolog-2b2yyy, done)
- `cache-budget` → gastrolog-338j51 — **and** a criterion-4 defect
  (documented default never applied), proven at factory.go:102 / manager.go:3561
- `memory-budget` → gastrolog-1qd5wz

Two policy knobs (`rotation-policy`, `retention-policy`) resolve as
**working-as-intended** (operator-confirmed): size bounds the vault, so age
and rotation may legitimately be unset. Not criterion-1 findings. Recorded,
not deferred.

No new unbounded-when-unset knob was found beyond the three already known.
The ingester and mechanism defaults are considered and honest — the ingester
placeholder-honesty rule holds by construction (defaults served from the
backend `ParamDefaults` RPC, not hardcoded in the UI).

## Audit closed

Both open questions resolved:

1. `rotation-policy` / `retention-policy` unset — **working as intended**
   (operator-confirmed); size is the bound.
2. Ingester placeholder honesty — **PASS by construction** (UI reads
   `ParamDefaults` from the RPC; no hardcoded placeholder to drift).

Remediation is tracked on the three children (`max-size` gastrolog-1epfgb,
`cache-budget` gastrolog-338j51, `memory-budget` gastrolog-1qd5wz) plus the
landed script fix gastrolog-2b2yyy.
