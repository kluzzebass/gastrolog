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

## Matrix — vault limits (`config vault create`)

| Knob | Unset | Crit 1 | Crit 2 | Crit 3 | Crit 4 | Verdict | Follow-up |
|---|---|---|---|---|---|---|---|
| `replication-factor` | `1` | bounded (1 copy = structural min) | limit | operator-knowledge | honest | **PASS** — the exemplar | — |
| `max-size` | `""` = unlimited | **unbounded** | limit | operator-knowledge (constant) | help says "empty means unlimited" (accurate, but the behaviour is the bug) | **FIX** | gastrolog-1epfgb (product) + gastrolog-2b2yyy (script) |
| `cache-budget` | `""` = **no cap** | **unbounded** | limit | derivable/op | **DISHONEST** — field comment says `default: "1GiB"`, code never applies it | **FIX** (crit 1 **and** 4) | gastrolog-338j51 |
| `memory-budget` | `0` = no budget | **unbounded** | limit | operator-knowledge | help silent on unbounded | **FIX** | gastrolog-1qd5wz |
| `disk-free-warn` | `""` = inherit node default | bounded | limit | derivable | honest ("empty inherits the node default") | **PASS** — the good pattern | — |
| `disk-free-floor` | `""` = inherit node default | bounded | limit | derivable | honest | **PASS** — the good pattern | — |
| `cache-ttl` | `""` | n/a (only used in `ttl` eviction mode) | mechanism | operator | honest | **OK-mech** | — |

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
| `rotation-policy` | `""` = no rotation | **REVIEW** | no rotation ⇒ one ever-growing active chunk; bounded indirectly by `max-size` once that's fixed, but worth an explicit stance |
| `retention-policy` | `""` = keep forever | **REVIEW** | `retention.go:529` skips when no rules ⇒ data never ages out; also bounded only by `max-size`. Age-unbounded is arguably fine *if* size is bounded — confirm that reasoning holds |
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

**Verdict:** expected **OK-mech** across the board. One remaining task: a UI
spot-check that placeholder text equals `ParamDefaults()` for two or three
(chatterbox, docker) — the rule exists but nothing enforces it, so drift is
possible. No finding expected; confirm.

## Matrix — mechanism defaults (schedules, sizes, thresholds)

| Knob | Default | Verdict | Note |
|---|---|---|---|
| segment-complete max bytes | 8 MiB | **OK-mech** | `orchestrator.go` |
| segment-complete max age | 10 s | **OK-mech** | |
| retention sweep cadence | every minute | **OK-mech** | |
| reconcile schedule | daily 03:00 | **OK-mech** | |
| suspect grace | 7 days | **OK-mech** | |
| disk-guard `floorFraction` | 0.03 | **REFERENCE** | see below |
| disk-guard `floorBytes` | 3 GiB | **REFERENCE** | `max(3% , 3 GiB)`, clamped ≤10% of volume |
| disk-guard warn share clamp | — | **REFERENCE** | |

### The disk guard is the reference implementation

`floorThreshold = min( max(fraction·total, floorBytes), share·total )` is
exactly a good derived default: a fraction for scale, a floor for small
volumes, a share clamp so it can't eat the disk. This is the pattern a
*derivable* limit (criterion 3) should follow. It is documented here as the
reference; `max-size` deliberately does **not** use it (a per-vault share
doesn't compose — see the design doc), but future per-node derived defaults
should.

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

Two policy knobs need an explicit stance rather than a fix:

- `rotation-policy` unset ⇒ no rotation
- `retention-policy` unset ⇒ no ageing

Both are bounded *indirectly* by `max-size` once it defaults, so they are not
criterion-1 findings on their own — but the audit should record the decision
that "size bounds them, so age/rotation may be unset" rather than leave it
implicit. **Open question for the operator** (below).

No new unbounded-when-unset knob was found beyond the three already known.
The ingester and mechanism defaults are considered and honest (pending the
UI placeholder spot-check).

## Open questions for the operator

1. **`rotation-policy` / `retention-policy` unset.** Is "size bounds the
   vault, so age and rotation may legitimately be unset" the intended stance?
   If yes, record it. If no (e.g. a vault should always rotate), these become
   findings with their own defaults.
2. **UI placeholder spot-check** — confirm, or file if drifted.
