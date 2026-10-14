# Product Defaults Policy

Status: in force. Every configurable knob is held to the rules below; the
current defaults are in "Defaults by resource".

## The principle

**An unset value must not express an unbounded claim on a finite, shared
resource.** Absence of configuration is the common case — the operator with
no opinion — and it must resolve to a bounded reading, not the maximal one.

- The vice is not "claiming a lot." It is "claiming *without a bound*."
  `replication-factor` unset resolves to `1` — a bounded, structural minimum
  (one copy is the least that still stores the data) — and is the exemplar.
- The bound must sit on the axis that composes. Disk is shared by every
  vault on a volume, so the disk bound belongs to the volume: the storage's
  free-space thresholds, expressed as a share of the volume, compose across
  any number of vaults; a per-vault share or a per-vault default does not.
- Absence must never be punished harder than any configuration the
  operator could write. A default that reaches a state the field cannot
  express — a bound that refuses when no typeable value of the field would —
  is a defect, not a safe choice.

## Defaults must be typeable

**A default is only legitimate if it is a value the operator could have
entered in the same field** (operator directive). A derived formula is a
legitimate default only when the field's own vocabulary can express it; six
constants no field grammar can express (`max(fraction·volume, hardBytes)`
with share clamps) are not a default.

That is what the percentage grammar is for. `disk-free-warn` /
`disk-free-floor` on a file storage accept an absolute size (`"10GB"`)
**or** a percentage of the volume (`"10%"`, fractional allowed), through one
shared parser (`system.ParseSizeOrPercent` / `ResolveSizeOrPercent`). The
defaults ARE the typeable strings `"10%"` (warn) and `"3%"` (floor)
(`orchestrator.DefaultDiskFreeWarn` / `DefaultDiskFreeFloor`), resolved per
node against that node's volume through the same resolver an explicit value
uses.

The percentage form is allowed on these two fields **only**. A threshold
guards its own volume, so a share composes. A percentage-of-volume
*budget* (`cache-budget`, `memory-budget`, a retention policy's
`max-size`) does not compose across vaults sharing a volume — N vaults at
10% each overcommit it — so budgets are size-only. A percentage on
`cache-budget` or `memory-budget` is rejected with that reason; on a
retention policy's `max-size` it fails the size parse.

## Representation: expressions at rest

Config quantities — sizes and durations — are stored as the operator's own
expression (`"5GiB"`, `"3m"`), not resolved to a number at ingress. They are
parsed at the point of use, through one shared resolver
(`system.SizeOrDefault` / `DurationOrDefault`, over the `ParseSize` /
`ParseDuration` primitives). No call site parses a quantity itself.

- **Readable at rest and in export.** A stored `"5GiB"` reads as `5GiB`, not
  `5368709120`; `config export` is meant to be reviewed and hand-edited.
- **Faithful.** Display is an exact echo — no reconstruct-a-human-string
  step to be lossy or ugly. What the operator typed is what comes back.
- **Consistent.** Durations across auth, query, cluster and lookup settings
  are Go-duration strings; policy quantities follow the same form.

Expressions are safe in replicated state: `ParseSize` / `ParseDuration` are
deterministic and pinned by round-trip tests (`quantity_test.go`), so every
node resolves a cluster quantity identically. Node-local thresholds
(disk-free) resolve per node against the volume actually sampled.

Empty means unset. `"0"` is an explicit zero, never unset
(`system.IsQuantityUnset`), and is rejected wherever it would mean "accept
nothing" or "no bound".

Counts (`max_records`, `max_chunks`) stay numeric — a count is unitless, so
there is no expression to preserve. Measured telemetry (chunk bytes,
timestamps) is numeric too; only operator-authored config is an expression.

The size grammar spans `B`…`EiB` (decimal `KB`–`EB`, binary `KiB`–`EiB`),
identical on both surfaces; `FormatBytesCompact` / `formatBytesBigint` emit
the largest unit that divides exactly, else raw bytes, so every value
round-trips.

## Defaults by resource

| Resource | Knob | Unset | Notes |
|---|---|---|---|
| Copies | `replication-factor` | `1` | Structural minimum |
| Volume disk | `disk-free-warn` / `disk-free-floor` (file storage) | `"10%"` / `"3%"` | Resolved per node at use; the disk bound for every vault placed on the storage |
| Vault disk claim | `max-size` (retention policy) | no per-vault bound | Opt-in on a retention policy; drains when set, refuses only with `refuse`. The vault is bounded by its storage thresholds. `"0"` rejected. See `docs/retention-size-budget-design.md` |
| Cached copies (cloud-backed vault) | `cache-budget` | `"1GiB"` | Stored at creation; `"0"` and percentages rejected |
| Memory (memory vault) | `memory-budget` | `"1GiB"` | Stored at creation; `"0"` and percentages rejected |

Budgets with a default (`cache-budget`, `memory-budget`) are defaulted
server-side at vault creation and **persisted** (`resolveVaultQuantities`,
`backend/internal/server/system_vaults.go`): every creation surface — CLI,
UI, `config import`, `cluster.sh` — inherits the default, none can produce
an unbounded vault by omission, and `inspect` and `config export` show the
real value. An update that omits the quantity keeps the stored value rather
than re-defaulting it. "Effectively unbounded" is an explicit large number
(`1PiB`), never a sentinel.

There is no per-vault disk default. A per-vault size floor refuses
admission without draining anything, so a vault that reaches it never
recovers; and since `refuse` defaults off on retention policies, it would
make an unconfigured vault stricter than any configured one. The storage
thresholds already bound the volume, on the axis that composes.

`scripts/cluster.sh` states its dev-cluster bound explicitly — a retention
policy with `--max-size "50GB" --refuse` — rather than relying on the
absence of one. The product default protects the operator who says
nothing; the script says something.

## Assessing a knob

Each configurable knob is assessed against four questions. The first is the
gate; the rest classify what kind of default it should have and whether its
surface is honest about it.

1. **Does "unset" express an unbounded claim on a finite shared resource?**
   If yes, it is a finding: absence must resolve to a bound, on the axis
   that composes.
2. **Mechanism or limit?** Mechanisms (sweep cadence, segment-complete size,
   reconcile schedule) legitimately carry opinionated defaults. Limits (how
   much of a finite resource something may claim) are the focus. This names
   which side a knob is on; it is not itself a defect.
3. **Derivable or operator-knowledge?** Derivable → a computed default is
   legitimate **only if it is typeable in the field's own vocabulary** (the
   disk floor's default is the expression `"3%"`, not a formula).
   Operator-knowledge → a conservative constant or minimum the product does
   not pretend to guess (`RF=1`).
4. **Does the surface tell the truth about the default?** Placeholder text
   must equal the real default (`ParamDefaults` for ingesters), and help,
   field comments and export must state the *applied* default. A
   documented-but-unapplied default is itself a finding.

Coverage spans every knob reachable through `config *` subcommands,
vault/ingester/policy creation, and cluster settings — including scripts and
UI surfaces, because defaults live at surfaces: CLI flags, UI placeholders,
and bootstrap scripts.
