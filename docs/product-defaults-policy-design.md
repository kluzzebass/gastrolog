# Product Defaults Policy — Design

## Origin

GastroLog1 (the soak cluster) deadlocked on 2026-07-16. `first-vault`
accumulated 449 GiB of segments across four nodes until the node disk guard
engaged at its 14 GiB floor and suspended admission cluster-wide, from which
it cannot recover on its own (gastrolog-5ct2av). The operative cause
(gastrolog-2b2yyy): the vault was created with no `--max-size`, and the
flag's own help says *"empty means unlimited"*. The bootstrap opted out of
the only per-vault bound, leaving the node-wide guard as the sole backstop.

That is one instance of a class. This document defines the policy that
governs the class and scopes the audit that applies it.

## The principle

**An unset value must not express an unbounded claim on a finite, shared
resource.** Absence of configuration is the common case — the operator with
no opinion — and it must resolve to the *conservative* reading, not the
maximal one.

The distinction that makes this precise (settled in design discussion):

- The vice is not "claiming a lot." It is "claiming *without a bound*."
  `replication-factor` unset resolves to `1` — a bounded, structural minimum
  (one copy is the least that still stores the data) — and is correct as-is.
  It is the exemplar, not a counterexample.
- The vice is `max-size` / `cache-budget` / `memory-budget` unset resolving
  to *unlimited* — an unbounded claim on a finite shared resource (disk,
  cache, memory). Absence reads as "take everything," on a resource other
  vaults and the node itself depend on.

Why "conservative" and not merely "bounded, value TBD": a bound that is too
small **announces itself** — the vault refuses records and the
`vault-max-size` alarm fires, and the operator raises it. A bound that is
absent is **silent** until it takes the node down. Small-and-loud beats
large-and-silent. That asymmetry is why the incident was invisible for
twelve hours: nothing claims "too much" until the moment it claims all of it.

### The canonical failure, in one sentence of existing code

`disk_guard.go` `SetVaultGuard` documents its own parameters:

> *"warn/floor of 0 inherit the node defaults with share clamps; maxSizeBytes
> of 0 means no budget."*

Two opposite meanings for "unset" in one sentence, on one function call.
`warnBytes=0` / `floorBytes=0` mean *compute the sensible default*.
`maxSizeBytes=0` means *no protection at all*. The precedent for the fix is
sitting in the same struct as the bug.

## Representation: expressions at rest (gastrolog-etcjdx)

The principle above is about *values*; this is about *storage*. Config
quantities — sizes and durations — are stored as the operator's own
expression (`"5GiB"`, `"3m"`), not resolved to a number at ingress. They are
parsed at the point of use, through one shared resolver
(`system.SizeOrDefault` / `DurationOrDefault`, over the `ParseSize` /
`ParseDuration` primitives). No call site parses a quantity itself.

Why the operator's string and not a resolved number:

- **Readable at rest and in export.** A stored `"5GiB"` reads as `5GiB`, not
  `5368709120`; `config export` is meant to be reviewed and hand-edited.
- **Faithful.** Display is an exact echo — no reconstruct-a-human-string step
  to be lossy (`5368709119` → `"5.0 GiB"` → 5368709120) or ugly. What the
  operator typed is what comes back.
- **It is the house style.** Auth/query/cluster/lookup durations were already
  stored as Go-duration strings; the numeric `max_age_nanos` policy fields
  were the minority. Uniform strings meets the codebase where it already was.

This reverses an earlier numeric-at-rest rule (recorded in `fsm.proto` and the
policy structs) that cited cross-node/version parser drift as a consensus
risk. Overridden deliberately: `ParseSize`/`ParseDuration` are deterministic
and pinned by round-trip tests (`quantity_test.go`), which is exactly what
Kubernetes relies on to store `"5Gi"` in replicated etcd. Resolution is
per-node for node-local thresholds (disk-free) and identical everywhere for
cluster quantities because the parser is stable.

Counts (`max_records`, `max_chunks`) stay numeric — a count is unitless, so
there is no expression to preserve. Measured telemetry (chunk bytes,
timestamps) is likewise numeric and out of scope; only operator-authored
config is an expression.

The size grammar spans `B`…`EiB` (decimal `KB`–`EB`, binary `KiB`–`EiB`),
identical on both surfaces; `FormatBytesCompact` / `formatBytesBigint` emit
the largest unit that divides exactly, else raw bytes, so every value
round-trips.

## Decisions (settled)

> Note: decisions 1–4 below predate the expression-at-rest flip and are
> superseded by it in *form* — the 1 GiB default is now the string `"1GiB"`,
> "explicit 0" is the string `"0"`, resolution moved from ingress to use — but
> their *intent* (bounded default, explicit-0 rejected, unlimited-is-explicit,
> per-vault-type scope) is unchanged and preserved by gastrolog-etcjdx.

1. **`max-size` default is a constant: 1 GiB per node.** Not derived from the
   volume. A constant was chosen deliberately over a volume-share formula: a
   per-vault share (e.g. 10% of the volume) does not compose — N vaults at
   10% each overcommit — so it bounds each vault without bounding the total,
   and it reintroduces exactly the "what decides the value" complexity the
   constant removes. 1 GiB is deliberately small: the safe failure is a
   per-vault refusal that alarms, not a node-wide deadlock.

2. **"Unlimited" is an explicit large number, not a sentinel.** There is no
   `--unlimited` flag and no magic zero. An operator who wants
   effectively-unbounded types `--max-size 1PiB`. Unbounded becomes a
   deliberate act, which is the whole point.

3. **The default is applied server-side at vault creation, and persisted.**
   When `max-size` is unset the vault is *stored* with the 1 GiB value — it
   is not left as 0 and reinterpreted at read time. Consequences:
   - Every creation surface inherits it: CLI, UI, `config import`,
     `cluster.sh`. None can produce an unbounded vault by omission.
   - `inspect` and `config export` show the real number. No value that means
     one thing in storage and another at read time.
   - This mirrors the `warn/floor=0 → computed default` precedent, with a
     constant substituted for the formula.

4. **Explicit `--max-size 0` is rejected at creation.** Unset resolves to
   1 GiB; a literal 0 is an error ("0 accepts no records; set a real size, or
   a large number for effectively-unlimited"). Accepting 0 silently would
   reintroduce the same accept-nothing / claim-nothing footgun the policy
   removes, just at the other extreme. This requires a vault-config
   validation path, which does not exist today.

5. **`cluster.sh` keeps its explicit `GLOG_VAULT_MAX_SIZE` (50 GB).** Already
   landed under gastrolog-2b2yyy. Consistent with the policy: soak wants
   headroom, so it states a large value explicitly rather than relying on the
   1 GiB product default. The product default protects the operator who says
   nothing; the script says something.

## Scope: a defaults audit epic

Modeled on gastrolog-2p313 (the backend architecture audit): **audit-only —
findings filed as child issues; remediation is separate.** One deliberate
difference from 2p313, which excludes scripts and frontend: this audit
**cannot**, because defaults live at surfaces. `--max-size` is a CLI flag,
`ParamDefaults` feeds UI placeholder text, and `cluster.sh` is where the
missing default actually bit. An audit that skipped the surfaces would have
found nothing this week.

### Audit criteria

Each configurable knob is assessed against four questions. The first is the
gate; the rest classify what kind of default it should have and whether its
surface is honest about it.

1. **Does "unset" express an unbounded claim on a finite shared resource?**
   The core test. If yes, it is a finding: absence must resolve to a bounded,
   conservative value. (`max-size`, `cache-budget`, `memory-budget` fail
   this today; `replication-factor` passes.)

2. **Mechanism or limit?** Mechanisms (sweep cadence, segment-complete size,
   reconcile schedule) legitimately carry opinionated defaults and do. Limits
   (how much of a finite resource a vault may claim) are the audit's focus.
   This criterion names which side a knob is on; it is not itself a defect.

3. **Derivable or operator-knowledge?** Decides *which kind* of default a
   knob gets when it needs one. Derivable → a computed value is legitimate
   (the disk floor's `max(3% , 3 GiB)`). Operator-knowledge → a conservative
   constant or minimum the product does not pretend to guess (`RF=1`; and,
   per the settled decision, `max-size`'s 1 GiB — treated as a constant
   rather than derived because the derived value does not compose).

4. **Does the surface tell the truth about the default?** The project already
   requires this for ingesters: placeholder text must equal the real default
   (`ParamDefaults`). Nothing enforces it for vault flags. Known suspect:
   `system` vault config comments `CacheBudget` as *"default: 1GiB"* — the
   audit must confirm whether that default is *applied* or merely
   *documented*. A documented-but-unapplied default is itself a finding of
   this criterion, and the same failure class as `config export` omitting
   routes (gastrolog-2nr3aa) and the SEALING-shown-as-active badge
   (gastrolog-5wh571): a surface asserting something it does not deliver.

### Coverage

Every configurable knob reachable through `config *` subcommands, vault/
ingester/policy creation, and cluster settings. Grouped for child-issue
decomposition:

- **Vault limits** — `max-size`, `cache-budget`, `memory-budget`,
  `replication-factor`. (max-size decision already made; the others inherit
  the same principle and need their own values.)
- **Ingester params** — the ten `ParamDefaults()` implementations. Verify
  each has a default where a default is safe, and that placeholder text
  matches (criterion 4). Expected mostly clean; confirm rather than assume.
- **Policies & schedules** — rotation, retention, reconcile cadence, suspect
  grace. Mechanism defaults; assess criterion 4 (are they surfaced
  honestly?) more than criterion 1.
- **Disk guard thresholds** — `floorFraction`, `floorBytes`, share clamps.
  The reference implementation of a good derived default; audit confirms it
  and documents it as the pattern others should follow.
- **Cluster / node settings** — anything created via `config cluster`,
  `config node`.

### Deliverables

- A short written **policy** (the principle above) in a durable location —
  `docs/` and, because it governs agent-authored config too, a pointer from
  the ubiquitous-language / conventions surface. This is what the next knob
  is held to.
- The **audit matrix** (`docs/audits/` alongside 2p313's), one row per knob,
  the four criteria assessed, verdict and follow-up issue.
- **Findings filed as child issues.** Remediation is separate work, except
  the `max-size` remediation, which is already specced here and small enough
  to land with the epic's first child.

## Out of scope

- The `5ct2av` deadlock itself. That is a **response** defect — the disk
  guard's node-wide suspension is unrecoverable where a per-vault refusal
  would degrade gracefully — not a defaults defect. Bounding vaults makes the
  deadlock far less *reachable* (a vault refuses at 1 GiB long before the
  node floor), but it does not fix the guard's recovery behavior. Tracked
  separately at gastrolog-5ct2av; this epic reduces its blast radius, it does
  not close it.
- Retention / chunking throughput (why 212 chunks never sealed). Real, and
  the reason segments piled up, but a pipeline-performance question, not a
  defaults one.

## Testing

- Vault created with no `--max-size` is stored with 1 GiB, visible in
  `inspect` and `export`.
- Explicit `--max-size 0` is rejected with a clear message.
- `--max-size 1PiB` is accepted (unlimited-by-explicit-number).
- Round-trip: create-with-default → export → import into a fresh store yields
  the same stored 1 GiB (guards against a surface that re-defaults on import).
- Multi-node: the stored budget is honored per-node by the disk guard on
  whichever node runs the vault; a vault at budget refuses records for itself
  while other vaults on the same node keep ingesting.
- Each remediated knob gets the analogous trio (unset→default, bad-explicit
  rejected, honest surface).
