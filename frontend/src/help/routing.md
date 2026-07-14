# Routes

Routes connect incoming records to vaults. The cluster has one **route
table**: a priority-ordered list of routes, each binding a **match
expression** to one or more **destination vaults**. Routes are configured in
[Settings → Routes](settings:routes).

## How It Works

Every record that finishes [digestion](help:digesters) enters the routing
stage. Routes are evaluated in **priority order** — lower priority fires
first, ties broken by name — and the **first matching route wins**. The
record is delivered to that route's destinations; no later route sees it.

A record that matches **no** route is discarded. This is an intentional,
counted drop — it shows up as **Unmatched** in the
[Routes inspector view](help:inspector-routes). If you never want silent
loss, add a catch-all route (`*`) at the lowest priority (highest number).

## Route Components

| Component | Description |
|-----------|-------------|
| **Priority** | Evaluation order. Lower fires first; routes with the same priority break ties by name. |
| **Match expression** | A boolean expression evaluated against each record's attributes (see [Query Language](help:query-language) for syntax). Determines whether this route fires. |
| **Destinations** | One or more vaults that receive records when the route fires. |
| **Distribution** | How records fan across destinations: **fanout** (all destinations, default), **round-robin** (rotate), or **failover** (first healthy). |
| **Enabled** | Disabled routes are skipped during evaluation. |

## Match Expressions

Two special forms exist alongside ordinary expressions:

- **`*`** (catch-all): Matches every record. Place one at the lowest
  priority to guarantee nothing goes unmatched.
- **empty**: Matches nothing. The route stays configured but never fires —
  useful for temporarily muting a route without deleting it.
- **Expression**: A boolean expression like `level=error AND env=prod`.

Expressions are evaluated after [digestion](help:digesters), so attributes
added by digesters (like `level`) are available for matching.

## Synthetic Attributes

At evaluation time each record is overlaid with reserved `_`-prefixed
attributes that describe where it came from. They exist only during route
matching and are never stored:

- **`_source`** — `"ingest"` for records arriving from an ingester,
  `"retention"` for records fed back through routing by a retention event.
- **`_ingester`** — the ingester ID (ingest records).
- **`_vault`** — the source vault ID (retention records).
- **`_reason`** — the retention reason (retention records).

Use them to keep input streams separate: `_source = "ingest"` matches live
traffic only; `_source = "retention" AND _vault = "<id>"` matches records
draining from a specific vault. A retention route that narrows on `_vault`
cannot loop — re-routed records only re-enter routing if their new vault
also fires a retention event with a routing disposition.

## Retention Routing

When a [retention event](help:policy-retention) fires on a vault **whose
retention disposition is "Send records to routing engine"**, the chunk's
records are streamed back through the route table with
`_source = "retention"` before the chunk is destroyed. Vaults with the
default "Delete" disposition skip routing entirely on retention.

## Common Patterns

**Separate by environment:** Routes with `env=prod`, `env=staging`,
`env=dev` at ascending priorities, each pointing at its own vault with
different retention.

**Catch-all safety net:** A route with expression `*` at the lowest
priority (e.g. priority 1000) pointing to a catch-all vault, so no record
ever goes unmatched.

**Mute noisy input:** Set a route's match expression to empty to park it,
or disable it outright.

**Archive via retention routing:** Route live logs to a fast local vault,
then add a route matching `_source = "retention" AND _vault = "<vault-id>"`
that sends aged records into a cloud-backed archive vault. **You must also
set the source vault's retention disposition to "Send records to routing
engine"** — the default disposition deletes records on retention without
invoking routing. See [Retention Policies](help:policy-retention) and
[Sealed Backing](help:storage-cloud).
