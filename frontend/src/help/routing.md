# Routes & Filtering

Routes connect data sources to vaults. Each route binds a **filter** to one or more **destination vaults** and is keyed by a **source predicate** that says where the records come from. Routes are configured in [Settings → Routes](settings:routes).

## How It Works

Routes carry a **set** of source-predicate kinds, telling the routing engine which kinds of input streams the route participates in. Two kinds exist today:

- **Ingest**: the route participates in live ingestion. When records arrive from an ingester and pass through [digestion](help:digesters), every enabled ingest-source route's filter is evaluated against each record.
- **Retention trigger**: the route is consulted only when a [retention event](help:policy-retention) fires on a vault. The chunk's records are streamed through every retention-trigger route whose filter matches.

A route may carry both kinds at once — useful if you want the same destination + filter to apply to both live traffic AND retired chunks from a vault.

Each source kind can optionally narrow further:

- **Ingest** can list specific ingester IDs. Empty list = match any ingester.
- **Retention trigger** can list specific source vault IDs. Empty list = match retention events from any vault.

A single record can match multiple routes and be written to multiple vaults. This is by design — you might want production errors in both a short-retention debugging vault and a long-retention compliance vault.

## Route Components

| Component | Description |
|-----------|-------------|
| **Filter** | A named filter expression (configured in [Settings → Filters](settings:filters)). Determines which records match this route. |
| **Destinations** | One or more vaults that receive matching records. |
| **Distribution** | How records are distributed across destinations: **fanout** (all destinations, default), **round-robin**, or **failover**. |
| **Sources** | A multi-select set of source-predicate kinds. **Match live ingest** (default) keeps the route on the live FilterSet; **Match retention events** has the route consulted when retention fires on a vault. Both can be on at once. Each kind has an optional narrower list (specific ingesters / specific source vaults; empty = any). |

## Filter Types

Filters are reusable expressions assigned to routes:

- **`*`** (catch-all): Matches every record. Use for a vault that should receive everything.
- **`+`** (catch-rest): Matches records that didn't match any other route's filter. Ensures nothing is silently dropped.
- **Expression**: A boolean expression like `level=error AND env=prod` (see [Query Language](help:query-language) for syntax). Only matching records are sent to this route's destinations.

Filters are evaluated after [digestion](help:digesters), so attributes added by digesters (like `level`) are available for filtering.

## Source Predicates

Source predicates keep different input streams separate:

- A route with **Match live ingest** is consulted for records from ingesters. The optional ingester picker narrows to specific ingesters; empty matches any.
- A route with **Match retention events** is consulted only when retention fires on a vault. The optional source-vault picker narrows to specific vaults; empty matches any. This prevents loops — re-routed records cannot bounce back through the ingestion pipeline unless the route also has **Match live ingest** checked.

A route can have both checked. The narrowers are independent — only the one matching the active source kind is consulted at evaluation time.

## Common Patterns

**Separate by environment:** Create ingest routes with filters for `env=prod`, `env=staging`, `env=dev` and route each to its own vault with different retention.

**Duplicate critical logs:** Route `level=error` to both a fast-expiring local vault (for debugging) and a cloud-backed vault with long retention (for compliance).

**Catch-rest safety net:** Always have at least one ingest route with a `+` filter pointing to a catch-all vault. This ensures no record is silently dropped if it doesn't match any other route.

**Cold storage via retention re-routing:** Route live logs to a fast local vault, then add a retention-trigger route that re-sends aged records into a cloud-backed cold vault. See [Retention Policies](help:policy-retention) and [Sealed Backing](help:storage-cloud).
