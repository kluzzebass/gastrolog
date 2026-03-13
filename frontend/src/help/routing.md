# Routes & Filtering

Routes connect the ingestion pipeline to vaults. Each route binds a **filter** to one or more **destination vaults**, controlling which records end up where. Routes are configured in [Settings → Routes](settings:routes).

## How It Works

When a record arrives from an ingester and passes through [digestion](help:digesters), every enabled route's filter is evaluated against it. If the filter matches, the record is sent to the route's destination vaults — including vaults on other [cluster nodes](help:clustering), where the record is forwarded automatically.

A single record can match multiple routes and be written to multiple vaults. This is by design — you might want production errors in both a short-retention debugging vault and a long-retention compliance vault.

## Route Components

| Component | Description |
|-----------|-------------|
| **Filter** | A named filter expression (configured in [Settings → Filters](settings:filters)). Determines which records match this route. |
| **Destinations** | One or more vaults that receive matching records. |
| **Distribution** | How records are distributed across destinations: **fanout** (all destinations, default), **round-robin**, or **failover**. |
| **Eject Only** | When enabled, the route is excluded from live ingestion and can only be used as an eject target (see below). |

## Filter Types

Filters are reusable expressions assigned to routes:

- **`*`** (catch-all): Matches every record. Use for a vault that should receive everything.
- **`+`** (catch-rest): Matches records that didn't match any other route's filter. Ensures nothing is silently dropped.
- **Expression**: A boolean expression like `level=error AND env=prod` (see [Query Language](help:query-language) for syntax). Only matching records are sent to this route's destinations.

Filters are evaluated after [digestion](help:digesters), so attributes added by digesters (like `level`) are available for filtering.

## Eject-Only Routes

Routes have an **Eject Only** toggle:

- **Ingestion routes** (default): Participate in live ingestion. When records arrive from ingesters, these routes' filters determine which vaults receive the records.
- **Eject-only routes**: Excluded from live ingestion entirely. They exist solely as targets for the [eject retention action](help:policy-retention). This prevents loops — ejected records cannot re-match ingestion routes and bounce back.

A route cannot be both. Use eject-only routes when you need to move records from one vault to another based on retention rules, with per-record filtering applied during the move.

## Common Patterns

**Separate by environment:** Create filters for `env=prod`, `env=staging`, `env=dev` and route each to its own vault with different retention.

**Duplicate critical logs:** Route `level=error` to both a fast-expiring local vault (for debugging) and a cloud-backed vault with long retention (for compliance).

**Catch-rest safety net:** Always have at least one route with a `+` filter pointing to a catch-all vault. This ensures no record is silently dropped if it doesn't match any other route.

**Cold storage via eject:** Route live logs to a fast local vault, then use a retention policy with eject to move aged records through an eject-only route into a cloud-backed vault. See [Retention Policies](help:policy-retention) and [Sealed Backing](help:storage-cloud).
