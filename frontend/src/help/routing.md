# Filtering

Filtering controls which records end up in which vaults. Each vault has a filter expression that is evaluated against every incoming record's attributes. A record is stored only if its filter matches.

## Filter Types

- **`*`** (catch-all): Matches every record. Use for a vault that should receive everything.
- **`+`** (catch-rest): Matches records that didn't match any other vault's filter. Ensures nothing is silently dropped.
- **Expression**: A boolean expression like `level=error AND env=prod` (see [Query Language](help:query-language) for syntax). Only matching records are sent to this vault.

## How Filtering Works

When a record arrives, every vault's filter is evaluated against it. A record can match multiple vaults and will be written to all of them. Filters are evaluated after [digestion](help:digesters), so attributes added by digesters (like `level`) are available for filtering.

## Configuration

Filters are managed in the Settings dialog under Filters. Each filter has a name and an expression. Vaults reference filters by name — you can share one filter across multiple vaults or give each vault its own.
