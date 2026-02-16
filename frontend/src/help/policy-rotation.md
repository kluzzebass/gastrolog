# Rotation Policies

A rotation policy defines when the active chunk should be sealed and a new one started. Multiple conditions can be combined — the chunk rotates when **any** condition is met (OR semantics).

## Conditions

| Condition | Config field | Description | Example |
|-----------|-------------|-------------|---------|
| **Size** | `maxBytes` | Seal when projected chunk size would exceed this limit | `64MB`, `1GB` |
| **Age** | `maxAge` | Seal when wall-clock age of the chunk exceeds this duration | `1h`, `24h` |
| **Record count** | `maxRecords` | Seal when the chunk reaches this many records | `100000` |
| **Cron** | `cron` | Seal on a cron schedule | `0 * * * *` (hourly) |

The size limit is a **soft limit** — it checks the projected size (current size plus the incoming record) before each append. If the projected size would exceed the limit, the chunk is sealed first and the record goes into a new chunk. Note that individual storage engines may impose their own hard size limits — see the storage engine pages for details.

## Value Formats

**Size** fields accept values with suffixes: `B`, `KB`, `MB`, `GB` (case-insensitive). A bare number is treated as bytes.

**Duration** fields accept Go duration syntax: `30s`, `5m`, `1h`, `24h`, `720h`. The age is measured from wall-clock time when the chunk was opened, not from the first record's timestamp.

**Cron** expressions use either 5-field (minute-level) or 6-field (second-level) syntax. Cron rotation only fires if the active chunk has at least one record.

## Example

A policy with `maxBytes: "256MB"` and `maxAge: "1h"` will seal the chunk when it reaches 256 MB **or** when it has been open for one hour, whichever comes first.
