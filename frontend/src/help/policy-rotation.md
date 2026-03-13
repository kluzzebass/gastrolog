# Rotation Policies

A rotation policy defines when the active chunk should be sealed and a new one started. Multiple conditions can be combined — the chunk rotates when **any** condition is met (OR semantics).

## Conditions

| Condition | Config field | Description | Example |
|-----------|-------------|-------------|---------|
| **Size** | `maxBytes` | Seal when projected chunk size would exceed this limit | `64MB`, `1GB` |
| **Age** | `maxAge` | Seal when wall-clock age of the chunk exceeds this duration | `1h`, `24h` |
| **Record count** | `maxRecords` | Seal when the chunk reaches this many records | `100000` |
| **Cron** | `cron` | Seal on a cron schedule | `0 * * * *` (hourly) |

The size limit is a **soft limit** — it checks the projected size (current size plus the incoming record) before each append. If the projected size would exceed the limit, the chunk is sealed first and the record goes into a new chunk. Note that individual storage engines may impose their own hard size limits — see [File Vault](help:storage-file) and [Memory Vault](help:storage-memory) for details.

## Value Formats

**Size** fields accept values with suffixes: `B`, `KB`, `MB`, `GB` (case-insensitive). A bare number is treated as bytes.

**Duration** fields accept Go duration syntax: `30s`, `5m`, `1h`, `24h`, `720h`. The age is measured from wall-clock time when the chunk was opened, not from the first record's timestamp.

**Cron** expressions use either 5-field (minute-level) or 6-field (second-level) syntax. Cron rotation only fires if the active chunk has at least one record.

In a [cluster](help:clustering), rotation is managed by the [node](help:clustering-nodes) hosting each vault.

## Example

A policy with `maxBytes: "256MB"` and `maxAge: "1h"` will seal the chunk when it reaches 256 MB **or** when it has been open for one hour, whichever comes first.

## Choosing a Strategy

Rotation affects index build frequency, query performance, and retention granularity. Here are the trade-offs:

**Smaller chunks** (e.g. 32–64 MB, or 5–15 minutes):
- Index builds complete faster, so new records become indexed sooner
- Retention deletes data in smaller increments (finer granularity)
- More chunks means more index files on disk and more metadata in memory
- Searches touch more chunks, though indexes make this fast

**Larger chunks** (e.g. 256 MB–1 GB, or 1–6 hours):
- Fewer index builds and less metadata overhead
- The active (unindexed) chunk is larger, so queries scanning it take longer
- Retention is coarser — you can't delete a fraction of a chunk

**Recommended starting points:**

| Scenario | Rotation | Why |
|----------|----------|-----|
| High-volume production | `maxBytes: 256MB`, `maxAge: 1h` | Balances index frequency with overhead |
| Low-volume or dev | `maxAge: 24h` | Avoids many tiny chunks when ingestion is slow |
| Compliance / time-aligned | `cron: 0 0 * * *` (daily) | Chunks align to calendar days for predictable retention |
| Cloud-backed vaults | `maxBytes: 128MB`, `maxAge: 1h` | Keeps upload sizes reasonable; each sealed chunk becomes one cloud blob |

Combine size and age for best results — size prevents unbounded growth during bursts, age ensures quiet periods still produce sealed, indexed chunks.
