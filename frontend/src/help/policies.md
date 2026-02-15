# Policies

Policies control the lifecycle of chunks within a store. **Rotation policies** determine when to seal the active chunk, and **retention policies** determine when to delete old sealed chunks.

## Rotation Policies

A rotation policy defines when the active chunk should be sealed and a new one started. Multiple conditions can be combined — the chunk rotates when **any** condition is met.

### Conditions

| Condition | Config field | Description | Example |
|-----------|-------------|-------------|---------|
| **Size** | `maxBytes` | Seal when projected chunk size exceeds this limit | `64MB`, `1GB` |
| **Age** | `maxAge` | Seal when wall-clock age of the chunk exceeds this duration | `1h`, `24h` |
| **Record count** | `maxRecords` | Seal when the chunk reaches this many records | `100000` |
| **Cron** | `cron` | Seal on a cron schedule | `0 * * * *` (hourly) |

The size limit is a **soft limit** — it checks the projected size before each append. A built-in hard limit at 4 GB prevents file corruption from 32-bit offset overflow.

### Cron Format

Cron expressions use either 5-field (minute-level) or 6-field (second-level) syntax:

- `0 * * * *` — every hour at minute 0
- `0 0 * * *` — daily at midnight
- `30 0 * * * *` — every hour at second 30 (6-field)

### Example

A policy with `maxBytes: "256MB"` and `maxAge: "1h"` will seal the chunk when it reaches 256 MB **or** when it has been open for one hour, whichever comes first.

## Retention Policies

A retention policy defines when sealed chunks should be deleted. Multiple conditions can be combined — a chunk is deleted if **any** condition says so.

### Conditions

| Condition | Config field | Description | Example |
|-----------|-------------|-------------|---------|
| **TTL** | `maxAge` | Delete chunks older than this duration (measured from chunk EndTS) | `720h` (30 days) |
| **Total size** | `maxBytes` | Keep total store size under this limit, deleting oldest chunks first | `10GB` |
| **Chunk count** | `maxChunks` | Keep at most this many sealed chunks, deleting oldest excess | `100` |

### How Retention Runs

Retention policies are evaluated periodically by the scheduler. On each run:

1. The policy receives a snapshot of all sealed chunks in the store, sorted oldest-first
2. Each condition identifies chunks to delete
3. The union of all deletions is applied (if any condition flags a chunk, it is deleted)

### Example

A retention policy with `maxAge: "720h"` and `maxBytes: "50GB"` will delete chunks older than 30 days **and** also delete the oldest chunks if total store size exceeds 50 GB.

## Assigning Policies to Stores

Each store references a rotation policy and a retention policy by name. You can share policies across multiple stores or create dedicated ones. Policies are configured in the Settings dialog under Rotation Policies and Retention Policies, and assigned to stores in the Stores settings.
