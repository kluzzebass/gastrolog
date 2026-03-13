# Retention Policies

A retention policy defines when sealed chunks should be deleted or moved. Multiple conditions can be combined — a chunk is acted on if **any** condition says so (union semantics).

## Conditions

| Condition | Config field | Description | Example |
|-----------|-------------|-------------|---------|
| **TTL** | `maxAge` | Act on chunks older than this duration | `720h` (30 days) |
| **Total size** | `maxBytes` | Keep total vault size under this limit, acting on oldest chunks first | `10GB` |
| **Chunk count** | `maxChunks` | Keep at most this many sealed chunks, acting on oldest excess | `100` |

## Actions

Each retention rule pairs a policy with an action:

- **Expire**: Deletes matching chunks permanently. Indexes are removed first, then the chunk data.
- **Eject**: Streams matching chunks' records through one or more [eject-only routes](help:routing). Each route's filter is evaluated per-record, so only matching records reach each route's destination vaults. After all records are delivered, the source chunk is deleted. This enables fan-out, per-record filtering, and multi-destination routing during retention — the same flexibility that live ingestion routes provide.

## How Retention Runs

Retention policies are evaluated periodically by a [background scheduler](help:inspector-jobs). On each run:

1. The policy receives a snapshot of all sealed chunks in the vault
2. **TTL**: Flags any chunk whose **EndTS** (the WriteTS of its last record) is older than the configured duration
3. **Total size**: Walks chunks from newest to oldest, keeping those that fit within the byte budget. Everything beyond the budget is flagged.
4. **Chunk count**: Keeps the newest N chunks, flags the rest
5. The union of all flagged chunks is processed according to the action (expire or eject)

In a [cluster](help:clustering), retention runs independently on the [node](help:clustering-nodes) hosting each vault. A [vault](help:storage) with no retention policy keeps chunks indefinitely. See also [Rotation](help:policy-rotation) for when chunks are sealed.

## Eject Configuration

To use eject:

1. Create one or more routes with **Eject Only** enabled in [Settings → Routes](settings:routes)
2. Configure filters and destinations on those routes as needed
3. In the vault's retention rule, select action **eject** and pick the target routes

Eject-only routes are excluded from live ingestion, so ejected records won't loop back through the ingestion pipeline.

## Example

A retention policy with `maxAge: "720h"` and `maxBytes: "50GB"` will act on chunks older than 30 days **and** also act on the oldest chunks if total vault size exceeds 50 GB.
