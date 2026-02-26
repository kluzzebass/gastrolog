# Retention Policies

A retention policy defines when sealed chunks should be deleted. Multiple conditions can be combined — a chunk is deleted if **any** condition says so (union semantics).

## Conditions

| Condition | Config field | Description | Example |
|-----------|-------------|-------------|---------|
| **TTL** | `maxAge` | Delete chunks older than this duration | `720h` (30 days) |
| **Total size** | `maxBytes` | Keep total vault size under this limit, deleting oldest chunks first | `10GB` |
| **Chunk count** | `maxChunks` | Keep at most this many sealed chunks, deleting oldest excess | `100` |

## How Retention Runs

Retention policies are evaluated periodically by a [background scheduler](help:inspector-jobs). On each run:

1. The policy receives a snapshot of all sealed chunks in the vault
2. **TTL**: Deletes any chunk whose **EndTS** (the WriteTS of its last record) is older than the configured duration
3. **Total size**: Walks chunks from newest to oldest, keeping those that fit within the byte budget. Everything beyond the budget is deleted.
4. **Chunk count**: Keeps the newest N chunks, deletes the rest
5. The union of all deletions is applied — if any condition flags a chunk, it is deleted

A [vault](help:storage) with no retention policy keeps chunks indefinitely. See also [Rotation](help:policy-rotation) for when chunks are sealed.

## Example

A retention policy with `maxAge: "720h"` and `maxBytes: "50GB"` will delete chunks older than 30 days **and** also delete the oldest chunks if total vault size exceeds 50 GB.
