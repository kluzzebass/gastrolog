# Memory Tier

Keeps all chunks in memory. Fast ingestion and queries, but data is lost on restart. Useful as the first tier in a chain — records flow in quickly, then eject to a durable tier via retention rules.

## Settings

| Setting | Description |
|---------|-------------|
| Replication Factor | Number of copies across cluster nodes. Each node keeps its replica in memory. |
| Rotation Policy | When to seal the active chunk. |
| Retention Rules | What to do with sealed chunks — typically eject to a file or cloud tier. |

## What You Should Know

- Memory tiers don't require file storages — they use RAM on whichever nodes they're placed on.
- Replication mirrors writes to follower nodes in real-time, so a memory tier with RF=2+ survives a single node failure.
- Commonly paired with the [Chatterbox ingester](help:ingester-chatterbox) for quick experimentation.
- No compression is applied — chunks are held in their raw form in memory.
