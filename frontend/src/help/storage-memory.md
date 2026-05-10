# Memory Vault

Keeps all chunks in memory. Fast ingestion and queries, but data is lost on restart. Useful as the upstream end of a route chain — records flow in quickly, then exit via retention to a durable downstream vault.

## Settings

| Setting | Description |
|---------|-------------|
| Replication Factor | Number of copies across cluster nodes. Each node keeps its replica in memory. |
| Rotation Policy | When to seal the active chunk. |
| Retention Rules | What to do with sealed chunks — typically `Send records to routing engine` so they flow to a durable file or cloud-backed vault. |

## What You Should Know

- Memory vaults don't require file storages — they use RAM on whichever nodes they're placed on.
- Replication mirrors writes to follower nodes in real-time, so a memory vault with RF=2+ survives a single node failure.
- Commonly paired with the [Chatterbox ingester](help:ingester-chatterbox) for quick experimentation.
- No compression is applied — chunks are held in their raw form in memory.
