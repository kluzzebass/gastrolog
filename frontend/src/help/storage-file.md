# File Tier

Persists chunks to local disk. Each chunk becomes a directory containing the record data, [indexes](help:indexers) for fast lookups, and record attributes. Sealed chunks are compressed with seekable zstd and memory-mapped for efficient reads.

## Settings

| Setting | Description |
|---------|-------------|
| Storage Class | Which [file storages](help:storage-config) this tier uses. The placement manager assigns one file storage per replica. |
| Replication Factor | Number of copies across available file storages with the matching class. |
| Rotation Policy | When to seal the active chunk and start a new one. |
| Retention Rules | What to do with sealed chunks that age out — delete or eject to another tier. |

## What You Should Know

- The tier's data directory is derived from its assigned file storage path — you don't set it manually.
- Only one process can open a chunk directory at a time (enforced by a lock file).
- Sealed chunks are automatically compressed — no configuration needed.
- If GastroLog crashes, it recovers on restart — at most the last partially-written record is lost.
- Maximum log file size within a chunk is **4 GB** (32-bit offsets in the file format).
- Multiple file storages with the same storage class form a pool. The placement manager spreads replicas across them.
