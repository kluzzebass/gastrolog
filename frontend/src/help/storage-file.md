# File Vault

Type: `file`

Persists logs to disk. Each [chunk](help:general-concepts) becomes a directory containing the record data, an [index](help:indexers) for fast lookups, and the record attributes. Sealed chunks are memory-mapped for efficient reads.

| Setting | Description | Default |
|---------|-------------|---------|
| Directory | Vault directory (required) | |
| Compress sealed chunks | Compress sealed chunks with zstd | off |

## What You Should Know

- The directory you configure is entirely yours to choose — it's not derived from any global setting
- Only one process can open a vault directory at a time (enforced by a lock file). In a [cluster](help:clustering), each node must have its own vault directories on local storage
- If GastroLog crashes, it recovers on restart — at most the last partially-written record is lost
- Maximum log file size within a chunk is **4 GB** (32-bit offsets in the file format)
