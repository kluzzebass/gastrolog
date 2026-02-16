# File Store

Type: `file`

Persists logs to disk. Each chunk becomes a directory containing the record data, an index for fast lookups, and the record attributes. Sealed chunks are memory-mapped for efficient reads.

| Param | Description | Default |
|-------|-------------|---------|
| `dir` | Store directory (required) | |
| `maxChunkBytes` | Soft size limit per chunk | `64MB` |
| `maxChunkAge` | Maximum wall-clock age before rotation | None |
| `fileMode` | Unix file permissions (octal) | `0644` |

## What You Should Know

- The `dir` you configure is entirely yours to choose — it's not derived from any global setting
- Only one process can open a store directory at a time (enforced by a lock file)
- If GastroLog crashes, it recovers on restart — at most the last partially-written record is lost
- Maximum chunk size is **4 GB** (32-bit offsets in the file format)
