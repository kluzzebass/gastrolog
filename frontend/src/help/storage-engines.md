# Storage Engines

GastroLog supports two storage engine types. Each store is configured with one engine that manages its chunks.

## File Store

Type: `file`

Disk-backed storage using a split-file format per chunk. This is the production engine.

### Directory Layout

The `dir` parameter points to the store's data directory. Within it:

```
<dir>/
  .lock                        # Exclusive process lock
  <chunk-id>/                  # 26-char base32hex, time-ordered
    raw.log                    # Record payloads (concatenated)
    idx.log                    # Record index (38-byte fixed entries)
    attr.log                   # Encoded key-value attributes
```

Each chunk is a directory containing three append-only files. The directory name is the chunk's UUIDv7 identifier, which is time-ordered so chunks sort chronologically by name.

### Split-File Format

All three files share a 4-byte header: signature (`0x69`), type byte, version, and flags. The sealed flag (bit 0 of the flags byte) is set permanently when a chunk is sealed.

**raw.log** stores the raw byte payloads of each record, concatenated end-to-end with no delimiters. Individual records are located by offset and size from the index.

**idx.log** stores a fixed-size 38-byte entry per record containing three timestamps (SourceTS, IngestTS, WriteTS as int64 nanoseconds), the byte offset and size in raw.log (uint32), and the byte offset and size in attr.log (uint32 + uint16). Because entries are fixed-size, any record can be located by index in constant time.

**attr.log** stores encoded attribute maps for each record. Each entry is a count followed by length-prefixed key-value pairs, sorted lexicographically by key. Maximum encoded size per record is 65,535 bytes.

### Sealed vs Active Chunks

- **Active chunks** are written sequentially via standard file I/O. Only one chunk per store is active at a time.
- **Sealed chunks** are memory-mapped for fast random-access reads during queries. The query engine can binary-search the idx.log to find records by timestamp without scanning.

### Crash Recovery

On startup, the file store recovers from incomplete writes:

- If raw.log or attr.log are larger than expected (computed from idx.log entries), they are truncated to the expected size. The last partially-written record is lost, but all prior records remain intact.
- If multiple unsealed chunks exist (e.g., from a crash during rotation), all but the newest are sealed automatically.

### Process Locking

A `.lock` file in the store directory prevents multiple processes from writing to the same store simultaneously. If the lock cannot be acquired, the store fails to open with a clear error.

### Hard Limits

Raw.log and attr.log use 32-bit unsigned offsets, giving a hard limit of **4 GB per file**. A built-in hard-limit rotation policy enforces this as a non-negotiable safety net — the chunk is sealed before any append would exceed the offset range. Configure your rotation policy to seal chunks well before this limit.

### Parameters

| Param | Description | Default |
|-------|-------------|---------|
| `dir` | Data directory for this store (required) | |
| `maxChunkBytes` | Soft size limit per chunk | `64MB` |
| `maxChunkAge` | Maximum wall-clock age before rotation | None |
| `fileMode` | Unix file permissions (octal) | `0644` |

## Memory Store

Type: `memory`

In-memory storage for testing and ephemeral data. Implements the same interface as the file store but nothing is persisted to disk.

### Parameters

| Param | Description | Default |
|-------|-------------|---------|
| `maxRecords` | Maximum records before rotation | `10000` |

### Trade-offs

- Fast — no disk I/O
- Data lost on restart
- Suitable for development, testing, and short-lived demo environments
