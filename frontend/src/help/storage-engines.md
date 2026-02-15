# Storage Engines

GastroLog supports two storage engine types. Each store is configured with one engine that manages its chunks.

## File Store

Type: `file`

Disk-backed storage using memory-mapped files for sealed chunks. This is the production engine.

### Directory Layout

```
<datadir>/stores/<store-uuid>/
  <chunk-id>/
    raw.log    # Record payloads
    idx.log    # Record metadata (timestamps, offsets)
    attr.log   # Key-value attributes per record
```

Each chunk is a directory containing three files with a shared binary header format (signature, type, version, flags).

### How It Works

- **Active chunks**: Written sequentially via append operations
- **Sealed chunks**: Memory-mapped for fast random access during queries
- **Atomic writes**: Uses temp-file-then-rename to prevent corruption
- **Directory locking**: One writer per chunk at a time

### Parameters

| Param | Description |
|-------|-------------|
| `datadir` | Base directory for store data |

### Limits

File offsets are 32-bit unsigned integers, giving a hard limit of 4 GB per file (`raw.log`, `attr.log`). The rotation policy should be configured to seal chunks well before this limit. A built-in hard-limit policy enforces this as a safety net.

## Memory Store

Type: `memory`

In-memory storage for testing and ephemeral data. Same interface as the file store but nothing is persisted to disk.

### Parameters

| Param | Description |
|-------|-------------|
| `maxRecords` | Maximum records before rotation (default varies) |

### Trade-offs

- Fast — no disk I/O
- Data lost on restart
- Suitable for development, testing, and short-lived staging environments
