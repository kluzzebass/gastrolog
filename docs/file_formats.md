# GastroLog File Formats

All multi-byte integers are **little-endian**. GLIDs are stored as raw 16-byte values (UUIDv7-shaped; see "IDs: GLID and encoding" below). Timestamps are stored as **int64 Unix nanoseconds**.

## Directory Layout

```
<home>/
  node_id                   Persistent GLID (UUIDv7-shaped) node identity
  node_name                 Human-readable petname
  config.json / config.db   Config store (format depends on store type)
  gastrolog.sock            Unix domain socket for local CLI access
  cluster-tls.json          Cluster mTLS certificates and join token

  raft/
    raft.db                 BoltDB: Raft log + stable store
    snapshots/              Raft file snapshot store

  stores/                   Per-vault data ("stores" for backward compat)
    <vault-id>/
      .lock                 Exclusive lock file for vault directory
      cloud.idx             B+ tree cache of cloud chunk metadata (if cloud enabled)

      <chunk-id>/
        raw.log             Raw log bytes (append-only)
        idx.log             Record metadata entries (append-only)
        attr.log            Record attributes (append-only)
        attr_dict.log       Attribute string dictionary (append-only)
        ingest.bt           B+ tree: IngestTS → record position (active only)
        source.bt           B+ tree: SourceTS → record position (active only)
        ingest.idx          IngestTS timestamp index (sealed only)
        source.idx          SourceTS timestamp index (sealed only)
        token.idx           Token inverted index (sealed only)
        json.idx            Structural JSON index (sealed only)
        attr_key.idx        Attribute key inverted index (sealed only)
        attr_val.idx        Attribute value inverted index (sealed only)
        attr_kv.idx         Attribute key-value pair index (sealed only)
        kv_key.idx          Heuristic KV key inverted index (sealed only)
        kv_val.idx          Heuristic KV value inverted index (sealed only)
        kv_kv.idx           Heuristic KV pair index (sealed only)

  managed-files/
    <file-id>/
      <filename>            Uploaded managed files (lookups, MMDB, etc.)
```

### Identifiers

All entity identifiers (vault, chunk, file, node, etc.) are **GLIDs**: UUIDv7 values encoded as 26-character lowercase base32hex strings (RFC 4648, no padding). The base32hex alphabet (`0-9a-v`) preserves lexicographic sort order, so directory listings are naturally ordered by creation time. On the proto wire format, GLIDs are raw 16-byte values.

| Placeholder     | Description                                                  |
|-----------------|--------------------------------------------------------------|
| `<home>`        | GastroLog data directory (set via `--home` flag)             |
| `<vault-id>`    | GLID of the vault that owns the chunks                       |
| `<chunk-id>`    | GLID of the chunk (UUIDv7, time-ordered)                     |
| `<file-id>`     | GLID of the managed file                                     |

### Notes

- `stores/` is the vault container directory (name kept for backward compatibility with existing data).
- Each chunk is self-contained: all data needed to reconstruct records is stored within the chunk directory.
- **Active chunks** have B+ tree indexes (`*.bt`) for mutable writes. **Sealed chunks** replace them with flat sorted indexes (`*.idx`) optimized for read-only access.
- Managed files (under `managed-files/`) are user-uploaded data files used by lookup tables, MMDB databases, etc. They are replicated across cluster nodes via the Raft-managed file system.

## Common Header Pattern

All binary files share a common 4-byte header prefix:

| Field     | Size | Description                              |
|-----------|------|------------------------------------------|
| signature | 1    | Always `0x69` (`'i'`)                    |
| type      | 1    | File type (see table below)                      |
| version   | 1    | Format version (file-type specific)      |
| flags     | 1    | Bit flags (file-type specific)           |

### Type Codes

| Byte   | Char | File              |
|--------|------|-------------------|
| `0x72` | `r`  | raw.log           |
| `0x69` | `i`  | idx.log           |
| `0x61` | `a`  | attr.log          |
| `0x64` | `d`  | attr_dict.log     |
| `0x62` | `b`  | ingest.bt / source.bt |
| `0x67` | `g`  | cloud blob (GLCB) |
| `0x49` | `I`  | ingest.idx        |
| `0x73` | `s`  | source.idx        |
| `0x6B` | `k`  | token.idx         |
| `0x4A` | `J`  | json.idx          |
| `0x4B` | `K`  | attr_key.idx      |
| `0x56` | `V`  | attr_val.idx      |
| `0x50` | `P`  | attr_kv.idx       |
| `0x4D` | `M`  | kv_kv.idx         |
| `0x4E` | `N`  | kv_key.idx        |
| `0x4F` | `O`  | kv_val.idx        |
| `0x4C` | `L`  | lookup table      |

### Flags

| Bit | Mask   | Meaning                                              |
|-----|--------|------------------------------------------------------|
| 0   | `0x01` | Sealed (data files) / Complete (index files)         |
| 1   | `0x02` | Compressed (data files only)                         |

---

## raw.log -- Raw Log Bytes

Append-only file containing concatenated raw log message bytes. The chunk ID is derived from the directory name (authoritative).

### Layout

```
+---------------------------+
|     Header (4 bytes)      |
+---------------------------+
|     Raw log bytes         |
|     (concatenated)        |
+---------------------------+
```

### Header (4 bytes)

| Offset | Size | Field     | Description                              |
|--------|------|-----------|------------------------------------------|
| 0      | 1    | signature | `0x69` (`'i'`)                           |
| 1      | 1    | type      | `0x72` (`'r'`)                           |
| 2      | 1    | version   | `0x01`                                   |
| 3      | 1    | flags     | Bit 0: sealed (`0x01` = sealed)          |

### Data Section

Raw log bytes are concatenated with no framing. The offset and size of each record's raw data is stored in the corresponding idx.log entry.

**Maximum file size: 4 GB** (limited by uint32 rawOffset field in idx.log entries)

---

## attr.log -- Record Attributes

Append-only file containing encoded attribute records. Each record's attributes are stored as a length-prefixed key-value sequence.

### Layout

```
+---------------------------+
|     Header (4 bytes)      |
+---------------------------+
|   Attribute Record 0      |
+---------------------------+
|   Attribute Record 1      |
+---------------------------+
|     ...                   |
+---------------------------+
|   Attribute Record N-1    |
+---------------------------+
```

### Header (4 bytes)

| Offset | Size | Field     | Description                              |
|--------|------|-----------|------------------------------------------|
| 0      | 1    | signature | `0x69` (`'i'`)                           |
| 1      | 1    | type      | `0x61` (`'a'`)                           |
| 2      | 1    | version   | `0x01`                                   |
| 3      | 1    | flags     | Bit 0: sealed (`0x01` = sealed)          |

### Attribute Record (variable size)

Each attribute record encodes a set of key-value string pairs:

| Offset | Size      | Field    | Description                              |
|--------|-----------|----------|------------------------------------------|
| 0      | 2         | count    | Number of key-value pairs (uint16)       |
| 2      | varies    | pairs    | Repeated key-value entries               |

Each key-value pair:

| Offset | Size      | Field    | Description                              |
|--------|-----------|----------|------------------------------------------|
| 0      | 2         | keyLen   | Length of key string (uint16)            |
| 2      | keyLen    | key      | Key string bytes (UTF-8)                 |
| 2+K    | 2         | valLen   | Length of value string (uint16)          |
| 4+K    | valLen    | value    | Value string bytes (UTF-8)               |

The pairs are repeated `count` times. An empty attribute set (count=0) uses 2 bytes.

**Maximum file size: 4 GB** (limited by uint32 attrOffset field in idx.log entries)

**Maximum record attributes: 64 KB** (limited by uint16 attrSize field in idx.log entries)

---

## attr_dict.log -- Attribute String Dictionary

Per-chunk dictionary mapping strings to sequential uint32 IDs. Used by `attr.log` to deduplicate attribute keys and values. When the dictionary is present, `attr.log` records use the dict-encoded format (keyID/valID pairs) instead of inline key/value strings.

### Layout

```
+---------------------------+
|     Header (4 bytes)      |
+---------------------------+
|     Entry 0               |
+---------------------------+
|     Entry 1               |
+---------------------------+
|     ...                   |
+---------------------------+
|     Entry N-1             |
+---------------------------+
```

### Header (4 bytes)

| Offset | Size | Field     | Description                              |
|--------|------|-----------|------------------------------------------|
| 0      | 1    | signature | `0x69` (`'i'`)                           |
| 1      | 1    | type      | `0x64` (`'d'`)                           |
| 2      | 1    | version   | `0x01`                                   |
| 3      | 1    | flags     | Bit 0: sealed (`0x01` = sealed)          |

### Entry (variable size)

| Offset | Size      | Field    | Description                    |
|--------|-----------|----------|--------------------------------|
| 0      | 2         | strLen   | Length of string (uint16)      |
| 2      | strLen    | string   | UTF-8 string bytes             |

Entries are appended sequentially. The entry's array index (0, 1, 2, ...) is its dictionary ID. A partial trailing entry is tolerated on crash recovery.

### Dict-Encoded Attribute Record

When `attr_dict.log` is present, each `attr.log` record uses this format instead of inline strings:

| Offset | Size        | Field     | Description                          |
|--------|-------------|-----------|--------------------------------------|
| 0      | 2           | count     | Number of key-value pairs (uint16)   |
| 2      | count × 8   | pairs     | [keyID:u32][valID:u32] repeated      |

Keys are sorted lexicographically for deterministic output.

---

## idx.log -- Record Metadata Index

Append-only file containing fixed-size metadata entries for each record. The chunk ID is derived from the directory name (authoritative).

### Layout

```
+---------------------------+
|     Header (12 bytes)     |
+---------------------------+
|     Entry 0 (58 bytes)    |
+---------------------------+
|     Entry 1 (58 bytes)    |
+---------------------------+
|     ...                   |
+---------------------------+
|     Entry N-1 (58 bytes)  |
+---------------------------+
```

### Header (12 bytes)

| Offset | Size | Field     | Description                              |
|--------|------|-----------|------------------------------------------|
| 0      | 1    | signature | `0x69` (`'i'`)                           |
| 1      | 1    | type      | `0x69` (`'i'`)                           |
| 2      | 1    | version   | `0x02`                                   |
| 3      | 1    | flags     | Bit 0: sealed (`0x01` = sealed)          |
| 4      | 8    | createdAt | Chunk creation timestamp (int64 Unix nanos) |

### Entry (58 bytes each)

| Offset | Size | Field         | Description                                   |
|--------|------|---------------|-----------------------------------------------|
| 0      | 8    | sourceTS      | Source timestamp (int64 Unix nanos, 0 if unknown) |
| 8      | 8    | ingestTS      | Ingest timestamp (int64 Unix nanos)           |
| 16     | 8    | writeTS       | Write timestamp (int64 Unix nanos)            |
| 24     | 4    | rawOffset     | Byte offset into raw.log data section (uint32)|
| 28     | 4    | rawSize       | Length of raw data in bytes (uint32)          |
| 32     | 4    | attrOffset    | Byte offset into attr.log data section (uint32)|
| 36     | 2    | attrSize      | Length of encoded attributes in bytes (uint16)|
| 38     | 4    | ingestSeq     | Per-ingester rolling sequence counter (uint32)|
| 42     | 16   | ingesterID    | Ingester GLID (raw bytes)                     |

### Position Semantics

Record positions throughout the system are **record indices** (0, 1, 2, ...), not byte offsets. To compute the file offset for record N:

```
idx_file_offset = 12 + (N * 58)
```

This enables O(1) seeking by record number and trivial bidirectional traversal.

### Deriving ChunkMeta

The `ChunkMeta` is derived from idx.log without a separate metadata file:

| ChunkMeta Field | Source                                      |
|-----------------|---------------------------------------------|
| ID              | Directory name (authoritative)              |
| StartTS         | Entry 0, writeTS field                      |
| EndTS           | Last entry, writeTS field                   |
| Sealed          | flags byte in header (bit 0)                |

---

## ingest.idx / source.idx -- Timestamp Indexes

Sorted timestamp indexes mapping IngestTS or SourceTS to record positions within a chunk. Only built for sealed chunks. Both files share the same format; they differ only in the type byte (`'I'` for ingest, `'s'` for source).

### Layout

```
+---------------------------+
|     Header (8 bytes)      |
+---------------------------+
|     Entry 0 (12 bytes)    |
+---------------------------+
|     Entry 1               |
+---------------------------+
|     ...                   |
+---------------------------+
|     Entry N-1             |
+---------------------------+
```

### Header (8 bytes)

| Offset | Size | Field      | Description                                       |
|--------|------|------------|---------------------------------------------------|
| 0      | 1    | signature  | `0x69` (`'i'`)                                    |
| 1      | 1    | type       | `0x49` (`'I'`) for ingest, `0x73` (`'s'`) for source |
| 2      | 1    | version    | `0x01`                                            |
| 3      | 1    | flags      | Bit 0: complete (`0x01`)                          |
| 4      | 4    | entryCount | Number of index entries (uint32)                  |

### Entry (12 bytes each)

| Offset | Size | Field     | Description                          |
|--------|------|-----------|--------------------------------------|
| 0      | 8    | timestamp | Timestamp (int64 Unix nanos)         |
| 8      | 4    | recordPos | Record index (uint32)                |

**Total file size: 8 + (entryCount × 12) bytes**

Entries are sorted by timestamp for binary search. Used by the query engine to seek to the first record at or after a given time.

---

## token.idx -- Token Index

Inverted index mapping each token to the list of record indices where that token appears within a chunk. Only built for sealed chunks.

### Tokenization Rules

Tokens are extracted with the following rules:

- **Valid characters**: ASCII only: a-z, A-Z (lowercased), 0-9, underscore, hyphen
- **Length**: 2-16 bytes (configurable max)
- **Excluded**: Numeric tokens (decimal, hex, octal, binary), hex-with-hyphens patterns
- **Delimiter**: Any byte outside the valid character set (including high bytes ≥0x80)

Each record is deduplicated: a token appears at most once per record in the posting list.

### Layout

```
+---------------------------+
|          Header           |
+---------------------------+
|     Key Table             |
|  (keyCount entries)       |
+---------------------------+
|     Posting Blob          |
|  (flat uint32 positions)  |
+---------------------------+
```

### Header (24 bytes)

| Offset | Size | Field    | Description                        |
|--------|------|----------|------------------------------------|
| 0      | 1    | signature| `0x69` (`'i'`)                     |
| 1      | 1    | type     | `0x6B` (`'k'`)                     |
| 2      | 1    | version  | `0x01`                             |
| 3      | 1    | flags    | `0x00` (reserved)                  |
| 4      | 16   | chunkID  | Chunk GLID (raw bytes)             |
| 20     | 4    | keyCount | Number of distinct tokens (uint32) |

### Key Table Entry (variable size)

| Offset | Size      | Field         | Description                                  |
|--------|-----------|---------------|----------------------------------------------|
| 0      | 2         | tokenLen      | Length of token string (uint16)              |
| 2      | tokenLen  | token         | Token string (UTF-8 bytes, lowercased)       |
| 2+N    | 4         | postingOffset | Byte offset into posting blob (uint32)       |
| 6+N    | 4         | postingCount  | Number of positions for this token (uint32)  |

Key entries are sorted by token string for deterministic output and binary search.

### Posting Blob

Flat array of `uint32` record indices (4 bytes each). Each key entry references a contiguous slice of this blob via `postingOffset` (byte offset from the start of the posting blob) and `postingCount` (number of indices).

**Total file size: 24 + (sum of key entry sizes) + (totalIndices x 4) bytes**

---

## ingest.bt / source.bt -- B+ Tree Indexes

File-backed B+ trees mapping timestamps to record positions within the active chunk. `ingest.bt` indexes IngestTS; `source.bt` indexes SourceTS. Both use the same on-disk format.

These indexes exist **only while the chunk is active** — they are deleted at seal time. Sealed chunks use `_time.idx` and `_token.idx` instead.

### Page Structure

All data is organized in fixed **4096-byte pages**. Page 0 is the meta page; page 1+ are tree nodes.

**Total file size: nextPage × 4096 bytes**

### Meta Page (page 0, 4096 bytes)

| Offset | Size | Field     | Description                              |
|--------|------|-----------|------------------------------------------|
| 0      | 1    | signature | `0x69` (`'i'`)                           |
| 1      | 1    | type      | `0x62` (`'b'`)                           |
| 2      | 1    | version   | `0x01`                                   |
| 3      | 1    | flags     | `0x00` (reserved)                        |
| 4      | 4    | root      | Page number of the root node (uint32)    |
| 8      | 8    | count     | Total number of entries (uint64)         |
| 16     | 2    | height    | Tree height, 1 = root is a leaf (uint16) |
| 18     | 4    | nextPage  | Next free page number (uint32)           |
| 22     | 2    | keySize   | Encoded key size in bytes (uint16)       |
| 24     | 2    | valSize   | Encoded value size in bytes (uint16)     |

Remaining bytes (26–4095) are zero-padded.

### Leaf Node Page

| Offset | Size                | Field    | Description                              |
|--------|---------------------|----------|------------------------------------------|
| 0      | 1                   | type     | `0x01` (leaf)                            |
| 1      | 2                   | count    | Number of entries (uint16)               |
| 3      | 4                   | nextLeaf | Page number of next leaf (uint32, 0 = none) |
| 7      | 4                   | prevLeaf | Page number of previous leaf (uint32, 0 = none) |
| 11     | count × (K + V)     | entries  | Key-value pairs, sorted                  |

Leaf nodes form a doubly-linked list for efficient range scans.

### Internal Node Page

| Offset | Size                | Field    | Description                              |
|--------|---------------------|----------|------------------------------------------|
| 0      | 1                   | type     | `0x02` (internal)                        |
| 1      | 2                   | count    | Number of keys (uint16)                  |
| 3      | count × K           | keys     | Separator keys, sorted                   |
| varies | (count + 1) × 4     | children | Child page numbers (uint32)              |

### Codec: Int64Uint32

Both `ingest.bt` and `source.bt` use `Int64Uint32`:

| Field | Size | Encoding | Description |
|-------|------|----------|-------------|
| Key   | 8    | int64 LE | Timestamp as Unix nanoseconds |
| Value | 4    | uint32 LE| Record position (index into idx.log) |

**Leaf capacity:** (4096 − 11) / 12 = **340 entries per leaf**

**Internal capacity:** (4096 − 3 − 4) / (8 + 4) = **340 keys per internal node**

Duplicate keys are allowed; entries with equal keys are secondarily ordered by value.

---

## Cloud Blob -- Archived Chunk

Single-object format for cloud-archived (sealed) chunks. Each chunk becomes one blob in S3/Azure/GCS with an uncompressed header/dictionary/index prefix followed by seekable zstd record data, enabling O(1) random access to any record.

Sorted IngestTS and SourceTS indexes are embedded after the seekable zstd section, with a TOC footer. This enables time-range seeking via S3 range requests without downloading the full blob.

### Layout

```
Uncompressed prefix:
+---------------------------+
|     Header (96 bytes)     |
+---------------------------+
|     Dictionary            |
|   (dictEntries entries)   |
+---------------------------+
|     Record Index          |
|   (recordCount × 12)     |
+---------------------------+
Seekable zstd section:
+---------------------------+
|     Compressed Record     |
|     Frames + Seek Table   |
+---------------------------+
TS indexes + TOC:
+---------------------------+
|   IngestTS Index          |
|   (recordCount × 12)     |
+---------------------------+
|   SourceTS Index          |
|   (N × 12, N ≤ records)  |
+---------------------------+
|     TOC (48 bytes)        |
+---------------------------+
```

### Header (96 bytes)

| Offset | Size | Field        | Description                                  |
|--------|------|--------------|----------------------------------------------|
| 0      | 1    | signature    | `0x69` (`'i'`)                               |
| 1      | 1    | type         | `0x67` (`'g'`)                               |
| 2      | 1    | version      | `0x01`                                       |
| 3      | 1    | flags        | Reserved (`0x00`)                            |
| 4      | 16   | chunkID      | Chunk GLID (raw bytes)                       |
| 20     | 16   | vaultID      | Vault GLID (raw bytes)                       |
| 36     | 4    | recordCount  | Total records (uint32)                       |
| 40     | 8    | writeStart   | Min WriteTS (int64 Unix nanos)               |
| 48     | 8    | writeEnd     | Max WriteTS (int64 Unix nanos)               |
| 56     | 8    | ingestStart  | Min IngestTS (int64 Unix nanos)              |
| 64     | 8    | ingestEnd    | Max IngestTS (int64 Unix nanos)              |
| 72     | 8    | sourceStart  | Min SourceTS (int64 Unix nanos, 0 = none)    |
| 80     | 8    | sourceEnd    | Max SourceTS (int64 Unix nanos, 0 = none)    |
| 88     | 4    | dictEntries  | Number of dictionary entries (uint32)        |
| 92     | 4    | dictSize     | Total bytes of dictionary section (uint32)   |

### Dictionary (variable size)

Shared string table for attribute keys and values, identical to `attr_dict.log` encoding:

| Offset | Size      | Field    | Description                    |
|--------|-----------|----------|--------------------------------|
| 0      | 2         | strLen   | Length of string (uint16)      |
| 2      | strLen    | string   | UTF-8 string bytes             |

Repeated `dictEntries` times. Entry index (0, 1, 2, ...) is the dictionary ID.

### Record Index (recordCount × 12 bytes)

Flat array of offset/size pairs enabling O(1) random access to any record:

| Offset | Size | Field         | Description                                         |
|--------|------|---------------|-----------------------------------------------------|
| 0      | 8    | offset        | Byte offset into decompressed record data (uint64)  |
| 8      | 4    | size          | Frame size excluding the u32 frameLen prefix (uint32)|

### Record Frame (variable size)

Each record is self-framing:

| Offset | Size        | Field       | Description                              |
|--------|-------------|-------------|------------------------------------------|
| 0      | 4           | frameLen    | Frame size excluding this field (uint32) |
| 4      | 8           | sourceTS    | Source timestamp (int64 nanos, 0 = none) |
| 12     | 8           | ingestTS    | Ingest timestamp (int64 nanos)           |
| 20     | 8           | writeTS     | Write timestamp (int64 nanos)            |
| 28     | 16          | ingesterID  | Ingester GLID (raw bytes)                |
| 44     | 4           | ingestSeq   | Per-ingester sequence counter (uint32)   |
| 48     | 2           | attrCount   | Number of attribute pairs (uint16)       |
| 50     | attrCount×8 | attrs       | [keyID:u32][valID:u32] pairs             |
| varies | 4           | rawLen      | Length of raw log body (uint32)          |
| varies | rawLen      | raw         | Raw log message bytes                    |

### Compression

Record data is compressed with seekable zstd (~256KB independent frames). The uncompressed prefix (header, dictionary, record index) is stored in the clear so metadata can be read without decompression. The seekable format enables random-access reads: each frame can be independently decompressed, so reading a single record only fetches the frame(s) containing it.

### Embedded TS Indexes

Two sorted timestamp indexes are appended after the seekable zstd section. Each index uses the same 12-byte entry format as `ingest.idx`/`source.idx`:

| Offset | Size | Field     | Description                          |
|--------|------|-----------|--------------------------------------|
| 0      | 8    | timestamp | Timestamp (int64 Unix nanos)         |
| 8      | 4    | recordPos | Record index (uint32)                |

- **IngestTS Index**: One entry per record, sorted by IngestTS. Size = `recordCount × 12` bytes.
- **SourceTS Index**: One entry per record with non-zero SourceTS, sorted by SourceTS. Size = `N × 12` bytes where N ≤ recordCount.

No header — the TOC provides section offsets and sizes. Entry count is derived from `sectionSize / 12`.

### TOC Footer (48 bytes)

The Table of Contents identifies embedded TS index sections for range-request access:

| Offset | Size | Field           | Description                                  |
|--------|------|-----------------|----------------------------------------------|
| 0      | 4    | magic           | `"GTOC"` (ASCII)                             |
| 4      | 4    | tocVersion      | `1` (uint32)                                 |
| 8      | 8    | ingestIdxOffset | Byte offset of IngestTS index (uint64)       |
| 16     | 8    | ingestIdxSize   | Byte size of IngestTS index (uint64)         |
| 24     | 8    | sourceIdxOffset | Byte offset of SourceTS index (uint64)       |
| 32     | 8    | sourceIdxSize   | Byte size of SourceTS index (uint64)         |
| 40     | 8    | reserved        | Reserved (zero)                              |

The TOC is always the last 48 bytes of the blob. Offsets are absolute (from blob start), enabling direct S3/GCS range requests to fetch individual index sections.

### Blob Object Metadata

When uploaded, the following user-defined metadata is set on the cloud object for filtering without download:

| Key            | Value                              |
|----------------|------------------------------------|
| `chunk_id`     | 26-char base32hex chunk ID         |
| `vault_id`     | GLID string (26-char base32hex)    |
| `record_count` | Decimal string                     |
| `start_ts`     | RFC 3339 timestamp                 |
| `end_ts`       | RFC 3339 timestamp                 |

---

## Lookup Table -- Binary Lookup Index

Sorted binary format for O(log n) key lookups with zero heap-allocated index. Used by JSON file and CSV lookup tables. The source data (JSON + jq transform, or CSV) is parsed once at load time and encoded into this format. The file is memory-mapped read-only for the lifetime of the lookup table.

### Layout

```
+---------------------------+
|     Header (20 bytes)     |
+---------------------------+
|     Column Names          |
+---------------------------+
|     Key Offset Table      |
+---------------------------+
|     Key Data (sorted)     |
+---------------------------+
|     Value Data            |
+---------------------------+
```

### Header (20 bytes)

| Offset | Size | Field           | Description                                  |
|--------|------|-----------------|----------------------------------------------|
| 0      | 1    | signature       | `0x69` (`'i'`)                               |
| 1      | 1    | type            | `0x4C` (`'L'`)                               |
| 2      | 1    | version         | `0x01`                                       |
| 3      | 1    | flags           | Bit 0: complete (`0x01`)                     |
| 4      | 4    | numRows         | Number of deduplicated rows (uint32)         |
| 8      | 4    | numCols         | Number of value columns (uint32)             |
| 12     | 4    | keyOffTblOffset | Byte offset of key offset table (uint32)     |
| 16     | 4    | keyDataOffset   | Byte offset of key data section (uint32)     |

### Column Names (starts at byte 20)

Value column names in sorted order. Read once at load time for `Suffixes()`.

```
For each of numCols columns:
    [uint16 nameLen][nameLen bytes]
```

### Key Offset Table (at keyOffTblOffset)

Fixed-size array enabling O(1) access to the nth key during binary search.

```
For each of numRows entries:
    [uint32 keyDataEntryOffset]   (relative to keyDataOffset)
```

Total size: `numRows x 4` bytes.

### Key Data (at keyDataOffset, sorted lexicographically by key)

```
For each key:
    [uint16 keyLen][keyLen bytes][uint32 valueDataOffset]
```

Keys are stored in lexicographic (byte) order. The `valueDataOffset` is an absolute byte offset into the file where this row's value data begins. It is stored adjacent to the key so that after a binary search comparison, the value pointer is in the same cache line.

Duplicate keys are removed by the encoder (first occurrence wins). Empty keys are skipped.

### Value Data

```
For each row (in sorted key order):
    For each of numCols columns:
        [uint16 valLen][valLen bytes]
```

All value columns are stored contiguously per row. The reader knows `numCols` from the header and reads that many length-prefixed strings sequentially.

### Lookup Algorithm

1. Binary search the key offset table using `sort.Search`.
2. For each probe at index `mid`: read `keyOffTblOffset + mid*4` to get the key entry offset. Jump to `keyDataOffset + entryOffset`. Read the uint16 key length, then compare key bytes.
3. On match: read the uint32 `valueDataOffset` that follows the key bytes. Jump to value data. Read `numCols` length-prefixed strings. Return as `map[string]string`.

O(log n) comparisons. Each comparison requires two reads from the mmap (offset table entry + key bytes). Zero heap allocation for the search; only the result map is allocated per lookup.

### String Length Limit

All strings (column names, keys, values) use uint16 length prefixes: maximum 65535 bytes per string. The encoder rejects strings exceeding this limit.

---

## Validation Summary

All file formats include validation checks on decode:

| File           | Checks                                                         |
|----------------|----------------------------------------------------------------|
| raw.log        | Min size (4 bytes), signature, type, version                   |
| idx.log        | Min size (12 bytes), signature, type, version, entry alignment |
| attr.log       | Min size (4 bytes), signature, type, version                   |
| attr_dict.log  | Min size (4 bytes), signature, type, version                   |
| ingest.bt      | Signature+type, version, codec size match on reopen            |
| source.bt      | Signature+type, version, codec size match on reopen            |
| ingest.idx     | Min size, signature+type, version, complete flag               |
| source.idx     | Min size, signature+type, version, complete flag               |
| token.idx      | Min size, signature+type, version, key size, posting size      |
| json.idx       | Min size, signature+type, version, complete flag, status byte  |
| attr_*.idx     | Min size, signature+type, version, complete flag               |
| kv_*.idx       | Min size, signature+type, version, complete flag, status byte  |
| cloud blob     | Signature+type, version, seekable zstd, TOC magic              |
| lookup table   | Min size (20), signature+type, version, complete flag          |
