# GastroLog File Formats

All multi-byte integers are **little-endian**. UUIDs are stored as raw 16-byte values. Timestamps are stored as **int64 Unix nanoseconds**.

## Directory Layout

```
<home>/
  <chunk-uuid>/
    raw.log             Raw log bytes (append-only)
    idx.log             Record metadata entries (append-only)
    attr.log            Record attributes (append-only)
    attr_dict.log       Attribute string dictionary (append-only)
    _time.idx           Sparse time index
    _token.idx          Token index (inverted posting list)
```

Each chunk has its own subdirectory named by its UUID. Chunks are self-contained: all data needed to reconstruct records is stored within the chunk directory.

## Common Header Pattern

All binary files share a common 4-byte header prefix:

| Field     | Size | Description                              |
|-----------|------|------------------------------------------|
| signature | 1    | Always `0x69` (`'i'`)                    |
| type      | 1    | File type: `'r'` raw, `'i'` idx, `'a'` attr, `'t'` time, `'k'` token |
| version   | 1    | Format version (file-type specific)      |
| flags     | 1    | Bit flags (file-type specific)           |

### Flags

| Bit | Mask   | Meaning |
|-----|--------|---------|
| 0   | `0x01` | Sealed  |

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
| 42     | 16   | ingesterID    | Ingester UUID (raw bytes)                     |

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

## _time.idx -- Time Index

Sparse time index mapping sampled timestamps to record indices within a chunk. Only built for sealed chunks.

### Layout

```
+---------------------------+
|          Header           |
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

### Header (24 bytes)

| Offset | Size | Field      | Description                        |
|--------|------|------------|------------------------------------|
| 0      | 1    | signature  | `0x69` (`'i'`)                     |
| 1      | 1    | type       | `0x74` (`'t'`)                     |
| 2      | 1    | version    | `0x01`                             |
| 3      | 1    | flags      | `0x00` (reserved)                  |
| 4      | 16   | chunkID    | Chunk UUID (raw bytes)             |
| 20     | 4    | entryCount | Number of index entries (uint32)   |

### Entry (12 bytes each)

| Offset | Size | Field     | Description                          |
|--------|------|-----------|--------------------------------------|
| 0      | 8    | timestamp | Record timestamp (int64 Unix nanos) |
| 8      | 4    | recordPos | Record index (uint32)                |

**Total file size: 24 + (entryCount x 12) bytes**

Entries are written in record order (the order records appear in idx.log). A sparsity parameter controls how many records are sampled: with sparsity N, every N-th record is indexed (plus the first record always).

---

## _token.idx -- Token Index

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
| 4      | 16   | chunkID  | Chunk UUID (raw bytes)             |
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

## Cloud Blob -- Archived Chunk

Single-object format for cloud-archived (sealed) chunks. Each chunk becomes one zstd-compressed blob in S3/Azure/GCS. Designed for streaming reads — no random access.

### Layout

```
[Zstd-compressed envelope]
+---------------------------+
|     Header (94 bytes)     |
+---------------------------+
|     Dictionary            |
|   (dictEntries entries)   |
+---------------------------+
|     Record Frame 0        |
+---------------------------+
|     Record Frame 1        |
+---------------------------+
|     ...                   |
+---------------------------+
|     Record Frame N-1      |
+---------------------------+
```

### Header (94 bytes)

| Offset | Size | Field        | Description                                  |
|--------|------|--------------|----------------------------------------------|
| 0      | 4    | magic        | `GLCB` (GastroLog Cloud Blob)                |
| 4      | 1    | version      | `0x01`                                       |
| 5      | 1    | flags        | Reserved (`0x00`)                            |
| 6      | 16   | chunkID      | Chunk UUID (raw bytes)                       |
| 22     | 16   | vaultID      | Vault UUID (raw bytes)                       |
| 38     | 4    | recordCount  | Total records (uint32)                       |
| 42     | 8    | startTS      | Min WriteTS (int64 Unix nanos)               |
| 50     | 8    | endTS        | Max WriteTS (int64 Unix nanos)               |
| 58     | 8    | ingestStart  | Min IngestTS (int64 Unix nanos)              |
| 66     | 8    | ingestEnd    | Max IngestTS (int64 Unix nanos)              |
| 74     | 8    | sourceStart  | Min SourceTS (int64 Unix nanos, 0 = none)    |
| 82     | 8    | sourceEnd    | Max SourceTS (int64 Unix nanos, 0 = none)    |
| 90     | 4    | dictEntries  | Number of dictionary entries (uint32)        |

### Dictionary (variable size)

Shared string table for attribute keys and values, identical to `attr_dict.log` encoding:

| Offset | Size      | Field    | Description                    |
|--------|-----------|----------|--------------------------------|
| 0      | 2         | strLen   | Length of string (uint16)      |
| 2      | strLen    | string   | UTF-8 string bytes             |

Repeated `dictEntries` times. Entry index (0, 1, 2, ...) is the dictionary ID.

### Record Frame (variable size)

Each record is self-framing:

| Offset | Size        | Field       | Description                              |
|--------|-------------|-------------|------------------------------------------|
| 0      | 4           | frameLen    | Frame size excluding this field (uint32) |
| 4      | 8           | sourceTS    | Source timestamp (int64 nanos, 0 = none) |
| 12     | 8           | ingestTS    | Ingest timestamp (int64 nanos)           |
| 20     | 8           | writeTS     | Write timestamp (int64 nanos)            |
| 28     | 16          | ingesterID  | Ingester UUID (raw bytes)                |
| 44     | 4           | ingestSeq   | Per-ingester sequence counter (uint32)   |
| 48     | 2           | attrCount   | Number of attribute pairs (uint16)       |
| 50     | attrCount×8 | attrs       | [keyID:u32][valID:u32] pairs             |
| varies | 4           | rawLen      | Length of raw log body (uint32)          |
| varies | rawLen      | raw         | Raw log message bytes                    |

### Compression

The entire blob is wrapped in a single zstd stream (not seekable zstd). Since cloud queries scan the full blob, frame-level random access is unnecessary.

### Blob Object Metadata

When uploaded, the following user-defined metadata is set on the cloud object for filtering without download:

| Key            | Value                              |
|----------------|------------------------------------|
| `chunk_id`     | 26-char base32hex chunk ID         |
| `vault_id`     | UUID string                        |
| `record_count` | Decimal string                     |
| `start_ts`     | RFC 3339 timestamp                 |
| `end_ts`       | RFC 3339 timestamp                 |

---

## Validation Summary

All file formats include validation checks on decode:

| File           | Checks                                                         |
|----------------|----------------------------------------------------------------|
| raw.log        | Min size (4 bytes), signature, type, version                   |
| idx.log        | Min size (12 bytes), signature, type, version, entry alignment |
| attr.log       | Min size (4 bytes), signature, type, version                   |
| attr_dict.log  | Min size (4 bytes), signature, type, version                   |
| _time.idx      | Min size, signature+type, version, chunkID, entry size match   |
| _token.idx     | Min size, signature+type, version, chunkID, key size, posting size |
| cloud blob     | Magic (`GLCB`), version, zstd decompression, frame sizes      |
