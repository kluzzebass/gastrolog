# GastroLog File Formats

All multi-byte integers are **little-endian**. UUIDs are stored as raw 16-byte values. Timestamps are stored as **int64 Unix nanoseconds**.

## Directory Layout

```
<home>/
  <chunk-uuid>/
    raw.log             Raw log bytes (append-only)
    idx.log             Record metadata entries (append-only)
    attr.log            Record attributes (append-only)
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

## idx.log -- Record Metadata Index

Append-only file containing fixed-size metadata entries for each record. The chunk ID is derived from the directory name (authoritative).

### Layout

```
+---------------------------+
|     Header (4 bytes)      |
+---------------------------+
|     Entry 0 (38 bytes)    |
+---------------------------+
|     Entry 1 (38 bytes)    |
+---------------------------+
|     ...                   |
+---------------------------+
|     Entry N-1 (38 bytes)  |
+---------------------------+
```

### Header (4 bytes)

| Offset | Size | Field     | Description                              |
|--------|------|-----------|------------------------------------------|
| 0      | 1    | signature | `0x69` (`'i'`)                           |
| 1      | 1    | type      | `0x69` (`'i'`)                           |
| 2      | 1    | version   | `0x01`                                   |
| 3      | 1    | flags     | Bit 0: sealed (`0x01` = sealed)          |

### Entry (38 bytes each)

| Offset | Size | Field         | Description                                   |
|--------|------|---------------|-----------------------------------------------|
| 0      | 8    | sourceTS      | Source timestamp (int64 Unix nanos, 0 if unknown) |
| 8      | 8    | ingestTS      | Ingest timestamp (int64 Unix nanos)           |
| 16     | 8    | writeTS       | Write timestamp (int64 Unix nanos)            |
| 24     | 4    | rawOffset     | Byte offset into raw.log data section (uint32)|
| 28     | 4    | rawSize       | Length of raw data in bytes (uint32)          |
| 32     | 4    | attrOffset    | Byte offset into attr.log data section (uint32)|
| 36     | 2    | attrSize      | Length of encoded attributes in bytes (uint16)|

### Position Semantics

Record positions throughout the system are **record indices** (0, 1, 2, ...), not byte offsets. To compute the file offset for record N:

```
idx_file_offset = 4 + (N * 38)
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

## Validation Summary

All file formats include validation checks on decode:

| File         | Checks                                                         |
|--------------|----------------------------------------------------------------|
| raw.log      | Min size (4 bytes), signature, type, version                   |
| idx.log      | Min size (4 bytes), signature, type, version, entry alignment  |
| attr.log     | Min size (4 bytes), signature, type, version                   |
| _time.idx    | Min size, signature+type, version, chunkID, entry size match   |
| _token.idx   | Min size, signature+type, version, chunkID, key size, posting size |
