# GastroLog File Formats

All multi-byte integers are **little-endian**. UUIDs are stored as raw 16-byte values. Timestamps are stored as **int64 Unix microseconds**.

## Directory Layout

```
<data_dir>/
  <chunk-uuid>/
    raw.log             Raw log bytes (append-only)
    idx.log             Record metadata entries (append-only)
    sources.bin         SourceID-to-LocalID mapping table
    _time.idx           Sparse time index
    _source.idx         Source index (inverted posting list)
    _token.idx          Token index (inverted posting list)
```

Each chunk has its own subdirectory named by its UUID.

## Common Header Pattern

All binary files share a common 4-byte header prefix:

| Field     | Size | Description                              |
|-----------|------|------------------------------------------|
| signature | 1    | Always `0x69` (`'i'`)                    |
| type      | 1    | File type: `'r'` raw, `'i'` idx, `'t'` time, `'s'` source, `'k'` token |
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

## idx.log -- Record Metadata Index

Append-only file containing fixed-size metadata entries for each record. The chunk ID is derived from the directory name (authoritative).

### Layout

```
+---------------------------+
|     Header (4 bytes)      |
+---------------------------+
|     Entry 0 (28 bytes)    |
+---------------------------+
|     Entry 1 (28 bytes)    |
+---------------------------+
|     ...                   |
+---------------------------+
|     Entry N-1 (28 bytes)  |
+---------------------------+
```

### Header (4 bytes)

| Offset | Size | Field     | Description                              |
|--------|------|-----------|------------------------------------------|
| 0      | 1    | signature | `0x69` (`'i'`)                           |
| 1      | 1    | type      | `0x69` (`'i'`)                           |
| 2      | 1    | version   | `0x01`                                   |
| 3      | 1    | flags     | Bit 0: sealed (`0x01` = sealed)          |

### Entry (28 bytes each)

| Offset | Size | Field         | Description                                  |
|--------|------|---------------|----------------------------------------------|
| 0      | 8    | ingestTS      | Ingest timestamp (int64 Unix micros)         |
| 8      | 8    | writeTS       | Write timestamp (int64 Unix micros)          |
| 16     | 4    | sourceLocalID | Local source ID (from sources.bin, uint32)   |
| 20     | 4    | rawOffset     | Byte offset into raw.log data section (uint32)|
| 24     | 4    | rawSize       | Length of raw data in bytes (uint32)         |

### Position Semantics

Record positions throughout the system are **record indices** (0, 1, 2, ...), not byte offsets. To compute the file offset for record N:

```
idx_file_offset = 4 + (N * 28)
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

## sources.bin -- Source ID Mapping Table

Append-only sequence of fixed-size records. Each record maps one `SourceID` (UUID) to a `localID` (uint32) used in idx.log entries.

### Record Layout

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        size (uint32)                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  version (1)  |                                               |
+-+-+-+-+-+-+-+-+                                               +
|                        sourceID (16 bytes)                    |
+               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|               |                  localID (uint32)             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     trailing size (uint32)                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Fields

| Offset | Size | Field         | Description                              |
|--------|------|---------------|------------------------------------------|
| 0      | 4    | size          | Total record size (always 29)            |
| 4      | 1    | version       | `0x01`                                   |
| 5      | 16   | sourceID      | Source UUID (raw bytes)                  |
| 21     | 4    | localID       | Local uint32 identifier (starts at 1)   |
| 25     | 4    | trailing size | Repeat of size field (must match)        |

**Total: 29 bytes per record**

Local IDs are assigned sequentially starting from 1. The file contains one record per distinct source seen by the chunk.

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
| 0      | 8    | timestamp | Record timestamp (int64 Unix micros) |
| 8      | 4    | recordPos | Record index (uint32)                |

**Total file size: 24 + (entryCount x 12) bytes**

Entries are written in record order (the order records appear in idx.log). A sparsity parameter controls how many records are sampled: with sparsity N, every N-th record is indexed (plus the first record always).

---

## _source.idx -- Source Index

Inverted index mapping each `SourceID` to the list of record indices where that source appears within a chunk. Only built for sealed chunks.

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
| 1      | 1    | type     | `0x73` (`'s'`)                     |
| 2      | 1    | version  | `0x01`                             |
| 3      | 1    | flags    | `0x00` (reserved)                  |
| 4      | 16   | chunkID  | Chunk UUID (raw bytes)             |
| 20     | 4    | keyCount | Number of distinct sources (uint32)|

### Key Table Entry (24 bytes each)

| Offset | Size | Field         | Description                                  |
|--------|------|---------------|----------------------------------------------|
| 0      | 16   | sourceID      | Source UUID (raw bytes)                       |
| 16     | 4    | postingOffset | Byte offset into posting blob (uint32)       |
| 20     | 4    | postingCount  | Number of positions for this source (uint32) |

Key entries are sorted by `sourceID` string representation for deterministic output and binary search.

### Posting Blob

Flat array of `uint32` record indices (4 bytes each). Each key entry references a contiguous slice of this blob via `postingOffset` (byte offset from the start of the posting blob) and `postingCount` (number of indices).

**Total file size: 24 + (keyCount x 24) + (totalIndices x 4) bytes**

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
| sources.bin  | Min size, version, trailing size match                         |
| _time.idx    | Min size, signature+type, version, chunkID, entry size match   |
| _source.idx  | Min size, signature+type, version, chunkID, key size, posting size |
| _token.idx   | Min size, signature+type, version, chunkID, key size, posting size |
