# GastroLog File Formats

All multi-byte integers are **little-endian**. GLIDs are stored as raw 16-byte values (UUIDv7-shaped; see [Identifiers](#identifiers) below). Timestamps are stored as **int64 Unix nanoseconds**.

## Directory Layout

```
<home>/
  node_id                   Advisory copy of the node's GLID; the identity itself lives in the cluster-ctl Raft stable store
  node_name                 Human-readable petname (the config store is authoritative)
  gastrolog.sock            Unix domain socket for local CLI access
  cluster-tls.json          Cluster CA cert, this node's cluster cert and private key (PEM, mode 0600)

  raft/
    wal/                    cluster-ctl Raft log + stable store (raftwal)
    groups/
      wal/                  raftwal shared by every vault-ctl group on this node
      cluster-ctl/          cluster-ctl Raft file snapshots
      vault/<vault-id>/ctl/ vault-ctl Raft file snapshots

  segments/
    <vault-id>/             Pipeline root of a vault this node holds no file-storage placement for
      working/<segment-id>    Segment being appended (Segmentation)
      completed/<segment-id>  Completed segment, published to vault-ctl
      pre-head/<segment-id>   In-flight pull on a home (Collection)
      head/<segment-id>       Verified segment awaiting chunking
      chunks/
        <chunk-id>/
          data.glcb           GLCB this home built from a sealed manifest

  vaults/
    <vault-id>/             File chunk manager directory of a vault (default location)
      .lock                 Exclusive lock file for the directory
      cloud.idx             B+ tree of cloud-backed chunk metadata (cloud-backed vaults)
      <chunk-id>/
        data.glcb           Sealed chunk, or the cached copy of a cloud-backed chunk
        raw.log             Raw log bytes (unsealed chunk written by the chunk manager)
        idx.log             Record metadata entries (unsealed)
        attr.log            Record attributes (unsealed)
        attr_dict.log       Attribute string dictionary (unsealed)
        ingest.bt           B+ tree: IngestTS → record position (unsealed)
        source.bt           B+ tree: SourceTS → record position (unsealed)
        token.idx           Token inverted index
        json.idx            Structural JSON index
        attr_key.idx        Attribute key inverted index
        attr_val.idx        Attribute value inverted index
        attr_kv.idx         Attribute key-value pair index
        kv_key.idx          Heuristic KV key inverted index
        kv_val.idx          Heuristic KV value inverted index
        kv_kv.idx           Heuristic KV pair index

  managed-files/
    <file-id>/
      data                  Uploaded managed file (lookups, MMDB, etc.)
```

A vault placed on a file storage keeps both trees on that storage instead of in
the home: the pipeline root at `<storage>/segments/<vault-id>/` and the chunk
manager directory at `<storage>/vaults/<vault-id>/<vault-id>/`. The chunk
manager directory defaults to `vaults/<vault-id>` under `--vaults` (the home
when unset); a vault's `dir` parameter overrides it.

### Identifiers

All entity identifiers (vault, chunk, segment, file, node, etc.) are **GLIDs**: UUIDv7 values encoded as 26-character lowercase base32hex strings (RFC 4648, no padding). The base32hex alphabet (`0-9a-v`) preserves lexicographic sort order, so directory listings are naturally ordered by creation time. On the proto wire format, GLIDs are raw 16-byte values.

| Placeholder     | Description                                                  |
|-----------------|--------------------------------------------------------------|
| `<home>`        | GastroLog data directory (set via `--home` flag)             |
| `<vault-id>`    | GLID of the vault that owns the chunks                       |
| `<chunk-id>`    | GLID of the chunk (UUIDv7, time-ordered)                     |
| `<segment-id>`  | GLID of the segment                                          |
| `<file-id>`     | GLID of the managed file                                     |

### Notes

- The pipeline writes records into segments and builds each sealed chunk's `data.glcb` under the pipeline root's `chunks/`. The chunk manager directory holds what is node-local about a chunk: its secondary indexes, the cached copy of a cloud-backed chunk, and chunks the chunk manager writes itself (record imports).
- A chunk the chunk manager writes starts as `raw.log` + `idx.log` + `attr.log` + `attr_dict.log` with B+ tree timestamp indexes (`*.bt`); at seal it is converted to `data.glcb`, whose embedded TS indexes replace the B+ trees.
- Managed files (under `managed-files/`) are user-uploaded data files used by lookup tables, MMDB databases, etc. Their metadata is cluster-ctl configuration; each node pulls the bytes from a peer (`PullManagedFile`) and verifies them against the recorded SHA-256.

## Common Header Pattern

Every binary file in this document starts with a common 4-byte header prefix:

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
| `0x67` | `g`  | data.glcb (GLCB)  |
| `0x53` | `S`  | segment           |
| `0x49` | `I`  | IngestTS index section (ITSI) — GLCB TOC section type |
| `0x73` | `s`  | SourceTS index section (STSI) — GLCB TOC section type |
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
|     Entry 0 (74 bytes)    |
+---------------------------+
|     Entry 1 (74 bytes)    |
+---------------------------+
|     ...                   |
+---------------------------+
|     Entry N-1 (74 bytes)  |
+---------------------------+
```

### Header (12 bytes)

| Offset | Size | Field     | Description                              |
|--------|------|-----------|------------------------------------------|
| 0      | 1    | signature | `0x69` (`'i'`)                           |
| 1      | 1    | type      | `0x69` (`'i'`)                           |
| 2      | 1    | version   | `0x01`                                   |
| 3      | 1    | flags     | Bit 0: sealed (`0x01` = sealed)          |
| 4      | 8    | createdAt | Chunk creation timestamp (int64 Unix nanos) |

### Entry (74 bytes each)

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
| 58     | 16   | nodeID        | Emitting node GLID (raw bytes)                |

### Position Semantics

Record positions throughout the system are **record indices** (0, 1, 2, ...), not byte offsets. To compute the file offset for record N:

```
idx_file_offset = 12 + (N * 74)
```

This enables O(1) seeking by record number and trivial bidirectional traversal.

### Deriving ChunkMeta

The chunk manager derives an unsealed chunk's metadata from idx.log without a separate metadata file (`loadChunkMeta`):

| ChunkMeta Field             | Source                                      |
|-----------------------------|---------------------------------------------|
| ID                          | Directory name (authoritative)              |
| RecordCount                 | `(fileSize - 12) / 74`                      |
| WriteStart                  | Entry 0, writeTS field                      |
| WriteEnd                    | Last entry, writeTS field                   |
| IngestStart / IngestEnd     | Min / max ingestTS over every entry         |
| SourceStart / SourceEnd     | Min / max non-zero sourceTS over every entry |
| Sealed                      | flags byte in header (bit 0)                |

---

## token.idx -- Token Index

Inverted index mapping each token to the list of record indices where that token appears within a chunk. Built for sealed chunks.

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

### Header (8 bytes)

| Offset | Size | Field    | Description                        |
|--------|------|----------|------------------------------------|
| 0      | 1    | signature| `0x69` (`'i'`)                     |
| 1      | 1    | type     | `0x6B` (`'k'`)                     |
| 2      | 1    | version  | `0x01`                             |
| 3      | 1    | flags    | Bit 0: complete (`0x01`)           |
| 4      | 4    | keyCount | Number of distinct tokens (uint32) |

The chunk is identified by the directory the file sits in, not by the file.

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

**Total file size: 8 + (sum of key entry sizes) + (totalIndices x 4) bytes**

---

## ingest.bt / source.bt -- B+ Tree Indexes

File-backed B+ trees mapping timestamps to record positions within an unsealed chunk the chunk manager writes. `ingest.bt` indexes IngestTS; `source.bt` indexes SourceTS. Both use the same on-disk format.

These indexes exist **only while the chunk is unsealed** — they are deleted when the chunk is converted to `data.glcb`, whose embedded IngestTS and SourceTS index sections take their place.

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

## Segment -- Pipeline Segment File

The unit the pipeline writes records into and moves between nodes
(`backend/internal/pipeline/segment`). Segmentation appends to a segment in
`working/`; at completion the writer appends the index tails, sets the
complete flag and renames the file into `completed/`. Homes pull completed
segments into `pre-head/` and promote them to `head/`, where chunking reads
them. The file name is the segment GLID.

### Layout

```
+-------------------------------+
|     Header (104 bytes)        |
+-------------------------------+
|     Record frames             |
|  [frameLen:u32][frame] × N    |
+-------------------------------+
|  EventID index (N × 48)       |   completed segments only
+-------------------------------+
|  SourceTS index (M × 12)      |   completed segments only
+-------------------------------+
```

The header is rewritten in place after every append, record first and header
second, so the header is never ahead of the data. `dataEnd` is the recovery
anchor: on open, the frame starting at `dataEnd` is the last committed record,
and bytes past its end are a torn append and are discarded.

### Header (104 bytes)

| Offset | Size | Field               | Description                                                        |
|--------|------|---------------------|--------------------------------------------------------------------|
| 0      | 1    | signature           | `0x69` (`'i'`)                                                     |
| 1      | 1    | type                | `0x53` (`'S'`)                                                     |
| 2      | 1    | version             | `0x01`                                                             |
| 3      | 1    | flags               | Bit 0: complete (`0x01`)                                           |
| 4      | 16   | segmentID           | Segment GLID (raw bytes)                                           |
| 20     | 16   | vaultID             | Vault GLID (raw bytes)                                             |
| 36     | 4    | recordCount         | Records appended (uint32)                                          |
| 40     | 4    | dataEnd             | Byte offset where the last written record starts (uint32)          |
| 44     | 8    | firstIngestTS       | IngestTS of the first record (int64 Unix nanos)                    |
| 52     | 8    | lastIngestTS        | IngestTS of the last record (int64 Unix nanos)                     |
| 60     | 4    | indexOffset         | Byte offset of the EventID index (uint32, 0 while working)         |
| 64     | 8    | segmentChecksum     | XXH64 of the record bytes `[104, end of the frame at dataEnd)`     |
| 72     | 4    | indexChecksum       | CRC32 (IEEE) of the EventID index                                  |
| 76     | 8    | firstSourceTS       | Min non-zero SourceTS (int64 Unix nanos, 0 = none)                 |
| 84     | 8    | lastSourceTS        | Max non-zero SourceTS (int64 Unix nanos, 0 = none)                 |
| 92     | 4    | sourceIndexOffset   | Byte offset of the SourceTS index (uint32)                         |
| 96     | 4    | sourceIndexCount    | SourceTS index entries (uint32)                                    |
| 100    | 4    | sourceIndexChecksum | CRC32 (IEEE) of the SourceTS index                                 |

The segment checksum is XXH64 rather than a CRC because every frame ends in
its own CRC32: a CRC rolled over `body ++ CRC(body)` is constant, so it would
not detect a same-length substitution.

### Record Frame (variable size)

Each frame is preceded by its `frameLen:u32` (body length, excluding the prefix):

| Offset | Size     | Field      | Description                                          |
|--------|----------|------------|------------------------------------------------------|
| 0      | 16       | ingesterID | EventID: ingester GLID                               |
| 16     | 16       | nodeID     | EventID: ingesting node GLID                         |
| 32     | 8        | ingestTS   | EventID: ingest timestamp (int64 Unix nanos)         |
| 40     | 4        | ingestSeq  | EventID: per-ingester sequence (uint32)              |
| 44     | 8        | sourceTS   | Source timestamp (int64 Unix nanos, 0 = none)        |
| 52     | 8        | writeTS    | Write timestamp (int64 Unix nanos)                   |
| 60     | varies   | attrs      | `[count:u16]` then `[keyLen:u16][key][valLen:u16][val]` × count, keys sorted |
| varies | 4        | rawLen     | Length of the raw log body (uint32)                  |
| varies | rawLen   | raw        | Raw log message bytes                                |
| varies | 4        | crc        | CRC32 (IEEE) of every preceding byte of the body     |

The fixed-width EventID leads the frame so the merge that builds a chunk can
order records by reading only the length prefix and the EventID.

### EventID Index (recordCount × 48 bytes)

One entry per record, sorted by EventID (then file position): the 44-byte
EventID followed by `filePos:u32`, the byte offset of the record's frame length
prefix. A position in EventID order is an index into this array.

### SourceTS Index (sourceIndexCount × 12 bytes)

One 12-byte entry (`timestamp:i64`, `pos:u32`) per record with a non-zero
SourceTS, sorted by timestamp then position; `pos` is the record's EventID-order
position. Records without a SourceTS have no entry.

---

## GLCB -- GastroLog Chunk Blob

Single-file, self-describing format for sealed chunks (`backend/internal/chunk/glcb`).
Every home stores one `data.glcb` per sealed chunk; a cloud-backed vault
uploads the same bytes to object storage, re-framed per section as a
transport layer (see "Cloud transport framing" below), and keeps a cached
copy under the same `data.glcb` name. The on-disk format is **uncompressed**:
every section is directly readable without a decompression step.

Sorted IngestTS and SourceTS indexes are embedded after the record index,
with a TOC footer. This enables time-range seeking against the local file
(or against a downloaded cache copy) without rescanning records.

### Layout

```
+---------------------------+
|     Preamble (4 bytes)    |
+---------------------------+
|  Layout metadata (128 B)  |
+---------------------------+
|     Records section       |
|  [frameLen:u32][frame] ×  |
|     recordCount           |
+---------------------------+
|     Dictionary            |
|   (dictEntries entries)   |
+---------------------------+
|     Record Index          |
|   (recordCount × 12)      |
+---------------------------+
|   IngestTS Index          |
|   (recordCount × 12)      |
+---------------------------+
|   SourceTS Index          |
|   (recordCount × 12)      |
+---------------------------+
|   TOC entries             |
|   (entryCount × 42)       |
+---------------------------+
|   TOC footer (44 bytes)   |
+---------------------------+
```

The records section starts at byte 132. A writer stages record frames in a
work file beside the finished blob and writes the fixed prefix, the records
and the variable sections at finalize, then renames the blob into place.

### Preamble (4 bytes)

| Offset | Size | Field        | Description                                  |
|--------|------|--------------|----------------------------------------------|
| 0      | 1    | signature    | `0x69` (`'i'`)                               |
| 1      | 1    | type         | `0x67` (`'g'`)                               |
| 2      | 1    | version      | `0x01`                                       |
| 3      | 1    | flags        | Reserved (`0x00`)                            |

### Layout Metadata (128 bytes, at offset 4)

Offsets below are relative to the start of the block; absolute offset = 4 + relative.

| Offset | Size | Field             | Description                                          |
|--------|------|-------------------|------------------------------------------------------|
| 0      | 16   | chunkID           | Chunk GLID (raw bytes)                               |
| 16     | 16   | vaultID           | Vault GLID (raw bytes)                               |
| 32     | 4    | recordCount       | Total records (uint32)                               |
| 36     | 8    | writeStart        | Min WriteTS (int64 Unix nanos)                       |
| 44     | 8    | writeEnd          | Max WriteTS (int64 Unix nanos)                       |
| 52     | 8    | ingestStart       | Min IngestTS (int64 Unix nanos)                      |
| 60     | 8    | ingestEnd         | Max IngestTS (int64 Unix nanos)                      |
| 68     | 8    | sourceStart       | Min non-zero SourceTS (int64 Unix nanos, 0 = none)   |
| 76     | 8    | sourceEnd         | Max non-zero SourceTS (int64 Unix nanos, 0 = none)   |
| 84     | 4    | dictEntries       | Number of dictionary entries (uint32)                |
| 88     | 4    | dictSize          | Total bytes of the dictionary section (uint32)       |
| 92     | 4    | recordsOff        | Absolute offset of the records section (uint32, 132) |
| 96     | 4    | recordsSize       | Bytes in the records section (uint32)                |
| 100    | 4    | dictOff           | Absolute offset of the dictionary (uint32)           |
| 104    | 4    | indexOff          | Absolute offset of the record index (uint32)         |
| 108    | 4    | indexSize         | Bytes in the record index (uint32)                   |
| 112    | 1    | ingestTSMonotonic | `1` when IngestTS never decreases in record (merge) order |
| 113    | 15   | —                 | Zero                                                 |

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
| 0      | 8    | offset        | Offset of the frame body, relative to the records section start (past the u32 frameLen prefix) (uint64) |
| 8      | 4    | size          | Frame size excluding the u32 frameLen prefix (uint32)|

### Records Section (uncompressed)

Each record is written as `[frameLen:u32][frame body]`, frame after frame, with no compression layer. The frame body has the encoding below.

### Record Frame (variable size)

| Offset | Size        | Field       | Description                              |
|--------|-------------|-------------|------------------------------------------|
| 0      | 8           | sourceTS    | Source timestamp (int64 nanos, 0 = none) |
| 8      | 8           | ingestTS    | Ingest timestamp (int64 nanos)           |
| 16     | 8           | writeTS     | Write timestamp (int64 nanos)            |
| 24     | 16          | ingesterID  | Ingester GLID (raw bytes)                |
| 40     | 16          | nodeID      | Originating node GLID (raw bytes)        |
| 56     | 4           | ingestSeq   | Per-ingester sequence counter (uint32)   |
| 60     | 2           | attrCount   | Number of attribute pairs (uint16)       |
| 62     | attrCount×8 | attrs       | [keyID:u32][valID:u32] pairs             |
| varies | 4           | rawLen      | Length of raw log body (uint32)          |
| varies | rawLen      | raw         | Raw log message bytes                    |

### Compression

The on-disk format is uncompressed. Compression exists **only on transport**: a cloud-backed vault uploads the blob as a transport-framed object whose frames are each zstd-compressed (see "Cloud transport framing" below); the local cache, after `DownloadAndUnwrap`, is a plain `data.glcb` byte-for-byte equivalent to a freshly-sealed local chunk.

Content reads on cloud-backed chunks download the whole object (reassembled once), populate the cached copy, and serve subsequent reads via the local-cursor fast path. Index sections are the exception: the transport framing (below) lets a reader fetch one section — footer, directory, one frame — in KB-scale range GETs, which is how uncached chunks answer histogram rank lookups exactly without a download.

### Embedded TS Indexes

Two sorted timestamp index sections follow the record index. Each is a flat array of 12-byte entries (`tsindex.EntrySize`), sorted by timestamp then record position:

| Offset | Size | Field     | Description                          |
|--------|------|-----------|--------------------------------------|
| 0      | 8    | timestamp | Timestamp (int64 Unix nanos)         |
| 8      | 4    | recordPos | Record index (uint32)                |

- **IngestTS Index**: One entry per record, sorted by IngestTS. Size = `recordCount × 12` bytes.
- **SourceTS Index**: One entry per record, keyed by the record's SourceTS, or by its IngestTS when it has none (`chunk.EffectiveSourceTS`), sorted by that key. Size = `recordCount × 12` bytes. The key is the record's place on the source axis, not a claim about the record: its frame keeps `sourceTS` = 0, and the layout's `sourceStart`/`sourceEnd` cover non-zero SourceTS only.

No header — the TOC provides section offsets and sizes. Entry count is derived from `sectionSize / 12`. A section the TOC lists with size 0 is an index with no entries (a chunk with no records); a section the TOC does not list is a missing index, and a read that needs it fails.

### TOC Entries (entryCount × 42 bytes)

Each TOC entry describes one section in the blob. Readers look up entries by section type to find offset + size + content hash. The writer emits three entries, each at section version 1: the layout metadata (`'B'`, offset 4), the IngestTS index (`'I'`) and the SourceTS index (`'s'`).

| Offset | Size | Field    | Description                                       |
|--------|------|----------|---------------------------------------------------|
| 0      | 1    | type     | Section type byte (e.g. `'I'` = IngestTS index)   |
| 1      | 1    | version  | Per-section format version                        |
| 2      | 4    | offset   | Byte offset from blob start (uint32 LE)           |
| 6      | 4    | size     | Byte count (uint32 LE)                            |
| 10     | 32   | hash     | SHA-256 of the section's bytes                    |

### TOC Footer (44 bytes, last 44 bytes of the file)

| Offset | Size | Field         | Description                                                         |
|--------|------|---------------|---------------------------------------------------------------------|
| 0      | 4    | entryCount    | Number of TOC entries above (uint32 LE)                             |
| 4      | 32   | blobDigest    | SHA-256 of bytes `[0, fileSize - 44)` — every byte except this footer |
| 36     | 4    | footerVersion | Footer schema version (`1`, uint32 LE)                              |
| 40     | 4    | magic         | `"GTOC"` (ASCII)                                                    |

**Read protocol.** Read the last 44 bytes as the footer, validate the
magic, then read `entryCount × 42` bytes immediately preceding the footer
to recover all section pointers + hashes. The blob digest covers
everything from the start of the file through the last entry. It is the
chunk's identity across homes: `CmdSealChunk` records the sealing leader's
digest in the vault-ctl FSM, a home compares its own copy's footer digest
against it, and a copy pulled from a peer is accepted only when that range
hashes to it.

### Blob Object Metadata

When uploaded, the following user-defined metadata is set on the cloud object, so a listing can reconstruct chunk metadata without downloading the object (`glcb.EncodeObjectMetadata`):

| Key            | Value                                             |
|----------------|---------------------------------------------------|
| `chunk_id`     | 26-char base32hex chunk ID                        |
| `vault_id`     | 26-char base32hex vault ID                        |
| `record_count` | Decimal string                                    |
| `raw_bytes`    | Decimal string: bytes in the records section      |
| `write_start` / `write_end`   | RFC 3339 (nanosecond) timestamps   |
| `ingest_start` / `ingest_end` | RFC 3339 (nanosecond) timestamps   |
| `source_start` / `source_end` | RFC 3339 (nanosecond) timestamps   |

A time key whose bound is zero is omitted rather than written as the epoch.

The cloud object key is `vault-<vault>/<chunk>.glcb`; the bytes carry the transport framing below and reassemble into a byte-identical plain `data.glcb` on download.

### Cloud transport framing

The object is the local blob re-framed so byte ranges of the object are
meaningful (`backend/internal/chunk/glcb/transport.go`):

```
object := bodyFrame sectionFrame* tailFrame directory footer

bodyFrame     zstd(preamble + layout + records + dict + record-index)
sectionFrame  zstd(one tail TOC section)          ITSI, STSI, future index sections
tailFrame     zstd(original TOC + footer)         verbatim tail bytes
directory     raw, 60 B per frame                 kind, type, version, offsets, sizes, SHA-256
footer        raw, 20 B                           directory offset, entry count, version, "GXFR" magic
```

Every frame is separately zstd'd, so the bulk — and any future index
section that rivals the records in size — stays compressed, while a
ranged reader fetches the footer + directory (one tail GET) and one
section frame. Frame hashes are verified on both the ranged path and the
full reassembly, so a truncated or corrupt object fails loudly.

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
| token.idx      | Min size, signature+type, version, key size, posting size      |
| json.idx       | Min size, signature+type, version, complete flag, status byte  |
| attr_*.idx     | Min size, signature+type, version, complete flag               |
| kv_*.idx       | Min size, signature+type, version, complete flag, status byte  |
| segment        | Signature+type, version, header/record reconciliation, XXH64 segment checksum, per-frame CRC32, index and SourceTS index CRC32 + layout |
| data.glcb (GLCB) | Signature+type, version, TOC footer magic + entry hashes     |
| lookup table   | Min size (20), signature+type, version, complete flag          |
