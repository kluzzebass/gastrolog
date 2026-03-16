# Query Execution Pipeline

How a query travels from RPC request to streamed results, covering file vaults,
memory vaults, local chunks, cloud-backed chunks, and multi-node fan-out.

## High-Level Flow

```mermaid
flowchart TD
    RPC["Search RPC"] --> Parse["Parse Expression"]
    Parse --> Route{"Pipeline?"}
    Route -->|No| Direct["searchDirect()"]
    Route -->|Yes| Pipeline["searchPipeline()"]

    Direct --> Local["Local Engine Search"]
    Direct --> Remote["collectRemote()"]

    Local --> Merge["Merge Local + Remote"]
    Remote --> Merge
    Merge --> Stream["Stream to Client"]
    Pipeline --> Stream
```

## Expression Parsing

The expression string (e.g. `level=error last=5m limit=10`) is parsed in
`server/query.go → parseExpression()`. Whitespace-separated tokens are classified
as either **directives** (key=value pairs the engine understands) or **filter
predicates** (fed to the query language parser).

```mermaid
flowchart LR
    Expr["'level=error last=5m reverse=true'"]
    Expr --> Split["Split on whitespace"]
    Split --> D1["last=5m → q.Start, q.End"]
    Split --> D2["reverse=true → q.IsReverse"]
    Split --> F1["level=error → BoolExpr"]
```

| Directive | Effect |
|-----------|--------|
| `last=<dur>` | Sets `Start = now-dur`, `End = now` |
| `start=<t>` / `end=<t>` | Explicit IngestTS bounds (RFC3339 or relative) |
| `source_start=` / `source_end=` | SourceTS bounds (runtime filter) |
| `limit=<n>` | Max records to return |
| `reverse=true` | Newest-first ordering |
| `order=source_ts` | Switch ordering from default IngestTS |
| `pos=<n>` | Single record by position |

Remaining tokens become filter predicates parsed by `querylang.ParsePipeline()`,
producing a `BoolExpr` tree (AND/OR/NOT over token matches, KV predicates, globs).

## Vault and Chunk Selection

```mermaid
flowchart TD
    Q["Query"] --> VF["ExtractVaultFilter()"]
    VF --> CF["ExtractChunkFilter()"]
    CF --> CV["collectVaultChunks()"]

    CV --> ListLocal["cm.List() per vault"]
    ListLocal --> SC["selectChunks()"]

    SC --> TimeCheck{"IngestTS overlap?"}
    TimeCheck -->|Yes| Include["Include chunk"]
    TimeCheck -->|No| Skip["Skip chunk"]

    SC --> ArchiveCheck{"Archived?"}
    ArchiveCheck -->|Yes| Skip
    ArchiveCheck -->|No| TimeCheck

    SC --> Sort["Sort by WriteStart"]
```

**`selectChunks`** filters chunks by time overlap. A chunk is included if its
`[IngestStart, IngestEnd]` range intersects the query's `[lower, upper]` bounds.
Unsealed (active) chunks are always included since their WriteEnd is not final.

For **file vaults**, `cm.List()` returns both in-memory metadata (local chunks)
and B+ tree entries (cloud chunks). For **memory vaults**, all chunks are in-memory.

## Merge Heap Priming

The engine uses a min-heap to merge records from multiple chunks in timestamp
order. Priming opens a scanner per chunk and pushes its first record onto the heap.

```mermaid
flowchart TD
    AllChunks["All matching chunks"] --> Classify{"Cloud-backed?"}

    Classify -->|Local| Prime["openAndPrimeScanner()"]
    Classify -->|Cloud| Defer["Add to deferred list"]

    Prime --> Heap["Push first record onto heap"]

    Heap --> Check{"heapLen > 0 AND limit > 0?"}
    Check -->|Yes| DeferCloud["Skip cloud chunks"]
    Check -->|No| PrimeCloud["Prime cloud chunks too"]

    DeferCloud --> MergeLoop
    PrimeCloud --> MergeLoop

    MergeLoop["runMergeLoop()"]
```

**Cloud deferral**: local chunks are primed first. If any local chunk produces
a record and the query has a limit, cloud chunks are deferred entirely — they're
never downloaded. This is why a `last=5m limit=10` query with an active chunk
serving data does zero cloud I/O. (The `limit=` directive works in both the UI
query input and the CLI's `--limit` flag.)

## Scanner Pipeline

Each chunk gets a scanner built from composable stages. The scanner determines
which records to read and in what order.

```mermaid
flowchart TD
    Build["buildScannerWithManagers()"] --> MinPos["setMinPositionsFromBounds()"]
    MinPos --> BoolExpr{"BoolExpr set?"}

    BoolExpr -->|Yes| DNF["Convert to DNF"]
    DNF --> TokenIdx["Token index → positions"]
    DNF --> KVIdx["KV index → positions"]
    DNF --> GlobIdx["Glob index → positions"]
    TokenIdx --> Intersect["Intersect position lists"]
    KVIdx --> Intersect
    GlobIdx --> Intersect
    Intersect --> RuntimeFilters["Add runtime filters"]

    BoolExpr -->|No| Sequential["Sequential scan"]

    RuntimeFilters --> TSScanner["buildTSOrderedScanner()"]
    Sequential --> TSScanner

    TSScanner --> TSCheck{"Sealed + TS index?"}
    TSCheck -->|Yes| TSIndex["Walk TS index in timestamp order"]
    TSCheck -->|No| Reorder["Buffer + sort (reorderByTS)"]

    TSIndex --> FinalBuild["b.build() → scanner"]
    Reorder --> FinalBuild
```

### Position Narrowing

For **sealed chunks** with indexes, the engine narrows which records to read:

1. **Time-based**: B+ tree (active) or flat index (sealed) finds the first
   position with IngestTS ≥ lower bound → `setMinPosition()`
2. **Token index**: posting lists intersected for all required tokens
3. **KV index**: positions of records matching key/value predicates
4. **Glob index**: prefix-based positions, verified at runtime

If positions are available, a **position scanner** seeks directly to each one.
Otherwise, a **sequential scanner** reads records in order, applying runtime
filters.

### TS-Ordered Scanning

Records are stored in physical write order (WriteTS) but must always be yielded
in IngestTS order (the default) or SourceTS order. Every query goes through the
TS-ordered scanning path:

| Chunk Type | Strategy |
|------------|----------|
| Sealed + TS index available | Walk embedded TS index, seek to positions in TS order |
| Sealed + no TS index | Buffer all records, sort by TS field |
| Active (unsealed) | Buffer all records, sort by TS field (`reorderByTS`) |
| Cloud + TS index cached | Same as sealed — TS index downloaded once, cached locally |

## Chunk Access Paths

```mermaid
flowchart TD
    Open["cm.OpenCursor(chunkID)"] --> LookupMeta["lookupMeta()"]

    LookupMeta --> IsCloud{"Cloud-backed?"}
    IsCloud -->|Yes| CloudPath["RemoteReader (range requests on demand)"]
    IsCloud -->|No| IsSealed{"Sealed?"}
    IsSealed -->|Yes| MmapPath["mmap raw.log + idx.log + attr.log"]
    IsSealed -->|No| StdioPath["pread on open file descriptors"]

    subgraph Memory Vault
        MemOpen["cm.OpenCursor()"] --> MemSlice["Direct slice access"]
    end
```

### File Vault Cursors

| Chunk State | Cursor Type | I/O Method |
|-------------|------------|------------|
| Active (unsealed) | `stdioCursor` | `pread` on raw.log, idx.log, attr.log |
| Sealed (local) | `mmapCursor` | Memory-mapped files, random access via offsets |
| Cloud-backed | `seekableCursor` | Range requests per zstd frame via `RemoteReader` (rare — deferred for bounded queries) |

### Memory Vault Cursors

Memory vaults hold records in Go slices. `OpenCursor()` returns a cursor backed
by direct slice indexing — no disk I/O, no file formats.

### Cloud Chunk Read Path

Cloud chunks are deferred during heap priming — if local chunks satisfy the
query's limit, cloud cursors are never opened and zero cloud I/O occurs.
Cloud cursors are opened when the query needs more records than local
chunks can provide — typically for longer time ranges or unbounded queries.

When a cloud cursor is opened, `RemoteReader` fetches only what's needed via
range requests:

```mermaid
sequenceDiagram
    participant QE as Query Engine
    participant CM as Chunk Manager
    participant S3 as Cloud Store

    QE->>CM: OpenCursor(chunkID)
    CM->>S3: DownloadRange(header + dict + index)
    S3-->>CM: ~few KB
    CM->>S3: DownloadRange(TOC, last 48 bytes)
    S3-->>CM: TS index offsets
    CM-->>QE: RemoteReader cursor

    loop Per record (TS-index-narrowed positions)
        QE->>CM: ReadRecord(pos)
        CM->>S3: DownloadRange(zstd frame)
        S3-->>CM: ~256KB compressed frame
        CM-->>QE: decompressed record
    end
```

The TS index narrows access to specific positions, so typically only a
handful of frames are fetched rather than the full blob.

## Cloud Index Infrastructure

Cloud chunk metadata lives in a B+ tree on disk (`cloud.idx`), not in the Go
heap. This keeps memory stable regardless of cloud chunk count.

```mermaid
flowchart LR
    subgraph "Per-Vault Disk"
        BTree["cloud.idx (B+ tree)"]
        TSCache["TS cache files"]
    end

    subgraph "Cloud Store"
        Blobs["*.glcb blobs"]
    end

    BTree -->|"ChunkID → metadata"| Meta["ChunkMeta"]

    Blobs -->|"Range read (TS index section)"| TSCache
    TSCache -->|"pread binary search"| Positions["Start position for time bounds"]
```

**TS index cache**: on first query, the TS index section (~840KB for 70K records)
is downloaded via a single range request and cached as a local file. Subsequent
queries use `pread`-based binary search on the cache file — no cloud I/O.

## Multi-Node Cluster Query

```mermaid
flowchart TD
    Client["Client"] --> Coord["Coordinator Node"]

    Coord --> LocalEng["Local Engine (local vaults)"]
    Coord --> Fan["Fan-out RPCs"]

    Fan --> Node2["Node 2 ForwardSearch(vault_a)"]
    Fan --> Node3["Node 3 ForwardSearch(vault_b)"]

    LocalEng --> MergeAll["mergeAndStream()"]
    Node2 --> KWay["kWayMerge()"]
    Node3 --> KWay
    KWay --> MergeAll

    MergeAll --> Client
```

The coordinator determines which vaults live on remote nodes via
`remoteVaultsByNode()`. For each remote vault, a streaming `ForwardSearch` RPC
is opened. Results flow back without buffering — `kWayMerge()` performs
selection-based merging across N streams (N is typically 1–3 vaults per node).

**Resume tokens** are split: local chunk positions stay on the coordinator,
remote vault tokens are opaque blobs forwarded back to their originating nodes
on the next page request.

## Merge Loop

```mermaid
flowchart TD
    Loop["runMergeLoop()"] --> Pop["heap.Pop() → oldest record"]
    Pop --> Track["Track position for resume"]
    Track --> Yield["yield(record)"]
    Yield --> LimitCheck{"Limit reached?"}
    LimitCheck -->|Yes| Token["Build resume token"]
    LimitCheck -->|No| Advance["advanceScanner() → push next record"]
    Advance --> HeapCheck{"Heap empty?"}
    HeapCheck -->|No| Pop
    HeapCheck -->|Yes| Done["mergeCompleted"]
    Token --> Done
```

The merge loop pops the entry with the smallest timestamp (or largest, for
reverse queries) from the heap, yields it to the client, and advances that
chunk's scanner to get the next record. When the limit is reached or the heap
is empty, it builds a resume token encoding each chunk's last-returned position.
