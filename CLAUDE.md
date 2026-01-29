# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GastroLog is a Go-based log management system built around chunk-based storage and indexing. The backend is written in Go 1.25.5. The project is in active development.

Module name: `gastrolog` (local module, not intended for external import)

## Go Style Rules

**Always write modern Go code targeting Go 1.25+:**

- Use `sync.WaitGroup.Go(func())` instead of `Add(1)` + `go func()` + `defer Done()`
- Use `iter.Seq2` for dual-value iterators (record + error)
- Use `slices` and `maps` packages instead of hand-rolled loops where appropriate
- Use `context.WithoutCancel` when a goroutine should outlive its initiator
- Use `errors.Is` / `errors.As` for error checking, never `==`
- Use `cmp.Or` for default values where appropriate

**Concurrency patterns:**

- All goroutines must be tracked with `sync.WaitGroup` for clean shutdown
- Use `WaitGroup.Go()` to launch and track goroutines in one call
- Channels should be explicitly closed by the sender when done
- Always select on `ctx.Done()` in long-running goroutines
- Copy values under lock, release lock before waiting (prevents deadlock)

**Error handling:**

- Return sentinel errors for all validation failures
- Use descriptive error names (`ErrSignatureMismatch`, not `ErrInvalid`)
- Wrap errors with context using `fmt.Errorf("operation: %w", err)`

**Code organization:**

- Keep implementations boring and explicit
- No magic, no clever tricks
- Prefer composition over inheritance
- Use sub-packages to avoid symbol prefixing (`time.NewIndexer` not `NewTimeIndexer`)

## Build & Test Commands

All commands run from the `backend/` directory:

```bash
go build ./...                                    # Build
go test ./...                                     # Run all tests
go test -v ./internal/chunk/file/                 # Run tests for a specific package
go test -v -run TestRecordRoundTrip ./internal/chunk/file/  # Run a single test
go test -cover ./...                              # Test with coverage
go test -race ./internal/index/...                # Race detector on index packages
go mod tidy                                       # Clean dependencies
```

## Architecture

### Core types (`backend/internal/chunk/types.go`)

- **ChunkManager** interface: `Append`, `Seal`, `Active`, `Meta`, `List`, `OpenCursor`
- **RecordCursor** interface: `Next`, `Prev`, `Seek`, `Close` -- bidirectional record iteration
- **MetaStore** interface: `Save`, `Load`, `List`
- **Record** -- log entry with `IngestTS` (`time.Time`), `SourceID` (UUID), and `Raw` payload
- **RecordRef** -- `ChunkID` + byte offset `Pos` (`uint64`); used for cursor positioning via `Seek`
- **ChunkMeta** -- `ID`, `StartTS`, `EndTS` (`time.Time`), `Size`, `Sealed`
- **ChunkID** / **SourceID** -- UUID v7 typed identifiers (time-ordered, sortable)

### Orchestrator package (`backend/internal/orchestrator/`)

Coordinates ingestion, indexing, and querying without owning business logic:

- **Orchestrator** -- routes records to chunk managers, triggers index builds on seal, delegates queries
- **Receiver** interface -- sources of log messages; emit `IngestMessage` to shared channel
- **IngestMessage** -- `{Attrs, Raw, IngestTS}` where `IngestTS` is set by receiver at receive time
- Registries keyed by string for future multi-tenant support

**Lifecycle:**
- `Start(ctx)` -- launches receiver goroutines + ingest loop, returns immediately
- `Stop()` -- cancels context, waits for receivers, closes channel, waits for ingest loop, waits for index builds
- All goroutines tracked with `sync.WaitGroup.Go()` for clean shutdown
- Double start returns `ErrAlreadyRunning`, stop without start returns `ErrNotRunning`

**Shutdown order:**
1. Cancel context (signals receivers and ingest loop)
2. Wait for receivers to exit (`receiverWg.Wait()`)
3. Close ingest channel
4. Ingest loop drains remaining messages, then exits
5. Wait for ingest loop (`ingestLoopWg.Wait()`)
6. Wait for index builds (`indexWg.Wait()`)

**Concurrency model:**
- `Register*` methods are startup-only by convention (read-only after setup)
- `Ingest` serialized via exclusive lock (required for seal detection)
- `Search*` methods use read lock, can run concurrently

**Known limitations (documented, acceptable for now):**
- Seal detection via Active() before/after comparison assumes single writer, no async sealing
- Receiver errors currently ignored (future: add error callback)
- Partial failure on fan-out: if CM A succeeds and CM B fails, no rollback

### Query package (`backend/internal/query/`)

High-level search API over chunks and indexes:

- **Engine** -- created with `New(ChunkManager, IndexManager)`, executes queries
- **Query** struct:
  - `Start`, `End` -- time bounds (if `End < Start`, results returned in reverse/newest-first order)
  - `Sources` -- filter by source IDs (OR semantics, nil = no filter)
  - `Tokens` -- filter by tokens (AND semantics, nil = no filter)
  - `Limit` -- max results (0 = unlimited)
- **Search(ctx, query, resume)** returns `(iter.Seq2[Record, error], func() *ResumeToken)`
  - Iterator yields records in timestamp order (forward or reverse)
  - Resume token enables pagination; valid as long as referenced chunk exists
  - `ErrInvalidResumeToken` returned if chunk was deleted

**Scanner pipeline (`scanner.go`):**

Uses composable filter pipeline to avoid combinatorial explosion of if-else branches:

- **scannerBuilder** -- accumulates position sources and runtime filters
  - `positions []uint64`: nil = sequential scan, empty = no matches, non-empty = seek positions
  - `filters []recordFilter`: applied in order (cheap filters first)
- **Index application functions** -- each tries to use an index, falls back to runtime filter if unavailable
  - `applySourceIndex()` -- contributes positions or adds `sourceFilter`
  - `applyTokenIndex()` -- contributes positions or adds `tokenFilter`
- **Graceful fallback** -- if index returns `ErrIndexNotFound` (sealed but not yet indexed), falls back to sequential scan

Adding a new filter type requires:
1. One `applyXxxIndex(builder, indexes, chunkID, params) (ok, empty bool)` function
2. One `xxxFilter(params) recordFilter` function
3. A few lines in `buildScanner` to call them

Query execution:
1. Selects chunks overlapping time range (sorted by StartTS, ascending or descending)
2. For each chunk, builds scanner via pipeline:
   - Try time index for start position
   - Try source/token indexes for position lists (intersected)
   - If index unavailable, add runtime filter instead
3. Scanner iterates positions (or sequentially) applying time bounds and filters
4. Respects limit, tracks position for resume token

### Index package (`backend/internal/index/`)

**Shared types (`index.go`):**

- **Indexer** interface: `Name() string`, `Build(ctx, chunkID) error`
- **IndexManager** interface: `BuildIndexes`, `OpenTimeIndex`, `OpenSourceIndex`, `OpenTokenIndex`
- **Index[T]** -- generic read-only wrapper over `[]T` with `Entries()` method
- **TimeIndexEntry** -- `{Timestamp time.Time, RecordPos uint64}`
- **SourceIndexEntry** -- `{SourceID chunk.SourceID, Positions []uint64}`
- **TokenIndexEntry** -- `{Token string, Positions []uint64}`

**BuildHelper (`build.go`):**

Shared concurrency primitive used by both file and memory managers:
- `singleflight.DoChan` deduplicates concurrent `BuildIndexes` calls for the same chunkID
- `errgroup.WithContext` parallelizes individual indexers within a single build
- `context.WithoutCancel` detaches the build from the initiator's context so one caller cancelling doesn't abort the shared build
- Early `ctx.Err()` check bails out immediately on already-cancelled contexts

**TimeIndexReader (`time_reader.go`):**

Shared binary search positioning over time index entries:
- `NewTimeIndexReader(chunkID, entries)` wraps decoded entries
- `FindStart(tStart)` returns `(RecordRef, true)` for the latest entry at or before `tStart`, or `(zero, false)` if `tStart` is before all entries (caller should scan from beginning)
- Uses `sort.Search` on timestamp-sorted entries

**SourceIndexReader / TokenIndexReader:**

Binary search lookup for inverted indexes:
- `Lookup(key)` returns `(positions []uint64, found bool)`
- Entries sorted by key for binary search

**Tokenizer (`token/tokenize.go`):**

Simple tokenizer used for both indexing and query-time matching:
- `Simple([]byte) []string` -- splits on non-alphanumeric, lowercases, returns unique tokens
- Same tokenizer used at index time and query time ensures consistent matching

### File-based chunk storage (`backend/internal/chunk/file/`)

Disk-persisted storage with custom binary formats:
- `manager.go` -- chunk lifecycle: rotation based on MaxChunkBytes, one active writable chunk, lazy loading from disk
- `record.go` -- binary record format with magic `0x69`, version `0x01`, leading+trailing size for bidirectional traversal
- `meta_store.go` -- per-chunk `meta.bin` with signature `'i'+'m'`, atomic writes (temp file + rename)
- `sources.go` -- bidirectional UUID-to-uint32 source ID mapping per chunk (`sources.bin`)
- `record_reader.go` -- `RecordCursor` implementation for file-based chunks
- `reader.go` / `mmap_reader.go` -- standard I/O and memory-mapped readers; mmap preferred for sealed chunks

### Memory-based chunk storage (`backend/internal/chunk/memory/`)

In-memory implementation of the same interfaces. Rotates chunks based on record count. Useful for testing.

### File-based index manager (`backend/internal/index/file/`)

- `manager.go` -- `IndexManager` implementation; delegates `BuildIndexes` to `BuildHelper`
- `time/` -- time indexer: sparse index mapping sampled timestamps to record positions; writes `_time.idx`
  - `reader.go` -- `Open` loads and validates index file, returns shared `*index.TimeIndexReader`
- `source/` -- source indexer: inverted index mapping SourceIDs to record positions; writes `_source.idx`
- `token/` -- token indexer: inverted index mapping tokens to record positions; writes `_token.idx`

### Memory-based index manager (`backend/internal/index/memory/`)

- `manager.go` -- `IndexManager` implementation with generic `IndexStore[T]` interface; delegates `BuildIndexes` to `BuildHelper`
- `time/` -- in-memory time indexer with `Get(chunkID)` satisfying `IndexStore[T]`
  - `reader.go` -- `Open` retrieves entries from indexer, returns shared `*index.TimeIndexReader`
- `source/` -- in-memory source indexer with `Get(chunkID)` satisfying `IndexStore[T]`
- `token/` -- in-memory token indexer with `Get(chunkID)` satisfying `IndexStore[T]`

## Binary Format Conventions

All binary files use **little-endian** byte order. Timestamps stored on disk are **int64 Unix microseconds** (converted to/from `time.Time` at the encode/decode boundary).

Common header prefix for index files and meta file:
```
signature (1 byte, 0x69 'i') | type (1 byte) | version (1 byte) | flags (1 byte)
```

Type bytes: `'m'` = meta, `'t'` = time index, `'s'` = source index, `'k'` = token index.

Index file headers include the chunk ID (16 bytes) after the common prefix. See `docs/file_formats.md` for full binary format specifications.

Record and source map files use leading+trailing size fields for bidirectional traversal.

## Testing

Use the **memory-based managers** (`chunk/memory` and `index/memory`) for integration tests instead of mocks. They implement the full `ChunkManager` and `IndexManager` interfaces in-memory:

```go
cm, _ := chunkmem.NewManager(chunkmem.Config{MaxChunkBytes: 1 << 20})
timeIdx := memtime.NewIndexer(cm, 1)   // sparsity 1 = index every record
srcIdx := memsource.NewIndexer(cm)
tokIdx := memtoken.NewIndexer(cm)
im := indexmem.NewManager([]index.Indexer{timeIdx, srcIdx, tokIdx}, timeIdx, srcIdx, tokIdx)
```

Append records, seal chunks, then pass `cm` and `im` to the code under test. See `query/query_test.go` for an example.

## Key Design Patterns

- Interface segregation: consumers depend on `chunk.ChunkManager` / `index.Indexer` / `index.IndexManager`, not concrete types
- Generics used to eliminate duplication: `Index[T]` for index entries, `IndexStore[T]` for memory stores
- `BuildHelper` provides singleflight deduplication + errgroup parallelism for concurrent index builds
- All managers are mutex-protected for concurrent access
- Atomic file operations for metadata and indexes (temp file then rename)
- `time.Time` used throughout the domain; int64 microseconds only at file encode/decode boundaries
- Cursors work on both sealed and unsealed chunks; indexers explicitly reject unsealed chunks
- Named constants for all binary format sizes (no magic numbers in encode/decode)
- Sentinel errors for all validation failures (`ErrSignatureMismatch`, `ErrVersionMismatch`, etc.)
- Sub-package pattern: `file/{time,source,token}` and `memory/{time,source,token}` avoid symbol prefixing (e.g. `time.NewIndexer` not `NewTimeIndexer`)
- Query uses `End < Start` convention for reverse order instead of separate bool field

## Directory Layout

```
backend/
  internal/
    callgroup/                  Singleflight-like concurrency primitive
    orchestrator/
      orchestrator.go           Orchestrator struct and New()
      lifecycle.go              Start() and Stop() methods
      ingest.go                 Ingest routing and seal detection
      search.go                 Search delegation to query engines
      receiver.go               Receiver interface, IngestMessage, ReceiverFactory
      registry.go               Register* methods for components
      factory.go                Factories struct and ApplyConfig
    chunk/
      chunk.go                  Interfaces (ChunkManager, RecordCursor, MetaStore), ManagerFactory
      types.go                  Data types (Record, ChunkMeta, ChunkID, SourceID, RecordRef)
      file/                     File-based chunk manager
      memory/                   Memory-based chunk manager
    index/
      index.go                  Shared types, Indexer/IndexManager interfaces, generic Index[T], ManagerFactory
      build.go                  BuildHelper (singleflight + errgroup)
      time_reader.go            Shared TimeIndexReader with FindStart binary search
      source_reader.go          Shared SourceIndexReader with Lookup
      token_reader.go           Shared TokenIndexReader with Lookup
      token/
        tokenize.go             Simple tokenizer (split, lowercase, dedupe)
      file/
        manager.go              File-based IndexManager
        time/                   File-based time indexer (_time.idx)
        source/                 File-based source indexer (_source.idx)
        token/                  File-based token indexer (_token.idx)
      memory/
        manager.go              Memory-based IndexManager with generic IndexStore[T]
        time/                   Memory-based time indexer
        source/                 Memory-based source indexer
        token/                  Memory-based token indexer
    query/
      query.go                  Query engine with Search API
      scanner.go                Composable scanner pipeline (scannerBuilder, filters)
    source/
      registry.go               Source identity resolution and persistence
      file/                     File-based source store
      memory/                   Memory-based source store
    config/
      config.go                 Config types (Store, Receiver definitions)
      file/                     File-based config store
      memory/                   Memory-based config store
  docs/
    file_formats.md             Binary format specifications
```
