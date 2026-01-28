# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GastroLog is a Go-based log management system built around chunk-based storage and indexing. The backend is written in Go 1.25.5. The project is in active development.

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

### Index package (`backend/internal/index/`)

**Shared types (`index.go`):**

- **Indexer** interface: `Name() string`, `Build(ctx, chunkID) error`
- **IndexManager** interface: `BuildIndexes`, `OpenTimeIndex`, `OpenSourceIndex`
- **Index[T]** -- generic read-only wrapper over `[]T` with `Entries()` method
- **TimeIndexEntry** -- `{Timestamp time.Time, RecordPos uint64}`
- **SourceIndexEntry** -- `{SourceID chunk.SourceID, Positions []uint64}`

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

### Memory-based index manager (`backend/internal/index/memory/`)

- `manager.go` -- `IndexManager` implementation with generic `IndexStore[T]` interface; delegates `BuildIndexes` to `BuildHelper`
- `time/` -- in-memory time indexer with `Get(chunkID)` satisfying `IndexStore[T]`
  - `reader.go` -- `Open` retrieves entries from indexer, returns shared `*index.TimeIndexReader`
- `source/` -- in-memory source indexer with `Get(chunkID)` satisfying `IndexStore[T]`

## Binary Format Conventions

All binary files use **little-endian** byte order. Timestamps stored on disk are **int64 Unix microseconds** (converted to/from `time.Time` at the encode/decode boundary).

Common header prefix for index files and meta file:
```
signature (1 byte, 0x69 'i') | type (1 byte) | version (1 byte) | flags (1 byte)
```

Type bytes: `'m'` = meta, `'t'` = time index, `'s'` = source index.

Index file headers include the chunk ID (16 bytes) after the common prefix. See `docs/file_formats.md` for full binary format specifications.

Record and source map files use leading+trailing size fields for bidirectional traversal.

## Testing

Use the **memory-based managers** (`chunk/memory` and `index/memory`) for integration tests instead of mocks. They implement the full `ChunkManager` and `IndexManager` interfaces in-memory:

```go
cm, _ := chunkmem.NewManager(chunkmem.Config{MaxChunkBytes: 1 << 20})
timeIdx := memtime.NewIndexer(cm, 1)   // sparsity 1 = index every record
srcIdx := memsource.NewIndexer(cm)
im := indexmem.NewManager([]index.Indexer{timeIdx, srcIdx}, timeIdx, srcIdx, nil)
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
- Sub-package pattern: `file/{time,source}` and `memory/{time,source}` avoid symbol prefixing (e.g. `time.NewIndexer` not `NewTimeIndexer`)

## Directory Layout

```
backend/
  internal/
    chunk/
      types.go                  Core interfaces and types
      file/                     File-based chunk manager
      memory/                   Memory-based chunk manager
    index/
      index.go                  Shared types, Indexer/IndexManager interfaces, generic Index[T]
      build.go                  BuildHelper (singleflight + errgroup)
      time_reader.go            Shared TimeIndexReader with FindStart binary search
      file/
        manager.go              File-based IndexManager
        time/
          format.go             Binary encode/decode for _time.idx
          indexer.go            File-based time indexer
          reader.go             Open → *index.TimeIndexReader
        source/
          format.go             Binary encode/decode for _source.idx
          indexer.go            File-based source indexer
      memory/
        manager.go              Memory-based IndexManager with generic IndexStore[T]
        time/
          indexer.go            Memory-based time indexer
          reader.go             Open → *index.TimeIndexReader
        source/
          indexer.go            Memory-based source indexer
  docs/
    file_formats.md             Binary format specifications
```
