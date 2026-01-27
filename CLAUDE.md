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
go mod tidy                                       # Clean dependencies
```

## Architecture

### Core types (`backend/internal/chunk/types.go`)

- **ChunkManager** interface: `Append`, `Seal`, `Active`, `Meta`, `List`, `OpenCursor`
- **RecordCursor** interface: `Next`, `Prev`, `Seek`, `Close` -- bidirectional record iteration
- **MetaStore** interface: `Save`, `Load`, `List`
- **Record** -- log entry with `IngestTS` (`time.Time`), `SourceID` (UUID), and `Raw` payload
- **RecordRef** -- `ChunkID` + byte offset `Pos` (`uint64`)
- **ChunkMeta** -- `ID`, `StartTS`, `EndTS` (`time.Time`), `Size`, `Sealed`
- **ChunkID** / **SourceID** -- UUID-based typed identifiers

### Indexer interface (`backend/internal/index/index.go`)

- **Indexer** interface: `Name() string`, `Build(ctx, chunkID) error`
- Indexers only operate on sealed chunks (reject unsealed with `ErrChunkNotSealed`)
- Build is idempotent -- overwrites existing artifacts

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

### Indexers (`backend/internal/index/`)

Each indexer type follows a sub-package pattern: shared types in the parent package, file-based and memory-based implementations in sub-packages.

**Time indexer** (`index/time/`):
- Sparse time index mapping sampled timestamps to record positions
- Shared type: `IndexEntry{Timestamp time.Time, RecordPos uint64}`
- File-based (`time/file/`): writes `_time.idx` with binary header + entries
- Memory-based (`time/memory/`): in-memory map

**Source indexer** (`index/source/`):
- Inverted index mapping each SourceID to its record positions
- Shared type: `IndexEntry{SourceID chunk.SourceID, Positions []uint64}`
- File-based (`source/file/`): writes `_source.idx` with header + key table + posting blob
- Memory-based (`source/memory/`): in-memory map

## Binary Format Conventions

All binary files use **little-endian** byte order. Timestamps stored on disk are **int64 Unix microseconds** (converted to/from `time.Time` at the encode/decode boundary).

Common header prefix for index files and meta file:
```
signature (1 byte, 0x69 'i') | type (1 byte) | version (1 byte) | flags (1 byte)
```

Type bytes: `'m'` = meta, `'t'` = time index, `'s'` = source index.

Index file headers include the chunk ID (16 bytes) after the common prefix. See `docs/file_formats.md` for full binary format specifications.

Record and source map files use leading+trailing size fields for bidirectional traversal.

## Key Design Patterns

- Interface segregation: consumers depend on `chunk.ChunkManager` / `index.Indexer`, not concrete types
- All managers are mutex-protected for concurrent access
- Atomic file operations for metadata and indexes (temp file then rename)
- `time.Time` used throughout the domain; int64 microseconds only at file encode/decode boundaries
- Cursors work on both sealed and unsealed chunks; indexers explicitly reject unsealed chunks
- Named constants for all binary format sizes (no magic numbers in encode/decode)
- Sentinel errors for all validation failures (`ErrSignatureMismatch`, `ErrVersionMismatch`, etc.)
- Use UUID v7 for all generated IDs (ChunkID, SourceID) -- v7 UUIDs are time-ordered, which makes them sortable and indexable

## Directory Layout

```
backend/
  internal/
    chunk/
      types.go                  Core interfaces and types
      file/                     File-based chunk manager
      memory/                   Memory-based chunk manager
    index/
      index.go                  Indexer interface
      time/
        format.go               Shared IndexEntry type
        file/                   File-based time indexer
        memory/                 Memory-based time indexer
      source/
        format.go               Shared IndexEntry type
        file/                   File-based source indexer
        memory/                 Memory-based source indexer
  docs/
    file_formats.md             Binary format specifications
```
