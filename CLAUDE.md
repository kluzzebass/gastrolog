# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GastroLog is a Go-based log management system focused on chunk-based log storage. The backend is written in Go 1.21.7. The project is in active development — the original syslog ingestion pipeline was removed in favor of a refactored chunk storage abstraction.

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

The core abstraction is a **chunk-based log storage system** defined by interfaces in `backend/internal/chunk/types.go`:

- **ChunkManager** — appends records, seals completed chunks, provides readers
- **RecordReader** — sequential record iteration
- **MetaStore** — chunk metadata persistence
- **Record** — log entry with IngestTS, SourceID (UUID), and Raw payload

Two implementations exist:

### File-based (`backend/internal/chunk/file/`)
Disk-persisted storage with a custom binary protocol:
- `manager.go` — chunk lifecycle: rotation based on MaxChunkBytes, one active writable chunk at a time, lazy loading of existing chunks from disk
- `record.go` — binary record format: `[size(4)][magic(1)][version(1)][ingestTS(8)][sourceLocalID(4)][rawLen(4)][payload][trailing_size(4)]`. Magic `0x69`, version `0x01`
- `meta_store.go` — per-chunk `meta.bin` file with atomic writes (temp file + rename)
- `sources.go` — bidirectional UUID ↔ uint32 source ID mapping per chunk (`sources.bin`)
- `reader.go` / `mmap_reader.go` — standard I/O and memory-mapped readers; mmap preferred for sealed chunks

### Memory-based (`backend/internal/chunk/memory/`)
In-memory implementation of the same interfaces, useful for testing and temporary storage. Rotates chunks based on record count rather than byte size.

## Key Design Patterns

- Interface segregation: consumers depend on `chunk.ChunkManager`, not concrete types
- All managers are mutex-protected for concurrent access
- Atomic file operations for metadata (temp file then rename)
- Binary protocol includes magic bytes, version, and size validation for corruption detection
- Records have trailing size fields to support reverse reading
