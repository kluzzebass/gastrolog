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

**Logging:**

- Use `slog` for structured logging (Go 1.21+)
- Dependency-inject loggers; never use `slog.SetDefault()` or global state
- Each component owns its scoped logger, created at construction with `slog.With()`
- If no logger provided, default to discard logger via `logging.Default(logger)`
- No logging in hot paths (tokenization, scanning, indexing inner loops)
- Standard component attributes: `component`, `type`, `instance` where applicable

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
- **Record** -- log entry with `IngestTS`, `WriteTS` (both `time.Time`), `SourceID` (UUID), and `Raw` payload
  - `IngestTS` -- when the receiver received the message
  - `WriteTS` -- when the chunk manager appended the record (monotonic within a chunk)
- **RecordRef** -- `ChunkID` + record index `Pos` (`uint64`); used for cursor positioning via `Seek`
- **ChunkMeta** -- `ID`, `StartTS`, `EndTS` (`time.Time`), `Sealed`
- **ChunkID** / **SourceID** -- UUID v7 typed identifiers (time-ordered, sortable)

### Logging package (`backend/internal/logging/`)

Provides helpers for structured logging with `slog`:

- `Discard() *slog.Logger` -- returns a logger that discards all output
- `Default(logger *slog.Logger) *slog.Logger` -- returns logger if non-nil, otherwise returns Discard()
- `ComponentFilterHandler` -- wraps an slog.Handler to filter logs by component-specific levels

Used by all components to handle nil logger gracefully:
```go
func New(cfg Config) *Foo {
    return &Foo{
        logger: logging.Default(cfg.Logger).With("component", "foo"),
    }
}
```

**ComponentFilterHandler** enables dynamic, attribute-based logging control:
- Inspects each log record for a "component" attribute
- Maintains a per-component minimum level map (copy-on-write for thread-safety)
- Records below the minimum level are dropped; others pass to wrapped handler
- Components without explicit levels fall back to a default level

```go
// Setup in main():
base := slog.NewTextHandler(os.Stderr, nil)
filter := logging.NewComponentFilterHandler(base, slog.LevelInfo)
logger := slog.New(filter)

// Runtime control (e.g., from control plane):
filter.SetLevel("orchestrator", slog.LevelDebug)  // Enable debug for one component
filter.ClearLevel("orchestrator")                  // Revert to default
```

Key design points:
- Loggers remain immutable; components never mutate logging state
- Filtering is centralized; policy lives outside components
- Lock-free reads in Handle() via atomic pointer to map
- Handlers created via `logger.With()` share the same level configuration

### Orchestrator package (`backend/internal/orchestrator/`)

Coordinates ingestion, indexing, and querying without owning business logic:

- **Orchestrator** -- routes records to chunk managers, triggers index builds on seal, delegates queries
- **Receiver** interface -- sources of log messages; emit `IngestMessage` to shared channel
- **IngestMessage** -- `{Attrs, Raw, IngestTS}` where `IngestTS` is set by receiver at receive time
- **Factories** -- holds factory functions and shared `Logger` for component creation
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

### Source package (`backend/internal/source/`)

Manages source identity and metadata with fast in-memory resolution:

- **Registry** -- maps attribute sets to SourceIDs, creates new sources on demand
- **Source** -- `{ID, Attributes, CreatedAt}` representing a log source
- **Store** interface -- persistence layer (`Save`, `LoadAll`)

**Concurrency model:**
- `Resolve(attrs)` is fast and fully in-memory (read-lock fast path, write-lock for creation)
- New sources queued for async persistence via buffered channel (non-blocking, best-effort)
- Persistence failures do not break ingestion
- On startup, loads existing sources from store

**Key methods:**
- `Resolve(attrs map[string]string) SourceID` -- returns existing or creates new source
- `Get(id) (*Source, bool)` -- retrieve source by ID
- `Query(filters map[string]string) []SourceID` -- find sources matching attribute filters
- `Close()` -- drains persist queue and stops background goroutine

### Receiver package (`backend/internal/receiver/`)

Contains receiver implementations. Receivers emit `IngestMessage` to the orchestrator's ingest channel.

**Chatterbox receiver (`chatterbox/`):**

Test receiver that generates random log messages at configurable intervals:
- Six log format generators with weighted random selection:
  - `PlainTextFormat` -- simple unstructured messages
  - `KeyValueFormat` -- structured key=value lines
  - `JSONFormat` -- JSON-structured logs with varied field schemas
  - `AccessLogFormat` -- Apache/Nginx-style access logs
  - `SyslogFormat` -- RFC 3164-style syslog messages
  - `WeirdFormat` -- malformed/edge-case data for tokenization stress testing
- `AttributePools` -- pre-generated pools of hosts, services, envs for consistent cardinality
- Configurable min/max intervals, instance ID, format weights

### Query package (`backend/internal/query/`)

High-level search API over chunks and indexes:

- **Engine** -- created with `New(ChunkManager, IndexManager)`, executes queries
- **Query** struct:
  - `Start`, `End` -- time bounds (if `End < Start`, results returned in reverse/newest-first order)
  - `Sources` -- filter by source IDs (OR semantics, nil = no filter)
  - `Tokens` -- filter by tokens (AND semantics, nil = no filter)
  - `Limit` -- max results (0 = unlimited)
  - `ContextBefore`, `ContextAfter` -- records to include around matches (for context windows)
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

### Callgroup package (`backend/internal/callgroup/`)

Generic call deduplication primitive:
- `Group[K comparable]` -- deduplicates concurrent function calls by key
- `DoChan(key, fn) <-chan error` -- if no call in flight for key, executes fn; otherwise returns channel that receives the existing call's result
- Once fn returns, the key is forgotten and future calls trigger new execution
- Used by `BuildHelper` to deduplicate concurrent index builds for the same chunk

### Format package (`backend/internal/format/`)

Shared binary format utilities for the common 4-byte header:
- `Header` struct with `Type`, `Version`, `Flags` fields
- `Encode() [4]byte` / `EncodeInto(buf) int` -- encode header
- `Decode(buf) (Header, error)` -- decode and validate signature
- `DecodeAndValidate(buf, expectedType, expectedVersion) (Header, error)` -- full validation
- Eliminates duplication across index and metadata file readers

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
- Uses `callgroup.Group[ChunkID]` to deduplicate concurrent `BuildIndexes` calls for the same chunkID
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

Tokenizer used for both indexing and query-time matching:
- `Simple([]byte) []string` -- extracts indexable tokens using `DefaultMaxTokenLen` (16)
- `SimpleWithMaxLen([]byte, int) []string` -- extracts tokens with custom max length
- `IterBytes(data, buf, maxLen, fn)` -- zero-allocation iterator for hot paths
- Token rules:
  - Valid characters: a-z, A-Z (lowercased), 0-9, underscore, hyphen (ASCII only)
  - Length: 2 to maxLen bytes (default 16)
  - Excluded: numeric tokens (decimal, hex, octal, binary), hex-with-hyphens, canonical UUIDs
- Same tokenizer used at index time and query time ensures consistent matching

### File-based chunk storage (`backend/internal/chunk/file/`)

Disk-persisted storage with split file format:
- `manager.go` -- chunk lifecycle: rotation based on MaxChunkBytes (soft) or 4GB (hard), one active writable chunk, lazy loading from disk
- `record.go` -- idx.log entry format (28 bytes): IngestTS, WriteTS, SourceLocalID, RawOffset, RawSize
- `sources.go` -- bidirectional UUID-to-uint32 source ID mapping per chunk (`sources.bin`)
- `record_reader.go` -- `RecordCursor` implementation with mmap for sealed chunks, stdio for active chunks

**Split file format:**
- `raw.log` (type 'r') -- 4-byte header + concatenated raw log bytes (no framing)
- `idx.log` (type 'i') -- 4-byte header + fixed-size 28-byte entries per record
- Position semantics: record indices (0, 1, 2, ...) not byte offsets
- ChunkMeta derived from idx.log (no separate meta.bin file)
- Sealed flag stored in header flags byte of both files

### Memory-based chunk storage (`backend/internal/chunk/memory/`)

In-memory implementation of the same interfaces. Rotates chunks based on record count. Useful for testing.

### File-based index manager (`backend/internal/index/file/`)

- `manager.go` -- `IndexManager` implementation; delegates `BuildIndexes` to `BuildHelper`
- `time/` -- time indexer: sparse index mapping sampled timestamps to record indices; writes `_time.idx`
  - `reader.go` -- `Open` loads and validates index file, returns shared `*index.TimeIndexReader`
- `source/` -- source indexer: inverted index mapping SourceIDs to record indices; writes `_source.idx`
- `token/` -- token indexer: inverted index mapping tokens to record indices; writes `_token.idx`

### Memory-based index manager (`backend/internal/index/memory/`)

- `manager.go` -- `IndexManager` implementation with generic `IndexStore[T]` interface; delegates `BuildIndexes` to `BuildHelper`
- `time/` -- in-memory time indexer with `Get(chunkID)` satisfying `IndexStore[T]`
  - `reader.go` -- `Open` retrieves entries from indexer, returns shared `*index.TimeIndexReader`
- `source/` -- in-memory source indexer with `Get(chunkID)` satisfying `IndexStore[T]`
- `token/` -- in-memory token indexer with `Get(chunkID)` satisfying `IndexStore[T]`

## Binary Format Conventions

All binary files use **little-endian** byte order. Timestamps stored on disk are **int64 Unix microseconds** (converted to/from `time.Time` at the encode/decode boundary).

Common 4-byte header prefix for all binary files:
```
signature (1 byte, 0x69 'i') | type (1 byte) | version (1 byte) | flags (1 byte)
```

Type bytes: `'r'` = raw.log, `'i'` = idx.log, `'t'` = time index, `'s'` = source index, `'k'` = token index, `'z'` = source registry, `'c'` = chunk source map.

Flags: bit 0 (`0x01`) = sealed.

The `format` package (`internal/format/`) provides shared encoding/decoding utilities for the common header.

Index file headers include the chunk ID (16 bytes) after the common 4-byte prefix. See `docs/file_formats.md` for full binary format specifications.

Source map files use leading+trailing size fields for bidirectional traversal.

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
- `BuildHelper` provides callgroup deduplication + errgroup parallelism for concurrent index builds
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
    callgroup/
      callgroup.go              Generic call deduplication by key (Group[K])
    format/
      header.go                 Shared binary header encoding/decoding
    logging/
      logging.go                Discard() and Default() helpers for slog
      filter.go                 ComponentFilterHandler for per-component log levels
    orchestrator/
      orchestrator.go           Orchestrator struct and New()
      lifecycle.go              Start() and Stop() methods
      ingest.go                 Ingest routing and seal detection
      search.go                 Search delegation to query engines
      receiver.go               Receiver interface, IngestMessage, ReceiverFactory
      registry.go               Register* methods for components
      factory.go                Factories struct and ApplyConfig
    receiver/
      chatterbox/               Test receiver generating random log messages
        receiver.go             Receiver implementation
        format.go               LogFormat interface and AttributePools
        format_plain.go         Plain text format
        format_kv.go            Key-value format
        format_json.go          JSON format
        format_access.go        Apache/Nginx access log format
        format_syslog.go        Syslog format
        format_weird.go         Edge-case/malformed data format
        factory.go              ReceiverFactory implementation
    chunk/
      chunk.go                  Interfaces (ChunkManager, RecordCursor, MetaStore), ManagerFactory
      types.go                  Data types (Record, ChunkMeta, ChunkID, SourceID, RecordRef)
      file/                     File-based chunk manager
      memory/                   Memory-based chunk manager
    index/
      index.go                  Shared types, Indexer/IndexManager interfaces, generic Index[T], ManagerFactory
      build.go                  BuildHelper (callgroup + errgroup)
      time_reader.go            Shared TimeIndexReader with FindStart binary search
      source_reader.go          Shared SourceIndexReader with Lookup
      token_reader.go           Shared TokenIndexReader with Lookup
      token/
        tokenize.go             Tokenizer with configurable max length
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
      query.go                  Query engine with Search API, context windows support
      scanner.go                Composable scanner pipeline (scannerBuilder, filters)
    source/
      registry.go               Source identity resolution with async persistence
      file/                     File-based source store
      memory/                   Memory-based source store
    config/
      config.go                 Config types (Store, Receiver definitions)
      file/                     File-based config store
      memory/                   Memory-based config store
docs/
  file_formats.md               Binary format specifications
```
