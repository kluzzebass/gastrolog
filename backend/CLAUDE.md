# Backend CLAUDE.md

Go 1.25+ backend. Module name: `gastrolog`.

## Build & Test

All commands run from `backend/`.

```bash
go build ./...                                    # Build all packages
go test ./...                                     # Run all tests
go test -v ./internal/chunk/file/                 # Single package
go test -v -run TestRecordRoundTrip ./internal/chunk/file/  # Single test
go test -cover ./...                              # Test with coverage
go test -race ./internal/index/...                # Race detector
go mod tidy                                       # Clean dependencies
```

## Running

```bash
just run                    # Server with config.json
just repl                   # Server + interactive REPL
just pprof                  # Server + pprof on :6060
```

The server listens on `:8080` by default (Connect RPC / gRPC-Web).

CLI flags: `-server`, `-repl`, `-config <path>`, `-pprof <addr>`.

## Proto Generation

Proto definitions live in `api/proto/gastrolog/v1/`. Generated Go code goes to `api/gen/gastrolog/v1/`.

```bash
cd api/proto && buf generate      # Regenerate Go code
cd ../../frontend && buf generate # Regenerate TypeScript code (from frontend dir)
```

Always regenerate both backend and frontend after proto changes.

## Data Directory

File-based stores write to `data/` by default (configured in `config.json`). Chunk directories are named by their 13-char base32hex ChunkID. Delete `data/` to start fresh after format changes.

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

## Testing

Use the **memory-based managers** (`chunk/memory` and `index/memory`) for integration tests instead of mocks. They implement the full `ChunkManager` and `IndexManager` interfaces in-memory:

```go
cm, _ := chunkmem.NewManager(chunkmem.Config{MaxChunkBytes: 1 << 20})
tokIdx := memtoken.NewIndexer(cm)
attrIdx := memattr.NewIndexer(cm)
kvIdx := kv.NewIndexer(cm)
im := indexmem.NewManager([]index.Indexer{tokIdx, attrIdx, kvIdx}, tokIdx, attrIdx, kvIdx, nil)
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
- Sub-package pattern: `file/{token,attr,kv}` and `memory/{token,attr,kv}` avoid symbol prefixing
- Query uses `End < Start` convention for reverse order instead of separate bool field

## Binary Format Conventions

All binary files use **little-endian** byte order. Timestamps stored on disk are **int64 Unix microseconds** (converted to/from `time.Time` at the encode/decode boundary).

Common 4-byte header prefix for all binary files:
```
signature (1 byte, 0x69 'i') | type (1 byte) | version (1 byte) | flags (1 byte)
```

Type bytes: `'r'` = raw.log, `'i'` = idx.log, `'a'` = attr.log, `'k'` = token index.

Flags: bit 0 (`0x01`) = sealed.

The `format` package (`internal/format/`) provides shared encoding/decoding utilities for the common header.

Index file headers include the chunk ID (8 bytes) after the common 4-byte prefix. See `docs/file_formats.md` for full binary format specifications.

## Architecture

### Core types (`internal/chunk/types.go`)

- **ChunkManager** interface: `Append`, `Seal`, `Active`, `Meta`, `List`, `OpenCursor`
- **RecordCursor** interface: `Next`, `Prev`, `Seek`, `Close` -- bidirectional record iteration
- **MetaStore** interface: `Save`, `Load`, `List`
- **Attributes** -- `map[string]string` with binary encode/decode methods; embedded directly in records
- **Record** -- log entry with `SourceTS`, `IngestTS`, `WriteTS` (all `time.Time`), `Attrs` (Attributes), and `Raw` payload
  - `SourceTS` -- when the log was generated at the source (parsed from syslog timestamp, Loki payload, etc.; zero if unknown)
  - `IngestTS` -- when the ingester received the message
  - `WriteTS` -- when the chunk manager appended the record (monotonic within a chunk)
  - `Attrs` -- key-value metadata stored alongside the record (no central registry)
- **RecordRef** -- `ChunkID` + record index `Pos` (`uint64`); used for cursor positioning via `Seek`
- **ChunkMeta** -- `ID`, `StartTS`, `EndTS` (`time.Time`), `Sealed`
- **ChunkID** -- 8-byte big-endian uint64 unix microseconds, encoded as 13-char base32hex string (time-ordered, sortable)

### Orchestrator (`internal/orchestrator/`)

Coordinates ingestion, indexing, and querying without owning business logic:

- Routes records to chunk managers, triggers index builds on seal, delegates queries
- **Ingester** interface -- sources of log messages; emit `IngestMessage` to shared channel
- **IngestMessage** -- `{Attrs, Raw, SourceTS, IngestTS}` where `SourceTS` is parsed from the log source and `IngestTS` is set by ingester at receive time

**Lifecycle:** `Start(ctx)` launches ingester goroutines + ingest loop. `Stop()` cancels context, waits for ingesters, closes channel, waits for ingest loop, waits for index builds. All goroutines tracked with `sync.WaitGroup.Go()`.

### Ingesters (`internal/ingester/`)

- **Chatterbox** -- test ingester generating random log messages (six format types with weighted selection)
- **HTTP** -- Loki-compatible `POST /loki/api/v1/push` endpoint (port 3100)
- **Syslog** -- RFC 3164/5424, UDP and TCP (port 514)

### Query engine (`internal/query/`)

- **Search(ctx, query, resume)** returns `(iter.Seq2[Record, error], func() *ResumeToken)`
- Composable scanner pipeline: `scannerBuilder` accumulates position sources and runtime filters
- Index application functions try indexes first, fall back to runtime filters if unavailable
- Adding a new filter type requires: one `applyXxxIndex()` function, one `xxxFilter()` function, a few lines in `buildScanner`

### Index system (`internal/index/`)

- **Indexer** interface: `Name() string`, `Build(ctx, chunkID) error`
- **IndexManager** interface: `BuildIndexes`, `OpenTokenIndex`
- Token indexer: inverted index mapping tokens to record positions
- Attr indexer: inverted indexes for record attributes (key, value, key+value)
- KV indexer: inverted indexes for key=value pairs extracted from log text (budget-based admission)
- `BuildHelper`: callgroup deduplication + errgroup parallelism for concurrent index builds

### Chunk storage (`internal/chunk/`)

**File-based** (`file/`): split file format (raw.log, idx.log, attr.log), rotation on MaxChunkBytes, mmap for sealed chunks, directory locking.

**Memory-based** (`memory/`): in-memory implementation for testing, rotates on record count.

### Server (`internal/server/`)

Connect RPC server with h2c. QueryServer, StoreServer, ConfigServer, LifecycleServer. Graceful shutdown with request draining. Kubernetes probes at `/healthz` and `/readyz`.

### REPL (`internal/repl/`)

Interactive CLI with readline, bubbletea pager, tab completion. Uses Client interface (GRPCClient for remote, EmbeddedClient for in-process). Commands: `query`, `follow`, `explain`, `chunks`, `stores`, `stats`, etc.

### Tokenizer (`internal/tokenizer/`)

- Token rules: ASCII alphanumeric + underscore + hyphen, 2-16 chars, lowercased, excludes numeric-only and UUIDs
- Same tokenizer at index time and query time ensures consistent matching
- KV extraction: `key=value`, `key="quoted"`, case-insensitive

## Directory Layout

```
internal/
  callgroup/        Generic call deduplication by key
  format/           Shared binary header encoding/decoding
  logging/          slog helpers: Discard(), Default(), ComponentFilterHandler
  orchestrator/     Ingest routing, seal detection, search delegation
  ingester/
    chatterbox/     Test ingester (random log messages)
    http/           Loki-compatible HTTP ingester
    syslog/         RFC 3164/5424 syslog ingester
  chunk/
    types.go        Record, Attributes, ChunkMeta, ChunkID, RecordRef
    chunk.go        ChunkManager, RecordCursor, MetaStore interfaces
    file/           File-based chunk manager
    memory/         Memory-based chunk manager
  index/
    index.go        Indexer, IndexManager interfaces
    build.go        BuildHelper (callgroup + errgroup)
    token_reader.go TokenIndexReader
    kv_reader.go    KV index readers
    inverted/       Generic inverted index encode/decode
    file/           File-based index manager + token/attr/kv indexers
    memory/         Memory-based index manager + token/attr/kv indexers
  query/
    query.go        Query engine with Search API
    scanner.go      Composable scanner pipeline
  tokenizer/        Token and KV extraction
  config/           Config types + file/memory stores
  repl/             Interactive REPL
  server/           Connect RPC server
api/
  proto/            Proto definitions
  gen/              Generated Go code
cmd/
  gastrolog/        Main entry point
docs/
  file_formats.md   Binary format specifications
```
