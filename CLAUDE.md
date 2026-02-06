# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## CRITICAL: DO NOT BUMP FILE VERSIONS OR CREATE MIGRATION CODE!

This project is NOT in production. When changing file formats, just change them. Do not increment version numbers or write backward-compatibility code. Delete old data and start fresh.

## Data Integrity: Facts Before Speculation

Never present derived, heuristic, or approximate data as if it were authoritative. If information comes directly from the system (stored in a record, returned by an API), show it. If it's reconstructed client-side via heuristics (regex extraction, approximation of server logic), either don't show it or label it clearly as derived. When in doubt, leave it out — showing wrong data is worse than showing less data.

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
- **Attributes** -- `map[string]string` with binary encode/decode methods; embedded directly in records
- **Record** -- log entry with `SourceTS`, `IngestTS`, `WriteTS` (all `time.Time`), `Attrs` (Attributes), and `Raw` payload
  - `SourceTS` -- when the log was generated at the source (parsed from syslog timestamp, Loki payload, etc.; zero if unknown)
  - `IngestTS` -- when the receiver received the message
  - `WriteTS` -- when the chunk manager appended the record (monotonic within a chunk)
  - `Attrs` -- key-value metadata stored alongside the record (no central registry)
- **RecordRef** -- `ChunkID` + record index `Pos` (`uint64`); used for cursor positioning via `Seek`
- **ChunkMeta** -- `ID`, `StartTS`, `EndTS` (`time.Time`), `Sealed`
- **ChunkID** -- UUID v7 typed identifier (time-ordered, sortable)

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
- **IngestMessage** -- `{Attrs, Raw, SourceTS, IngestTS}` where `SourceTS` is parsed from the log source and `IngestTS` is set by receiver at receive time
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
- No routing logic: all records go to all registered chunk managers (fan-out only)

### Receiver package (`backend/internal/receiver/`)

Contains receiver implementations. Receivers emit `IngestMessage` to the orchestrator's ingest channel.

**IngestMessage:**
- `Attrs` -- key-value metadata (map[string]string)
- `Raw` -- log line bytes
- `IngestTS` -- when the receiver received this message
- `Ack` -- optional channel for write acknowledgement (nil = fire-and-forget)

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

**HTTP receiver (`http/`):**

Loki-compatible HTTP receiver for log ingestion:
- `POST /loki/api/v1/push` -- main endpoint (Loki Push API)
- `POST /api/prom/push` -- legacy endpoint
- `GET /ready` -- health check
- JSON format: `{streams: [{stream: {labels}, values: [[ts_ns, line, metadata?]]}]}`
- Timestamp as nanoseconds since epoch (string, per Loki spec)
- Stream labels and structured metadata merged into attrs
- Supports gzip compression (`Content-Encoding: gzip`)
- Default port 3100 (Loki's default)
- `X-Wait-Ack: true` header for acknowledged mode (GastroLog extension)
- Attribute limits: max 32 attrs, 64 char keys, 256 char values
- Compatible with Promtail, Grafana Alloy, Fluent Bit

**Syslog receiver (`syslog/`):**

Standard syslog receiver supporting both RFC 3164 (BSD) and RFC 5424 (IETF):
- UDP and TCP listeners (configurable, can enable one or both)
- Auto-detects RFC 3164 vs RFC 5424 format
- TCP supports newline-delimited and octet-counted framing
- Parses priority into `facility`, `severity`, `facility_name`, `severity_name`
- Extracts `hostname`, `app_name`, `proc_id`, `msg_id` into attrs
- Adds `remote_ip` attr from sender address
- Raw message preserved as-is (including priority)
- Default port 514 (standard syslog port)

### Query package (`backend/internal/query/`)

High-level search API over chunks and indexes:

- **Engine** -- created with `New(ChunkManager, IndexManager)`, executes queries
- **Query** struct:
  - `Start`, `End` -- time bounds (if `End < Start`, results returned in reverse/newest-first order)
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
  - `applyTokenIndex()` -- contributes positions or adds `tokenFilter`
- **Graceful fallback** -- if index returns `ErrIndexNotFound` (sealed but not yet indexed), falls back to sequential scan

Adding a new filter type requires:
1. One `applyXxxIndex(builder, indexes, chunkID, params) (ok, empty bool)` function
2. One `xxxFilter(params) recordFilter` function
3. A few lines in `buildScanner` to call them

Query execution:
1. Selects chunks overlapping time range (sorted by StartTS, ascending or descending)
2. For each chunk, builds scanner via pipeline:
   - Use binary search on idx.log for start position (idx.log has fixed 30-byte entries with WriteTS)
   - Try token index for position lists
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
- **IndexManager** interface: `BuildIndexes`, `OpenTokenIndex`
- **Index[T]** -- generic read-only wrapper over `[]T` with `Entries()` method
- **TokenIndexEntry** -- `{Token string, Positions []uint64}`

**BuildHelper (`build.go`):**

Shared concurrency primitive used by both file and memory managers:
- Uses `callgroup.Group[ChunkID]` to deduplicate concurrent `BuildIndexes` calls for the same chunkID
- `errgroup.WithContext` parallelizes individual indexers within a single build
- `context.WithoutCancel` detaches the build from the initiator's context so one caller cancelling doesn't abort the shared build
- Early `ctx.Err()` check bails out immediately on already-cancelled contexts

**TokenIndexReader (`token_reader.go`):**

Binary search lookup for inverted token index:
- `Lookup(token)` returns `(positions []uint64, found bool)`
- Entries sorted by token string for binary search

**KVIndexReader (`kv_reader.go`):**

Binary search lookup for KV inverted indexes (extracted key=value pairs from log text):
- `KVKeyIndexReader` -- lookup by key string
- `KVValueIndexReader` -- lookup by value string
- `KVIndexReader` -- lookup by key+value pair
- All return `(positions []uint64, found bool)`
- Entries sorted for binary search

**Inverted index format (`inverted/inverted.go`):**

Shared generic encode/decode for inverted index file formats:
- `EncodeKeyIndex[T]`, `EncodeValueIndex[T]`, `EncodeKVIndex[T]` -- generic encoding
- `DecodeKeyIndex[T]`, `DecodeValueIndex[T]`, `DecodeKVIndex[T]` -- generic decoding
- Used by both attr and kv index packages to eliminate duplication
- Binary format: `[header][entry_count:u32][string_table][posting_blob]`

### File-based chunk storage (`backend/internal/chunk/file/`)

Disk-persisted storage with split file format:
- `manager.go` -- chunk lifecycle: rotation based on MaxChunkBytes (soft) or 4GB (hard), one active writable chunk, lazy loading from disk
- `record.go` -- idx.log entry format (38 bytes): SourceTS, IngestTS, WriteTS, RawOffset, RawSize, AttrOffset, AttrSize
- `record_reader.go` -- `RecordCursor` implementation with mmap for sealed chunks, stdio for active chunks

**Split file format:**
- `raw.log` (type 'r') -- 4-byte header + concatenated raw log bytes (no framing)
- `idx.log` (type 'i') -- 4-byte header + fixed-size 38-byte entries per record
- `attr.log` (type 'a') -- 4-byte header + concatenated encoded attribute records
- Position semantics: record indices (0, 1, 2, ...) not byte offsets
- ChunkMeta derived from idx.log (no separate meta.bin file)
- Sealed flag stored in header flags byte of all three files
- Chunks are self-contained: all data needed to reconstruct records is in the chunk directory

**Directory locking:**
- `.lock` file in data directory with exclusive flock
- Prevents multiple processes from corrupting shared data
- Lock acquired on `NewManager`, released on `Close`
- Second process attempting to open same directory fails with `ErrDirectoryLocked`

### Memory-based chunk storage (`backend/internal/chunk/memory/`)

In-memory implementation of the same interfaces. Rotates chunks based on record count. Useful for testing.

### File-based index manager (`backend/internal/index/file/`)

- `manager.go` -- `IndexManager` implementation; delegates `BuildIndexes` to `BuildHelper`
- `token/` -- token indexer: inverted index mapping tokens to record indices; writes `_token.idx`
- `attr/` -- attribute indexer: inverted indexes for record attributes (key, value, key+value); writes `_attr_key.idx`, `_attr_val.idx`, `_attr_kv.idx`
- `kv/` -- KV indexer: inverted indexes for key=value pairs extracted from log text; writes `_kv_key.idx`, `_kv_val.idx`, `_kv_kv.idx`
  - Budget-based admission control with frequency-based sorting
  - Defensive hard caps (MaxUniqueKeys, MaxValuesPerKey, MaxTotalEntries)
  - Status byte indicates complete or capped (query must fall back to runtime filtering)

### Memory-based index manager (`backend/internal/index/memory/`)

- `manager.go` -- `IndexManager` implementation with generic `IndexStore[T]` interface; delegates `BuildIndexes` to `BuildHelper`
- `token/` -- in-memory token indexer with `Get(chunkID)` satisfying `IndexStore[T]`
- `attr/` -- in-memory attribute indexer
- `kv/` -- in-memory KV indexer with budget-based admission control

### Tokenizer package (`backend/internal/tokenizer/`)

Text tokenization utilities used for indexing and query-time matching:

**Token extraction (`token.go`):**
- `Simple([]byte) []string` -- extracts indexable tokens using `DefaultMaxTokenLen` (16)
- `SimpleWithMaxLen([]byte, int) []string` -- extracts tokens with custom max length
- `IterBytes(data, buf, maxLen, fn)` -- zero-allocation iterator for hot paths
- Token rules:
  - Valid characters: a-z, A-Z (lowercased), 0-9, underscore, hyphen (ASCII only)
  - Length: 2 to maxLen bytes (default 16)
  - Excluded: numeric tokens (decimal, hex, octal, binary), hex-with-hyphens, canonical UUIDs
- Same tokenizer used at index time and query time ensures consistent matching

**Key-value extraction (`kv.go`):**
- `ExtractKeyValues([]byte) []KeyValue` -- extracts key=value pairs from log text
- Handles various formats: `key=value`, `key="quoted value"`, `key='quoted'`
- Keys and values are lowercased for case-insensitive matching
- Used by KV indexer for heuristic log field extraction

**Common utilities (`common.go`):**
- `IsLetter`, `IsDigit`, `IsHexDigit`, `IsWhitespace` -- ASCII character classification
- `Lowercase` -- fast ASCII lowercase for single byte
- `ToLowerASCII` -- in-place ASCII lowercasing for byte slices

### Server package (`backend/internal/server/`)

Connect RPC server exposing orchestrator functionality via gRPC/HTTP:

- **Server** -- main server struct, creates HTTP handler with h2c for HTTP/2 without TLS
- **QueryServer** -- implements QueryService (Search, Follow, Explain)
- **StoreServer** -- implements StoreService (ListStores, ListChunks, GetChunk, GetIndexes, AnalyzeChunk, GetStats)
- **ConfigServer** -- implements ConfigService (GetConfig, UpdateStoreRoute, etc.)
- **LifecycleServer** -- implements LifecycleService (Health, Shutdown)

**Server methods:**
- `Serve(listener)` -- serve on any net.Listener
- `ServeUnix(path)` -- serve on Unix socket
- `ServeTCP(addr)` -- serve on TCP address
- `Handler()` -- returns http.Handler for embedding/testing
- `Stop(ctx)` -- graceful shutdown
- `ShutdownChan()` -- channel closed when shutdown initiated via RPC

**Graceful shutdown with drain:**
- `inFlight sync.WaitGroup` tracks in-flight requests
- `draining atomic.Bool` rejects new requests during drain
- `trackingMiddleware` wraps handlers to increment/decrement WaitGroup
- When `Shutdown(drain=true)` called, sets draining flag, waits for in-flight requests, then signals shutdown
- When `Shutdown(drain=false)` called, signals shutdown immediately

**Kubernetes probe endpoints:**
- `/healthz` -- liveness probe, always returns 200
- `/readyz` -- readiness probe, returns 200 when orchestrator running and not draining, 503 otherwise

**Proto definitions** in `backend/api/proto/gastrolog/v1/`:
- `query.proto` -- Query, Record, ChunkPlan messages; streaming Search/Follow RPCs
- `store.proto` -- StoreInfo, ChunkMeta, IndexInfo messages
- `config.proto` -- configuration management RPCs
- `lifecycle.proto` -- Health and Shutdown RPCs

Generated code in `backend/api/gen/gastrolog/v1/`.

### REPL package (`backend/internal/repl/`)

Interactive command-line interface for querying a running GastroLog system:

- Built on readline for command input with history and tab completion
- Built-in pager using bubbletea for viewing query results and live follow streams
- Observes system via public APIs only (no lifecycle control)
- Dynamic status prompt showing active query state: `[query] > `

**Client abstraction:**

The REPL uses a `Client` interface to abstract the backend, enabling both local and remote operation:

- **Client** interface -- `Search`, `Explain`, `ListStores`, `ChunkManager`, `IndexManager`, `Analyzer`, `IsRunning`
- **GRPCClient** -- makes Connect RPC calls to a remote server
- **EmbeddedClient** -- uses in-memory HTTP transport to talk gRPC to an in-process server

```go
// Remote connection
client := repl.NewGRPCClient("http://localhost:8080")

// Unix socket connection
client := repl.NewGRPCClientUnix("/var/run/gastrolog.sock")

// Embedded mode (gRPC over in-memory transport)
client := repl.NewEmbeddedClient(orch)
```

The REPL always talks gRPC internally, whether connecting to a remote server or running in embedded mode. This minimizes differences between standalone and client-server operation.

**Commands:**
- `query [filters...]` -- execute query with filters, display in pager
- `follow [filters...]` -- stream new records in real-time (like tail -f), display in live pager
- `explain [filters...]` -- show query execution plan (which indexes will be used)
- `next [count]` -- fetch next page of results (non-interactive mode only)
- `stores` -- list available stores
- `set key=value` -- configure REPL settings
- `reset` -- clear query state
- `chunks` -- list all chunks with metadata
- `chunk <id>` -- show details for a specific chunk
- `indexes <chunk-id>` -- show index status for a chunk
- `analyze [chunk-id]` -- analyze index health (all chunks if no ID)
- `stats` -- show overall system statistics
- `status` -- show live system state
- `help` / `?` -- show help
- `exit` / `quit` / Ctrl-D -- exit

**Built-in Pager:**

Query results and follow streams display in a full-screen pager with navigation:
- `j`/`k` or arrows -- scroll up/down one line
- `Space`/`b` or PgDn/PgUp -- scroll up/down one page
- `g`/`G` or Home/End or Alt+Up/Down -- jump to top/bottom
- `h`/`l` or left/right arrows -- scroll left/right
- `0`/`$` or Ctrl+A/Ctrl+E or Alt+Left/Right -- jump to line start/end
- `n` -- fetch more results (query pager only)
- `q` -- quit pager
- Mouse wheel -- vertical scrolling

The live pager (for `follow`) auto-scrolls when at the bottom, stays in place when scrolled up.
Output is sanitized to replace control characters with � to prevent terminal corruption.

**Query filters:**
- Bare words -- token search (AND semantics): `query error warning`
- `start=TIME` -- start time (RFC3339 or Unix timestamp)
- `end=TIME` -- end time (RFC3339 or Unix timestamp)
- `limit=N` -- maximum results
- `key=value` -- filter by key=value in attrs OR message body (AND semantics)
- `key=*` -- filter by key existence (any value)
- `*=value` -- filter by value existence (any key)

**Explain command:**

Shows query execution plan with index pipeline for each chunk. For multi-store queries, shows store ID:
```
Chunk 1: [default] 019c1b36-... (sealed)
  Records: 10000

  Index Pipeline:
    1. time           10000 →  9500 [seek] reason=binary_search skip 500 via idx.log
    2. token           9500 →  2500 [indexed] reason=indexed 1 token(s) intersected
    3. kv              2500 →   800 [indexed] reason=indexed attr_kv=800 msg_kv=0

  Scan: index-driven
  Estimated Records Scanned: ~800
  Runtime Filter: time bounds
```

Pipeline step reasons: `indexed`, `binary_search`, `index_missing`, `non_ascii`, `numeric`, `not_indexed`, `empty_intersection`, `no_match`, `budget_exhausted`, `value_not_indexed`

**Settings (via `set`):**
- `pager=N` -- records per page (0 = no paging, show all at once)

**File organization:**
- `repl.go` -- main REPL infrastructure, readline loop, command dispatch
- `pager.go` -- bubbletea-based pager for static and live (follow) output
- `parse.go` -- shared query argument parsing (used by query, follow, explain)
- `cmd_*.go` -- one file per command group (help, query, explain, chunks, etc.)
- `client.go` -- Client interface definition
- `client_grpc.go` -- GRPCClient for remote/embedded connections
- `client_embedded.go` -- EmbeddedClient with in-memory HTTP transport

## Binary Format Conventions

All binary files use **little-endian** byte order. Timestamps stored on disk are **int64 Unix microseconds** (converted to/from `time.Time` at the encode/decode boundary).

Common 4-byte header prefix for all binary files:
```
signature (1 byte, 0x69 'i') | type (1 byte) | version (1 byte) | flags (1 byte)
```

Type bytes: `'r'` = raw.log, `'i'` = idx.log, `'a'` = attr.log, `'k'` = token index.

Flags: bit 0 (`0x01`) = sealed.

The `format` package (`internal/format/`) provides shared encoding/decoding utilities for the common header.

Index file headers include the chunk ID (16 bytes) after the common 4-byte prefix. See `docs/file_formats.md` for full binary format specifications.

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
      http/                     Loki-compatible HTTP receiver
        receiver.go             HTTP server with Loki Push API
        factory.go              ReceiverFactory implementation
      syslog/                   RFC 3164/5424 syslog receiver
        receiver.go             UDP and TCP syslog listeners
        factory.go              ReceiverFactory implementation
    chunk/
      chunk.go                  Interfaces (ChunkManager, RecordCursor, MetaStore), ManagerFactory
      types.go                  Data types (Record, Attributes, ChunkMeta, ChunkID, RecordRef)
      file/                     File-based chunk manager (raw.log, idx.log, attr.log)
      memory/                   Memory-based chunk manager
    index/
      index.go                  Shared types, Indexer/IndexManager interfaces, generic Index[T], ManagerFactory
      build.go                  BuildHelper (callgroup + errgroup)
      token_reader.go           Shared TokenIndexReader with Lookup
      kv_reader.go              Shared KV index readers (key, value, kv)
      inverted/
        inverted.go             Generic encode/decode for inverted index formats
      file/
        manager.go              File-based IndexManager
        token/                  File-based token indexer (_token.idx)
        attr/                   File-based attribute indexer (_attr_*.idx)
        kv/                     File-based KV indexer (_kv_*.idx)
      memory/
        manager.go              Memory-based IndexManager with generic IndexStore[T]
        token/                  Memory-based token indexer
        attr/                   Memory-based attribute indexer
        kv/                     Memory-based KV indexer
    query/
      query.go                  Query engine with Search API, context windows support
      scanner.go                Composable scanner pipeline (scannerBuilder, filters)
    tokenizer/
      token.go                  Token extraction with configurable max length
      kv.go                     Key-value pair extraction from log text
      common.go                 ASCII character utilities
    config/
      config.go                 Config types (Store, Receiver definitions)
      file/                     File-based config store
      memory/                   Memory-based config store
    repl/
      repl.go                   Interactive REPL with bubbletea, status prompt
      parse.go                  Shared query argument parsing
      cmd_help.go               Help command
      cmd_query.go              Query, follow, next, reset commands
      cmd_explain.go            Explain command with index pipeline analysis
      cmd_store.go              Store command
      cmd_set.go                Set command
      cmd_chunks.go             Chunks, chunk commands
      cmd_indexes.go            Indexes command
      cmd_analyze.go            Analyze command
      cmd_stats.go              Stats, status commands
      client.go                 Client interface definition
      client_grpc.go            GRPCClient for remote connections
      client_embedded.go        EmbeddedClient with in-memory transport
    server/
      server.go                 Main server, h2c handler, lifecycle
      query.go                  QueryServer (Search, Follow, Explain)
      store.go                  StoreServer (ListStores, ListChunks, etc.)
      config.go                 ConfigServer
      lifecycle.go              LifecycleServer (Health, Shutdown)
  api/
    proto/gastrolog/v1/         Proto definitions
      query.proto               Query service and messages
      store.proto               Store service and messages
      config.proto              Config service and messages
      lifecycle.proto           Lifecycle service and messages
    gen/gastrolog/v1/           Generated Go code
  cmd/
    gastrolog/
      main.go                   Main entry point
docs/
  file_formats.md               Binary format specifications
```
