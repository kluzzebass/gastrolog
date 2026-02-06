# GastroLog Backend Roadmap

## Current State

Go 1.25+ backend with Connect RPC server, interactive REPL, chunk-based storage, and multi-index query engine. Single binary with server, REPL, and embedded modes.

### What's Done

- **Core**: Chunk-based storage (file + memory), orchestrator with ingest routing and seal detection
- **Indexing**: Token, attribute, and KV indexes with budget control and callgroup deduplication
- **Query**: Boolean expressions, time bounds, pagination, context windows, explain plans, multi-store search
- **REPL**: 12 commands, bubbletea pager, live follow mode, vim-style navigation, multi-store support
- **Receivers**: Chatterbox (test, 6 formats with SourceTS), HTTP/Loki (Push API), Syslog (RFC 3164/5424, UDP+TCP)
- **Server**: Connect RPC with 4 services, h2c, graceful shutdown with drain, k8s probes
- **Config**: File-based with runtime reconfiguration
- **ChunkID**: 13-char base32hex timestamps (lexicographically time-sorted)

## Phase 1: Retention & Storage

### 1.1 Retention
- [ ] TTL-based chunk deletion
- [ ] Size-based retention (keep last N bytes)
- [ ] Policy per store
- [ ] Background cleanup goroutine

### 1.2 Compression
- [ ] Optional compression for sealed chunks (zstd or lz4)
- [ ] Transparent decompression on read

## Phase 2: Receivers

### 2.1 New Receivers
- [ ] OTLP (OpenTelemetry logs, gRPC and HTTP)
- [ ] Fluent Forward (Fluent Bit/Fluentd forward protocol)
- [ ] Kafka consumer for log pipelines

### 2.2 Receiver Features
- [ ] Backpressure signaling
- [ ] Per-receiver metrics

## Phase 3: Indexing

### 3.1 Format-Specific Indexes
- [ ] JSON index (top-level fields, nested paths, arrays)
- [ ] Apache/Nginx index (method, path, status, bytes, user_agent)
- [ ] Syslog index (facility, severity, hostname, app, pid)
- [ ] Logfmt index (structured key=value, more precise than heuristic KV)

## Phase 4: Query

### 4.1 Aggregations
- [ ] `count()`, `count_distinct()`
- [ ] `min()`, `max()` on timestamps
- [ ] Group by time buckets
- [ ] Group by attribute values

### 4.2 Pattern Matching
- [ ] Regex support in token search
- [ ] Prefix/suffix matching
- [ ] Glob patterns

### 4.3 Query Performance
- [ ] Query cost estimation in explain
- [ ] Query timeout enforcement
- [ ] Result size limits

## Phase 5: Observability & Operations

### 5.1 Metrics
- [ ] Prometheus endpoint
- [ ] Ingestion rate, query latency, index build time
- [ ] Per-store and per-receiver metrics

### 5.2 Deployment
- [ ] Dockerfile
- [ ] Helm chart
- [ ] systemd service file

### 5.3 Config
- [ ] Replace file-based config with SQLite
- [ ] Config versioning and rollback

## Phase 6: Security

### 6.1 Authentication
- [ ] mTLS for gastrolog-to-gastrolog communication
- [ ] API tokens for REPL/CLI access

### 6.2 Authorization
- [ ] Store-level read/write permissions
- [ ] Query restrictions per user/token

## Priority Order

1. **Phase 1** - Retention & Storage (required before any real usage)
2. **Phase 2** - Receivers (broaden ingestion sources)
3. **Phase 3** - Indexing (query performance for structured logs)
4. **Phase 4** - Query (power user features)
5. **Phase 5** - Observability & Operations (production readiness)
6. **Phase 6** - Security (multi-user scenarios)
