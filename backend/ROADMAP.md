# GastroLog Backend Roadmap

## Current State

Go 1.25+ backend with Connect RPC server, interactive REPL, chunk-based storage, and multi-index query engine. Single binary with server, REPL, and embedded modes.

### What's Done

- **Core**: Chunk-based storage (file + memory), orchestrator with store filters and seal detection
- **Indexing**: Token, attribute, and KV indexes with budget control and callgroup deduplication
- **Query**: Boolean expressions, time bounds, pagination, context windows, explain plans, multi-store search
- **REPL**: 12 commands, bubbletea pager, live follow mode, vim-style navigation, multi-store support
- **Ingesters**: Chatterbox (test, 6 formats with SourceTS), HTTP/Loki (Push API), Syslog (RFC 3164/5424, UDP+TCP)
- **Ingester identity**: Each ingester stamps `ingester_type` and `ingester_id` on every message via factory-provided ID
- **Server**: Connect RPC with 4 services, h2c, graceful shutdown with drain, k8s probes
- **Histogram**: Time-bucketed record counts via binary search, severity-stacked level breakdown using KV indexes
- **Config**: SQLite-backed with runtime CRUD (stores, ingesters, rotation policies)
- **ChunkID**: 13-char base32hex timestamps (lexicographically time-sorted)

## Phase 1: Retention & Storage

### 1.1 Retention
- [ ] TTL-based chunk deletion
- [ ] Size-based retention (keep last N bytes)
- [x] Policy per store (rotation policies with max bytes/records/age, assignable to stores)
- [ ] Background cleanup goroutine

### 1.2 Compression
- [ ] Optional compression for sealed chunks (zstd or lz4)
- [ ] Transparent decompression on read

## Phase 2: Ingesters

### 2.1 New Ingesters
- [ ] OTLP (OpenTelemetry logs, gRPC and HTTP)
- [ ] Fluent Forward (Fluent Bit/Fluentd forward protocol)
- [ ] Kafka consumer for log pipelines

### 2.2 Ingester Features
- [ ] Backpressure signaling
- [ ] Per-ingester metrics

## Phase 3: Indexing

### 3.1 Format-Specific Indexes
- [ ] JSON index (top-level fields, nested paths, arrays)
- [ ] Apache/Nginx index (method, path, status, bytes, user_agent)
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
- [ ] Per-store and per-ingester metrics

### 5.2 Deployment
- [ ] Dockerfile
- [ ] Helm chart
- [ ] systemd service file

### 5.3 Config
- [x] Replace file-based config with SQLite (runtime CRUD for stores, ingesters, rotation policies)
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
2. **Phase 2** - Ingesters (broaden ingestion sources)
3. **Phase 3** - Indexing (query performance for structured logs)
4. **Phase 4** - Query (power user features)
5. **Phase 5** - Observability & Operations (production readiness)
6. **Phase 6** - Security (multi-user scenarios)
