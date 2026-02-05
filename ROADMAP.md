# GastroLog Roadmap

This document outlines planned features and improvements. Items are roughly grouped by theme, not priority.

## Architecture

### Single Binary, Two Modes
- `gastrolog serve` - run as server
- `gastrolog repl [address]` - connect to server via socket
- `gastrolog repl --embedded` - embedded mode for quick local debugging (no server needed)

### Connect RPC API
- gRPC over Unix socket for local REPL and CLI tools
- Connect protocol for browser compatibility (future web UI)
- Service definitions: query, config, lifecycle, streaming

### Config Store
- Replace file-based config with SQLite
- Runtime config changes persisted automatically
- Config versioning and rollback
- Watch for external config changes

## Receivers

### Production Receivers

**Standards & Protocols:**
- **Syslog** - UDP and TCP (RFC 3164, RFC 5424)
- **RELP** - Reliable Event Logging Protocol with ack support
- **OTLP** - OpenTelemetry logs (gRPC and HTTP)
- **GELF** - Graylog Extended Log Format (UDP/TCP, Docker-native)
- **Beats** - Elastic Beats protocol (Filebeat, etc.)
- **Fluent Forward** - Fluent Bit/Fluentd forward protocol

**Container/Orchestration:**
- **Docker** - JSON file driver or Docker socket API
- **Kubernetes** - Log files from /var/log/containers or API
- **Journald** - systemd journal integration

**Generic:**
- **HTTP** - POST endpoint for log ingestion
- **File/Tail** - Watch and tail log files
- **Kafka** - Consumer for log pipelines

### Receiver Features
- Backpressure signaling
- Error callbacks (currently silently ignored)
- Per-receiver metrics

## Storage

### Retention & Cleanup
- TTL-based chunk deletion
- Size-based retention (keep last N bytes)
- Policy per store
- Background cleanup goroutine

### Compression
- Optional compression for sealed chunks
- Configurable algorithm (zstd, lz4)
- Transparent decompression on read

### Cloud Storage (Future)
- S3-compatible backend
- Tiered storage (hot local, cold S3)

## Indexing

### Format-Specific Indexes

Specialized indexers that parse and extract fields from known log formats. Each indexer tries to parse records and skips those that don't match. A single chunk can have entries in multiple format indexes if it contains mixed log types.

- **JSON Index** - extracts fields from JSON logs
  - Top-level fields: `level=error`, `msg=failed`
  - Nested paths: `user.id=123`, `request.headers.content-type=application/json`
  - Arrays: `tags[]=production`

- **Apache/Nginx Index** - parses access log formats
  - Common Log Format and Combined Log Format
  - Fields: `method`, `path`, `status`, `bytes`, `referer`, `user_agent`, `remote_addr`

- **Syslog Index** - parses RFC 3164/5424 structure
  - Fields: `facility`, `severity`, `hostname`, `app`, `pid`, `msgid`

- **Logfmt Index** - parses key=value logfmt style
  - More structured than the heuristic KV indexer

Query examples:
```
query status=500 method=POST    # uses apache index
query level=error user.id=123   # uses json index
```

## Query

### Aggregations
- `count()`, `count_distinct()`
- `min()`, `max()` on timestamps
- Group by time buckets
- Group by attribute values

### Pattern Matching
- Regex support in token search
- Prefix/suffix matching
- Glob patterns

### Query Performance
- Query cost estimation in explain
- Query timeout enforcement
- Result size limits

## Observability

### Metrics
- Prometheus endpoint
- Ingestion rate, query latency, index build time
- Per-store and per-receiver metrics
- Routing distribution (messages per store)

### Telemetry
- Message drop tracking (TODO in route.go)
- Index build failures
- Query errors

## Security

### Authentication
- mTLS for gastrolog-to-gastrolog communication
- API tokens for REPL/CLI access
- Optional authentication for local Unix socket

### Authorization
- Store-level read/write permissions
- Query restrictions per user/token

## Operations

### Health & Lifecycle
- Health check endpoint
- Graceful shutdown with drain
- Ready/live probes for k8s

### Deployment
- Dockerfile
- Helm chart
- systemd service file

## REPL Enhancements

### Client-Server REPL
- Reconnection on server restart
- Session persistence
- Multiple concurrent sessions

### New Commands
- `tail` - live stream with filters (like `follow` but in client mode)
- `config` - view/edit configuration
- `retention` - manage retention policies
- `receivers` - list/manage receivers

## Documentation

- Architecture overview
- Deployment guide
- Query language reference
- Receiver configuration guide
- API reference (once Connect RPC exists)

---

## Current State (What Exists)

For reference, here's what's already implemented:

- **Core**: Chunk-based storage (file + memory), orchestrator, routing
- **Indexing**: Token, attribute, and KV indexes with budget control
- **Query**: Boolean expressions, time bounds, pagination, context windows, explain
- **REPL**: 11 commands (query, follow, explain, chunks, indexes, analyze, etc.)
- **Receivers**: Chatterbox (test receiver with 6 log formats)
- **Config**: File-based with runtime reconfiguration support
