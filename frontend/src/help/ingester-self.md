# Self

Type: `self`

Captures GastroLog's own internal log output and feeds it into the ingest pipeline. This makes the server's operational logs persistent, searchable, and visible in the timeline alongside application logs.

## Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Minimum Level | warn | Minimum log severity to capture. Records below this level still appear on stderr but are not ingested. Options: debug, info, warn, error |

## How It Works

A capture handler is installed in the slog handler chain. Every log record that passes both the component-level filter and the minimum capture level is copied to a bounded channel. The self ingester reads from this channel and emits each record as a structured JSON log line.

Records from pipeline-internal components (orchestrator, ingester, chunk, digest, index, scheduler, record-forwarder, broadcast, dispatch) are **not captured** to prevent feedback loops.

## Attributes

| Attribute | Source |
|-----------|--------|
| `level` | Log severity: debug, info, warn, error |
| `component` | Source component (server, raft, cert, etc.) |
| *(slog attributes)* | All key-value pairs from the log record |

## JSON Body

Each record is a JSON object containing all slog fields:

```json
{"time":"2025-01-15T10:30:00Z","level":"WARN","msg":"TLS certificate expires soon","component":"cert","days_left":7}
```

## Querying

Because records are structured JSON, the KV index picks up all fields automatically:

- `component=raft` — all Raft consensus messages
- `component=server AND level=error` — server errors
- `msg="*replication*"` — messages mentioning replication

## Backpressure

If the capture channel fills up (4096 records), new log records are silently dropped from capture — they still appear on stderr as usual. This ensures internal logging never blocks or slows the application.
