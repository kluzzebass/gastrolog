# Self

Type: `self`

Captures GastroLog's own internal log output and feeds it into the ingest pipeline. This makes the server's operational logs persistent, searchable, and visible in the timeline alongside application logs.

There are no user-configurable settings for this ingester.

## How It Works

A capture handler is installed in the slog handler chain. Every log record that passes the component-level filter is copied to a bounded channel. The self ingester reads from this channel and emits each record as a structured JSON log line.

Records from pipeline-internal components (orchestrator, ingester, chunk, digest, index, scheduler) are **not captured** to prevent feedback loops.

## Attributes

| Attribute | Source |
|-----------|--------|
| `level` | Log severity: debug, info, warn, error |
| `component` | Source component (server, raft, dispatch, etc.) |
| *(slog attributes)* | All key-value pairs from the log record |

## JSON Body

Each record is a JSON object containing all slog fields:

```json
{"time":"2025-01-15T10:30:00Z","level":"INFO","msg":"node identity","component":"app","node_id":"01234..."}
```

## Querying

Because records are structured JSON, the KV index picks up all fields automatically:

- `component=raft` — all Raft consensus messages
- `component=server AND level=error` — server errors
- `msg="*replication*"` — messages mentioning replication

## Backpressure

If the capture channel fills up (4096 records), new log records are silently dropped from capture — they still appear on stderr as usual. This ensures internal logging never blocks or slows the application.
