# OTLP (OpenTelemetry)

Type: `otlp`

Accepts OpenTelemetry log records via both HTTP and gRPC transports. Compatible with any OpenTelemetry SDK or collector configured to export logs.

| Setting | Description | Default |
|---------|-------------|---------|
| HTTP Address | OTLP/HTTP listen address (POST /v1/logs) | `:4318` |
| gRPC Address | OTLP/gRPC listen address | `:4317` |

**HTTP** accepts both protobuf (`application/x-protobuf`) and JSON (`application/json`) request bodies, with optional gzip compression.

**gRPC** implements the `opentelemetry.proto.collector.logs.v1.LogsService/Export` RPC.

## Attributes

| Attribute | Source |
|-----------|--------|
| *(resource attributes)* | All key-value pairs from the resource |
| *(scope attributes)* | All key-value pairs from the instrumentation scope |
| *(record attributes)* | All key-value pairs from the log record (highest precedence) |
| `severity` | `SeverityText` field |
| `severity_number` | `SeverityNumber` field |
| `trace_id` | Hex-encoded trace ID (if present) |
| `span_id` | Hex-encoded span ID (if present) |

When attribute keys collide, record attributes take precedence over scope attributes, which take precedence over resource attributes.

The log record body is used as the raw log line. Complex body values (arrays, maps) are JSON-serialized.

## TLS

When TLS is enabled, both listeners — the HTTP port and the gRPC port — serve TLS from the same configuration. Select a server certificate from the certificate store — certificates are managed in the Certificates settings tab, and rotations take effect without a restart.

For mutual TLS, also select a Client CA Certificate: clients must then present a certificate signed by that CA, and a plaintext or unverified client is refused before any of its bytes are parsed. The Allowed Client CN field optionally narrows which client certificates are accepted using a wildcard pattern (e.g. `producer-*`).

## Timestamps

| Field | Source |
|-------|--------|
| SourceTS | `TimeUnixNano` (application event time) if present, otherwise `ObservedTimeUnixNano` (collector observation time). |
| IngestTS | GastroLog arrival time. |
| `time_unix_nano` attr | Application event time, always stored when present. |
| `observed_ts` attr | Collector observation time, always stored when present. |

## Backpressure

Returns HTTP 429 (Too Many Requests) or gRPC `RESOURCE_EXHAUSTED` when the ingest queue is near capacity. Clients should retry with backoff.
