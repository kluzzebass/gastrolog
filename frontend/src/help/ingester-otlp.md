# OTLP (OpenTelemetry)

Type: `otlp`

Accepts OpenTelemetry log records via both HTTP and gRPC transports. Compatible with any OpenTelemetry SDK or collector configured to export logs.

| Param | Description | Default |
|-------|-------------|---------|
| `http_addr` | HTTP listen address (POST /v1/logs) | `:4318` |
| `grpc_addr` | gRPC listen address (LogsService/Export) | `:4317` |

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

## Backpressure

Returns HTTP 429 (Too Many Requests) or gRPC `RESOURCE_EXHAUSTED` when the ingest queue is near capacity. Clients should retry with backoff.
