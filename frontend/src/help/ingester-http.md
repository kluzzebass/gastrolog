# HTTP (Loki-Compatible)

Type: `http`

Accepts log pushes via the Loki HTTP API. Compatible with Promtail, Grafana Agent, and other Loki clients. If you're already shipping logs to Loki, you can point them at GastroLog instead. Messages pass through [digestion](help:digesters) for level and timestamp extraction.

| Setting | Description | Default |
|---------|-------------|---------|
| Listen Address | TCP address for the HTTP/Loki Push API | `:3100` |

**Endpoints**: `POST /loki/api/v1/push` and `POST /api/prom/push` (legacy)

Supports gzip-compressed request bodies.

## Attributes

| Attribute | Source |
|-----------|--------|
| *(stream labels)* | All labels from the Loki push request's `stream` field become attributes (e.g., `job`, `env`, `host`) |
| *(structured metadata)* | Key-value pairs from the third element of value arrays, if present |

Labels are validated: max 32 attributes per message, keys up to 64 characters, values up to 256 characters.

By default, the HTTP ingester returns `204 No Content` immediately (fire-and-forget). Clients can send `X-Wait-Ack: true` to wait for the record to be persisted before receiving the response.

## TLS

When TLS is enabled, the push endpoint serves HTTPS. Select a server certificate from the certificate store — certificates are managed in the Certificates settings tab, and rotations take effect without a restart.

For mutual TLS, also select a Client CA Certificate: clients must then present a certificate signed by that CA, and a plaintext or unverified client is refused before any of its bytes are parsed. The Allowed Client CN field optionally narrows which client certificates are accepted using a wildcard pattern (e.g. `producer-*`).

## Timestamps

SourceTS is set from the Loki push request's nanosecond entry timestamp, which is always present in the protocol. IngestTS is set to GastroLog arrival time.

## Recipe

See [Promtail / Grafana Agent](help:recipe-promtail) for configuration examples.
