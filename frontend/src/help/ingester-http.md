# HTTP (Loki-Compatible)

Type: `http`

Accepts log pushes via the Loki HTTP API. Compatible with Promtail, Grafana Agent, and other Loki clients. If you're already shipping logs to Loki, you can point them at GastroLog instead.

| Param | Description | Default |
|-------|-------------|---------|
| `addr` | Listen address | `:3100` |

**Endpoints**: `POST /loki/api/v1/push` and `POST /api/prom/push` (legacy)

Supports gzip-compressed request bodies.

## Attributes

| Attribute | Source |
|-----------|--------|
| *(stream labels)* | All labels from the Loki push request's `stream` field become attributes (e.g., `job`, `env`, `host`) |
| *(structured metadata)* | Key-value pairs from the third element of value arrays, if present |

Labels are validated: max 32 attributes per message, keys up to 64 characters, values up to 256 characters.

By default, the HTTP ingester returns `204 No Content` immediately (fire-and-forget). Clients can send `X-Wait-Ack: true` to wait for the record to be persisted before receiving the response.
