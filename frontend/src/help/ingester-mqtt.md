# MQTT

Type: `mqtt`

Subscribes to one or more MQTT topics on a broker. Each published message becomes a log record with the message payload as the raw log line.

| Setting | Description | Default |
|---------|-------------|---------|
| Broker | Broker URL (required). Schemes: `mqtt://`, `tcp://`, `ssl://`, `tls://`, `ws://`, `wss://` | |
| Topics | Comma-separated topic patterns (required). Supports `+` (single-level) and `#` (multi-level) wildcards | |
| Client ID | MQTT client identifier. Auto-generated from the ingester ID if left empty | `gastrolog-<last 8 chars of ID>` |
| Protocol Version | `v3.1.1` or `v5` | v3.1.1 |
| Enable TLS | Use TLS for the broker connection | off |
| Clean session | Start with a clean session (see below) | on |
| Username | Username for broker authentication | |
| Password | Password for broker authentication | |

The ingester always subscribes at QoS 1 (at least once). The broker delivers each message at the minimum of the publisher's QoS and the subscriber's QoS, so this never downgrades messages while avoiding the overhead of QoS 2.

## Attributes

| Attribute | Source |
|-----------|--------|
| `mqtt_topic` | Topic the message was published on |
| `mqtt_qos` | QoS level of the message |
| `mqtt_retained` | Whether the message was a retained message |
| `mqtt_message_id` | MQTT packet identifier |

The message payload is used as the raw log line. MQTT does not include a protocol-level timestamp, so the source timestamp is left unset and the ingest timestamp is used instead.

## Clean Session

When **enabled** (default), the broker discards any previous subscriptions and queued messages when the client connects. Each connection starts fresh.

When **disabled**, the broker remembers this client (by its client ID) across disconnections. Messages published while the ingester is offline are queued by the broker and delivered when it reconnects. This prevents message loss during restarts, but depends on the broker's storage limits and message expiry settings.

Disable clean session if you need durable subscriptions — for example, when ingesting from topics that publish infrequently and you can't afford to miss messages during maintenance windows.

## Reconnection

The ingester uses an auto-reconnecting client. If the broker goes down, it retries with a 10-second backoff until the connection is restored, then re-subscribes to all configured topics automatically.

## Backpressure

When the ingest queue is near capacity, the blocking channel send in the message handler naturally pauses message processing, which causes the MQTT client to stop acknowledging messages (for QoS 1/2), applying TCP-level backpressure to the broker.
