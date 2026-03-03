# MQTT

Type: `mqtt`

Subscribes to one or more MQTT topics on a broker. Each published message becomes a log record with the message payload as the raw log line.

| Param | Description | Default |
|-------|-------------|---------|
| `broker` | Broker URL (required). Schemes: `mqtt://`, `tcp://`, `ssl://`, `tls://`, `ws://`, `wss://` | |
| `topics` | Comma-separated topic patterns (required). Supports `+` (single-level) and `#` (multi-level) wildcards | |
| `client_id` | MQTT client identifier | `gastrolog-<id>` |
| `qos` | Quality of Service: `0`, `1`, or `2` | `1` |
| `version` | Protocol version: `3` (v3.1.1) or `5` (v5) | `3` |
| `tls` | Enable TLS | `false` |
| `clean_session` | Start with a clean session (discard prior subscriptions) | `true` |
| `username` | Username for broker authentication | |
| `password` | Password for broker authentication | |

## Attributes

| Attribute | Source |
|-----------|--------|
| `mqtt_topic` | Topic the message was published on |
| `mqtt_qos` | QoS level of the message |
| `mqtt_retained` | Whether the message was a retained message |
| `mqtt_message_id` | MQTT packet identifier |

The message payload is used as the raw log line. MQTT does not include a protocol-level timestamp, so the source timestamp is left unset and the ingest timestamp is used instead.

## Reconnection

The ingester uses an auto-reconnecting client. If the broker goes down, it retries with a 10-second backoff until the connection is restored, then re-subscribes to all configured topics automatically.

## Backpressure

When the ingest queue is near capacity, the blocking channel send in the message handler naturally pauses message processing, which causes the MQTT client to stop acknowledging messages (for QoS 1/2), applying TCP-level backpressure to the broker.
