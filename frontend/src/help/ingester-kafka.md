# Kafka

Type: `kafka`

Consumes messages from a Kafka topic using a consumer group. Each message value becomes a log record.

| Param | Description | Default |
|-------|-------------|---------|
| `brokers` | Comma-separated broker addresses (required) | |
| `topic` | Topic to consume (required) | |
| `group` | Consumer group ID | `gastrolog` |
| `tls` | Enable TLS | `false` |
| `sasl_mechanism` | SASL auth: `plain`, `scram-sha-256`, or `scram-sha-512` | *(none)* |
| `sasl_user` | SASL username | |
| `sasl_password` | SASL password | |

## Attributes

| Attribute | Source |
|-----------|--------|
| `kafka_topic` | Source topic name |
| `kafka_partition` | Partition number |
| `kafka_offset` | Message offset |
| *(record headers)* | All Kafka record headers as key-value pairs |

The message value is used as the raw log line. The Kafka record timestamp is used as the source timestamp.

## Offset Management

Offsets are auto-committed (default interval: 5 seconds). This provides at-least-once delivery semantics, which is appropriate for log ingestion.

## Backpressure

When the ingest queue is near capacity, the blocking channel send naturally pauses fetch polling, preventing the consumer from pulling more messages than can be processed.
