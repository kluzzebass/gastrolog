# Kafka

Type: `kafka`

Consumes messages from a Kafka topic using a consumer group. Each message value becomes a log record.

| Setting | Description | Default |
|---------|-------------|---------|
| Brokers | Comma-separated list of Kafka broker addresses (required) | |
| Topic | Kafka topic to consume (required) | |
| Consumer Group | Consumer group ID | `gastrolog` |
| Enable TLS | Secure connection to brokers | off |
| SASL Mechanism | Authentication mechanism: PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512 | (none) |
| SASL User | Username for SASL authentication | |
| SASL Password | Password for SASL authentication | |

## Attributes

| Attribute | Source |
|-----------|--------|
| `kafka_topic` | Source topic name |
| `kafka_partition` | Partition number |
| `kafka_offset` | Message offset |
| *(record headers)* | All Kafka record headers as key-value pairs |

The message value is used as the raw log line.

## Timestamps

SourceTS is set from the Kafka record timestamp, which is always present. Depending on the topic's `message.timestamp.type` setting, this is either `CreateTime` (producer-set) or `LogAppendTime` (broker-set). IngestTS is set to GastroLog arrival time.

## Offset Management

Offsets are auto-committed (default interval: 5 seconds). This provides at-least-once delivery semantics, which is appropriate for log ingestion.

## Backpressure

When the ingest queue is near capacity, the blocking channel send naturally pauses fetch polling, preventing the consumer from pulling more messages than can be processed.
