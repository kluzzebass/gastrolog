# Fluent Forward

Type: `fluentfwd`

Accepts messages via the Fluent Forward protocol over TCP. Compatible with Fluentd and Fluent Bit using the `forward` output plugin.

| Param | Description | Default |
|-------|-------------|---------|
| `addr` | TCP listen address | `:24224` |

Supports all four Fluent Forward message modes: Message, Forward, PackedForward, and CompressedPackedForward. EventTime extension type (nanosecond precision) is supported.

## Attributes

| Attribute | Source |
|-----------|--------|
| `tag` | Fluent tag from the message |
| *(record keys)* | All keys from the record map, stringified |

The raw log line is extracted from the first matching key: `message`, `log`, or `msg`. If none is found, the entire record is JSON-serialized.

## Acknowledgements

If the sender includes a `chunk` key in the message options, GastroLog responds with an ack after the message is queued. This provides delivery confirmation for senders that require it.

## Backpressure

When the ingest queue is near capacity, the blocking channel send naturally delays ack responses, causing TCP backpressure to propagate upstream to the sender.
