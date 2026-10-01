# Fluent Forward

Type: `fluentfwd`

Accepts messages via the Fluent Forward protocol over TCP. Compatible with Fluentd and Fluent Bit using the `forward` output plugin.

| Setting | Description | Default |
|---------|-------------|---------|
| Listen Address | TCP address for Fluent Forward protocol | `:24224` |

Supports all four Fluent Forward message modes: Message, Forward, PackedForward, and CompressedPackedForward. EventTime extension type (nanosecond precision) is supported.

## Attributes

| Attribute | Source |
|-----------|--------|
| `tag` | Fluent tag from the message |
| *(record keys)* | All keys from the record map, stringified |

The raw log line is extracted from the first matching key: `message`, `log`, or `msg`. If none is found, the entire record is JSON-serialized.

## TLS

When TLS is enabled, the Forward listener serves TLS. Select a server certificate from the certificate store — certificates are managed in the Certificates settings tab, and rotations take effect without a restart.

For mutual TLS, also select a Client CA Certificate: clients must then present a certificate signed by that CA, and a plaintext or unverified client is refused before any of its bytes are parsed. The Allowed Client CN field optionally narrows which client certificates are accepted using a wildcard pattern (e.g. `producer-*`).

## Timestamps

SourceTS is set from the Fluentd event timestamp, which is always present in the protocol. IngestTS is set to GastroLog arrival time.

## Acknowledgements

If the sender includes a `chunk` key in the message options, GastroLog responds with an ack after the message is queued. This provides delivery confirmation for senders that require it.

## Backpressure

When the ingest queue is near capacity, the blocking channel send naturally delays ack responses, causing TCP backpressure to propagate upstream to the sender.
