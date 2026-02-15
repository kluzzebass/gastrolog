# Ingesters

Ingesters receive log messages from external sources and feed them into GastroLog's ingestion pipeline. Each ingester runs independently and emits records with attributes, timestamps, and the raw payload.

## Syslog

Type: `syslog`

Receives syslog messages over UDP and/or TCP. Supports both RFC 3164 (BSD) and RFC 5424 (structured) formats. Parses facility, severity, hostname, app name, and timestamp from the syslog header.

| Param | Description | Default |
|-------|-------------|---------|
| `udp_addr` | UDP listen address | `:514` (if neither addr specified) |
| `tcp_addr` | TCP listen address | `:514` (if neither addr specified) |

## HTTP (Loki-Compatible)

Type: `http`

Accepts log pushes via the Loki HTTP API. Compatible with Promtail, Grafana Agent, and other Loki clients.

| Param | Description | Default |
|-------|-------------|---------|
| `addr` | Listen address | `:3100` |

**Endpoint**: `POST /loki/api/v1/push`

Labels from the Loki push request are converted to record attributes.

## RELP

Type: `relp`

Receives messages via the Reliable Event Logging Protocol. RELP provides reliable delivery with acknowledgements, commonly used with rsyslog.

| Param | Description | Default |
|-------|-------------|---------|
| `addr` | Listen address | `:2514` |

## Tail

Type: `tail`

Follows local log files, similar to `tail -f`. Tracks file offsets across restarts so no lines are missed or duplicated.

| Param | Description | Default |
|-------|-------------|---------|
| `paths` | JSON array of glob patterns (required) | |
| `poll_interval` | How often to check for new data | `30s` |

**Example paths**: `["/var/log/*.log", "/opt/app/logs/**/*.log"]`

## Docker

Type: `docker`

Streams container logs from a Docker daemon. Automatically discovers containers and attaches to their log streams.

| Param | Description | Default |
|-------|-------------|---------|
| `host` | Docker daemon address | `unix:///var/run/docker.sock` |
| `label_filter` | Docker label filter (`key=value`) | |
| `name_filter` | Container name regex | |
| `image_filter` | Image name regex | |
| `poll_interval` | Container discovery interval | `30s` |
| `stdout` | Capture stdout | `true` |
| `stderr` | Capture stderr | `true` |
| `tls` | Enable TLS for TCP connections | `true` |
| `tls_ca` | CA certificate name (from certificate store) | |
| `tls_cert` | Client certificate name | |
| `tls_verify` | Verify server TLS certificate | `true` |

Container metadata (name, image, container ID) is added as record attributes.

## Chatterbox

Type: `chatterbox`

Test ingester that generates random log messages in various formats. Useful for development and demo environments.

| Param | Description | Default |
|-------|-------------|---------|
| `minInterval` | Minimum delay between messages | `100ms` |
| `maxInterval` | Maximum delay between messages | `1s` |
| `formats` | Comma-separated format list | All formats |
| `formatWeights` | Format=weight pairs for selection | Equal weights |
| `hostCount` | Number of simulated hosts | `10` |
| `serviceCount` | Number of simulated services | `5` |

**Supported formats**: plain, kv, json, access, syslog, weird, multirecord

## Routing

Ingesters don't target specific stores directly. Instead, each store defines a **filter** expression that is evaluated against the attributes of every ingested message. A single message can be routed to multiple stores if their filters match. See the General Concepts topic for filter syntax details.
