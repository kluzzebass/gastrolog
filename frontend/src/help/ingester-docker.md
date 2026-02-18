# Docker

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

## Attributes

| Attribute | Source |
|-----------|--------|
| `container_id` | Full container ID |
| `container_name` | Container name |
| `image` | Image name/tag |
| `stream` | Log source: `stdout`, `stderr`, or `tty` |

Handles both TTY and multiplexed log streams. Docker timestamps are extracted automatically, so the [Timestamp digester](help:digester-timestamp) skips these messages. The [Level digester](help:digester-level) still runs to extract severity from the message content.

## Recipe

See [Docker with mTLS](help:recipe-docker-mtls) for a complete walkthrough of setting up Docker daemon TCP access with client and server certificate authentication.
