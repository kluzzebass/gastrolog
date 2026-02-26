# Docker

Type: `docker`

Streams container logs from a Docker daemon. Automatically discovers containers and attaches to their log streams.

| Param | Description | Default |
|-------|-------------|---------|
| `host` | Docker daemon address | `unix:///var/run/docker.sock` |
| `filter` | Container filter expression (see below) | |
| `poll_interval` | Container discovery interval | `30s` |
| `stdout` | Capture stdout | `true` |
| `stderr` | Capture stderr | `true` |
| `tls` | Enable TLS for TCP connections | `true` |
| `tls_ca` | CA certificate name (from certificate store) | |
| `tls_cert` | Client certificate name | |
| `tls_verify` | Verify server TLS certificate | `true` |

## Filter expressions

The `filter` param uses the same expression language as [search queries](help:query-language) and [vault filters](help:routing). Containers are matched against these attributes:

| Attribute | Source |
|-----------|--------|
| `name` | Container name |
| `image` | Image name/tag |
| `label.<key>` | Docker label (e.g., `label.env`) |

**Examples:**

- `image=nginx*` — containers running nginx
- `name=web*` — containers whose name starts with "web"
- `label.env=prod` — containers with Docker label `env=prod`
- `label.logging=*` — containers that have a `logging` label (any value)
- `image=nginx* OR image=redis*` — nginx or redis containers
- `name=web* AND label.env=prod` — web containers in prod
- `NOT image=postgres*` — everything except postgres

Omit `filter` to collect logs from all containers.

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
