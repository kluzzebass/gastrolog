# Docker

Type: `docker`

Streams container logs from a Docker daemon. Automatically discovers containers and attaches to their log streams.

| Setting | Description | Default |
|---------|-------------|---------|
| Docker Host | Docker daemon address | `unix:///var/run/docker.sock` |
| Container Filter | Container filter expression (see below) | |
| Poll Interval | Container discovery interval | `30s` |
| Stdout | Capture stdout | on |
| Stderr | Capture stderr | on |
| Enable TLS | Secure connection for TCP hosts | on |
| CA Certificate | CA certificate name (from certificate store) | |
| Client Certificate | Client certificate name | |
| Verify server certificate | Verify server TLS certificate | on |

## Filter expressions

The Container Filter uses the same expression language as [search queries](help:query-language) and [vault filters](help:routing). Containers are matched against these attributes:

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

Leave the Container Filter empty to collect logs from all containers.

## Attributes

| Attribute | Source |
|-----------|--------|
| `container_id` | Full container ID |
| `container_name` | Container name |
| `image` | Image name/tag |
| `stream` | Log source: `stdout`, `stderr`, or `tty` |

Handles both TTY and multiplexed log streams. The [Level digester](help:digester-level) still runs to extract severity from the message content.

## Timestamps

SourceTS is set from the Docker log entry timestamp — the time the Docker daemon captured the container's output. IngestTS is set to GastroLog arrival time.

## Recipe

See [Docker with mTLS](help:recipe-docker-mtls) for a complete walkthrough of setting up Docker daemon TCP access with client and server certificate authentication.
