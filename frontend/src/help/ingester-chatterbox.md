# Chatterbox

Type: `chatterbox`

A test ingester that generates random log messages in various formats. Useful for development and trying things out. Messages pass through [digestion](help:digesters) like any other source.

## Settings

**Log Formats** — toggle which formats to generate, and set their relative weights. Higher weight means that format is selected more often. All formats are enabled by default with equal weight.

| Setting | Description | Default |
|---------|-------------|---------|
| Min Interval | Minimum delay between messages | `100ms` |
| Max Interval | Maximum delay between messages | `1s` |
| Host Count | Number of distinct simulated hosts | `10` |
| Service Count | Number of distinct simulated services | `5` |

## Formats

### Plain Text

Unstructured text messages like "starting worker pool" or "connection failed". No embedded timestamps.

**Attributes:** `service`, `host`

### Key-Value

Structured key=value log lines in several styles: HTTP requests, database queries, user actions, and trace context.

Example: `level=INFO msg="request completed" method=GET path=/api/users status=200 latency_ms=45`

**Attributes:** `service`, `env`, `host`

### JSON

JSON objects with `level`, `msg`, `ts` fields plus variation-specific data: HTTP metrics, error details, business events, system metrics, or distributed tracing spans.

**Attributes:** `service`, `env`, `host`

### Access Log

Apache/Nginx combined log format.

Example: `192.168.1.100 - user1 [02/Jan/2006:15:04:05 -0700] "GET /api/users HTTP/1.1" 200 4567 "https://example.com/" "Mozilla/5.0..."`

**Attributes:** `service` (always `nginx`), `vhost`, `host`

### Syslog

RFC 3164 syslog format with priority, timestamp, hostname, and program name.

Example: `<85>Jan  2 15:04:05 host-1 sshd[1234]: Failed password for root from 192.168.1.100 port 22 ssh2`

**Attributes:** `service` (program name: sshd, sudo, kernel, etc.), `facility`, `host`

### Weird

Deliberately malformed data for stress-testing: random bytes, control characters, invalid UTF-8, very long tokens, repeated patterns, empty lines, and broken JSON.

**Attributes:** `service` (always `unknown`), `host`

### Multi-Record

Multi-line output that arrives as separate records sharing a single source timestamp. Generates stack traces (Go, Java, Python) and CLI help output (kubectl, docker, terraform, etc.).

**Attributes:** `service`, `host`, `format` (`stack` or `help`), plus `language` for stack traces or `command` for help output
