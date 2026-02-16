# Syslog

Type: `syslog`

Receives syslog messages over UDP and/or TCP. Supports both RFC 3164 (BSD) and RFC 5424 (structured) formats, auto-detected per message. TCP supports both newline-delimited and octet-counted framing.

| Param | Description | Default |
|-------|-------------|---------|
| `udp_addr` | UDP listen address | `:514` (if neither addr specified) |
| `tcp_addr` | TCP listen address | `:514` (if neither addr specified) |

## Attributes

| Attribute | Source |
|-----------|--------|
| `remote_ip` | Sender's IP address |
| `facility` | Numeric facility code (0-23) from syslog priority |
| `facility_name` | Human-readable: kern, user, mail, daemon, auth, syslog, lpr, news, uucp, cron, authpriv, ftp, ntp, audit, alert, clock, local0-local7 |
| `severity` | Numeric severity (0-7) from syslog priority |
| `severity_name` | Human-readable: emerg, alert, crit, err, warning, notice, info, debug |
| `hostname` | From syslog header |
| `app_name` | Application/program name |
| `proc_id` | Process ID (from `[PID]` notation) |
| `msg_id` | Message ID (RFC 5424 only) |
