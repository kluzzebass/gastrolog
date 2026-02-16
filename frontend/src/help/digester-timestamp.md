# Timestamp Digester

Extracts a source timestamp (SourceTS) from the message text. This tells you when the log event actually happened at the source, as opposed to when GastroLog received it.

Skipped if the ingester already set SourceTS (e.g., syslog and Docker ingesters parse timestamps from their protocol).

## Recognized Formats

- RFC 3339 / ISO 8601: `2024-01-15T10:30:45.123Z`
- Apple unified log: `2024-01-15 10:30:45.123456-0800`
- Syslog BSD / RFC 3164: `Jan  5 15:04:02`
- Common Log Format: `[02/Jan/2006:15:04:05 -0700]`
- Go/Ruby datestamp: `2024/01/15 10:30:45`
- Ctime / BSD: `Fri Feb 13 17:49:50 2026`

All formats support fractional seconds and timezone offsets. When a format lacks a year (e.g., RFC 3164), the current year is inferred.
