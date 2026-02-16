# Level Digester

Extracts a severity level from the message and sets a normalized `level` attribute. This is what powers severity filtering (`level=error`) and the per-level histogram counts.

Skipped if the message already has a `level`, `severity`, or `severity_name` attribute (e.g., set by a syslog ingester).

## Where It Looks

- KV patterns in the text: `level=ERROR`, `severity=warn`
- JSON fields: `"level":"error"`, `"severity":"warn"`
- Syslog priority headers: `<NNN>` (severity = priority % 8)

## Normalization

| Normalized | Also matches |
|------------|-------------|
| `error` | err, fatal, critical, emerg, emergency, alert, crit |
| `warn` | warning |
| `info` | notice, informational |
| `debug` | |
| `trace` | |
