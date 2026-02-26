# Digestion

After ingestion and before storage, every message passes through **digesters** — stages that extract structured information from the raw log text. Each digester looks for a specific kind of data and adds it as a record attribute so you can filter and search on it later.

Rather than requiring each ingester to understand every log format, digesters provide a uniform enrichment layer. A syslog message and a JSON-structured HTTP push both get normalized severity levels and extracted timestamps, regardless of their source. Digesters run before filter evaluation, so their extracted attributes (like `level`) can be used in vault filter expressions.

Digestion is best-effort: if a message doesn't match any recognized pattern, it passes through unchanged. Nothing is lost.

## Digesters

| Digester | What it extracts |
|----------|-----------------|
| **Level** | A normalized severity level (`error`, `warn`, `info`, `debug`, `trace`) from the log content |
| **Timestamp** | A source timestamp (SourceTS) from embedded date patterns in the message text |

See [Level](help:digester-level) and [Timestamp](help:digester-timestamp) for details on each.
