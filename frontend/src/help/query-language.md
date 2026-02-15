# Query Language

GastroLog uses a structured query language to search and filter log records.

## Token Search

Bare words filter by token. Multiple tokens use AND semantics.

- `error` — records containing "error"
- `error timeout` — records containing both "error" and "timeout"

## Boolean Operators

Combine filters with boolean logic. AND binds tighter than OR.

- `error AND warn` — explicit AND (same as implicit)
- `error OR warn` — either token matches
- `NOT debug` — exclude records with "debug"
- `(error OR warn) AND NOT debug` — parentheses for grouping

## Key=Value Filters

Filter by key=value in record attributes or message body.

- `level=error` — exact key=value match
- `key="value with spaces"` — quoted values for special characters
- `host=*` — key exists with any value
- `*=error` — value exists under any key

## Time Bounds

Filter by timestamp. Accepts RFC3339 or Unix timestamps.

| Filter | Description |
|--------|-------------|
| `start=TIME` | Inclusive lower bound on WriteTS |
| `end=TIME` | Exclusive upper bound on WriteTS |
| `source_start=TIME` | Lower bound on SourceTS (log origin time) |
| `source_end=TIME` | Upper bound on SourceTS |
| `ingest_start=TIME` | Lower bound on IngestTS (receiver time) |
| `ingest_end=TIME` | Upper bound on IngestTS |

## Result Control

- `limit=N` — maximum number of results
- `reverse=true` — return results newest-first

## Scoping

- `store=NAME` — search only the named store
- `chunk=ID` — search only the named chunk
- `pos=N` — exact record position within a chunk

## Examples

- `error timeout` — token search
- `level=error host=*` — KV filter with wildcard
- `(error OR warn) AND NOT debug` — boolean expression
- `store=prod level=error` — scoped search
- `reverse=true limit=50 level=error` — latest 50 errors
