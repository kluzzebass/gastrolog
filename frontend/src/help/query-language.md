# Query Language

GastroLog uses a structured query language to search and filter log records.

## Token Search

Bare words filter by token. Multiple tokens use AND semantics.

- `error` — records containing "error"
- `error timeout` — records containing both "error" and "timeout"

Token matching is case-insensitive and works on whole tokens (word boundaries), not substrings. A search for `err` will not match a record containing only "error" — use `error` instead. Tokens are 2-16 characters; shorter or longer terms won't match via the [token index](help:indexers).

Numeric values (e.g., `404`, `0xff`) and UUIDs are not indexed as tokens. Searching for a number still works via runtime scanning, but won't benefit from [index acceleration](help:indexers) on sealed chunks.

## Boolean Operators

Combine filters with boolean logic. AND binds tighter than OR.

- `error AND warn` — explicit AND (same as implicit)
- `error OR warn` — either token matches
- `NOT debug` — exclude records with "debug"
- `(error OR warn) AND NOT debug` — parentheses for grouping

## Glob Patterns

Match against tokenized words using shell-style glob patterns. Case-insensitive.

- `error*` — tokens starting with "error" (error, errors, error123)
- `*timeout` — tokens ending with "timeout" (connection_timeout, timeout)
- `err?r` — single character wildcard (error, errir)
- `[Ee]rror` — character class (Error, error)

Globs work in key=value positions too:

- `level=err*` — value matches glob pattern
- `err*=value` — key matches glob pattern
- `err*=*` — key matching glob exists
- `*=err*` — any key has a value matching glob

Glob patterns combine with all boolean operators:

- `error* AND NOT debug*` — combine with negation
- `(err* OR warn*) AND level=error` — combine with grouping

Prefix globs (like `error*`) benefit from [index acceleration](help:indexers) on sealed chunks via prefix lookup on the token index.

## Regex Search

Match the raw log line against a regular expression with `/pattern/` syntax. Case-insensitive by default.

- `/error\d+/` — matches "error42", "Error100", etc.
- `/failed.*connection/` — matches "failed to establish connection"
- `/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/` — matches IPv4 addresses
- `/path\/to\/file/` — escaped slashes for literal `/`

Regex predicates combine with all boolean operators:

- `level=error AND /timeout/` — combine with key=value filters
- `NOT /debug/` — negation
- `/error/ OR /warn/` — alternation (can also be written as `/error|warn/`)

Regex filters always run at scan time (no index acceleration). Uses [RE2 syntax](https://github.com/google/re2/wiki/Syntax) (no backreferences or lookahead). Override case-sensitivity with inline flags, e.g. `/(?-i)CaseSensitive/`.

## Key=Value Filters

Filter by key=value in record attributes or message body. Both sources are checked: a query for `level=error` matches records where `level=error` appears in the stored attributes **or** in the raw message text.

- `level=error` — exact key=value match
- `key="value with spaces"` — quoted values for special characters
- `host=*` — key exists with any value
- `*=error` — value exists under any key

Key-value matching is case-insensitive.

## Time Bounds

Filter by timestamp. Accepts RFC 3339 format (e.g., `2024-01-15T08:00:00Z`) or Unix timestamps (seconds since epoch).

| Filter | Bounds on | Description |
|--------|-----------|-------------|
| `start=TIME` | WriteTS | Inclusive lower bound |
| `end=TIME` | WriteTS | Exclusive upper bound |
| `source_start=TIME` | SourceTS | Lower bound on log origin time |
| `source_end=TIME` | SourceTS | Upper bound on log origin time |
| `ingest_start=TIME` | IngestTS | Lower bound on ingester receive time |
| `ingest_end=TIME` | IngestTS | Upper bound on ingester receive time |

WriteTS bounds (`start`/`end`) are used for [chunk](help:general-concepts) selection — chunks outside the time range are skipped entirely. SourceTS and IngestTS bounds are applied as runtime filters on individual records.

## Result Control

- `limit=N` — maximum number of results
- `reverse=true` — return results newest-first (default is oldest-first)

## Scoping

- `store=NAME` — search only the named store
- `chunk=ID` — search only the named chunk
- `pos=N` — exact record position within a chunk

## Examples

```
error timeout
```
Records containing both tokens (implicit AND).

```
level=error host=web-01
```
Records where level is "error" and host is "web-01", in attributes or message text.

```
(error OR warn) AND NOT debug
```
Boolean expression with grouping and negation.

```
store=prod level=error reverse=true limit=50
```
Latest 50 errors from the "prod" store.

```
start=2024-01-15T00:00:00Z end=2024-01-16T00:00:00Z level=error
```
All errors from January 15, 2024.

```
host=* NOT service=*
```
Records with a host attribute but no service attribute.

```
/error\d+/ level=error
```
Records matching the regex pattern with level "error".

```
NOT /debug|trace/
```
Exclude records matching a regex alternation.

```
error* AND NOT debug*
```
Records with tokens starting with "error" but not "debug".

```
level=err* host=web-*
```
Records where level matches "err*" and host matches "web-*".
