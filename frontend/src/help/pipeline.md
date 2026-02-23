# Pipeline Queries

Pipeline queries extend the [query language](help:query-language) with pipe operators that transform search results into aggregate tables and charts. A pipeline starts with a filter expression, followed by one or more pipe operators separated by `|`.

```
filter | operator | operator ...
```

## Stats Operator

The `stats` operator aggregates matching records into a table. It requires at least one aggregation function, and optionally groups results with a `by` clause.

```
filter | stats agg, agg, ... by group, group, ...
```

### Aggregation Functions

| Function | Description |
|----------|-------------|
| `count` | Number of records (no argument needed) |
| `count(field)` | Number of records where field is present |
| `sum(field)` | Sum of numeric field values |
| `avg(field)` | Average of numeric field values |
| `min(field)` | Minimum numeric field value |
| `max(field)` | Maximum numeric field value |
| `dcount(field)` | Count of distinct values |
| `median(field)` | Median of numeric field values |
| `first(field)` | First non-missing value seen |
| `last(field)` | Last non-missing value seen |
| `values(field)` | Comma-separated list of distinct values |

Fields are extracted automatically from record attributes, key=value pairs in the message text, and JSON message bodies. Attributes take precedence over extracted fields when names collide.

Non-numeric values are silently skipped by `sum`, `avg`, `min`, and `max`.

### Aliases

Aggregation results are named automatically (`count`, `sum_duration`, `avg_bytes`, etc.). Use `as` to provide a custom name:

```
* | stats count as total, avg(duration) as avg_ms
```

### Grouping

The `by` clause splits results into groups. Each unique combination of group values produces a separate row.

```
level=error | stats count by host
```

Multiple group fields are comma-separated:

```
* | stats count by level, host
```

### Time Bucketing with bin()

Use `bin(duration)` in the `by` clause to bucket results into time intervals. When a query includes `bin()`, results are displayed as a time series chart instead of a table.

```
* | stats count by bin(5m)
```

Supported duration units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).

Combining `bin()` with field grouping produces a multi-series chart — one line per group value:

```
* | stats count by bin(1m), level
```

By default, `bin()` uses the record's write timestamp. To bucket by a different timestamp, pass it as a second argument:

```
* | stats count by bin(5m, _source_ts)
```

Built-in timestamp fields: `_write_ts`, `_ingest_ts`, `_source_ts`.

## Expressions

Aggregation arguments and `where` conditions support arithmetic and [scalar functions](help:scalar-functions). These also work directly in [filter expressions](help:query-language) as expression predicates.

```
* | stats sum(bytes_sent + bytes_received) as total_bytes
```

## Where Operator

The `where` operator filters records after the initial search but before aggregation. It uses the same syntax as the main [query language](help:query-language).

```
level=error | where status>=500 | stats count by host
```

Most filters belong in the main query (left of the first `|`), where they benefit from [index acceleration](help:indexers). Use `where` for conditions that only make sense after field extraction — for example, filtering on a computed or derived field that isn't indexed.

Multiple `where` operators can be chained — all conditions must match.

## Eval Operator

The `eval` operator computes new fields or overwrites existing ones using [expressions](help:scalar-functions). Computed fields are added to each record's attributes and are visible to subsequent operators.

```
* | eval duration_ms = duration / 1000
```

Multiple assignments are comma-separated. Later assignments can reference fields created by earlier ones:

```
* | eval bytes_total = bytes_sent + bytes_received, kb_total = bytes_total / 1024
```

Eval works both before and after `stats`:

```
* | stats sum(bytes) as total by host | eval total_mb = total / 1048576
```

## Sort Operator

The `sort` operator orders records or table rows by one or more fields. Prefix a field name with `-` for descending order. Fields are compared numerically when possible, falling back to string comparison.

```
* | sort status
* | sort -count, status
```

Combine with `head` for top-N queries:

```
level=error | stats count by host | sort -count | head 10
```

Sort is not supported in follow mode (it requires all records before producing output).

## Head Operator

The `head` operator keeps only the first N records or rows, discarding the rest.

```
* | head 100
```

When used without `sort`, `head` can optimize the underlying scan to stop early.

## Rename Operator

The `rename` operator changes field names. Multiple renames are comma-separated. The keyword `as` separates the old name from the new name.

```
* | rename src as source, dst as destination
```

Works on both records and table columns:

```
* | stats count by host | rename count as total
```

## Fields Operator

The `fields` operator controls which fields are visible. In keep mode (default), only the listed fields are retained. In drop mode (prefix with `-`), the listed fields are removed.

Keep mode — show only these fields:

```
* | fields host, level, message
```

Drop mode — remove these fields:

```
* | fields - debug, trace, pid
```

## Raw Operator

The `raw` operator forces the pipeline output into a plain table — no charts, no single-value display. Useful for debugging what the pipeline actually produces.

Without stats, `raw` converts records into a table with columns for timestamps, all attributes, and the raw message:

```
level=error | head 10 | raw
```

After stats, `raw` forces the result into a flat table even when `bin()` would normally produce a chart:

```
* | stats count by bin(5m) | raw
```

## Result Display

Pipeline results are shown depending on the query:

- **Record list** — when there is no `stats` operator. Records are displayed in the standard log entry view with any computed or filtered fields.
- **Single value** — when `stats` produces a single column and single row (e.g. `| stats count`). Displayed as a large formatted number.
- **Table** — when there is no `bin()` in the group clause. Displays rows and columns with sort and export controls.
- **Time series chart** — when `bin()` is present. Hover to inspect individual data points. A Chart/Table toggle lets you switch to a raw data view.

Results can be exported to CSV or JSON using the export button.

## Auto-Refresh

Pipeline results include an auto-refresh control that re-runs the query at a fixed interval (5s, 10s, 30s, or 1m). This is useful for monitoring live metrics.

## Examples

Count all records:

```
* | stats count
```

Error rate per minute over the last hour:

```
level=error start="1 hour ago" | stats count by bin(1m)
```

Top hosts by error count:

```
level=error | stats count by host
```

Average response time by service, filtered to slow requests:

```
duration>1000 | stats avg(duration) as avg_ms, count by service
```

Memory usage over time with multi-series breakdown:

```
ingester_type=metrics | stats max(heap_inuse_bytes) by bin(30s)
```

Bytes transferred per minute by direction:

```
* | stats sum(bytes_sent) as sent, sum(bytes_received) as received by bin(5m)
```

Top 10 hosts by error count:

```
level=error | stats count by host | sort -count | head 10
```

Compute a derived field and filter on it:

```
* | eval duration_s = duration / 1000 | where duration_s>5
```

Show only specific fields:

```
service=api | fields host, method, status, duration
```

Rename columns for readability:

```
* | stats count, avg(duration) by method | rename count as requests, avg_duration as latency_ms
```
