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

## Result Display

Pipeline results are shown in two formats depending on the query:

- **Table** — when there is no `bin()` in the group clause. Displays rows and columns with sort and export controls.
- **Time series chart** — when `bin()` is present. Hover to inspect individual data points.

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
