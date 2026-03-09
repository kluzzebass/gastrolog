# Visualization Operators

Visualization operators control how [pipeline](help:pipeline) results are displayed. They appear at the end of a pipeline, after `stats` or `timechart`, and instruct the UI to render the data as a specific chart type instead of the default table or time series view.

```
filter | stats ... | visualization
```

If the data doesn't meet the chart's requirements, the result falls back to a table. Visualization operators do not transform data — they only affect presentation.

All visualization operators are unsupported in follow mode.

## Auto-Detection

When no explicit visualization operator is present, the UI automatically selects a chart type based on the result shape:

- **Time series chart** — when `bin()` is present in the `by` clause.
- **Single value** — when `stats` produces exactly 1 column and 1 row.
- **Donut chart** — when the result has exactly 2 columns, 2–12 rows, and a numeric last column.
- **Heatmap** — when the result has exactly 3 columns, 4+ rows, a numeric last column, and both axes have 2–30 distinct values.
- **Table** — everything else.

Use an explicit operator to override auto-detection, or append `| raw` to force a plain table.

## Linechart

The `linechart` operator renders a line chart. The first column must contain timestamps, and at least one remaining column must be numeric. Requires at least 2 rows.

Use `linechart` when `stats ... by bin()` auto-selects a stacked bar chart but you'd prefer lines, or when you want to force line rendering for non-bin time series data.

```
linechart
```

### Requirements

| Condition | Rule |
|-----------|------|
| Columns | 2 or more |
| Rows | 2 or more |
| First column | Must be parseable as a timestamp |
| Remaining columns | At least one must be numeric |

### Examples

Request rate over time as a line chart:

```
last=1h | stats count by bin(5m) | linechart
```

Multi-series line chart — error rate by level per minute:

```
last=30m | stats count by bin(1m), level | linechart
```

Response time percentiles over time:

```
last=1h | stats avg(duration) as avg, max(duration) as max by bin(5m) | linechart
```

Bytes transferred over time:

```
last=6h | stats sum(bytes_sent) as sent, sum(bytes_received) as received by bin(15m) | linechart
```

## Barchart

The `barchart` operator renders a horizontal bar chart. The last column must be numeric — it determines bar length. All preceding columns form the category labels.

```
barchart
```

### Requirements

| Condition | Rule |
|-----------|------|
| Columns | 2 or more |
| Rows | 2 or more |
| Last column | Must be numeric |

### Examples

Top HTTP status codes:

```
last=1h | stats count by status | sort -count | head 10 | barchart
```

Error count by host:

```
level=error | stats count by host | sort -count | barchart
```

Average response time by endpoint:

```
last=1h | stats avg(duration) as avg_ms by path | sort -avg_ms | head 15 | barchart
```

Bytes by service:

```
last=24h | stats sum(bytes) as total by service | sort -total | head 10 | barchart
```

## Donut

The `donut` operator renders a donut (ring) chart showing proportional breakdown. The last column is the numeric value; all preceding columns form the slice labels. The center of the ring displays the total.

A donut chart is also **auto-selected** when the result has exactly 2 columns, 2–12 rows, and a numeric last column — no explicit operator needed.

```
donut
```

### Requirements

| Condition | Rule |
|-----------|------|
| Columns | Exactly 2 |
| Rows | 2 or more |
| Last column | Must be numeric |

### Examples

Log level distribution:

```
last=1h | stats count by level | donut
```

Traffic by protocol:

```
last=1h | stats count by protocol | donut
```

Disk usage by vault:

```
| stats sum(bytes) as total by vault_id | donut
```

Request methods breakdown:

```
last=30m | stats count by method | donut
```

Since donut auto-detects, these queries also render as donut charts without the explicit operator:

```
last=1h | stats count by level
```

## Heatmap

The `heatmap` operator renders a color-intensity grid. The first two columns define the X and Y axes (categorical or time values), and the third column provides the numeric intensity value. Color ranges from cool (low) through warm (high) using a spectral ramp, with interactive sliders to filter the visible range.

A heatmap is also **auto-selected** when the result has exactly 3 columns, 4+ rows, a numeric last column, and both axes have 2–30 distinct values.

```
heatmap
```

### Requirements

| Condition | Rule |
|-----------|------|
| Columns | Exactly 3 |
| Rows | 4 or more |
| Last column | Must be numeric |

### Examples

Error rate by hour and severity:

```
last=24h | stats count by bin(1h), level | heatmap
```

Status codes by endpoint:

```
last=1h | stats count by path, status | sort -count | head 50 | heatmap
```

Request volume by day-of-week and hour (requires extracted fields):

```
last=7d | stats count by day_of_week, hour_of_day | heatmap
```

Latency by service and time bucket:

```
last=6h | stats avg(duration) as avg_ms by bin(30m), service | heatmap
```

## Scatter

The `scatter` operator renders a scatter plot. You specify the X and Y column names — both must contain numeric values. Any additional columns in the table appear as labels in tooltips, making it easy to identify individual data points.

```
scatter <x_column> <y_column>
```

### Requirements

| Condition | Rule |
|-----------|------|
| X column | Must exist and be numeric |
| Y column | Must exist and be numeric |
| Rows | 2 or more |

### Examples

Latency vs. throughput by host:

```
last=1h | stats avg(duration) as latency, sum(bytes) as throughput by host | scatter latency throughput
```

Error count vs. request count per service:

```
last=1h | stats count as requests, count(level="error") as errors by service | scatter requests errors
```

CPU vs. memory usage:

```
last=30m | stats avg(cpu_percent) as cpu, avg(memory_mb) as memory by host | scatter cpu memory
```

Request size vs. response time:

```
last=1h | stats avg(request_bytes) as req_size, avg(duration) as latency by path | scatter req_size latency
```

## Map

The `map` operator renders geographic data on a world map. It has two modes:

### Choropleth

Shades countries by value. The specified column must contain [ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2) country codes (e.g. `US`, `DE`, `JP`). Empty values are skipped.

```
map choropleth <country_column>
```

Typically used with the `geoip` [lookup table](help:lookup-tables) to resolve IP addresses to country codes.

#### Requirements

| Condition | Rule |
|-----------|------|
| Country column | Must exist, values must be 2-letter uppercase codes |
| Rows | 2 or more |

#### Examples

Requests by country:

```
last=1h | lookup geoip client_ip | stats count by client_ip_country | map choropleth client_ip_country
```

Bandwidth by origin country:

```
last=24h | lookup geoip src_ip | stats sum(bytes) as total by src_ip_country | map choropleth src_ip_country
```

Error rate by country:

```
level=error | lookup geoip remote_addr | stats count by remote_addr_country | map choropleth remote_addr_country
```

### Scatter

Plots points on a world map using latitude and longitude coordinates. Both columns must be numeric. Additional columns (like count or hostname) appear in tooltips.

```
map scatter <lat_column> <lon_column>
```

#### Requirements

| Condition | Rule |
|-----------|------|
| Latitude column | Must exist and be numeric |
| Longitude column | Must exist and be numeric |
| Rows | 2 or more valid (non-empty) coordinate pairs |

#### Examples

Client locations:

```
last=1h | lookup geoip client_ip | stats count by client_ip_latitude, client_ip_longitude | map scatter client_ip_latitude client_ip_longitude
```

Top source IPs on a map:

```
last=1h | stats count by src_ip | sort -count | head 50 | lookup geoip src_ip | map scatter src_ip_latitude src_ip_longitude
```

Server locations by error count:

```
level=error | lookup geoip host_ip | stats count by host_ip_latitude, host_ip_longitude, host | map scatter host_ip_latitude host_ip_longitude
```

## Chart / Table Toggle

All visualization results include a **Chart / Table** toggle in the results header. Click **Table** to see the raw data behind any chart. This is useful for verifying the numbers, copying specific values, or exporting the data.

## Tips

- **Force a table**: Append `| raw` to any pipeline to bypass all visualization and see the raw tabular output.
- **Combine with sort and head**: Chart readability improves when you limit the data. Use `| sort -count | head 10` before a visualization operator to show only the top results.
- **Multiple aggregations**: Visualization operators work with multi-aggregation stats. For example, `| stats count, avg(duration) by host | barchart` uses the last numeric column for bar length.
- **Rename for clarity**: Use `| rename` before a visualization to give columns human-readable names that appear in chart labels and tooltips.
