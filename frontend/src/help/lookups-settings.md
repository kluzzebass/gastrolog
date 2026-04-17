# Lookups

The Lookups tab in [Settings](settings:lookups) configures the lookup tables used by the [`| lookup` pipeline operator](help:lookup-tables). Each lookup has a **Name** that becomes the table name in queries — for example, a lookup named `assets` is used as `| lookup assets src_ip`.

The built-in `rdns` and `useragent` tables are always available and don't appear here. This page covers the configurable table types.

## MMDB Lookups

MMDB lookups map IP addresses to metadata using MaxMind-format `.mmdb` database files.

### Settings

- **Name** — registry name used in queries, e.g. `| lookup geoip`. Defaults to `geoip` for city databases and `asn` for ASN databases.
- **Database Type** — determines which fields are extracted:
  - **GeoIP City** — country, city, subdivision, latitude, longitude, timezone, accuracy radius
  - **ASN** — AS number and organization name
- **MMDB File** — upload a custom `.mmdb` file, or leave empty to use the auto-downloaded database from [MaxMind Auto-Download](settings:files).

You can create multiple MMDB lookups with different names pointing to different database files — for example, a `geoip` lookup using GeoLite2-City and a `geoip_precise` lookup using a commercial GeoIP2-City database.

## HTTP Lookups

HTTP lookups fetch data from an external HTTP endpoint at query time. Each lookup call makes one HTTP request with the field value substituted into the URL.

### Settings

- **Name** — registry name used in queries, e.g. `| lookup users`.
- **URL Template** — the endpoint URL with `{param}` placeholders matching parameter names below. For example: `https://api.example.com/users/{user_id}`.
- **Parameters** — ordered list of named URL template parameters. Each name becomes a `{name}` placeholder in the URL. In queries, fields map positionally: `| lookup users user_id` passes the `user_id` field as the first parameter.
- **Response Paths** — jq expressions to extract fields from the JSON response. Each path produces a named suffix. Results from multiple paths are merged. Leave empty to flatten the entire response.
- **Headers** — custom HTTP request headers (e.g. `Authorization: Bearer token123`).
- **Timeout** — maximum time to wait for a response. Default: `5s`.
- **Cache TTL** — how long to cache successful responses. Default: `5m`. Set to `0` to disable caching.
- **Cache Size** — maximum number of cached responses. Default: `10000`.

### Tips

- Use the **Test** button on each lookup card to verify the endpoint returns expected data before using it in queries. Enter test values for each parameter and inspect the extracted fields.
- Caching is important for performance — without it, every matching record triggers an HTTP request. Set a reasonable TTL based on how often the external data changes.
- HTTP lookups add latency proportional to the number of unique values being looked up. Use them after `stats` aggregation when possible to reduce the number of calls:

```
* | stats count by user_id | lookup users user_id
```

## CSV File Lookups

CSV file lookups map a key column to one or more value columns from an uploaded `.csv` or `.tsv` file. The file is hot-reloaded automatically when it changes on disk.

### Settings

- **Name** — registry name used in queries, e.g. `| lookup assets`.
- **File** — upload a `.csv` or `.tsv` file, or select an existing managed file. A preview of the file's columns and first rows appears automatically after upload.
- **Key Column** — select the column to use as the lookup key from the dropdown. Defaults to the first column. The preview table highlights the key column.
- **Value Columns** — all non-key columns are included by default. Uncheck any columns you don't need in lookup results.

### Tips

- The preview table shows the first 10 rows and total row count so you can verify the file was parsed correctly and choose the right key/value columns.
- If multiple rows share the same key, the first occurrence wins.
- Rows with empty keys are skipped.
- TSV files (tab-delimited) are detected automatically from the `.tsv` extension.

## JSON File Lookups

JSON file lookups query a managed JSON file using a jq-style expression. Useful for structured reference data that doesn't fit a flat CSV model — nested objects, arrays, or complex key schemes.

### Settings

- **Name** — registry name used in queries, e.g. `| lookup hosts`.
- **File** — upload a `.json` file or select an existing managed file.
- **Query** — a jq expression with `{param}` placeholders. The expression is evaluated against the JSON file with parameter values substituted. For example: `.hosts[] | select(.ip == "{value}")` finds the host object whose `ip` field matches the lookup value.
- **Parameters** — named parameters that map to `{name}` placeholders in the query expression. Fields map positionally in queries, just like HTTP lookups.
- **Response Paths** — jq expressions to extract specific fields from the query result. Leave empty to flatten the entire matched object into key-value pairs.

Source files are limited to 10 MB — for larger datasets use CSV, which is mmapped directly without a transform step.

## YAML File Lookups

YAML file lookups work identically to JSON file lookups — same jq transform, same key/value column picker, same 10 MB cap. The only difference is the file format at rest: `.yaml` / `.yml` instead of `.json`. Use YAML when your reference data is already in YAML (Kubernetes manifests, Ansible inventories, etc.) and you'd rather not convert it.

Under the hood the YAML is parsed into the same tree JSON produces, so any jq expression that works on one works on the other.
