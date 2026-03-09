# Lookups

The Lookups tab in [Settings](settings:lookups) configures the lookup tables used by the [`| lookup` pipeline operator](help:lookup-tables).

## MMDB Lookups

MMDB lookups map IP addresses to metadata using MaxMind-format `.mmdb` database files. Each lookup has a **type** (`city` or `asn`) that determines which fields are extracted.

If no file is uploaded for a lookup, the auto-downloaded database matching its type is used (see [Files → MaxMind Auto-Download](settings:files)).

## HTTP Lookups

HTTP lookups fetch data from an external HTTP endpoint at query time. Configure a **URL template** with `{{key}}` placeholders, optional headers, and **response paths** (JSONPath expressions) to extract fields from the response.

Use the **Test** button on an HTTP lookup card to verify the endpoint returns the expected data.

## CSV File Lookups

CSV file lookups map a key column to one or more value columns. Upload a CSV or TSV file, and a preview of the file's columns and first rows appears automatically.

- **Key Column** — select the column to use as the lookup key from the dropdown (default: first column)
- **Value Columns** — all non-key columns are included by default; uncheck any you don't need

The entire CSV is loaded into memory and indexed by key for O(1) lookups. The file is hot-reloaded when it changes.

Example: a CSV with columns `ip,hostname,datacenter,owner` lets you run `| lookup assets ip` to enrich records with hostname, datacenter, and owner fields.

## JSON File Lookups

JSON file lookups query a managed JSON file using a [jq-style expression](https://jqlang.github.io/jq/manual/). Upload the file in [Files](settings:files), then reference it by ID. Define **parameters** to pass named values into the query expression.
