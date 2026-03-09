# Lookups

The Lookups tab in [Settings](settings:lookups) configures the lookup tables used by the [`| lookup` pipeline operator](help:lookup-tables).

## MMDB Lookups

MMDB lookups map IP addresses to metadata using MaxMind-format `.mmdb` database files. Each lookup has a **type** (`city` or `asn`) that determines which fields are extracted.

If no file is uploaded for a lookup, the auto-downloaded database matching its type is used (see [Files → MaxMind Auto-Download](settings:files)).

## HTTP Lookups

HTTP lookups fetch data from an external HTTP endpoint at query time. Configure a **URL template** with `{{key}}` placeholders, optional headers, and **response paths** (JSONPath expressions) to extract fields from the response.

Use the **Test** button on an HTTP lookup card to verify the endpoint returns the expected data.

## JSON File Lookups

JSON file lookups query a managed JSON file using a [jq-style expression](https://jqlang.github.io/jq/manual/). Upload the file in [Files](settings:files), then reference it by ID. Define **parameters** to pass named values into the query expression.
