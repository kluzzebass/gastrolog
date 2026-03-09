# Lookup Tables

Lookup tables enrich log records with additional fields by mapping a value to metadata. They are used via the [`| lookup` pipeline operator](help:pipeline).

```
* | lookup <table> <field>
```

The lookup result fields are added as `<field>_<suffix>` attributes. For example, `| lookup geoip src_ip` adds `src_ip_country`, `src_ip_city`, etc. Records with no match are passed through unchanged — enrichment misses are normal, not errors.

Lookup works on both raw records and table rows after `stats`:

```
* | lookup useragent user_agent | where user_agent_device_type="mobile"
* | stats count by src_ip | lookup geoip src_ip
```

## Built-in Tables

Built-in tables are always available without configuration.

### rdns — Reverse DNS

Resolves IP addresses to hostnames via live DNS PTR queries.

| Suffix | Description | Example |
|--------|-------------|---------|
| `hostname` | Resolved PTR hostname | `dns.google` |

```
* | lookup rdns src_ip
```

Results are cached with a short TTL (5 minutes for hits, 1 minute for misses) to avoid flooding DNS resolvers. Lookups that don't resolve within 2 seconds return a miss.

Best for: identifying hosts in firewall logs, correlating IPs to known infrastructure.

### useragent — User-Agent Parser

Parses browser user-agent strings into structured fields. Pure string parsing — fast, no external dependencies or network calls.

| Suffix | Description | Example |
|--------|-------------|---------|
| `browser` | Browser or bot name | `Chrome` |
| `browser_version` | Full version string | `120.0.0.0` |
| `os` | Operating system name | `Windows` |
| `os_version` | OS version | `10` |
| `device` | Device model (when available) | `iPhone` |
| `device_type` | One of `desktop`, `mobile`, `tablet`, or `bot` | `desktop` |

```
* | lookup useragent user_agent
```

Not every field is present for every user-agent. Bot user-agents typically produce `browser` (the bot name) and `device_type` = `bot`, but no device or OS fields. Unknown or unparseable strings return no fields.

Best for: web access log analysis, breaking down traffic by browser/OS/device type.

#### Examples

Traffic breakdown by browser:

```
* | lookup useragent user_agent | stats count by user_agent_browser | sort -count | head 10
```

Mobile vs desktop over time:

```
* | lookup useragent user_agent | stats count by bin(5m), user_agent_device_type
```

Find bot traffic:

```
* | lookup useragent user_agent | where user_agent_device_type="bot"
```

## MMDB Tables

MMDB tables use MaxMind-format `.mmdb` database files to map IP addresses to metadata. They require a database file — either uploaded manually in [Settings → Files](settings:files) or fetched automatically via [MaxMind Auto-Download](settings:files). Create MMDB lookups in [Settings → Lookups](settings:lookups).

### geoip — Geographic Location

Maps IP addresses to geographic metadata. Requires a **GeoLite2-City** or **GeoIP2-City** database.

| Suffix | Description | Example |
|--------|-------------|---------|
| `country` | ISO 3166-1 alpha-2 country code | `NO` |
| `city` | City name (English) | `Haugesund` |
| `subdivision` | State, province, or region | `Rogaland` |
| `latitude` | Decimal latitude | `59.4138` |
| `longitude` | Decimal longitude | `5.2680` |
| `timezone` | IANA time zone | `Europe/Oslo` |
| `accuracy_radius` | Accuracy radius in km | `50` |

```
* | lookup geoip src_ip
```

Private and reserved IP ranges (10.x, 172.16–31.x, 192.168.x, 127.x) return no results.

#### Examples

Top countries by request count:

```
* | lookup geoip client_ip | stats count by client_ip_country | sort -count | head 10
```

Requests by country on a world map:

```
* | lookup geoip client_ip | stats count by client_ip_country | map choropleth client_ip_country
```

Scatter plot of request origins:

```
* | lookup geoip client_ip | stats count by client_ip_latitude, client_ip_longitude | map scatter client_ip_latitude client_ip_longitude
```

### asn — Autonomous System

Maps IP addresses to autonomous system (network operator) information. Requires a **GeoLite2-ASN** or **GeoIP2-ISP** database.

| Suffix | Description | Example |
|--------|-------------|---------|
| `asn` | AS number | `AS15169` |
| `as_org` | AS organization name | `GOOGLE` |

```
* | lookup asn src_ip
```

#### Examples

Top networks by traffic volume:

```
* | lookup asn src_ip | stats sum(bytes) as total by src_ip_as_org | sort -total | head 10
```

Combine with geoip for full context:

```
* | lookup geoip src_ip | lookup asn src_ip
```

## Custom Tables

Custom tables are configured in [Settings → Lookups](settings:lookups). They let you bring your own data into the enrichment pipeline.

### CSV File

CSV file lookups map a key column to one or more value columns from an uploaded `.csv` or `.tsv` file. The file is hot-reloaded automatically when it changes on disk.

The output suffixes are the value column names from the CSV header.

```
* | lookup assets src_ip
```

If the CSV has columns `ip,hostname,datacenter,owner` and key column is `ip`, then `| lookup assets src_ip` adds `src_ip_hostname`, `src_ip_datacenter`, and `src_ip_owner`.

Best for: asset inventories, IP-to-hostname mappings, team/owner assignments, custom classification lists, threat indicator lists.

#### Examples

Enrich with asset metadata and filter to a specific datacenter:

```
* | lookup assets src_ip | where src_ip_datacenter="us-east-1"
```

Count events by asset owner:

```
* | lookup assets src_ip | stats count by src_ip_owner | sort -count
```

### HTTP

HTTP lookups fetch data from an external HTTP endpoint at query time. The URL template uses `{param}` placeholders that are substituted with field values. Response data is extracted using JSONPath expressions.

Output suffixes are derived from the configured response paths.

```
* | lookup users user_id
```

HTTP lookups include configurable timeouts and response caching (TTL and cache size) to limit external API calls. Use the **Test** button in settings to verify the endpoint before using it in queries.

Best for: integrating with external APIs (user directories, CMDBs, enrichment services) where a static file isn't practical.

### JSON File

JSON file lookups query an uploaded JSON file using a jq-style expression. Parameters are passed into the query as named variables. Output suffixes are derived from the configured response paths.

```
* | lookup config service_name
```

Best for: structured reference data that doesn't fit a flat CSV model (nested objects, arrays, complex key schemes).

## Combining Tables

Multiple lookups can be chained. Each lookup adds its own suffixed fields without interfering with others:

```
* | lookup geoip src_ip | lookup asn src_ip | lookup rdns src_ip | lookup useragent user_agent
```

This produces `src_ip_country`, `src_ip_asn`, `src_ip_hostname`, `user_agent_browser`, etc. — all on the same record. Lookups execute left-to-right, so later lookups can filter on fields produced by earlier ones:

```
* | lookup geoip src_ip | where src_ip_country="US" | lookup rdns src_ip
```

Lookups also work after `stats`, enriching aggregated table rows:

```
* | stats sum(bytes) as total by src_ip | sort -total | head 20 | lookup rdns src_ip | lookup geoip src_ip
```
