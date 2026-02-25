# Lookup Tables

Lookup tables enrich log records with additional fields by mapping a value (typically an IP address) to metadata. They are used via the [`| lookup` pipeline operator](help:pipeline).

```
* | lookup <table> <field>
```

The lookup result fields are added as `<field>_<suffix>` attributes. For example, `| lookup geoip src_ip` adds `src_ip_country`, `src_ip_city`, etc.

## Built-in Tables

### rdns — Reverse DNS

Resolves IP addresses to hostnames via live DNS PTR queries. No configuration needed.

| Suffix | Description |
|--------|-------------|
| `hostname` | Resolved PTR hostname |

```
* | lookup rdns src_ip
```

Results are cached with a short TTL to avoid flooding DNS resolvers.

### geoip — Geographic Location

Maps IP addresses to geographic metadata using a MaxMind MMDB database. Requires a **GeoLite2-City** or **GeoIP2-City** database file.

| Suffix | Description | Example |
|--------|-------------|---------|
| `country` | ISO 3166-1 country code | `NO` |
| `city` | City name (English) | `Haugesund` |
| `subdivision` | State/province/region name | `Rogaland` |
| `latitude` | Decimal latitude | `59.4138` |
| `longitude` | Decimal longitude | `5.2680` |
| `timezone` | IANA time zone | `Europe/Oslo` |
| `accuracy_radius` | Accuracy radius in km | `50` |

```
* | lookup geoip src_ip
```

Configure the database path in **Settings → Lookups → GeoIP**.

### asn — Autonomous System

Maps IP addresses to autonomous system information using a MaxMind MMDB database. Requires a **GeoLite2-ASN** or **GeoIP2-ISP** database file.

| Suffix | Description | Example |
|--------|-------------|---------|
| `asn` | AS number | `AS15169` |
| `as_org` | AS organization name | `GOOGLE` |

```
* | lookup asn src_ip
```

Configure the database path in **Settings → Lookups → ASN**.

## Configuration

The `geoip` and `asn` tables each need a path to a MaxMind `.mmdb` file. Set these in **Settings → Lookups**. Free GeoLite2 databases are available from [MaxMind](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data) with a free account.

Database files are hot-reloaded when modified on disk — no restart required. Hot-reload watches the file directly and does not follow symlinks.

## Combining Tables

Multiple lookups can be chained on the same field:

```
* | lookup geoip src_ip | lookup asn src_ip | lookup rdns src_ip
```

This produces `src_ip_country`, `src_ip_city`, `src_ip_asn`, `src_ip_as_org`, `src_ip_hostname`, etc. — all on the same record.
