# Lookups

The Lookups tab in [Settings](settings:lookups) configures the databases used by the [`| lookup` pipeline operator](help:lookup-tables) for IP address enrichment.

## MaxMind Auto-Download

GastroLog can automatically download and update the free MaxMind GeoLite2 databases (GeoLite2-City for [GeoIP](help:lookup-tables) and GeoLite2-ASN for [ASN](help:lookup-tables) lookups). To enable this:

1. Create a free account at [MaxMind](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data)
2. Generate a license key in your MaxMind account dashboard
3. Enter your **Account ID** and **License Key** in the Lookups settings
4. Check **Enable Automatic Database Downloads**

When enabled, databases are fetched twice weekly (Tuesday and Friday at 03:00). The last update timestamp is shown in the settings.

## Manual Database Paths

You can also point GastroLog at `.mmdb` files on disk. Manual paths take priority over auto-downloaded databases. This is useful for:

- Offline environments without internet access
- Commercial MaxMind databases (GeoIP2-City, GeoIP2-ISP)
- Custom or third-party MMDB databases
- Pinning a specific database version for reproducibility

Set the **GeoIP Manual Path** and/or **ASN Manual Path** fields to absolute paths on the server filesystem.

## Hot-Reload

Database files are monitored and reloaded automatically when they change on disk — no server restart needed. This applies to both auto-downloaded and manually configured databases.

Note: hot-reload watches the file directly and does not follow symlinks. If your deployment uses symlinks (e.g., Kubernetes ConfigMap mounts), point the path at the final file, not the symlink.

## Validation

After saving, each configured database is validated. The results show:

- **Success** — database type, build date, and node count
- **Failure** — an error message explaining what went wrong (e.g., file not found, invalid format)
