# Files

Cluster-wide file management. Upload files once and they are automatically distributed to all nodes.

## Uploading

Drag and drop a file onto the upload zone, or click to browse. Files are streamed to disk (never buffered in memory), hashed, and committed to the cluster manifest via Raft. All other nodes pull the file automatically.

## Distribution

When a file is uploaded, every node in the cluster receives it:

- **Immediate**: Nodes online at upload time pull the file within seconds.
- **Startup**: Nodes that were offline pull missing files when they rejoin.
- **Periodic**: A background reconciliation runs every 5 minutes to catch any gaps.

## Replacement

Uploading a file with the same name as an existing file adds a new version: the card lists the newest upload as **current** and the earlier ones under **Previous versions**, each deletable on its own. Nothing is removed automatically — every version keeps its own ID, so a lookup bound to a version keeps using it, while resolution by name picks the newest version. An upload identical in name and content to an existing version is discarded and the existing entry kept.

## Deleting

Click a file card to expand it, then use the delete button. Deletion removes the file from the cluster manifest and cleans up disk on all nodes.

## MaxMind Auto-Download

GastroLog can automatically download and update the free MaxMind GeoLite2 databases (GeoLite2-City for [GeoIP](help:lookup-tables) and GeoLite2-ASN for [ASN](help:lookup-tables) lookups). To enable this:

1. Create a free account at [MaxMind](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data)
2. Generate a license key in your MaxMind account dashboard
3. Enter your **Account ID** and **License Key**
4. Check **Enable Automatic Database Downloads**

When enabled, databases are fetched twice weekly (Tuesday and Friday at 03:00). The last update timestamp is shown in the card. The card is always on the Files tab, with its status reading **enabled** or **disabled**; **Disable** is offered only while downloads are enabled.

MMDB lookups with no uploaded file will automatically use the auto-downloaded database matching their type.

## Used By

- **Lookups**: MMDB files for `| lookup geoip` and `| lookup asn` can be uploaded here or via the convenience drop zone in [Lookups settings](settings:lookups).
- **JSON File Lookups**: JSON data files referenced by JSON file lookups.
