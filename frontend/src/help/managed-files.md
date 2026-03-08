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

Uploading a file with the same name as an existing file replaces it. The old file is removed from all nodes.

## Deleting

Click a file card to expand it, then use the delete button. Deletion removes the file from the cluster manifest and cleans up disk on all nodes.

## Used By

- **Lookups**: MMDB files for `| lookup geoip` and `| lookup asn` can be uploaded here or via the convenience drop zone in [Lookups settings](settings:lookups).
