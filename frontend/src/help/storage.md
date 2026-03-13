# Storage

Once a record has been ingested and digested, [routes](help:routing) direct it into one or more **vaults** based on filter expressions. Each vault appends the record to its active chunk.

Vaults manage the full lifecycle of your data:

- [**Routes**](help:routing) control which records reach which vaults
- [**Rotation**](help:policy-rotation) controls when the active chunk is sealed and a new one begins
- [**Retention**](help:policy-retention) controls when old sealed chunks are deleted or moved
- The **storage engine** determines how data is persisted (disk or memory)

## Storage Engines

| Type | What it does |
|------|-------------|
| [**File**](help:storage-file) | Persists logs to disk with memory-mapped reads — for production use |
| [**Memory**](help:storage-memory) | Keeps everything in memory — fast but lost on restart, for testing |

File vaults support optional [**sealed backing**](help:storage-cloud) — sealed chunks can be uploaded to S3, Azure Blob Storage, or GCS for long-term cloud archival while the active chunk stays on local disk.

## Compression

All file vaults compress sealed chunks automatically using seekable zstd. Compression runs asynchronously after sealing — there is no impact on ingestion latency. Log data typically compresses 5–10x, and the seekable format allows random-access reads without decompressing the entire chunk.

## Tiered Storage

Combine local and cloud-backed vaults to build a hot/cold storage tier:

- **Hot tier:** A local file vault with short retention (e.g. 7 days, 50 GB cap). Fast queries, fast ingestion, bounded disk use.
- **Cold tier:** A file vault with [sealed backing](help:storage-cloud) pointing to S3, GCS, or Azure. Cheap, virtually unlimited capacity. Queries work but download each chunk on demand.

Use [retention eject](help:policy-retention) to move records from hot to cold automatically. Set up an eject-only [route](help:routing) targeting the cold vault, then configure the hot vault's retention to eject through that route. Records age out of the hot tier and land in cloud storage without manual intervention.

Queries automatically search both tiers — results from cloud-backed chunks are merged transparently with local results.

## Clustering

In a [cluster](help:clustering), each vault is assigned to a specific [node](help:clustering-nodes). Log data is stored locally on that node and is **not replicated** — clustering replicates configuration only. Searches automatically reach vaults on all nodes.

Select a topic from the sidebar for details on engines, rotation, and retention.
