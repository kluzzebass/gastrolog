# Storage

Once a record has been ingested and digested, [routes](help:routing) direct it into one or more **vaults** based on filter expressions. Each vault contains an ordered chain of **tiers** that manage data through its lifecycle — from fast ingestion to long-term archival.

## Vaults and Tiers

A **vault** is a logical container. It doesn't store data itself — its tiers do. Each tier is a full chunk manager with its own storage, rotation, retention, and replication settings. Records enter the first tier (the ingestion tier) and flow through the chain via [retention eject rules](help:policy-retention).

Example: a vault with three tiers:

| Tier | Type | Purpose |
|------|------|---------|
| Tier 1 | [File](help:storage-file) | Hot storage on NVMe. 1-minute rotation, 5-minute retention, eject to tier 2. |
| Tier 2 | [Cloud](help:storage-cloud) | Warm storage in S3. Sealed chunks uploaded to the cloud, local cache for queries. |
| Tier 3 | [File](help:storage-file) | Cold archive on HDD. Long retention, slow but cheap. |

## Tier Types

| Type | What it does |
|------|-------------|
| [**File**](help:storage-file) | Persists chunks to local disk with memory-mapped reads |
| [**Memory**](help:storage-memory) | Keeps chunks in RAM — fast but lost on restart |
| [**Cloud**](help:storage-cloud) | Active chunk on local disk, sealed chunks uploaded to S3/GCS/Azure |
| **JSONL** | Append-only JSON lines file — write-only sink for debugging or export |

## Replication

Each tier has its own **replication factor** (RF). Replicas are placed on [file storages](help:storage-config) with the matching storage class. The placement manager prefers different nodes (availability) but allows same-node placement on different disks (redundancy).

- **RF=1** — no replication. Single copy.
- **RF=2** — one primary, one secondary (nonvoter). Redundancy without fault tolerance.
- **RF=3+** — full quorum. Survives node failures.

## Compression

All file and cloud tiers compress sealed chunks automatically using seekable zstd. Compression runs asynchronously after sealing — no impact on ingestion latency. Log data typically compresses 5-10x, and the seekable format allows random-access reads without decompressing the entire chunk.

## Queries

Queries automatically search all tiers in a vault. Results from cloud-backed chunks, local sealed chunks, and active chunks are merged transparently. Cloud chunks are fetched on demand via range requests — no full download required.

Select a topic from the sidebar for details on each tier type.
