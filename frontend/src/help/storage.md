# Vaults

Once a record has been ingested and digested, [routes](help:routing) direct it into one or more **vaults** based on filter expressions. Each vault is a self-contained storage container — chunks, indexes, rotation, retention, replication — described by a single storage shape.

## What a vault is

A **vault** owns its records, indexes, retention rules, and access shape. It has one **type** (memory, file, jsonl) and — for file vaults — an optional **cloud service** binding. The vault is the storage unit, and the cluster runs each vault independently.

Hot/warm/cold layering is composed by **chaining vaults via routes**. Records exit one vault when retention triggers and (with `Send records to routing engine` disposition) flow into the next vault through the routing table.

Example: a hot/warm/cold deployment with three vaults:

| Vault | Type | Purpose |
|-------|------|---------|
| `api-hot` | [Memory](help:storage-memory) | RAM-backed. 1-minute rotation, 5-minute retention. Records eject to `api-warm` on retention. |
| `api-warm` | [File on cloud-backed storage](help:storage-cloud) | Sealed chunks upload to S3, local cache for queries. 7-day retention, eject to `api-cold`. |
| `api-cold` | [File on slow disk](help:storage-file) | Long retention, slow but cheap. |

The chain is wired with three [routes](help:routing): one per source/destination link. The retention disposition on each upstream vault must be `Send records to routing engine` for records to flow to the next vault.

## Storage shapes

| Shape | What it does |
|------|-------------|
| [**File**](help:storage-file) | Persists chunks to local disk with memory-mapped reads |
| [**Memory**](help:storage-memory) | Keeps chunks in RAM — fast but lost on restart |
| [**Cloud-backed file**](help:storage-cloud) | File vault with a cloud service binding — active chunk on local disk, sealed chunks uploaded to S3/GCS/Azure |
| **JSONL** | Append-only JSON lines file — write-only sink for debugging or export |

## Replication

Each vault has its own **replication factor** (RF). Replicas are placed on [file storages](help:storage-config) with the matching storage class. The placement manager prefers different nodes (availability) but allows same-node placement on different disks (redundancy).

- **RF=1** — no replication. Single copy.
- **RF=2** — one leader, one follower (nonvoter). Redundancy without fault tolerance.
- **RF=3+** — full quorum. Survives node failures.

## Compression

File vaults (local-only and cloud-backed) compress sealed chunks automatically using seekable zstd. Compression runs asynchronously after sealing — no impact on ingestion latency. Log data typically compresses 5-10x, and the seekable format allows random-access reads without decompressing the entire chunk.

## Queries

Queries automatically search all vaults. Results from cloud-backed chunks, local sealed chunks, and active chunks are merged transparently. Cloud-backed chunks are fetched on demand via range requests — no full download required.

Select a topic from the sidebar for details on each storage shape.
