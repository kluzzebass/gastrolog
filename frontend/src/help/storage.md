# Storage

Once a record has been ingested and digested, it is [filtered](help:routing) into one or more **vaults** based on filter expressions. Each vault appends the record to its active chunk.

Vaults manage the full lifecycle of your data:

- [**Filtering**](help:routing) controls which records reach which vaults
- [**Rotation**](help:policy-rotation) controls when the active chunk is sealed and a new one begins
- [**Retention**](help:policy-retention) controls when old sealed chunks are deleted to reclaim space
- The **storage engine** determines how data is persisted (disk or memory)

## Storage Engines

| Type | What it does |
|------|-------------|
| [**File**](help:storage-file) | Persists logs to disk with memory-mapped reads — for production use |
| [**Memory**](help:storage-memory) | Keeps everything in memory — fast but lost on restart, for testing |
| [**Cloud**](help:storage-cloud) | Archives sealed chunks to S3, Azure Blob, or GCS — for long-term retention |

In a [cluster](help:clustering), each vault is assigned to a specific [node](help:clustering-nodes). Log data is stored locally on that node and is **not replicated** — clustering replicates configuration only. Searches automatically reach vaults on all nodes.

Select a topic from the sidebar for details on engines, rotation, and retention.
