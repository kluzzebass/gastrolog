# Storage

Once a record has been ingested and digested, it is [routed](help:routing) to one or more **stores** based on filter expressions. Each store appends the record to its active chunk.

Stores manage the full lifecycle of your data:

- [**Routing**](help:routing) controls which records reach which stores
- [**Rotation**](help:policy-rotation) controls when the active chunk is sealed and a new one begins
- [**Retention**](help:policy-retention) controls when old sealed chunks are deleted to reclaim space
- The **storage engine** determines how data is persisted (disk or memory)

## Storage Engines

| Type | What it does |
|------|-------------|
| [**File**](help:storage-file) | Persists logs to disk with memory-mapped reads — for production use |
| [**Memory**](help:storage-memory) | Keeps everything in memory — fast but lost on restart, for testing |

Select a topic from the sidebar for details on engines, rotation, and retention.
