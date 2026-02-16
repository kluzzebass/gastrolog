# Storage

Once a record has been ingested and digested, it is routed to one or more **stores** based on filter expressions. Each store appends the record to its active chunk.

Stores manage the full lifecycle of your data:

- **Rotation** controls when the active chunk is sealed and a new one begins
- **Retention** controls when old sealed chunks are deleted to reclaim space
- The **storage engine** determines how data is persisted (disk or memory)

## Storage Engines

| Type | What it does |
|------|-------------|
| **File** | Persists logs to disk with memory-mapped reads — for production use |
| **Memory** | Keeps everything in memory — fast but lost on restart, for testing |
