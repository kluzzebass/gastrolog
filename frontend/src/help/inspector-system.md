# System

The System view shows health and performance metrics for each node.

## Local node (rich view)

For the node you're connected to, the view includes:

- **Status** — healthy, degraded, or unhealthy
- **Version** and **uptime**
- **CPU** usage percentage
- **Memory** — RSS, heap alloc/in-use/idle/released, stack, virtual, heap objects, GC cycles
- **Ingest queue** — depth, capacity, and a fill bar
- **Storage** — total records, size, chunk counts, time span, and a per-vault breakdown
- **Ingestion** — per-ingester message/byte counts and error rates

## Remote nodes (compact view)

For other [cluster](help:clustering) nodes, metrics are collected via [broadcasting](help:clustering-broadcasting) and include:

- **CPU** and **memory** usage
- **Goroutines** and **GC** cycle count
- **Ingest queue** depth and capacity
- **Raft state** — current role (leader/follower/candidate), term, and applied log index
- **Per-ingester** message and byte count summaries

Remote node data is refreshed at the [broadcast interval](help:clustering-broadcasting) (default 5 seconds). Nodes that haven't broadcast recently show stale timestamps.
