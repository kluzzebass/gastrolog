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

For other cluster nodes, metrics are collected via gossip and include CPU, memory, goroutines, GC, ingest queue, Raft state, and per-ingester summaries.
