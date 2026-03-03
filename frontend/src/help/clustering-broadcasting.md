# Broadcasting

Broadcasting is the mechanism by which cluster nodes share runtime statistics with each other. Each node periodically sends a snapshot of its current state to all peers.

## What Gets Broadcast

Each broadcast message includes:

- **System metrics** — CPU usage, memory (RSS, heap), goroutine count, GC stats
- **Ingest queue** — current depth and capacity
- **Ingester stats** — per-ingester message counts, byte counts, error rates
- **Storage stats** — per-vault record counts, chunk counts, sizes
- **Raft state** — current role (leader/follower/candidate), term, applied index

This data powers the [Inspector](help:inspector)'s cluster-wide view, letting you monitor all nodes from any single node.

## Broadcast Interval

The broadcast interval controls how often stats are sent. Configure it in **Settings > [Cluster](settings:service)** under the Broadcasting section. The default is `5s`.

Lower intervals give fresher data in the Inspector but increase network traffic between nodes. For most deployments, the default is a good balance. In large clusters (5+ nodes) or high-latency networks, consider increasing it to `10s` or `15s`.

## Staleness

If a node hasn't broadcast within several intervals, its peers mark it as potentially offline. The [Inspector](help:inspector-system) shows remote node metrics with their last-seen timestamp so you can tell how fresh the data is.
