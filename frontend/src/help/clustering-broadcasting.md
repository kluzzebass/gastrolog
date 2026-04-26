# Broadcasting

Broadcasting is the mechanism by which cluster nodes share runtime state and liveness with each other. Each node sends two kinds of message on different cadences: a heavy stats snapshot at the **broadcast interval**, and a lightweight liveness ping at the **heartbeat interval**.

## What Gets Broadcast

The full stats broadcast (every broadcast interval) includes:

- **System metrics** — CPU usage, memory (RSS, heap), goroutine count, GC stats
- **Ingest queue** — current depth and capacity
- **Ingester stats** — per-ingester message counts, byte counts, error rates
- **Storage stats** — per-vault record counts, chunk counts, sizes
- **Raft state** — current role (leader/follower/candidate), term, applied index

This data powers the [Inspector](help:inspector)'s cluster-wide view, letting you monitor all nodes from any single node.

The heartbeat broadcast (every heartbeat interval) is a small empty marker — sender ID and timestamp only. It carries no runtime state. Its only job is to refresh the peer's last-seen timestamp so paused or frozen nodes are detected quickly without making the heavy stats payload fly more often than it needs to.

## Broadcast Interval

Controls how often the **full stats payload** is sent. Configure it in **Settings > [Cluster](settings:service)** under the Broadcasting section. Default `5s`.

Lower intervals give fresher data in the Inspector but increase network traffic. For most deployments, the default is a good balance. In large clusters (5+ nodes) or high-latency networks, consider `10s` or `15s`.

## Heartbeat Interval

Controls how often the **liveness ping** is sent. Default `1s`. The peer offline-detection threshold is 4× this value (default `4s`) — a node that misses ~3 consecutive heartbeats is marked offline.

Lowering this gives faster paused-node detection. Raising it reduces RPC cadence at the cost of slower failure detection. The heartbeat interval should be much shorter than the broadcast interval; if you raise it above the broadcast interval, the broadcast itself becomes the effective heartbeat (defeating the split).

## Staleness

A peer's last-seen timestamp is refreshed by either signal — heartbeat or stats broadcast. If neither arrives within the offline threshold (4× heartbeat interval), the peer drops from the live-peer set. The [Inspector](help:inspector-system) shows remote node metrics with their last-seen timestamp so you can tell how fresh the data is.
