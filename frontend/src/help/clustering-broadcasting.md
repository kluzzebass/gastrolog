# Broadcasting

Broadcasting is the mechanism by which cluster nodes share runtime state with each other: every node sends a stats snapshot to every peer at the **broadcast interval**.

Note what broadcasting is *not* responsible for. Peer **liveness** — whether a node is up and reachable — comes from Raft, not from this broadcast. Nodes are already exchanging Raft replication heartbeats several times a second, and that traffic is the authoritative reachability signal. Broadcasting carries observability payload; nothing in the cluster waits on its cadence to notice a dead peer.

## What Gets Broadcast

The stats broadcast includes:

- **System metrics** — CPU usage, memory (RSS, heap), goroutine count, GC stats
- **Ingest queue** — current depth and capacity
- **Ingester stats** — per-ingester message counts, byte counts, error rates
- **Storage stats** — per-vault record counts, chunk counts, sizes
- **Raft state** — current role (leader/follower/candidate), term, applied index

This data powers the [Inspector](help:inspector)'s cluster-wide view, letting you monitor all nodes from any single node.

## Broadcast Interval

Controls how often the stats payload is sent. Configure it in **Settings > [Cluster](settings:service)** under the Broadcasting section. Default `5s`.

Lower intervals give fresher data in the Inspector but increase network traffic. For most deployments, the default is a good balance. In large clusters (5+ nodes) or high-latency networks, consider `10s` or `15s` — raising it slows down how quickly Inspector numbers refresh, but it does not slow down failure detection.

## Staleness

A peer's cached stats expire if no broadcast arrives within three broadcast intervals (default `15s`), tunable with the `GLOG_PEER_TTL_MULTIPLIER` environment variable. The [Inspector](help:inspector-system) shows peer node metrics with their last-seen timestamp so you can tell how fresh the data is.

## Liveness

A peer is considered reachable when this node has recent Raft contact with it — an answered replication heartbeat, or any Raft message received from it, on any Raft group the two share. A node that stops answering while we are still replicating to it drops out of the live set within roughly twice the Raft heartbeat timeout (default `4s`).

Two nodes do not always share Raft traffic: in each Raft group, followers exchange messages only with the leader, so two co-followers may have no direct Raft contact at all. For those pairs, and during the brief window at startup before groups exist, liveness falls back to the arrival of the stats broadcast. Nothing is ever declared unreachable merely because no shared Raft group exists.
