# Ingesters

The Ingesters tab shows each configured [ingester](help:ingestion) with its runtime status and metrics. The list is flat and sorted by name; in a [cluster](help:clustering), each card names the [node](help:clustering-nodes) it is pinned to, or **N nodes** when it is pinned to several (hover for their names).

## Status

The header badge reads the ingester's run state against the nodes eligible to run it:

- **N/M running** — running on N of the M eligible nodes. Green when every eligible node runs it; amber when some have not started it; red when an eligible pinned node is unreachable
- **no eligible node** — no live node may run it: pin a live node, or allow all nodes, in [Settings](settings:ingesters)
- **disabled** — the ingester runs on no node until it is enabled

## Nodes

Expand an ingester to see its node chips, one per eligible node (every live node when it is allowed on all nodes): green when the node runs the ingester, amber when the node is eligible but not running it, red when the cluster has not heard from the node. Hover a chip for its state in words.

The metrics beneath are:

- **Messages ingested**: Total messages received since the ingester started
- **Bytes ingested**: Total data volume received
- **Errors**: Failed ingester runs — times the ingester exited with an error and was restarted

**No status — no node is reporting this ingester.** means no node has published a status for it: it has not run anywhere since the cluster started.
