# System

The System view shows health and performance metrics for each node.

## Cluster summary

In a cluster, the first card summarizes the whole cluster:

- **Leader** — the cluster-ctl Raft leader. **none — no quorum** means no
  node leads the group and configuration is read-only until a quorum elects
  one (see [Quorum Loss](help:clustering#quorum-loss-read-only-mode)).
- **Vaults, Records, Data, Chunks** — what the cluster's vaults hold, with
  each vault counted once however many nodes keep a copy of it. **Data** is
  the size of the records themselves, one copy of each chunk, without
  indexes. Every node reports the same figures, whichever node you are
  connected to. While the figures are still loading they show `—`; if some
  vault has no figures yet, a note says how many of the vaults the totals
  cover.
- **Throughput** — routing and append rates summed across nodes. Each record
  is appended once, by the node that received it, so the sum is the
  cluster's rate.
- **Combined Resources** and **Ingest Queue** — CPU, memory, goroutines, and
  queue depth added up across every node that is reporting stats.

Disk usage, which does count every copy, is the **Storage** figure in the
header.

## Local node (rich view)

Every node — the one you are connected to included — is shown with the same
view, filled from that node's stats [broadcast](help:clustering-broadcasting);
there is no separate local view. In a [cluster](help:clustering) each node is
a card with its node-state badge, the local node first, expanded, and badged
**this node**. The rows are listed in the next section.

## Peer nodes (compact view)

The per-node view, the same for every node:

Every row carries its meaning on hover.

- **System** — CPU, Goroutines, Mem In-Use, RSS, Heap Alloc, Heap Idle,
  Stack, GC Cycles, and **Uptime** since the process started. **Log Drops**
  appears once the self ingester has discarded a diagnostic record because
  its capture channel was full — a capacity signal to trend, not a fault.
- **Addresses** — the node's API and pprof addresses, when it reports them.
- **Ingest Queue** — Depth against capacity, and **Pressure** once the ingest
  pressure level leaves normal; the pipeline throttles itself, so there is
  nothing to act on.
- **Throughput** — see below.
- **Raft State** — State (this node's role in the cluster-ctl group), Term
  (rises with every election), Applied (last index applied to the FSM),
  Commit (last index persisted on a quorum), FSM Pending (committed entries
  not yet applied — grows while apply is slow), and Last Contact (time since
  the leader last reached this node, rounded to one decimal; `never` means
  no leader contact since start).
- **Raft liveness** rows, once the node has appended to its WAL —
  **WAL Append** average and max latency across every Raft group on the node,
  **Elections** total and per minute, **Leader Losses** (leaderships this node
  held and lost), and **Failed HBs** (replication heartbeats a follower did
  not answer). Elections sustained above about 3/min is an election storm; a
  WAL max latency near 1s means bulk I/O is starving consensus.

A node card with no rows says why: **No stats yet — waiting for this node's
first broadcast.** when the cluster has never heard from it, or **No stats —
the cluster last heard from this node N ago.** when it has gone silent. The
System tab itself says **No cluster status yet** while the node you are
connected to has not reported its member list.

## Throughput

Nodes with routing activity or a local vault writer show a **Throughput**
section:

- **Routed / Matched** — this node's routing rates: records per second
  entering the routing stage, and records per second matched to at least one
  [route](help:routing). The difference is this node's live drop rate.
- **Per-vault rows** — for each vault this node writes locally: the append
  rate (records per second written to the vault's working segment) with a
  sparkline of recent history, and the segmentation **queue** depth against
  its capacity.

The number shown is the instantaneous rate over the last stats tick
(~5 seconds) with its sparkline; hovering a row reveals the 1m/5m/15m
exponentially weighted moving averages (the Unix load-average technique) —
the sustained-rate figures for before/after comparisons. A vault whose queue stays
near capacity while its durable-commit rate lags its append rate is
experiencing write-path backpressure (usually fsync pressure on that node's
disk).

Peer node data is refreshed at the [broadcast interval](help:clustering-broadcasting) (default 5 seconds). The stats carry no freshness marker; a node the cluster has not heard from shows an **offline** badge in the node list, with how long it has been silent.

## Network

The **Network** section of a node's pane lists its inter-node traffic: one row
per peer with the combined Tx/s and Rx/s rates and a sparkline of recent
history, and beneath it one lane row per connection between the two nodes,
both directions merged. `raft` lanes carry one Raft group each: the lane whose
detail reads `—` is the cluster-ctl group; each other names the vault whose
vault-ctl group it carries (hover for the group ID). `service #N` lanes are
the pooled RPC connections, numbered by pool slot. A service lane's detail is
a purpose strip with one slot per purpose — search, chunk apply, vault apply,
broadcast, forward, forward RPC, eviction, remove node, suffrage, chunk
transfer, chunk wait, replicate, replicate catchup, segment pull, file
transfer, raft — lit when that purpose used the lane during the last stats
interval and dimmed otherwise; hover a slot for its name. A rate reads `—`
until the lane has history, and a node with no recorded connections says so.

## System alerts

The header's alert pill opens the alarm list: every **standing alarm** in
the cluster, attributed to the node that raised it, with priority, cause
and response text from the alarm catalog. Alarms are state, not events —
they stand while a condition holds, and there are no per-alarm operator
states: an alarm is standing or it is not.

The list is flat, sorted by priority then age. A row expands to its cause
and response text. An alarm clears on its own when the condition resolves —
there is nothing to acknowledge or dismiss. **Latching** alarms (such as
`orchestrator-lock-leak`) are the exception: a software fault stands until
the affected node restarts, because a fault that "went away" is still a
fault — report it, then restart.

Alarm state is in-memory only and does not survive a node restart — after
a restart, a condition that still holds is simply re-detected and raised
again.

The same list is available from a shell: `gastrolog alerts`.
