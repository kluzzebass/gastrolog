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

## Peer nodes (compact view)

For other [cluster](help:clustering) nodes, metrics are collected via [broadcasting](help:clustering-broadcasting) and include:

- **CPU** and **memory** usage
- **Goroutines** and **GC** cycle count
- **Ingest queue** depth and capacity
- **Raft state** — current role (leader/follower/candidate), term, and applied log index
- **Per-ingester** message and byte count summaries

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

Peer node data is refreshed at the [broadcast interval](help:clustering-broadcasting) (default 5 seconds). Nodes that haven't broadcast recently show stale timestamps.

## System alerts

The header's alert pill opens the alarm list: every **standing alarm** in
the cluster, attributed to the node that raised it, with priority, cause
and response text from the alarm catalog. Alarms are state, not events —
they stand while a condition holds.

Each alarm is in one of two **states**:

- **Active** — the condition stands. This is the default list, sorted by
  priority then age. A row expands to its cause and response text. The
  alarm clears on its own when the condition resolves — there is nothing
  to acknowledge or dismiss. **Latching** alarms (such as
  `orchestrator-lock-leak`) are the exception: a software fault stands
  until the affected node restarts, because a fault that "went away" is
  still a fault — report it, then restart.
- **Shelved** — an operator suppressed the alarm for a chosen duration.
  Shelves **always expire** (there are no permanent shelves) and shelved
  alarms stay visible in a collapsed section. When the shelve lapses with
  the condition still true, the alarm returns to the active list.

Some alarm types refuse shelving because deferral is meaningless for them:
software faults (nothing improves during the window — report them instead)
and `alarm-flood` (hiding the degradation indicator defeats the alarm
system's self-monitoring). Those rows show no shelve control.

Shelves apply **cluster-wide**: the node serving the request forwards the
operation to every node raising the alarm ID. Alarm state is in-memory
only and does not survive a node restart — after a restart, a condition
that still holds is simply re-detected and raised as an active alarm.

The same controls are available from a shell: `gastrolog alerts`,
`gastrolog alerts shelve <id> --for 2h`, and
`gastrolog alerts unshelve <id>`.
