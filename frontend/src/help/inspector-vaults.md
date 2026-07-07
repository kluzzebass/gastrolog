# Vaults

The Vaults tab shows each configured [vault](help:storage) with its [chunks](help:general-concepts) and [indexes](help:indexers).

## Vault Overview

Each vault lists its name, type, enabled/disabled status, total chunk count, and record count. Expand a vault to see its chunk timeline. In a [cluster](help:clustering), vaults are grouped by their owning [node](help:clustering-nodes).

## Chunks

Each chunk shows its ID, time range, per-node seal pips, record count, and size. The active chunk is the one currently accepting writes — all others are sealed and immutable. Chunks are sealed according to the vault's [rotation policy](help:policy-rotation).

## Seal Pips

The pip row shows one circle per placement node, in the same node order on every row — a sick node reads as a vertical stripe down the table. Fill degree carries the lifecycle; color is secondary.

A **chunk seal** is the cluster-wide fact that a chunk's record membership is frozen — it happens once. A **copy seal** is one node's completed local copy of that sealed chunk. A late copy seal (a node catching up after rejoining) is normal operation.

**Birth fills green:**

- Hollow copper ring — chunk active on this node
- Half-filled amber, pulsing — copy seal pending or building (routine while the chunk seals)
- Half-filled amber with a glow — copy lagging: the chunk is already sealed cluster-wide and this node is catching up
- Calm green — copy sealed on this node

Sealed pips are deliberately quiet everywhere. Anomalous pips — lagging, unreachable, stale — glow in their own color, so the node with the problem is the loudest thing in its row.

**Death drains red** (while a delete runs):

- Solid red, pulsing — node still holds bytes and owes its delete acknowledgment; the laggard blocking the delete is the last dot still red
- Dim hollow red ring — node acknowledged, bytes gone

**Node and anomaly states:**

- Dashed red slashed ring, glowing — placement node unreachable (a node condition, not a chunk state)
- Muted glowing dot after a gap — stale residency: a copy on a node that is no longer in the placement
- Bordered label instead of pips — cloud-backed chunk; bytes live in the named blob store, not on placement nodes

Expand a chunk row to see the same pips with node names beside them.

## Indexes

Sealed chunks list their [indexes](help:indexers) with name, status, entry count, and size. An index in **ready** status is being used by the [query engine](help:query-engine). A **building** index is still being constructed in the background.

## Validate

The Validate button checks data integrity for a vault — verifying that chunk files are consistent and indexes match their data. Use it if you suspect corruption after a crash or disk issue.

## Throughput

Each vault card shows the pipeline as three stage rates, per node, with
sparklines (recent per-tick history) and Unix-load-style 1m/5m/15m averages
on hover:

- **Append** — origin ingress: records written into the vault's working
  segments on the node where they land. The queue gauge appears when the
  writer's bounded queue holds records (backpressure); a *durable* figure
  appears when fsync commits lag appends.
- **Collected** — home ingress: records arriving in `head/` on each
  placement node, whether pulled from a peer or promoted locally.
- **Sealed** — records materialized into sealed, queryable GLCB chunks on
  each home.

**Collected and Sealed sums exceed Append by design.** Every placement
member collects and seals its own copy, so their totals count each record
once *per home* — with a replication factor of 4, one appended record shows
up as four collection events. Those rows measure replication *work* (real
bytes moved to each node), not record throughput; the totals row is labeled
"Σ N homes" and its status column says **×N replication** when the sum
tracks append × homes. **+ catch-up** means the sum is running ahead of
that — a rejoined node backfilling missed segments or a backlog draining —
and **catch-up** alone means replication is working with no live ingest.
Divide by the number of active homes to compare against the Append rate.

Reading the panel: the three rates should track each other at steady state.
A downstream stage falling away from its upstream is a pipeline stall in
progress — e.g. **Sealed** flatlining at 0 while **Append** runs means
chunks are accumulating unsealed (the Pipeline Backlog panel below shows
where the inventory stacks).

Quiet nodes state their reason in the STATUS column: **caught up** (that
node's `head/` holds every published segment), **no ingest** (no records
arriving on that node), **up to date** (nothing eligible to seal), or
**behind N segments** — which, on a node showing no activity, means that
node's stage has stalled and is highlighted as a warning.

