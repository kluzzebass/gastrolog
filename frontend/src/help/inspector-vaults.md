# Vaults

The Vaults tab shows each configured [vault](help:storage) with its [chunks](help:general-concepts) and [indexes](help:indexers).

## Vault Overview

Each vault lists its name, type, enabled/disabled status, total chunk count, and record count. Expand a vault to see its chunk timeline. In a [cluster](help:clustering), vaults are grouped by their owning [node](help:clustering-nodes).

## Chunks

Each chunk shows its ID, time range, status (active or sealed), record count, and size. The active chunk is the one currently accepting writes — all others are sealed and immutable. Chunks are sealed according to the vault's [rotation policy](help:policy-rotation).

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
"Σ N homes" as a reminder. Divide by the number of active homes to compare
against the Append rate.

Reading the panel: the three rates should track each other at steady state.
A downstream stage falling away from its upstream is a pipeline stall in
progress — e.g. **Sealed** flatlining at 0 while **Append** runs means
chunks are accumulating unsealed (the Pipeline Backlog panel below shows
where the inventory stacks).

Nodes listed as **idle** on a stage have no current activity. Idle usually
means *caught up* — a node whose `head/` count equals the registry's
published count has simply finished collecting. Compare the per-node counts
in Pipeline Backlog to distinguish caught-up from stalled.

