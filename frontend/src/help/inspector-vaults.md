# Vaults

The Vaults tab shows each configured [vault](help:storage) with its [chunks](help:general-concepts) and [indexes](help:indexers).

## Vault Overview

Each vault lists its name and type, then its header badges. The counts come from the chunk list, collected from every node hosting the vault; until that collection returns, the header shows **loading chunks…** rather than a count. Expand a vault to see its chunk timeline. The list is flat and sorted by name, whichever [node](help:clustering-nodes) you are connected to; where a vault's copies live is in its **Topology** section.

| Badge | Meaning |
|-------|---------|
| **cloud-backed** | A [cloud-backed vault](help:storage-cloud): sealed chunks are uploaded to its cloud service |
| **disabled** | The vault accepts no records; enable it in [Settings](settings:vaults) |
| **refusing: _cause_, …** | The vault refuses new records; every active cause is named — see **Refusal** below |
| **partial — N nodes unreached** | The chunk list and counts leave out what those nodes hold — see [Partial results](help:inspector#partial-results) |
| **N chunks**, **N records** | Chunks in the vault cluster-wide, and records across them |
| **size on disk** | The sum of each chunk's local on-disk claim as the hosting nodes report it — a cloud-backed chunk with no local copy counts 0, so a vault largely in the cloud reads small here by design |

## Refusal

A **refusing** badge appears on the header when the vault is currently refusing new records. Expand the card to see the **Refusal** section, which lists each active cause with the backend's own detail text — which [file storage](help:storage-config) is below its floor and its free-vs-floor numbers, or which retention bound is violated and its value. Nothing here is reconstructed in the browser; it's exactly what the responding node's admission gate reports.

## Chunks

Each chunk shows its ID, time range, per-node seal pips, record count, and size. The Size column is the chunk's disk claim — the figure the header sums and a retention size bound counts: its file size where the node reports one, its record bytes where it does not. A value marked **cloud** is the cloud object's size for a cloud-backed chunk with no local copy, whose disk claim is zero. The active chunk is the one currently accepting writes — all others are sealed and immutable. Chunks are sealed according to the vault's [rotation policy](help:policy-rotation). The chunk list is collected from every node hosting the vault, so **No chunks in this vault yet.** is a cluster-wide fact; **No chunks reported — N nodes unreached.** means the merge missed those nodes and the list is uncertain.

Beside the pips, a chunk row can carry:

- **retention pending** — the chunk is queued for its [retention event](help:policy-retention): it is deleted, or its records routed, when the event fires
- **archived** — the cloud object sits in an offline storage class (named on hover); restore it before it can be queried

An expanded cloud-backed chunk lists its **Cloud Storage**: the **blob** row is the sealed chunk object in the cloud store and its size; **cached** is the local copy on the responding node, or `not cached`; **ts-index embedded** means the timestamp indexes travel inside the object, with no separate file to build or lose; **class** is the object's cloud storage class, `standard` when it is readable without a restore.

## Seal Pips

The pip row shows one circle per placement node, in the same node order on every row — a sick node reads as a vertical stripe down the table. Fill degree carries the lifecycle; color is secondary.

A **chunk seal** is the cluster-wide fact that a chunk's record membership is frozen — it happens once. A **copy seal** is one node's completed local copy of that sealed chunk. A late copy seal (a node catching up after rejoining) is normal operation.

**Birth fills green:**

- Hollow copper ring — chunk active on this node
- Half-filled amber, pulsing — copy seal pending or building (routine while the chunk seals)
- Half-filled amber with a glow — copy lagging: the chunk is already sealed cluster-wide and this node is catching up
- Calm green — copy sealed on this node

A row with nothing wrong — every copy sealed, no stale copy trailing it — dims its green pips to a calm grey-green: the quiet default. Beside an anomaly the sealed pips stay full green, so the healthy copies read as the contrast to the sick one. Anomalous pips — lagging, unreachable, stale — glow in their own color, so the node with the problem is the loudest thing in its row.

**Death drains red** (while a delete runs):

- Solid red, pulsing — node still holds bytes and owes its delete acknowledgment; the laggard blocking the delete is the last dot still red
- Dim hollow red ring — node acknowledged, bytes gone

**Node and anomaly states:**

- Dashed red slashed ring, glowing — placement node unreachable (a node condition, not a chunk state)
- Muted glowing dot after a gap — stale residency: a copy on a node that is no longer in the placement. It never counts toward the copies that let segments be released, and it disappears once the departed node is pruned from the vault — right after the placement change, or when the node is removed from the cluster
- Bordered label after the pips — cloud-backed chunk; bytes are durable in the named blob store. Its pips report the LOCAL CACHE per node: calm green for a cached copy, a dim hollow green ring where no local copy exists (normal once bytes are in the store)

Expand a chunk row to see the same pips with node names beside them.

## Indexes

Sealed chunks (cloud-backed ones aside) list their [indexes](help:indexers) by name. The list is answered by the node you are connected to when it holds the chunk, otherwise by the first hosting node that does — so what it describes is one node's copy, the index files that node's query engine would open.

- **ok** — the index opened on the node that answered; its entry count and size follow
- **missing** — the node that answered could not open that index: its file is absent there (never built on that node, or removed) or unreadable, so the [query engine](help:query-engine) on that node cannot use it for the chunk. `gastrolog reindex` rebuilds a vault's indexes; the timestamp indexes are not listed because they live inside the chunk's GLCB object and cannot go missing on their own.
- **Indexes unavailable — no node answered for this chunk.** — no hosting node reported holding the chunk
- **Indexes unknown — … N nodes unreached.** — the nodes that answered do not hold the chunk and some hosting node could not be reached, so the answer is uncertain rather than empty

## Validate

The Validate button checks data integrity for a vault — verifying that chunk files are consistent and indexes match their data. Use it if you suspect corruption after a crash or disk issue.

## Topology

The **Topology** block on a vault card shows where the vault's copies live, read from the vault's placements in the cluster configuration. A jsonl vault also shows its path.

| Field | Meaning |
|-------|---------|
| **Leader** | The node whose placement leads the vault. Reads `unplaced` when no placement is marked leader — usually because no node has a file storage of the vault's storage class |
| **Followers** | The other placement nodes. A follower shown with `(class N)` in warn colour has no file storage of the vault's class and falls back to its first storage, which is of class N |
| **RF=N** | The vault's replication factor; shown only when it is above 1 |

## Throughput

A vault card shows a **Throughput** section once some node reports rates for
the vault; a vault no node reports rates for has none. The section reads the
pipeline as three stage rates, per node, with sparklines (recent per-tick
history) and Unix-load-style 1m/5m/15m averages on hover.

Two node roles recur here and in the [pipeline backlog](help:inspector-vaults#pipeline-backlog)
panel below it:

- An **origin** is a node that received records for the vault and wrote them
  into a segment. Any node can be an origin.
- A **home** is a node in the vault's placement: one that should hold the
  vault's segments and chunks. Every home collects every segment and builds
  its own copy of every chunk.

The stages:

- **Append** — records per second written into the working segment on each
  origin. A **queue N/M** note appears when the writer's bounded queue holds
  records (backpressure); a **durable** figure appears when fsync commits lag
  appends.
- **Collected** — records per second arriving in the head staging area on
  each home, pulled from the origin or promoted locally.
- **Sealed** — records per second each home sealed into chunks.

**Collected and Sealed sums exceed Append by design.** Every home collects
and seals its own copy, so those totals count each record once *per home* —
with a replication factor of 4, one appended record shows up as four
collection events. They measure replication *work* (real bytes moved to each
node), not record throughput. Their totals row is labeled **Σ N homes**, the
sum over the N homes reporting the stage, and its status column says
**×N replication** when the sum tracks append × homes. **+ catch-up** means
the sum is running ahead of that — a rejoined node backfilling missed segments
or a backlog draining — and **catch-up** alone means replication is working
with no live ingest. Divide by the number of homes to compare against the
Append rate.

Reading the panel: the three rates should track each other at steady state.
A downstream stage falling away from its upstream is a pipeline stall in
progress — e.g. **Sealed** flatlining at 0 while **Append** runs means
segments are accumulating unchunked, and the pipeline backlog panel shows
where they stack.

### Status notes

A quiet row states its reason in the STATUS column. A note in warning colour
is a stall; the others are healthy rest.

| Note | Stage | Meaning |
|------|-------|---------|
| **no ingest** | Append | No records are arriving on that origin |
| **idle** | Collected | That node reports no staging counts for the vault |
| **nothing published** | Collected | No origin has published a segment yet, so there is nothing to collect |
| **caught up** | Collected | That home's head holds every published segment |
| **behind N segments** | Collected | Published segments that home has not collected; on a quiet row, its collection has stalled |
| **N segments eligible to chunk** | Sealed | Registry segments the chunking planner can take but has not; chunking is behind |
| **sealed manifest awaiting build** | Sealed | A chunk is sealed cluster-wide and the homes have not built their copies yet |
| **up to date** | Sealed | Nothing is waiting to be chunked |

## Pipeline backlog

The **Pipeline backlog** panel shows where a vault's records are between
ingest and a sealed chunk: segments in the vault's registry, and segment
files in each staging area, cluster-wide and per node. The registry figures
come from the vault's replicated control log and are identical on every node;
the staging counts are collected from every node, and a badge beside the
heading says when a node could not be reached and the totals omit it.

The headline row:

- **Eligible / registry segments** — segments the chunking planner can take
  now, out of every segment published to the registry. A segment stays in the
  registry until its records are in a replicated chunk and it is released; a
  large eligible count means chunking is not keeping up and shows in warning
  colour.
- **Registry records** — records held by the segments in the registry.
- **Open manifest** — the chunk the planner is currently filling: the
  segments it references and the records they hold.
- **Oldest unchunked ingest** — the last ingest time of the oldest eligible
  segment: how far behind the clock unchunked records reach.

**Staging areas** are the per-vault directories a segment file moves through.
The cluster-total table lists each one; the two tables beneath split them by
node role (see [Throughput](help:inspector-vaults#throughput) for origin and
home):

- **working** — the segment an origin is currently appending to.
- **completed** — completed segments on the origin: published to the
  registry and served to homes until released.
- **pre-head** — segments a home is pulling and has not yet verified.
- **head** — verified segments on a home awaiting chunking; where its recent
  records are queried.

**Origins (segmentation)** lists working and completed segments on each node
that wrote them. **Homes (collection)** lists pre-head and head segments on
each placement node.

When the connected node reports no backlog for the vault, the panel says so in
place of the figures.

