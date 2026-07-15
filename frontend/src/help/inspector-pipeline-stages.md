# Pipeline stages

Every record a vault ingests travels a fixed path before it lands in a sealed,
replicated chunk. **Pipeline stages** counts the discrete milestones along that
path — the events you used to grep out of the cluster log — as first-class,
cross-node metrics.

Each number is the **cluster total**: the sum across every node, where each
milestone is counted exactly once by the node that owns it. Hover any row to
see the **per-node breakdown**.

## What each milestone means

**Segments**

- **Segments completed** — working segments finalized and promoted to
  `completed/` on the origin. This is the first durable milestone after ingest.
- **Segments published** — completed segments whose metadata the origin
  committed to the vault control log, making them cluster-visible for homes to
  pull.
- **Segments released** — segments dropped from the registry once their records
  are superseded by replicated chunks. A healthy pipeline releases at roughly
  the rate it publishes.

**Chunks**

- **Chunks planned** — open chunk manifests the leader opened to gather segment
  references.
- **GLCB builds** — build operations across all homes. Every home materializes
  its own copy of each sealed chunk's GLCB, so the cluster total counts
  *builds*, not chunks: expect roughly chunks sealed × replication factor.
  Compare this rate against the **Append** rate in *Throughput* above: if
  builds fall behind ingest, the consume side is not keeping up.
- **Chunks sealed** — chunk seals the leader committed; counted once per chunk
  cluster-wide. In a calm cluster, GLCB builds ≈ sealed × RF; builds lagging
  that ratio means one or more homes are not materializing their copies.

**Recovery & retention**

- **Head purges** — segment copies deleted from `head/` after their records were
  materialized into a replicated chunk.
- **GLCB pulls attempted / failed** — replica catch-up pulls a home started to
  recover a chunk blob it was missing, and how many no peer could satisfy. A
  climbing **failed** count (shown in warning color) is a durability incident:
  some chunk has no healthy source.
- **Retention deletes** — chunks the retention engine expired, counted on the
  leader that made the expiration decision.

## Reading the rates

The milestones with a **Rate** column (segments completed/published, chunks
built/sealed) carry a per-second rate and sparkline computed server-side over
the cumulative totals — the same rolling-window mechanism as the *Throughput*
readout. The spark shows the busiest node's recent shape; the rate is the
cluster sum. Milestones without a rate show only their running total.

Rows stay hidden until their counter moves — the panel is quiet until there is
something to show.
