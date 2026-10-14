# Fan-out data plane

How a record gets from an ingester to a sealed chunk on every home of its
vault: leaderless fan-out, where any node that receives a record writes it
durably to a local segment, and each home of the vault pulls those segments
and builds the vault's chunks itself.

- [v3/pipeline.md](v3/pipeline.md) — the V3 pipeline: phases and managers,
  EventID, routing, segments and their file format, distribution, collection,
  chunk planning, build, seal and verification, release and purge,
  backpressure, and queries over not-yet-sealed chunks.

Related:

- [../vault-control-plane-architecture.md](../vault-control-plane-architecture.md)
  — the vault-ctl Raft group, the chunk lifecycle on its FSM, GLCB catch-up and
  chunk tombstones.
- [../file_formats.md](../file_formats.md) — the GLCB chunk format.
- [../ubiquitous_language.md](../ubiquitous_language.md) — section 9,
  Pipeline, for the canonical terms.
