# GastroLog Ubiquitous Language

This document is the shared vocabulary for talking about GastroLog. Every term below
has one canonical meaning. When two or more words mean the same thing in the codebase,
this document names the canonical one and flags the others as synonyms to phase out.

The goal is not prescriptive purity — it's that any engineer can read any file and
already know what the nouns and verbs mean. Code review comments, issue titles, commit
messages, logs, and UI copy should all draw from this vocabulary.

If you find a term missing from this document, add it before merging. If you find a
term used inconsistently in the code, open an issue or fix it in-place.

---

## Reading map

GastroLog is split into **nine bounded contexts**. The boundaries are not arbitrary;
each one corresponds to a package tree with its own abstractions and its own lexicon.
A term that crosses a boundary (e.g. "Record") may look the same but can carry subtly
different guarantees on each side — those crossings are called out explicitly below.

1. [Storage](#1-storage) — how log records are persisted.
2. [Ingestion](#2-ingestion) — how records enter the system.
3. [Query](#3-query) — how records are read back.
4. [Cluster Coordination](#4-cluster-coordination) — how nodes agree on state.
5. [Orchestration](#5-orchestration) — the per-node coordinator that binds everything.
6. [Replication & Forwarding](#6-replication--forwarding) — cross-node data movement.
7. [Observability](#7-observability) — visibility into the system at runtime.
8. [Identity & Config](#8-identity--config) — operator-controlled state + auth.
9. [Pipeline](#9-pipeline) — the write path from ingest to sealed chunk.

At the end, a [Consistency rules](#consistency-rules) section names every known
synonym pair and picks the canonical side, plus conventions for timestamps, IDs,
and cross-context identifiers.

---

## 1. Storage

The physics of the system: bytes on disk (or in memory, or in cloud), organized
for append-heavy write patterns and time-ordered reads.

The layers, bottom up — each built from the one below, none naming another's
concept:

| Layer | Term | What it is |
|-------|------|------------|
| 1 | **Record** | one log entry |
| 2 | **Segment** | a node's durable unit of ingested records in the pipeline (§9) |
| 3 | **Chunk** | a vault's immutable set of records: identity, vault chunk manifest entry, lifecycle |
| 4 | **Open chunk** / **sealed object** | how a chunk's records exist: an open chunk's segment slices, or one sealed object per sealed chunk |
| 5 | **Chunk format** | the sealed object's encoding; GLCB is the current one |
| 6 | **Copy**, **cloud object**, **cached copy** | where a sealed object's bytes sit |
| 7 | **Vault** | a named container of chunks, with policies and placements |

### Aggregates

- **Vault** — a named container for log records. The unit of independent
  storage and the only abstraction over the chunk layer. A vault carries its full
  storage shape directly: a **type** (memory or file), a **storage class**, an
  optional **cloud service** binding, a **rotation policy**, **retention rules** and
  a **retention disposition**, a **replication factor**, a memory budget (memory
  vaults), and cache tuning. Operators create and delete vaults; the
  system manages placements. Declarative config in
  [`system.VaultConfig`](../backend/internal/system/vault.go); per-node runtime in
  [`VaultInstance`](../backend/internal/orchestrator/vault_instance.go) — the
  bundle of chunk manager, index manager, query engine, and Raft callbacks.

- **Chunk** — an immutable, self-contained set of records in one vault. A
  chunk has an identity (`ChunkID`, a sortable GLID), an entry in the vault
  chunk manifest (its metadata, projected as `ChunkMeta`) and a lifecycle
  (Active → Sealing → Sealed); none of these depend on how its records are
  encoded. One active chunk per vault (its open chunk manifest on the
  vault-ctl FSM) accepts new records; it is **sealed** when a rotation policy
  fires (or manually). Sealed chunks are eventually expired by retention.
  Defined: [`chunk/types.go`](../backend/internal/chunk/types.go).

- **Open chunk** — an Active or Sealing chunk: its records are the segment
  slices its open (or sealed) chunk manifest names, read from each home's
  segments (`OpenPipelineChunkCursor`).

- **Sealed object** — the encoding of a sealed chunk's records in a chunk
  format: one per chunk, byte-identical on every node, identified by its
  **sealed digest** in the vault chunk manifest. The unit of storage on a
  home, of peer pull, and of cloud upload.

- **Chunk format** — the encoding of a sealed object: how it is built at
  seal, read, verified against the sealed digest, laid out as a cloud object,
  and where its indexes live. **GLCB** is the current chunk format.

### Value objects

- **Record** — a single log entry: `SourceTS`, `IngestTS`, `WriteTS`, `EventID`,
  `Attrs`, `Raw`. The pipeline carries `record.Record`; query results carry
  `chunk.Record`, which adds `Ref` and `VaultID`, populated by the query engine.
  The ingest ack travels on the ingest message, not the record (see
  [Ack semantics](#ack-semantics)).

- **EventID** — compound identity for a record: `IngesterID + NodeID + IngestTS +
  IngestSeq`. Makes the same record distinguishable from near-duplicates across
  parallel ingesters.

- **Attributes** (a.k.a. `Attrs`) — key-value pairs on a record. Persisted in
  the record's frame, in segments and in the GLCB records section. Queryable as
  fields, beside the first-class record fields (`ingest_ts`, `source_ts`,
  `write_ts`, `ingester_id`, `node_id`, `ingest_seq`).

- **RecordRef** — pointer to a record inside a chunk: `ChunkID + Pos`. Used
  wherever one record needs to refer to another (GetContext anchors, indexes).

- **ChunkMeta** — the stats bag for a chunk: lifecycle `State` (and the
  `Sealed` flag), cloud-backed/archived flags, record count, byte counts
  (`Bytes`, local `DiskBytes`, `CloudBytes`), timestamps (`WriteStart/End`,
  `IngestStart/End`, `SourceStart/End`). The API `ChunkMeta` adds a
  retention-pending flag read from the vault-ctl FSM, and the replica count.

### States a chunk passes through

- **Active** — the chunk is described by the vault's **open chunk manifest**:
  the segment slices the vault-ctl leader's chunking planner extends. No
  GLCB exists yet; each home reads the records from the open chunk
  manifest's segments in its own `head/` (or `completed/` on the origin).
- **Sealing** — the leader has frozen the open chunk manifest
  (`SealOpenChunkManifest`) into a **sealed chunk manifest** in the seal
  queue, and every home builds the chunk's GLCB from its local segments.
- **Sealed** — the vault-ctl leader committed `CmdSealChunk` after its own
  GLCB build. Immutable; eligible for retention and, on a cloud-backed vault,
  cloud upload. Homes that have not finished their build keep building, and
  a home missing the GLCB pulls it from a peer home.
- **Chunk seal** — the cluster-wide fact: `CmdSealChunk` applied, record
  membership frozen. Happens exactly once per chunk.
- **Copy seal** — one home's completed local copy (GLCB build or replica
  pull) of a sealed chunk. There are up to RF copy seals per chunk, and a
  LATE copy seal — a rejoining node catching up — is normal operation, not
  a re-seal. The inspector's per-node seal pips render copy seals.
- **Cloud-backed** — a sealed chunk whose sealed object has been uploaded to
  the vault's cloud store; the **cloud object** is then its durable copy, and
  every home's copy becomes a **cached copy**. Marked with
  `CloudBacked = true` in `ChunkMeta`. A cloud-backed vault is a file vault
  with `CloudServiceID` set; there is no separate "cloud" vault type.
- **Cloud uploader** — the node that uploads a cloud-backed vault's sealed
  chunks: its placement leader, read from the replicated placement at every
  upload. It uploads a
  chunk once its own agreed copy has landed; an upload still queued when the
  role moves is left to the new cloud uploader, whose catch-up sweep picks up
  every sealed chunk not yet cloud-backed. Every other home reads the cloud
  store and never writes the object.
- **Memory vault** — a pipeline vault whose homes serve its sealed GLCBs from
  RAM (held, not mapped) and whose **memory budget** bounds the bytes of its
  sealed chunks, enforced on the vault's placement leader: over budget,
  retention retires the oldest sealed chunks with the vault's disposition
  (reason `memory-budget`). Its
  intake and GLCBs are durable on disk like a file vault's; it is served by
  the file chunk manager with a memory budget set.
- **Archived** — in an offline cloud storage class (e.g. Glacier). Unreadable
  until `RestoreChunk` completes. Tracked via `Archived = true`.
- **Retention-pending** — marked in the vault FSM for deletion on the next sweep.

- **Unreadable chunk** — a sealed chunk whose bytes a retention sweep could
  not read on the responding node. The sweep keeps it and retries with
  exponential backoff rather than destroying what it cannot verify
  (`retention.go` `unreadableEntry`); the vault card's **Retry Unreadable**
  resets the backoff so the next sweep retries every one at once. Held in
  memory per retention runner — a restart clears the list.

### Physical storage

- **FileStorage** — a directory on a node's disk, identified by a GLID, tagged
  with a **StorageClass**. A node can have many file storages (different disks,
  different performance classes). Carries the disk-guard free-space thresholds
  (`DiskFreeWarn`/`DiskFreeFloor`, size-or-percent expressions; empty inherits
  the node defaults): the thresholds guard the volume, not the vaults sharing
  it. A storage below its
  floor puts every vault placed on it into admission refuse (cause
  `STORAGE_DISK_PROTECT`). [`system.FileStorage`](../backend/internal/system/storage.go).

- **NodeStorageConfig** — the list of file storages on one node. Runtime state
  (not operator-authored). [`system.NodeStorageConfig`](../backend/internal/system/storage.go).

- **StorageClass** (`uint32`) — non-zero integer grouping storages by performance
  or role. A file vault's `StorageClass` selects which FileStorage on each node
  hosts its chunks and its pipeline root; a memory vault has no class
  requirement.

- **Pipeline root** — the one directory holding every pipeline storage area a
  node keeps for a vault (`working/`, `completed/`, `pre-head/`, `head/`,
  `chunks/`). On a node with a placement on a file storage it is
  `<storage>/segments/<vault-id>` on the FileStorage that placement chose;
  otherwise (a memory-vault placement, or a node that only originates
  segments for the vault) it is the node home's `segments/<vault-id>`. A placement change moves it, and the next registration
  gathers the vault's files into it. Resolved in one place
  (`orchestrator.pipelineRootFor`) and carried on the vault's pipeline
  registration for every reader.

- **CloudService** — a cluster-wide cloud endpoint (S3, Azure, GCS) with
  optional archival lifecycle. [`system.CloudService`](../backend/internal/system/storage.go).

### Copies of a sealed object

- **Copy** — a sealed object's bytes on a node, verified against the sealed
  digest. A home that holds a verified copy is a **holder** of the chunk.

- **Cloud object** — a cloud-backed chunk's sealed object in the vault's
  cloud store, at `vault-<vault-id>/<chunk-id>.glcb`. Its layout belongs to
  the chunk format: for GLCB, **frames** (below).

- **Cached copy** — a node's local copy of a cloud-backed chunk: kept while
  read, evicted by the vault's cache budget or TTL, downloaded again on the
  next read. A cloud-backed chunk with no local copy on a node is
  **uncached** there.

- **Frame** — (GLCB) one separately zstd-compressed piece of a GLCB cloud
  object (`chunk/glcb/transport.go`): a body frame (records and everything
  before the index sections), one frame per index section, and a tail frame
  (the GLCB's TOC and footer), located by a directory and footer at the
  object's end. A reader that needs one index section range-fetches the
  footer, the directory and that one frame (`FetchRemoteSection`); a full
  download decompresses the frames in order back into the byte-identical
  local `data.glcb`, which is itself uncompressed. Not to be confused with a
  **record frame**, the length-prefixed encoding of one record in a segment
  or in the GLCB records section.

### Placement

- **VaultPlacement** — a mapping of a vault to a specific `FileStorage.ID` (a
  synthetic storage ID for a memory vault) on a specific node, with a `Leader`
  flag. A vault normally has N placements (N =
  replication factor). [`system.VaultPlacement`](../backend/internal/system/storage.go).

- **SyntheticStorageID** — the storage ID of every memory-vault placement:
  memory vaults are not placed on a FileStorage. Format: `node:<nodeID>`.

- **Unplaced** — a vault with no placement marked leader (an empty placement
  list, `LeaderNodeID` empty): no node hosts its active chunk, so it accepts
  nothing. The usual cause is that no node has a file storage of the vault's
  storage class. The inspector's **Leader** row reads `unplaced`, and the
  vault card's placement block says why.

---

## 2. Ingestion

Everything between "bytes arrive from the outside" and "record is appended to
a vault's active chunk".

### Aggregates

- **Ingester** — a protocol adapter that receives external input and produces
  `IngesterMessage` envelopes. Types: `syslog`, `relp`, `http`, `otlp`,
  `fluentfwd` (listeners), `kafka`, `mqtt`, `docker`, `tail`, `metrics`, `self`
  (self-ingesting GastroLog logs), and the synthetic generators `chatterbox`
  and `scatterbox`.
  The `Ingester` contract and its message/factory types live in
  [`pipeline/ingestion`](../backend/internal/pipeline/ingestion) — ingesters are
  "dumb" and depend only on those contracts, never on the orchestrator.
  Declarative config: [`system.IngesterConfig`](../backend/internal/system/vault.go).

- **Singleton ingester** — an ingester that must run on exactly one node at a
  time (e.g. a Kafka consumer with a fixed partition). The placement manager
  assigns it; if the assigned node dies, another takes over. Has a
  `Singleton = true` flag.

- **Parallel ingester** — runs on every eligible node simultaneously (`NodeIDs`,
  or every node in the cluster when `AllNodes` is set). Each
  instance maintains a per-ingester sequence counter so records don't
  collide.

- **Ingester mode** — how an ingester type meets its input; the mode badge on
  every ingester card. A type is a **listener** when its factory registers
  listen addresses (`ListenAddrs`): it binds a port and accepts what clients
  push to it (syslog, HTTP, RELP, OTLP, Fluent Forward), on every assigned
  node — the API calls this `PASSIVE`. Every other type is **active**: it
  pulls from its upstream source itself (Kafka, MQTT, Docker, tail, metrics,
  self, the generators) — `ACTIVE` in the API.

### Domain events

- **IngesterMessage** — the envelope an ingester emits for a raw message
  entering the pipeline. Holds the raw bytes, ingester-specific metadata (syslog
  priority, HTTP headers), ingest timestamp, and ingester identity. The ingestion
  manager mints an EventID and forwards an **IngestMessage** (the digestion-queue
  element) downstream; the digestion manager runs the digesters over it and
  builds the `Record`.

- **Digester** — an enrichment step the digestion manager runs, in order, on
  every IngestMessage regardless of ingester type: it may add attributes and
  fill a missing SourceTS, and never changes `Raw`. Two exist: the level
  digester (a normalized `level` attribute from key-value, JSON or syslog
  priority) and the timestamp digester (SourceTS from common timestamp
  formats). Protocol parsing belongs to the ingesters. Digestion happens on
  the node that received the message, before routing.

### Routing

- **Route** — a row in the cluster-wide routing table:
  `{priority, name, stages, destinations, distribution, enabled}`.
  Routes are evaluated in priority order (lower fires first; name as
  deterministic tiebreaker) with **first-match-wins** semantics. No
  match → drop silently. [`system.RouteConfig`](../backend/internal/system/vault.go).

- **Stage** — one step in a route's pipeline (`RouteConfig.Stages`).
  The only variant is `MatchStage{expression}`, which gates the route
  on a boolean match expression.

- **Match expression** — a query-language predicate evaluated against
  each record's attributes. Special forms: `"*"` = match everything,
  empty string = match nothing (route enrolled but never fires —
  useful for muting). A catch-all is an explicit `*` route at the lowest
  priority.

- **Synthetic attributes** — reserved-prefix attributes (`_source`,
  `_ingester`, `_vault`, `_reason`) overlaid on a record's real
  attrs at routing-evaluation time only. The overlay is computed
  per call and never mutates the record's persisted attrs. They
  unify source-predicate and content-filter into a single expression
  language. Routes match on `_source = "ingest"`,
  `_source = "retention" AND _vault = "<id>"`,
  `_source = "retention" AND _reason = "age"`, etc. User records
  carrying `_`-prefixed attrs collide with this namespace and aren't
  supported.

- **Source kinds** — the canonical wire values of the `_source`
  synthetic: `"ingest"` (records arriving from an ingester) and
  `"retention"` (records the retention engine is feeding back through
  the routing table for a vault that's draining).

- **Distribution mode** — how a single route spreads matched records over
  its enabled destinations (`routing.Distribution`, chosen per record by
  `routing.Manager.selectDestinations`):
  - `fanout` — every destination (default); a destination refusing
    admission refuses the whole record.
  - `round-robin` — one destination per record, rotating from a per-route
    position the routing manager keeps across table swaps; a refusing
    destination is skipped.
  - `failover` — the first destination, in configured order, that admits
    the record.
  Round-robin and failover refuse a record only when every destination
  refuses it. A **disabled** destination vault is left out of the
  distribution, and each record the route would have sent it counts as
  dropped for that vault.

- **Routing table** (`routing.Table`) — the compiled, priority-sorted
  routes on a node, published to the pipeline supervisor
  (`SetRoutingTable`) by `reloadPipelineFromConfig` whenever routes, vaults
  or placements change.

- **Route match** — `routing.Table.MatchRoute` returns the first matching
  route and its destination vault IDs. Routing knows vaults only, never
  node placement: the routing manager hands the record to each matched
  vault's segmentation queue on the receiving node, which every node
  registers for every route destination.

- **ValidateExpression RPC** — read-only, node-local check that an
  expression parses and uses only supported predicates. Drives the
  route editor's live feedback; uses the same compiler
  (`routing.CompileRoute`) the orchestrator applies when it builds the
  routing table, so the editor's verdict matches what the routing table
  accepts. `PutRoute` itself does not parse the expression.

### Ack semantics

- **Ack-gated** — an ingester that puts an ack channel on the ingest message
  and acknowledges its upstream sender only when that ack resolves: RELP, and
  HTTP requests carrying `X-Wait-Ack: true`. Routing fans the record out to
  every matched vault's segmentation queue on the receiving node and joins
  the per-vault results; each segmentation writer releases its parked acks
  after the group commit that fsyncs the record into the origin segment
  (`SegmentDisableFsync`, a dev/load-testing setting, drops the fsync).
  The ack is nil only once every matched vault has the record durable in an
  origin segment — not once it reaches other homes or a chunk. A failed write,
  an unencodable record, a degraded writer, or a gated destination nacks
  it, and the sender retries. A record that matches no route is acked nil (a
  counted, intentional drop).

- **Fire-and-forget** — every other ingester: no ack channel. The writer
  batches the record and commits on a lazy sync window or at the batch size
  cap; nothing waits for that commit. A degraded writer drops these records
  and counts them.

---

## 3. Query

Reading records back, filtering, aggregating, and rendering them.

### Aggregates

- **Query** — a search specification. Either a structured form (proto
  `Query`: `start`, `end`, `tokens`, `kv_predicates`, `limit`,
  `context_before`, `context_after`) or an `expression` string parsed via the
  query language; reverse order is the `reverse` directive in the
  expression. Both forms compile to the same internal `query.Query`.

- **Pipeline** — a sequence of query-language **operators** applied after the
  initial filter: `stats`, `timechart`, `where`, `eval`, `sort`, `head`,
  `tail`, `slice`, `rename`, `fields`, `dedup`, `raw`, `lookup`, `export`, and
  the chart operators `linechart`, `barchart`, `donut`, `heatmap`, `scatter`,
  `map`. The pipeline turns a stream of records into a `TableResult` or a
  filtered stream.

- **TableResult** — the output of an aggregating pipeline: columns + rows,
  plus a `result_type` distinguishing plain tables from time-series outputs
  (which get charted).

### Values

- **HistogramBucket** — counts for one time window: `timestamp_ms`, `count`,
  optional `group_counts` (for per-severity breakdowns), flags indicating
  whether any records came from cloud storage.

- **HistogramGrid** — the one bucket grid a search's page histogram is counted
  on: start, end and bucket count. The coordinator chooses it from the
  search's window and every node counts its vaults' share on it, so the shares
  sum bucket by bucket.

- **ChunkPlan** — per-chunk execution metadata inside `Explain`: time bounds,
  which indexes were usable, whether the chunk was skipped and why.

- **PipelineStep** — one index or filter step in a `ChunkPlan` (or a
  branch plan): its estimated input and output, and whether it seeks, uses an
  index, filters at runtime, or is skipped, with the reason.

- **QueryPipelineStage** — one pipeline operator in `Explain`'s output, with
  its execution mode: streaming, materializing, short-circuit,
  coordinator-only, or render-hint.

- **ResumeToken** — opaque pagination handle. Continue a search from a prior
  position without re-reading earlier chunks.

- **Lookup table name** — the name a configured lookup is addressed by in
  `| lookup <name> <field>`: the **Name** field of every lookup kind (CSV,
  MMDB, HTTP, static, JSON/YAML file). The built-in `rdns` and `useragent`
  tables are always present under their own names.

### Operations

- **Search** — return matching records, streamed. The canonical read RPC.

- **Histogram** — record counts bucketed by time, returned beside a Search
  as `SearchResponse.histogram` (not a separate RPC). Used for the
  timeline/overview UI.

- **GetContext** — return records surrounding an anchor (one record), ordered
  by `WriteTS`. Used for the "show me 50 records before and after this one"
  investigation flow.

- **Explain** — return the execution plan for a query without running it.
  Used to debug query behavior and to inspect which chunks would be read.

- **GetFields** — return field-name/count/top-values triples for records
  matching an expression. Used by the field-discovery UI.

- **Follow** — stream records as they arrive in real time. A long-lived RPC.

### Cursors and iteration

- **Cursor** — an iterator over records in a chunk. Holds an mmap'd region;
  callers that want records to outlive the cursor must call `Record.Copy()`
  (otherwise the cursor's next call invalidates the bytes).

- **RecordIterator** — a general `func() (Record, error)` iterator used by
  cross-chunk and cross-node paths. Terminates with `chunk.ErrNoMoreRecords`.

- **collectRemote** — `QueryServer.collectRemote` (server package): fans a
  search out to remote vaults and merges the results. Used by Search,
  GetContext, GetFields, ExportToVault and pipeline queries; Explain uses
  `collectRemoteExplain`. Its histogram counterpart, `collectRemoteHistogram`, asks each
  remote vault's leader for its page histogram beside the record stream.

---

## 4. Cluster Coordination

Nodes agreeing on what the cluster believes, via Raft.

### Identity

- **Node** — one running GastroLog process. Identified by a `NodeID` (GLID).
  A node hosts vault instances, runs ingesters, serves queries, and
  participates in Raft groups.

- **NodeConfig** — the declarative (`ID`, `Name`, `State`, `StateSince`)
  record for a node. Lives
  in the cluster-ctl Raft's config. The `Name` is for humans; `ID` is canonical.

- **Peer** — another node, from this node's perspective. "Peer" is always
  relative; the same node is "local" to itself and "peer" to everyone else.

### Raft layers

GastroLog runs **multiple Raft groups** per node, multiplexed over a single
gRPC transport:

- **Cluster-ctl Raft** — one group per cluster. Replicates `system.Config`
  (operator-authored) and `system.Runtime` (cluster-managed). Every node is
  a member: a new node joins as a nonvoter and the learner promoter makes it
  a voter once it has caught up. Leader changes propagate config via FSM apply; dispatcher drives
  downstream effects. Wire surface: `cluster_ctl_raft_index` on
  GetSystem/WatchSystem/SettingsMutationEcho.

- **Vault Control-Plane Raft** (a.k.a. "vault-ctl Raft", "vault-ctl group") —
  one group *per vault*, spanning every cluster node whether or not it homes
  the vault, so every node holds every vault's chunk metadata locally. Uses
  the `vaultraft.FSM`, whose state is a map of vault chunk FSMs keyed by
  vault ID, addressed by `OpVaultChunkFSM` commands. See
  [`vault-control-plane-architecture.md`](./vault-control-plane-architecture.md)
  for the design rationale.

- **Vault chunk FSM** (`vaultctlfsm.FSM`) — the per-vault sub-state-machine
  inside a vault-ctl FSM. Holds the vault's four kinds of manifest — the
  **vault chunk manifest** (each chunk's metadata: state, retention-pending,
  holders), the **vault segment manifest** (each published segment, with its
  holders), the **open chunk manifest** and the **sealed chunk manifests** —
  plus the deletion records: pending deletes, chunk tombstones and
  released-segment tombstones.

- **Manifest** — an authoritative list of facts, kept on the vault-ctl FSM.
  The word marks the list as authoritative; it never names a kind on its
  own. There are four kinds, each named for the facts it holds:
  - **Vault chunk manifest** — the chunks in a vault and their facts: one
    entry per chunk (`ManifestEntry`) with its state, bounds, counts, sealed
    digest, holders and cloud state. The authoritative answer to "which
    chunks should exist in this vault?" Compared against local disk in the
    reconcile sweep; disagreement means orphan cleanup.
  - **Vault segment manifest** — the published segments of a vault and their
    facts: size, checksum, origin, holders and publish time, one entry per
    segment (`CompletedSegmentEntry`; the code still calls it the
    completed-segment registry). A segment enters on publish and leaves on
    release. The FSM keeps two more per-segment facts in maps of their own:
    the next record number to chunk (`segmentResume`) and the chunks that
    hold the segment's records (`segmentChunks`).
  - **Open chunk manifest** — the segment slices in the vault's open chunk
    (`OpenChunkManifest`), which the chunking leader extends until rotation.
  - **Sealed chunk manifest** — the segment slices of a chunk frozen at
    rotation, which homes build from. It waits in the **seal queue** while
    the chunk is Sealing (`sealed_manifests` in the FSM snapshot) and is kept
    after `CmdSealChunk` (`sealed_chunk_manifests`) until one of its segments
    is released or the chunk is deleted. One kind at two stages; the code
    keeps the two stages in separate maps.

  Deletion records are not manifests: a **pending delete**, a **chunk
  tombstone** and a **released-segment tombstone** each exist only while a
  deletion is being made safe, and drain away once every member has acted on
  it.

- **Pending delete** (`vaultctlfsm.PendingDelete`) — a chunk delete
  committed in the vault FSM (`CmdRequestDelete`) and awaiting a
  `CmdAckDelete` from every node in its `ExpectedFrom` set; the vault chunk
  manifest entry goes when the last ack commits (or a node prune — see **Holder** —
  drains the set).
  The chunk has already left the retained set: retention sweeps and the
  age/count bound re-check skip it, while the vault chunk manifest and
  residency keep reporting it until the acks land.

- **Chunk tombstone** (vault-ctl) — the FSM's record that a chunk was
  deleted, kept after its vault chunk manifest entry is gone and tagged with the
  **delete sequence** it was deleted at (`deleteSeq`, counting the applies
  that tombstoned a chunk — never a wall-clock time). A `CreateChunk`,
  `OpenChunkManifest` or `RepatriateChunk` naming one is refused, a
  retention-transfer announce naming one is deferred, and it is the only
  evidence that lets a node delete local bytes the vault chunk manifest no
  longer lists
  (`FSM.IsTombstoned`).

- **Chunk purge confirmation** (vault-ctl) — a node's
  `ConfirmChunkPurgeCommand`: it holds no copy, in its chunk store or chunk
  staging, of any chunk tombstoned at or before the named delete sequence.
  Every node sends one after its chunk tombstone sweep
  (`Orchestrator.sweepChunkTombstones`); the vault-ctl leader's
  `PruneChunkTombstonesCommand` drops the tombstones every member of its Raft
  configuration has confirmed, and the confirmations of nodes no longer in
  it. Same rule as the released-segment **purge confirmation**.

### Raft primitives (hashicorp/raft vocabulary)

- **Term** — logical clock; increments on every election.
- **Log** — ordered sequence of entries. Entry types: `LogCommand` (goes
  through FSM.Apply), `LogConfiguration` (membership change), `LogNoop`
  (post-election commit), `LogBarrier`.
- **Commit index** — highest log index a quorum has ack'd; entries up to
  this are durable.
- **Applied index** — highest index this node's FSM has processed. Always
  `≤ commit index`. Vault readiness reads the vault-ctl FSM's own applied
  count, not Raft's applied index (which advances before the FSM has run):
  `vaultraft.FSM.Ready`, wired as the `isFSMReady` callback in
  [`reconfig_vaults.go`](../backend/internal/orchestrator/reconfig_vaults.go).
- **Leader / Follower / Candidate** — Raft roles. Use "leader" and "follower"
  consistently; never "primary/secondary".
- **Voter / Non-voter** — membership state. Voters participate in elections;
  non-voters just catch up.
- **Initial leader election** — a Raft group's elections before this node
  has seen it have a leader: on group creation (one per new vault-ctl group)
  or node start. Not churn; the node's election counters skip it.
- **Election storm** — three or more elections within the last minute in
  groups that already had a leader. A diagnostic event (transition-edge log
  + `NodeStats` rate), not an alarm.
- **Snapshot** — FSM state serialized to disk; lets log be truncated.
- **InstallSnapshot** — RPC for streaming a snapshot to a slow follower.

### Shared WAL (`raftwal`)

The write-ahead log every Raft group's log and stable state shares on a node:
one segmented file set, one batch-writer goroutine.
[`backend/internal/raftwal`](../backend/internal/raftwal).

- **Reclamation** — returning dead space in the shared WAL by removing
  drained segments, oldest sealed segment first. Runs on the batch writer,
  strictly after batch waiters are notified, so no caller ever waits on it.
- **Scavenge** — the reclamation path for a nearly-drained oldest segment:
  its live records are re-appended through the write path onto the active
  segment, fsynced, and the index repointed at the copies, before the
  original is unlinked.
- **Drained** — a WAL segment with zero live payload bytes: the in-memory
  index holds no reference into it, so unlinking it loses no live state.
- **Group drop** — the WAL record that decommissions a Raft group
  (`DropGroup`): it releases the group's live bytes and masks everything
  earlier for the group on replay. The drop record is a tombstone that
  evaporates when its own segment becomes the oldest — by then nothing it
  masks can replay. Dropping is for a group deleted cluster-wide;
  a group merely stopping on a node (vault reassignment) keeps its WAL
  state and recovers it when it restarts.

### Placement & membership

- **VaultPlacement** — covered under [Storage](#1-storage); also a cluster
  concept because the placement manager (in cluster-ctl Raft) decides which
  nodes host each vault.

- **Ingester placement** — the singleton-ingester assignment map in
  `system.Runtime`. The placement manager picks an alive node per singleton
  ingester; failover is automatic.

- **Node state** — the lifecycle state on `NodeConfig`, replicated in
  cluster-ctl (`system.NodeState`); the node card shows a state badge
  whenever the node is not simply live.
  - **Live** — a member in Raft contact; the default (the zero value,
    `NodeStateUnknown`, reads as live).
  - **Unreachable** — the unreachable sweep on the cluster-ctl leader found
    the node's last-seen older than the unreachable threshold (five minutes
    by default). Its placements wait for it; a warning alarm raises once it
    stays unreachable past a second threshold. Fresh contact clears it back
    to live on the broadcast fabric, not on a tick.
  - **Maintenance** — an operator declared the node absent; the cluster
    expects it back and keeps its placements.
  Legal transitions: live → unreachable | maintenance; unreachable → live |
  maintenance; maintenance → live. A node leaves the cluster by removal
  (`cluster remove-node`, which deletes its NodeConfig), not by a state.

  **Offline** is not a node state. It is the stats surface's verdict on a
  node whose broadcast is older than the stats TTL, shown with how long it
  has been silent. A node can read offline on screen while still live in
  the FSM, and the two converge only through the sweep above.

- **Placement manager** — the subsystem that owns placement decisions. Runs
  on the cluster-ctl Raft leader, reacts to node join/leave, vault create/delete,
  and ingester changes. Only a live node takes a placement it does not already
  hold; a node in any other state keeps what it holds, and so does a live node
  whose heartbeat has lapsed.

- **Working replica** — a placement member that counts toward the vault's
  replication factor: its node is live and its storage for the vault is not
  under disk protect. A **degraded member** — on an unreachable or
  maintenance node, or under disk protect — is retained but
  not counted, and the placement manager backfills a follower on a live node
  until it recovers.

- **Removal gate** — a leader-side check that runs before a node removal
  commits, refusing removals that would cost data or redundancy. Two gates
  exist, evaluated in severity order and bypassed together by `--force`:
  the **orphan-refusal gate** (removal would leave a vault with zero
  placements — data loss) and the **RF-preservation gate** (removal would
  leave a vault below its replication factor with too few eligible Live
  nodes to re-place onto — reduced redundancy). Refusals name the affected
  vaults; the RF gate wraps the `ErrWouldDropBelowRF` sentinel.
  [`backend/internal/app/cluster.go`](../backend/internal/app/cluster.go).

- **Removal record** — the replicated set of node IDs the cluster removed
  (`system.Runtime.RemovedNodes`, written by `MarkNodeRemoved` before the
  Raft configuration change). Permanent: enrolment and membership requests
  naming a recorded ID are refused, so a removed node's key cannot bring its
  identity back. The machine returns as a new node, under a new node ID.
  See [`docs/cluster_enrollment.md`](cluster_enrollment.md).

- **Removed node** — a node the cluster removed, as it sees itself. It
  learns of its **node removal** from the leader, or, when it was down at
  the time, from a member's **removed-node refusal** (a `PermissionDenied`
  carrying `RemovedNodeRefusal`): a claim, counted only from a current
  member of its configuration that is not itself removed and never as the
  answer to an eviction notice, and acted on only once a majority of the
  other voters have made it. The eviction notice counts only from the
  leader it follows; a join refusal never counts. It shuts its member side down, records the
  removal (time and reason) beside its node identity in its home's
  cluster-ctl stable store, and from then on — in that process and on every
  start of the same home — serves only the account of its removal: one
  error-level log line with the remedy and `GetClusterStatusResponse.removal`
  on its Unix socket; its TCP listener shows only the probes and a bare
  "removed from cluster". It never rejoins, mints a new identity, or runs as
  a cluster of its own; the remedy is an empty home and emptied vault
  storage. A home holding the cluster certificate but no node identity is an
  **incomplete home** and is refused at start.

- **Admission** — the leader's answer to a membership request. The request
  carries no suffrage: a node outside the configuration enters as a nonvoter
  (learner), a member is re-added at its current suffrage, and a node in the
  removal record is refused. Votes come only from the learner promoter or an
  operator's promotion.

- **Removal policy** — who asked for a node removal, which decides the
  RF-preservation gate's stance. **Operator** (`cluster remove-node`) is
  pessimistic: refuse and let the operator choose a remedy. **Self**
  (`cluster demote-self`, a node removing itself as it terminates) is
  optimistic: allow, because the node is terminating regardless and placement reconcile re-places.
  Carried across the follower → leader forward hop so the stance does not
  depend on which node the caller reached.

- **Dispatcher** — the subsystem that reacts to *applied* config changes
  (from cluster-ctl Raft FSM) and drives their side effects into the local
  orchestrator (register vault, build vault instance, start ingester, etc.).

### Peer liveness

- **Raft contact** — positive evidence that a peer node was alive: an outbound
  Raft RPC to it came back, or an inbound Raft RPC from it was handled, on any
  Raft group the two nodes share. Aggregated **any-group, most-recent-wins**;
  silence in one group is never evidence of anything, because it usually just
  means no Raft edge exists there.

- **Raft probe** — an *attempt* at an outbound Raft RPC to a peer, whether or
  not it succeeded. Distinct from contact and recorded separately: only while
  this node is probing a peer does the absence of contact mean the peer is
  gone. Followers exchange Raft traffic only with their leader, so two
  co-followers have no Raft edge and their mutual silence carries no
  information.

- **Peer liveness** — the short-window verdict "is this peer reachable right
  now" (`PeerState.IsLive` / `LivePeers`). Fresh Raft contact wins; otherwise a
  fresh probe with no contact is an authoritative *not live*; otherwise there
  is no Raft edge and the verdict falls back to stats-broadcast freshness.

- **Last-seen** — the long-horizon `PeerState.LastSeen`: the newest moment of
  *any* positive evidence, Raft contact or broadcast arrival. Never suppressed
  by a failing probe. The unreachable sweep and the stale-voter reaper measure
  their multi-minute thresholds against this, not against peer liveness.

- **Stats TTL** — how long a received stats broadcast stays queryable on the
  node that received it (`PeerState.StatsTTL`, a multiple of the broadcast
  interval). Past it, `Get` returns nil and the node reads as offline. Carried
  to the UI as `GetClusterStatusResponse.stats_ttl` together with each
  `ClusterNode.stats_received_at`, so every stats surface dates its figures
  and marks a sample older than the TTL as **stale**. The responding node
  reports its own stats as last collected for broadcast, dated by that
  collection, and the system status stream sends the last snapshot built off
  the request path — rebuilt on each stats update only while someone
  subscribes — so neither waits on the subsystems a collection reads.

- **Responding node** — the node that answered a given RPC. The API has no
  session affinity, so behind a load balancer each request may be answered by
  a different node; the UI labels a node "this node" only while every
  response in the session has come from the same one.

### Request routing

How an operator-issued RPC finds the node that should execute it. See
[`cluster_routing.md`](./cluster_routing.md).

- **Routing strategy** — the per-procedure classification in
  `server/routing/routes.go`: `RouteLocal`, `RouteLeader`, `RouteToResourceOwner`,
  `RouteFanOut`. Every generated procedure must be classified.

- **Resource owner** — the node (or nodes) that own a named resource and
  must therefore execute imperative actions on it: the **vault leader** for
  a vault, the nodes **running** an ingester for an ingester. Distinct from
  the **Raft leader**, which owns config mutations. Actions like seal,
  reindex, and trigger go to the resource owner directly — they are not
  config mutations and must not be tunnelled through Raft to reach a node.

- **Owner resolver** — the per-`ResourceKind` implementation
  (`routing.OwnerResolver`) that answers "which node(s) own this resource",
  reading only replicated cluster state so every node answers identically.
  Returns a deterministically-ordered set, because ownership is plural for
  parallel ingesters.

### Transport

- **Multiraft transport** — gRPC service that multiplexes AppendEntries,
  RequestVote, InstallSnapshot, etc. across all groups, each group on its own
  PeerConns Raft lane. [`backend/internal/multiraft/transport.go`](../backend/internal/multiraft/transport.go).

- **PeerConns** (`cluster.PeerConnManager`) — the node's cluster-port
  connection manager: a pooled service lane per peer for cross-node RPCs
  (forward-apply, search fan-out, segment and GLCB pulls, etc.) and one lane
  per Raft group, which the multiraft transport uses.

- **Lane** — one connection between two nodes as the inspector's Network
  panel lists it, both directions merged (`MergedPeerLane`; the lane kind is
  `cluster.ConnLane`, `raft` or `service`). A **raft** lane carries one Raft
  group, and the detail names it; an **rpc pool** lane is one slot of the
  service pool, numbered by its `PoolIndex`.

- **Cluster-port purpose** — the label a subsystem gives when it acquires a
  pooled connection (`AcquireOpts.Purpose`); a lane lists the purposes it
  has carried, dimming any idle during the last stats interval. Canonical
  order and meaning (`cluster.AllPurposes`): **search** (search fan-out),
  **chunk apply** and **vault apply** (a follower forwarding a vault-ctl
  chunk or vault command to that group's leader), **broadcast** (stats
  push), **forward** (a cluster-ctl config command forwarded to its leader,
  `cluster.Forwarder`), **forward RPC** (a client RPC the routing
  interceptor forwards to another node, `cluster.ForwardRPC`),
  **eviction** (the leader's notice to a node it removed), **remove node**,
  **suffrage** (voter/nonvoter changes), **chunk transfer** (streaming a
  chunk to a new home), **segment pull** (pipeline segment bytes and GLCB catch-up pulls),
  **file transfer** (managed files), **raft**.

---

## 5. Orchestration

Each node has one **Orchestrator**. It is the top-level glue: holds vault
registry, dispatches jobs, manages lifecycle, coordinates retention and
rotation, and serves as the in-process API that RPC handlers delegate to.

### Aggregates

- **Orchestrator** — `*orchestrator.Orchestrator`. Owns:
  - `o.vaults map[glid.GLID]*Vault` — local vault registry (protected by
    `o.mu`, a `locktrack.RWMutex`).
  - `o.scheduler` — the job queue + cron runner.
  - `o.groupMgr` — handle to the `raftgroup.GroupManager`.
  - `o.pipeline` — the ingest pipeline supervisor; holds the routing table.
  - `o.segmentPuller`, `o.chunkGLCBPuller`, `o.transferrer`,
    `o.peerConns` — cross-node I/O.

- **Factories** — the bundle passed to `Orchestrator.ApplyConfig` that
  contains component constructors (chunk manager factory, index manager
  factory, ingester registrations), the home and vaults directories, the
  logger, and cluster wiring (GroupManager, NodeAddressResolver, PeerConns).

### Lifecycle

- **`orch.Start(ctx)`** — start the scheduler, prime the disk guard, hand
  the ingesters to the pipeline supervisor and start the pipeline (whose
  ingestion manager launches them), then the auxiliary loops (pressure gate,
  progress emitters, readiness refresher). Returns when startup completes.

- **`orch.Stop()`** — cancel all background goroutines, wait for in-flight
  writes, close chunk managers. Called via `t.Cleanup` in tests.

- **Shutting down (`o.phase`)** — a `lifecycle.Phase` atomic flag. When set,
  retention leaves chunks untouched (sweep, route fan-out, transfer) and the
  vault-ctl `Announcer` skips its applies. Used to suppress benign errors
  during shutdown.

- **Vault readiness** — a vault on this node is "ready" iff it has a local
  instance AND that instance's `IsFSMReady()` callback returns `true` (i.e.
  the vault-ctl group's FSM has applied at least one entry — a command or a
  configuration — on this node, or restored from a snapshot). An instance whose vault-ctl group creation is
  still deferred on this node is not ready; on a node without vault-ctl
  Raft the callback is nil and the instance is ready. Canonical definition in
  [`vault_readiness.go`](../backend/internal/orchestrator/vault_readiness.go).
  Checked by every read and write path before touching vault state.

- **Not-ready answer** — how a node answers a forwarded query for a vault
  that is not ready there: an `Unavailable` status carrying the
  `VAULT_NOT_READY` `ErrorInfo` reason. On the calling node it matches
  `ErrVaultNotReady` (the orchestrator's sentinel is the cluster package's),
  so a coordinator handles a vault not ready on another node exactly as one
  not ready on its own: a search, its histogram, a pipeline and Explain
  leave it out. Distinct from a peer with no executor wired, which also
  answers `Unavailable` but without the reason, and from a transport
  failure; both of those still fail the query. See
  [`vault_not_ready.go`](../backend/internal/cluster/vault_not_ready.go).

### Scheduler & Jobs

- **Scheduler** — the cron/queue subsystem. Runs scheduled jobs
  (retention sweep, archival sweep, disk guard, per-vault cron rotation,
  catch-up and reconcile sweeps) and one-shot tasks (post-seal indexing,
  cloud upload, GLCB catch-up pulls).

- **Job** — a scheduled or one-shot unit of work. Proto
  [`Job`](../backend/api/proto/gastrolog/v1/job.proto) tracks
  status (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`) and kind
  (`TASK`, `SCHEDULED`).

### Policies

- **Rotation policy** (`RotationPolicyConfig`) — when to seal the active
  chunk. Shapes: `MaxSize`, `MaxAge`, `MaxRecords`, `Cron`, or a
  composite. Per-vault: `vault.RotationPolicyID` points at a policy.

- **Retention rule** (`RetentionRule`) — per-vault, per-policy: "when do
  sealed chunks fire retention events". A fired event destroys the chunk,
  optionally streaming its records through the routing engine first
  depending on the vault's retention disposition (see below).

- **Retention policy** (`RetentionPolicyConfig`) — named, reusable
  policy referenced by `RetentionRule`.

- **Refuse** (`RetentionPolicyConfig.Refuse`) — one boolean per retention
  policy, extending `MaxSize`'s refuse behavior to every bound the policy
  states (`MaxAge`, `MaxSize`, `MaxChunks`). Default off: unset reads as
  false, so a policy opts in explicitly, and a policy without the flag only
  drains, even one that sets `MaxSize`. Every set parameter on a
  `refuse=true` policy is a **hard bound**: drain restores it, and refusal
  guards it while violated. `refuse=false` (unset or explicit) makes every
  set parameter a **soft bound**: drain still restores it, but only the
  node-level disk guard's own floor/warn bands backstop the vault while
  violated. One flag per policy, not one per parameter.
  - **Min-per-kind resolution**: a vault's effective bound, per KIND
    (age/size/count), is the min over every attached policy that states
    that kind. Refuse-eligibility follows the STATING policy's own flag —
    a vault mixing a hard and a soft policy refuses only on the hard
    one's bounds, even if the soft one's bound is the tighter (and
    therefore drain-triggering) one.
  - **Violation predicate for age/chunks**: normal operation transiently
    violates both between a chunk's seal and the next retention sweep, so a
    violation counts as refusal-worthy only once the retention runner has
    SWEPT AND FAILED TO CLEAR it (`retentionRunner.checkBoundViolations`,
    called at every sweep exit against a fresh post-sweep chunk listing that
    skips pending deletes, so a delete the sweep committed counts as cleared
    while nodes still ack it) — clock-free, no streak, no slack duration:
    the sweep's own outcome, re-observed once per sweep, IS the predicate.
    `MaxSize`'s refuse check is instantaneous (measured every disk-guard
    tick) — it is resource-backed (disk fills regardless of sweep cadence),
    unlike age/count, which are purely retention-policy-derived.
  - Surfaced as `VaultAdmissionCause` `MAX_SIZE_BOUND` / `AGE_BOUND` /
    `CHUNK_COUNT_BOUND` and the `vault-bound-capped` alarm — see
    docs/alarm-management-design.md.

- **Max size (retention policy)** (`RetentionPolicyConfig.MaxSize`) — the
  vault's disk-claim bound, carried on a retention policy. It means both
  things at once:
  - **Drain** (evaluated by `SizeRetentionPolicy`): drains the oldest sealed
    chunks, per the vault's disposition, once the vault's disk claim exceeds
    the bound. Scope: the chunk store retention can act on.
  - **Refuse** (evaluated by the disk guard's Admission gate; the
    size-specific, instantaneous case of `Refuse` above): refuses ingest
    admission for the vault once its whole local footprint (chunk store +
    pipeline segment backlog) exceeds the same bound, while the stating
    policy's `Refuse` is true. Scope: everything the vault holds on this
    node. The backstop while drain catches up or is deferred.

  Effective per-vault REFUSE bound = min over the refuse-eligible
  (`Refuse` on) attached policies' `MaxSize`, and NO bound when none states
  one: there is no per-vault default, and a vault with no refusing size
  policy is backstopped by the node-level guard alone. The DRAIN trigger
  mins over ALL stating policies regardless of the flag. Feeds
  `refreshVaultDiskGuards` → `orchestrator.resolveVaultSizeBound` → the disk
  guard's Admission gate. A policy that sets only `MaxSize` is a drain
  policy that also bounds the vault.

  Measures the **disk claim** (`chunk.DiskClaim`): `DiskBytes` when
  recorded (also what a cached cloud-backed chunk's cache file reports), 0
  for a cloud-backed chunk with no local copy (an evicted chunk's
  destruction frees nothing locally, so drain never selects one), otherwise
  logical `Bytes` plus index sizes. One field, one measurement, one bound:
  drain and refuse measure the same quantity.

  **`DiskBytes` vs `CloudBytes`** (`ChunkMeta`) — two
  distinct currencies that must never substitute for each other. `DiskBytes`
  is always the LOCAL on-disk footprint on the responding node: for a
  cloud-backed chunk this is its cached copy's size (0 once evicted), live and
  per-node — it changes on upload, eviction, and the next download. `CloudBytes` is the compressed cloud object's
  transport size: cluster-wide, fixed at upload time, unaffected by any
  node's local cache turnover. `chunk.DiskClaim` and every consumer that
  measures against the disk-claim bound above reads `DiskBytes` only, never
  `CloudBytes` — a chunk evicted from this node's cache must claim 0 here
  even though its `CloudBytes` is unchanged and its object is still in the
  cloud store.

- **Retention event** — the cluster-visible signal that a chunk has aged
  out. Fires unconditionally on policy match; the vault's retention
  disposition decides whether the records are forwarded through the
  routing engine, transferred to another vault, or dropped before the
  chunk's local copy is destroyed.

- **Retention reason** (`chunk.RetentionReason`) — the trigger that
  selected a chunk for its retention event, overlaid as the `_reason`
  synthetic attribute on every record a `route` disposition streams through
  the routing engine. Values: `age` (TTL / `MaxAge`), `size` (`MaxSize`
  drain), `count` (`MaxChunks`), `memory-budget` (a memory vault over its
  memory budget). A chunk several triggers select — within one policy or
  across the vault's attached policies — carries one reason, the first in
  that order (`chunk.PrecedingRetentionReason`), so a route on
  `_reason = "age"` receives every chunk that aged out.

- **Retention disposition** (`VaultConfig.RetentionDisposition`) — per-vault flag
  controlling what happens to records when a retention event fires.
  Three canonical values:
  - **`delete`** (default): records drop, storage frees, the routing
    engine is never invoked. The safe default — no risk of accidental
    cascades. `transfer` cannot become the zero-config default: it
    requires a target vault, and a default must be a value the operator
    could type into the field.
  - **`route`**: records flow through the routing engine with synthetic
    `_source = "retention"`, `_vault = "<id>"` and `_reason` (the retention
    reason), so operator-configured
    routes can forward them to archive vaults, other storage, etc.
  - **`transfer`** (`VaultConfig.RetentionTransferTargetVaultID`): the sealed chunk is re-homed to the target vault
    UNCHANGED — no record decode, no re-route, no re-ingest. Where
    `route` filters/re-tags/fans out and `delete` drops, `transfer`
    moves; it is the recommended primary pattern for archive vaults
    (route stays the tool for filtering/re-tagging).
    Mechanism: the destination's homes pull the sealed GLCB via the
    same verify-before-promote replica catch-up machinery same-vault
    replication uses (`glcb_catchup.go`), addressed at the source vault
    via `ManifestEntry.TransferSourceVaultID`; destination holder
    receipts must reach the destination's replication factor before the
    source expires its local copy (`AckChunkHolderCommand` receipts —
    no loss window, nothing marked on the source until the
    destination confirms). The destination-side retention clock starts
    FRESH at arrival (`SealedAt` stamped on landing via the reused
    `CmdRepatriateChunk` announce-import), so a shorter destination TTL
    does not re-fire retention the moment the chunk lands; the chunk's
    own record timestamps and identity are untouched. File or memory
    vaults that are not cloud-backed, as source and target
    (`VaultConfig.TransferEligible`); self-transfer and transfer cycles
    are rejected at `PutVault`. A stalled
    or deferred transfer retains the chunk with the one-shot
    unconsumed, same as a stalled route fan-out. See
    docs/retention-transfer-disposition-design.md.

  Empty/unrecognized values resolve to `delete` via
  `VaultConfig.ResolveRetentionDisposition()`.

  Note: do not confuse the drain half of max size with the unrelated
  **Drain gate** below (disk-guard backpressure — a different concept that
  happens to share the word "drain").

### Core state transitions (verbs)

- **Seal** — finalize an active chunk. The vault-ctl leader's planner seals
  the open chunk manifest when the rotation policy fires; every home builds the
  chunk's GLCB (timestamp indexes embedded) from its local segments; the
  leader commits `CmdSealChunk`, carrying its build's sealed digest, after its
  own build; and each home whose copy carries the sealed digest schedules its
  own secondary-index build and commits a holder receipt. See
  [States a chunk passes through](#states-a-chunk-passes-through).

- **Rotate** — seal the open chunk manifest and let the planner open the
  next one. Sealed chunk manifests queue FIFO on the vault-ctl FSM, so rotation does not wait
  for earlier chunks to finish building.

- **Expire** — destroy a chunk that has aged out according to retention.
  With `disposition = delete` (default), records drop and the chunk is
  destroyed. With `disposition = route`, the records are first streamed
  through the routing engine (synthetic `_source = "retention"`), then
  the chunk is destroyed. With `disposition = transfer`, the chunk is
  destroyed on the source once the target vault holds it. A
  retention-trigger route directing records to another vault is how
  retention routing chains are expressed.

- **Disk guard** — the per-node free-space guard job (`disk_guard.go`):
  samples the node's data volumes every 15s and drives two staged gates
  plus the per-vault caps (max-size budget, backlog budget) and a
  per-storage evaluation (`evaluateStorages`) — one statfs/warn-floor
  verdict per locally-hosted `FileStorage`, regardless of how many vaults
  share it; the floor belongs to the storage, not to a vault.

- **Admission gate** (`protect`) — the disk guard's outer gate: suspends
  ingest admission and catch-up pulls. Engages below the free-space
  floor, releases only above the warn band (asymmetric deadband so the
  release burst cannot re-cross the floor).

- **Drain gate** (`deferWrites`) — the disk guard's inner gate: pauses
  chunking builds, collection pulls, and retention route fan-out and
  transfer.
  Engages below the floor, releases just above it — before the
  admission gate, so the paths that free space run while admission is
  still suspended.

- **Retention deferral** — a sweep whose configured non-delete
  disposition (route fan-out or transfer) could not run (drain gate
  engaged, destination/target vault gated, receipts stalled, etc.); the
  chunk is retained for a later sweep. Consecutive deferrals raise the
  `retention-deferred` alarm.

- **Transfer (retention disposition)** — a third `RetentionDisposition`
  value alongside `delete` and `route`: when a retention event fires,
  the sealed chunk is re-homed to `RetentionTransferTargetVaultID`
  UNCHANGED — no record decode, no re-route, no re-ingest. The
  destination's homes pull the sealed GLCB (the same verify-before-
  promote replica catch-up machinery same-vault replication uses,
  addressed at the source vault); destination holder receipts must
  reach the destination's replication factor before the source expires
  its local copy — no loss window. The destination-side retention clock
  starts fresh at arrival (`SealedAt` stamped on landing), so a shorter
  destination TTL does not re-fire retention the moment the chunk
  lands. File and memory vaults that are not cloud-backed, as source
  and target; self-transfer and transfer cycles are rejected at
  `PutVault`. See docs/retention-transfer-disposition-design.md.

- **Reconcile** — compare the vault chunk manifest against local disk:
  delete local chunks it no longer lists only on positive evidence (a chunk
  tombstone, or a ghost: sealed, zero records, sealed more than five minutes
  ago), keep and alert on an **unknown orphan** (a sealed chunk with
  records the vault chunk manifest does not list), pull the GLCB of any
  sealed chunk the vault chunk manifest lists that is missing locally
  (**GLCB catch-up**), and earn or revoke this node's chunk holder receipts
  to match what is on disk.

- **Cloud-index audit** — compare a cloud-backed vault's **objects in the
  blob store** against the vault chunk manifest (which chunks the cluster expects)
  and against the node's **cloud index** (its local cache of that). Read-only.
  Six findings, and they call for different responses: a *missing object* is a
  durability incident, a *size mismatch* likewise, an *untracked object* is
  bytes being paid for that nothing reads, a *tombstoned object* is a delete
  still propagating, and *stale / unindexed cloud-index entries* are node-local
  cache drift. Surfaced by `ValidateVault` (CLI: `gastrolog validate`).
  Distinct from **Reconcile** above, which compares the vault chunk manifest
  with local disk.

- **Cloud-index rebuild** — repair the node's cloud index from the blob store:
  drop cached entries whose object is gone, reset drifted sizes, index objects
  the cache never recorded. Repairs a **cache** only — it never deletes an
  object and never changes cluster state, because an object the cache cannot
  explain is the case where the cache is what is wrong. CLI:
  `gastrolog reconcile`. A missing object stays reported as missing afterwards;
  removing a chunk the store lost is a separate, grace-period-gated decision
  owned by the nightly cloud-reconcile sweep.

- **Vault deletion** — the operator removing a vault from the config; every
  node then deletes its share (`Orchestrator.DeleteVault`, from the
  vault-deleted notification), whether or not the vault is registered there.
  The delete drops the vault from the node's registries and returns; a
  background **vault teardown** — a `vault-teardown:` scheduler job, one at a
  time per vault, abandoned at shutdown — then decommissions its vault-ctl
  group (WAL state and directory), stops its pipeline, closes the vault, and
  removes the vault's directories. A vault re-created under the same ID waits
  for a pending teardown, which would otherwise remove its files. The
  directories removed are each
  **pipeline root** candidate and each chunk-manager directory, all named by
  the vault's ID. **Delete data** decides the files holding records — staging
  segments, sealed chunks and their indexes, a cloud-backed vault's cloud
  objects: with it they go — the cloud objects by key prefix, after the
  vault's uploads are stopped and the in-flight ones have returned, so none
  lands afterwards; without it they stay, unreachable, and only the
  chunk-manager bookkeeping (lock file, cloud index) and the directories left
  empty go. Config read before a deletion still names the vault: whatever
  applies config under the orchestrator lock (routing reload, the pipeline
  config reconcile, vault add, startup `ApplyConfig`) re-reads it when a
  deletion was applied since the read, so the vault is not registered again.
  A vault's pipeline root is created only by its registration, so a
  write in flight when the root is removed fails rather than recreating it
  (`paths.MkdirBelow`).

- **GLCB catch-up** — a home missing a sealed chunk's GLCB (it missed the
  build and the source segments are released) pulls the blob from a peer
  home, verifies its record count against the vault chunk manifest entry and its bytes
  against the sealed digest, renames it into place and commits a holder
  receipt (`glcb_catchup.go`,
  `ChunkGLCBPuller`). Each pull is a one-shot scheduler job, scheduled by
  the reconciler's missing-replica sweep and when a landed copy fails the
  sealed-digest check. The same pull, addressed at the
  source vault, is the destination half of a retention transfer.

---

## 6. Replication & Forwarding

Cross-node data movement. Keep these apart:

- **Vault-ctl Raft replication** — chunk metadata (create/seal/delete/upload
  events), the vault segment manifest, the open and sealed chunk manifests, and
  segment and chunk holder receipts. Flows through hraft via the multiraft
  transport. Committed only when a majority acks. This is the
  **authoritative** metadata replication.

- **Segment distribution** — record bytes between homes. The origin
  publishes a completed segment's metadata to vault-ctl; each home's
  Collection pulls the bytes from a holder into `pre-head/`, verifies and
  promotes them to `head/`, and commits a segment holder receipt. Bytes move
  only by pull. See [Pipeline](#9-pipeline).

- **Chunk replication** — every home builds its own GLCB from a sealed
  chunk manifest and its local segments, so no chunk is pushed. A home that
  missed the build pulls the GLCB from a peer home (**GLCB catch-up**).
  Chunk holder receipts, not pushes, are what count toward the replication
  factor.

- **Replica count** — how many nodes hold a chunk: the size of its holder
  set on the vault-ctl FSM (`ChunkResidency`; while a delete is in flight,
  the nodes that still owe a delete ack). Surfaced on
  `ChunkMeta.ReplicaCount`; falls back to the number of nodes that reported
  the chunk only when no vault-ctl FSM knows it.

There is no record forwarding: a record that matches a vault becomes a
segment on the node that received it (its origin), whether or not that node
is one of the vault's homes.

### The actors

- **SegmentPuller** — streams a completed segment's bytes from a peer for
  Collection. [`cluster/segment_puller.go`](../backend/internal/cluster/segment_puller.go).

- **ChunkGLCBPuller** — streams a sealed chunk's GLCB from a peer home for
  GLCB catch-up. [`cluster/chunk_glcb_puller.go`](../backend/internal/cluster/chunk_glcb_puller.go).

- **Forwarder** — synchronous forwarder of pre-marshaled cluster-ctl config
  commands to the cluster-ctl leader (`ForwardApply`), used by raftstore on
  follower nodes.
  [`cluster/forwarder.go`](../backend/internal/cluster/forwarder.go).

- **VaultApplyForwarder** — forwards vault-ctl Raft applies from a
  follower node to the current vault-ctl leader. Used when `PeerConns` is
  wired. [`cluster/vault_apply_forwarder.go`](../backend/internal/cluster/vault_apply_forwarder.go).

- **VaultCtlChunkApplyForwarder** — forwards a chunk-FSM command (wrapped in
  `OpVaultChunkFSM`) to the vault-ctl leader. Same shape as the vault
  forwarder, different wrapping.
  [`cluster/vault_ctl_chunk_apply_forwarder.go`](../backend/internal/cluster/vault_ctl_chunk_apply_forwarder.go).

- **Apply-wait barrier** — the read-after-write guarantee on every
  forwarded Raft apply (cluster-ctl and vault-ctl alike): the leader
  returns the log index it applied, and the forwarding node blocks until
  its own local FSM has applied that index before returning. Event-driven
  — the FSM feeds an `applywait.Tracker` from its Apply path (and from
  snapshot restore), waking waiters the moment the mutation is locally
  visible; never a poll. [`applywait/applywait.go`](../backend/internal/applywait/applywait.go).

  The same barrier serves reads, via `Store.Barrier`: a miss on replicated
  state is not proof of absence, because the node serving the request may
  not have applied the write that created the entity. Config
  read-modify-write confirms a miss behind the barrier before acting on
  it — otherwise a credential merge stores an empty secret and a
  duplicate-name check hands out a name the cluster already gave away.

### Connection management

- **PeerConns** — shared gRPC connection pool: up to
  `DefaultServicePoolMaxPerPeer` service connections per peer node, reused by
  all callers (Broadcaster, SearchForwarder, SegmentPuller, ChunkGLCBPuller,
  the apply forwarders). `Invalidate(nodeID, err)` drops a
  stuck connection so the next call re-dials.

- **Multiraft transport** (`multiraft.Transport`) — per-node multiplexing
  transport for Raft RPCs (`AppendEntries`, `RequestVote`, etc.), carried on
  PeerConns' per-group Raft lanes; application RPCs use PeerConns' service
  pool.

---

## 7. Observability

How the cluster reports what it's doing to itself, to operators, and to the UI.

- **Broadcaster** — per-node push mechanism for peer-to-peer state: stats,
  jobs, alerts. `Send()` delivers to this node's own subscribers
  synchronously, then fans out to peers **fire-and-forget**: per-peer
  goroutines with their own timeouts do the work. A slow or paused peer
  cannot stall the caller. [`cluster/broadcaster.go`](../backend/internal/cluster/broadcaster.go).

- **BroadcastMessage** — typed envelope: `sender_id`, `timestamp`, one-of
  payload (`NodeStats`, `NodeJobs`, ...). Dispatched via the cluster's
  `Subscribe`/broadcast mux.

- **StatsCollector** — per-node ticker (default 5s). Each tick:
  collects local metrics (CPU, memory, Raft state, ingest queue depth,
  per-vault stats, alerts) and pushes them to peers via Broadcaster.
  [`cluster/statscollector.go`](../backend/internal/cluster/statscollector.go).

- **NodeStats** — the proto carrying a single node's snapshot:
  process metrics, Raft metrics, ingest queue, per-vault stats, per-peer
  byte counts, active alerts. Consumed by peers (via Broadcast) and by
  the local inspector UI.

- **PeerState** — per-node cache of the most recent `NodeStats` from each
  peer. Its stats TTL (3× the broadcast interval, 15s by default;
  `GLOG_PEER_TTL_MULTIPLIER`) bounds how long cached stats stay queryable;
  peer liveness comes from Raft contact first and falls back to the stats
  TTL (see [Peer liveness](#peer-liveness)).
  [`cluster/peerstate.go`](../backend/internal/cluster/peerstate.go).

- **PeerJobState** — parallel cache for `NodeJobs` broadcasts; aggregates
  active jobs from all peers.
  [`cluster/peerjobstate.go`](../backend/internal/cluster/peerjobstate.go).

- **PeerByteMetrics** — cumulative wire bytes sent/received to each peer
  (monotonic counters; rates are left to consumers). The inspector's
  per-peer rates and sparklines come from the stats collector's sampling of
  PeerConns lanes, not from these counters.

- **RouteStats** / **VaultRouteStats** / **PerRouteStats** — routing
  counters: global `Routed` (records that entered routing), `Matched`
  (matched a route and were fanned out), `Unmatched` (no route matched;
  intentional, counted drop), plus per-vault and per-route `Matched`.
  `Routed = Matched + Unmatched`. Surfaced in `NodeStats` and aggregated
  cluster-wide. Delivery drops are a distinct quantity: the routing
  manager's `PerVaultDropped` counts records *already counted as Matched*
  whose fan-out delivery to one vault's segmentation queue failed (sink
  revoked mid-flight, or shutdown). They are a per-vault sub-account of
  `Matched`, never part of the `Routed = Matched + Unmatched` sum.

- **Peer fan-out** — the unary inspector-RPC pattern where the connected
  node queries every peer that hosts a vault concurrently, each under a
  per-peer timeout, and merges the answers (`server/peer_fanout.go`). A
  peer that times out or errors is elided from the merge instead of
  blocking the handler — the inspector stays responsive on a degraded
  cluster. Distinct from the Broadcaster (push, steady-state stats) — a
  fan-out is a pull, on demand, for one request.

- **Contribution report** (`ContributionReport`) — the record of which
  peers failed to answer a peer fan-out, attached to the merged response
  so a partial result reads as **visibly partial** instead of silently
  dropping the peer. Names each **degraded peer** (`DegradedPeer`: node
  ID + short reason — `timeout` or transport error text). Omitted
  entirely when every peer contributed (quiet-until-needed), so its
  presence alone means "this merge is incomplete." Benign placement-churn
  non-answers (a peer reconfigured out of the vault) are expected
  reconfiguration, not degradation, and never appear. Surfaced in the UI
  as a single "partial" badge.

- **Contributing vaults** (`SearchResponse.contributing_vault_ids`) — the
  per-vault stream-health signal on a merged **search**: the remote vaults
  the search fanned out to and merged. A remote vault whose leader gave a
  **not-ready answer** is left out of the search and of this set, as a vault
  the coordinator leads is left out of its own engine's search while not
  ready. Distinct from a contribution report
  because search is **fail-on-remote-failure** — any other remote stream error
  aborts the whole search (there is no partial search), so every vault in
  this set contributed to any response the client actually receives. It is
  the positive record of the merge's cross-vault span, not a degraded
  signal; empty for a purely-local search. The pipeline aggregation path
  (`| stats`, `| timechart`) shares the same fail-hard policy — a partial
  aggregate is a wrong scalar presented as authoritative, so a failed
  remote vault fails the whole query rather than silently undercounting.

- **Alarm** — a condition that **requires an operator action**, carrying a
  documented cause and response. The governing test is the whole definition:
  *does the operator have to do something?* If no, it is not an alarm. An
  alarm announces a condition waiting on a human — a condition the system is
  already handling (ingest throttling, a chunk healing itself) is not waiting
  on anyone and is therefore not an alarm, however interesting it is.
  Surfaced in the alarm list. See `docs/alarm-management-design.md` for the
  catalog and the per-alarm response text.

- **Event** — a record that something happened. No operator action. An event
  is a **log message**: a structured slog line, captured by the self ingester
  and searchable like any other logs — never an alarm-list entry. Most
  demoted diagnostics are events. There is no separate event store: the log
  pipeline records, stores and searches events.

- **Metric** — a measured quantity to trend on a health surface. Note that
  demoting a condition to a **log** and demoting it to a **metric** are not
  interchangeable: the self ingester captures slog, so any condition
  *caused by* pressure or volume (channel saturation, ingest pressure,
  dropped log records) must become a metric — logging it feeds the condition
  it reports.

- **Alarm catalog** — the static `AlarmType` registry in `internal/alert`,
  one entry per alarm type: `Priority` (the consequence × urgency verdict),
  `Source`, `Cause`, `Response`, plus the suppression fields (`DelayOn`,
  `DelayOff`, `Latching`), all enforced by the collector. Call sites raise
  by type ID (`alerts.Raise(typeID, instanceKey, detail)`) and cannot
  choose a priority — the collector stamps it from the catalog. The catalog
  and the table in `docs/alarm-management-design.md` must agree.

- **Alarm register** — alarm text (`detail`, `Cause`, `Response`) is read by
  an operator mid-incident, which makes it a different register from code and
  logs even when it names the same concepts. Three rules: name entities the
  way the operator knows them (a vault by name, never a bare GLID); never
  print source notation (`min(2, placement)` is `HolderQuorum` spelled
  as Go — say "a second node must hold a copy"), and never
  `time.Duration.String()` (use `system.FormatDuration`, which drops the zero
  components that make "3m0s"); and lead a Critical alarm with the
  consequence, not the mechanism, since Critical *means* records are being
  lost. Domain nouns the UI already teaches — chunk, segment, seal, home,
  holder, GLCB — stay; they are what the inspector labels its own counters.
  The three fields do not restate each other: `detail` is what happened here,
  `Cause` is why this condition exists, `Response` is what to do.

- **Chattering suppression** — the collector-enforced remedies for a
  flapping condition producing a flapping alarm (EEMUA 191 principle 3):
  **delay-on** (the condition must persist that long before the alarm
  activates; flaps below the window never annunciate), **delay-off** (an
  active alarm's condition must stay clear that long before auto-clear; a
  return inside the window is the same occurrence), and **latching** (plain
  sticky: the alarm stays standing after the condition clears, until
  process restart — no release path, by design). Driven by the catalog
  entry; call sites raise and clear the raw condition and carry no alarm
  timers of their own. Windows evaluate lazily against the collector's
  injectable clock; `FirstSeen` is condition start, not activation time.

- **Standing alarm** — an annunciated alarm: its condition holds (or
  held, for a latched fault). An alarm is **standing or it is not** —
  there are no per-alarm operator states. Alarms are state with
  suppression: they stand while the condition holds, clear when it
  resolves, and nothing persists across restart — a re-detected condition
  after boot is simply standing again. There is no acknowledgment and no
  shelving: awareness bookkeeping is ceremony, and management machinery
  presumes the alarm volume the alarm definition exists to eliminate. See
  `docs/alarm-management-design.md`.

- **Occurrence** — one continuous condition episode of an alarm ID in the
  suppression sense: a clear-and-return inside the delay-off window is
  the same occurrence (the alarm stays active, `FirstSeen` preserved); a
  raise after the alarm released is a fresh alarm.

- **Priority** — `alert.Priority`, the cataloged verdict per alarm type:
  `Critical` (data loss in progress or scheduled), `High` (durability or
  availability degraded, will compound), `Low` (needs attention on a human
  timescale). Priority always comes from the catalog, with zero
  exceptions; call sites never choose one, and there are no
  operator-defined alarms (`retention-rate` is an ordinary catalog row). **Software faults** (e.g. `orchestrator-lock-leak`) are
  a class apart — defect tripwires whose response is to report, so they
  carry no priority.

- **Alarm collector** (`alert.Collector`) — per-node in-memory store of standing alarms with
  their suppression state (no file I/O; nothing survives restart). Alarms
  have a stable key (`typeID` or `typeID:instanceKey`) for dedup; the
  standing list (`Standing()`) is included in each NodeStats broadcast so
  the full attributed cluster list is readable from any node. There is no
  alarm-flood self-monitor.

- **SystemAlert** — one alarm on the wire (the proto form of `alert.Alarm`): `ID`, `Priority`, `Source`,
  `Detail` (per-instance specifics), `Cause`/`Response` (from the
  catalog), `SoftwareFault`, `FirstSeen`, `LastSeen`. Keyed ("alarm X for
  reason Y on node Z") so repeated identical alarms don't accumulate.

---

## 8. Identity & Config

Operator-controlled state, user authentication, and the Raft-replicated
config store.

### State model

- **System** — `system.System`: the top-level cluster state. Two halves:
  - **Config** — operator-controlled (vaults, routes, ingesters,
    policies, cloud services, server settings). Routes carry their
    match expressions inline on `RouteConfig.Stages`; there is no
    separate `Filter` entity.
  - **Runtime** — cluster-managed (node membership, vault placements,
    ingester assignments, setup wizard dismissal).

  Both are replicated via the cluster-ctl Raft group.

- **Store** (`system.Store`) — the read/write interface over `System`.
  Two implementations: in-memory (`system/memory`, behind the `memory`
  config-store type and inside the Raft FSM as its state) and Raft-backed
  (`system/raftstore`).

- **StoreProxy** — a wrapper whose inner store is swapped atomically while
  a node joins a cluster; calls made during the swap fail with
  `ErrJoining`.

### Configuration defaults

**Policy: an unset value must not express an unbounded claim on a finite,
shared resource.** Absence of configuration is the common case — the operator
with no opinion — and it must resolve to the *conservative* reading, not the
maximal one. A too-small bound announces itself (records refused, alarm
raised) and is corrected; an absent bound is silent until it exhausts the
resource. Small-and-loud beats large-and-silent.

- A knob whose unset value claims *the minimum* is correct: `replication-factor`
  defaults to 1 (one copy is the least that still stores the data).
- A knob whose unset value claims *the maximum* of a finite shared resource
  (disk, cache, memory) is a defect: `max-size` / `cache-budget` /
  `memory-budget` unset must resolve to a bounded default, not "unlimited".
- "Unlimited" is an explicit large value the operator types, never the effect
  of saying nothing.

When adding a config knob, apply this before shipping it. See
[`docs/product-defaults-policy-design.md`](./product-defaults-policy-design.md)
for the criteria and the audit.

### Server settings

Live on `Config` directly (not as entities):

- **AuthConfig** — JWT secret, token and refresh-token durations, password
  policy.
- **QueryConfig** — query timeout, max follow duration, max result count.
- **SchedulerConfig** — scheduler concurrency (`MaxConcurrentJobs`).
- **TLSConfig** — external API TLS: default certificate, HTTPS enable and
  port, HTTP→HTTPS redirect.
- **LookupConfig** — lookup table configuration (HTTP, JSON file, YAML
  file, MMDB, CSV, static).
- **Destination policy** — the rule deciding which resolved addresses an
  outbound fetch may reach (`internal/safefetch`). Denies everything outside
  public unicast; a stored lookup's `AllowPrivateDestinations` is the
  operator's opt-in for a service on their own network, and never reaches
  link-local or address-translation ranges.
- **ClusterConfig** — broadcast interval override and the backlog budget
  (see [Pipeline](#9-pipeline)).
- **MaxMindConfig** — credentials and state for automatic MaxMind
  database download.
- **Managed file** — an uploaded file (CSV, MMDB, JSON, YAML) the cluster
  keeps for lookups: metadata in config (`ManagedFileConfig`), bytes under
  each node's home. Uploading under an existing name adds a **version**;
  every version keeps its own ID and nothing is removed automatically. The
  newest version is **current** — what resolution by name picks — while a
  lookup bound to a version's ID keeps using that version.

### Authentication

- **User** — identified by username; has `Role`, password hash,
  zero-or-more refresh tokens. Managed via `SystemCommand_CreateUser`,
  `UpdatePassword`, etc.

- **Role** — coarse permission set carried on `User.Role` and in the JWT.
  Two values: `admin` and `user`, enforced where a role is written
  ([`server/auth.go`](../backend/internal/server/auth.go)).

- **AuthLevel** — the authorization an RPC requires, declared on the
  method itself via the `auth_level` option in
  [`authz.proto`](../backend/api/proto/gastrolog/v1/authz.proto):
  `PUBLIC` (no token), `AUTHENTICATED` (any role), `ADMIN` (role
  `admin`). The interceptor builds its table from the method
  descriptors and denies any procedure that declares no level.

- **JWT** (access token) — short-lived bearer token. Carries claims:
  `sub` (username), `uid` (user ID), `role`, `sid` (session), `exp`, `iat`, and `iat_ns`
  (issue time at the precision revocation ranks against).

- **Session** — one login on one device. Identified by the ID of its
  refresh-token row, which the access token names in its `sid` claim.
  Rotation keeps the session; logout deletes the row, which ends the
  refresh token and the access token together, leaving the user's other
  sessions alone.

- **RefreshToken** — long-lived credential, stored in the cluster-ctl Raft,
  one row per session. Exchanged for a new JWT without re-entering the
  password; the exchange is a single Raft entry, so concurrent use of one
  token yields one live session, not two. A password change, rename, role
  change, or user delete ends every session via
  `DeleteUserRefreshTokens`.

- **Cluster TLS** — mTLS material (`CA`, `Node cert`, `Node key`)
  generated at cluster-init. Used exclusively for intra-cluster gRPC.
  Separate from external API TLS.

- **Certificate** — operator-stored TLS material for HTTPS and ingester
  TLS, kept under a name and picked by name or ID. Two kinds, by where the
  key material lives: a **pasted certificate** (badge `pem`) holds PEM
  content in the cluster configuration; a **file-path certificate** (badge
  `files`) names certificate and key files on the node's disk, which are
  watched and reloaded when they change. The **default certificate** is
  the one the HTTPS listener serves.

### Commands & FSM

- **SystemCommand** — the Raft log entry for config/runtime mutations.
  A oneof of Put/Delete variants per entity type. Applied by
  [`system/raftfsm/fsm.go`](../backend/internal/system/raftfsm/fsm.go).

- **Notification** — the structured event emitted when a config apply
  completes. Consumed by the dispatcher to drive orchestrator changes
  and to push WatchSystem events to subscribers.

### IDs

- **GLID** — GastroLog ID: 16 bytes, UUIDv7-shaped, lexicographically
  sortable by creation time. Every entity (vault, user, chunk, node,
  storage) has a GLID. 26-character base32hex string form is canonical in
  URLs, logs, and user-facing surfaces.
  [`backend/internal/glid/glid.go`](../backend/internal/glid/glid.go).

---

## 9. Pipeline

The write path (`backend/internal/pipeline`, supervised by
`backend/internal/orchestrator/pipeline`): ingest → digest → route → segment →
distribute → collect → chunk. One manager per phase, each separated from the
next by a queue. Design rationale in
[the V3 pipeline design](./fan-out/v3/pipeline.md); everything below
describes what the code does today.

### Roles

- **Origin** — the node whose segmentation writer produced a segment. The
  origin serves pull requests for the segment's bytes and never forwards:
  the leader publishes intent, not bytes.

- **Home** — a node in a vault's placement (the **home set**): a node that
  *should* hold the vault's segments and chunks. Homes run Collection and
  Chunking for the vault. "Home" is desired state; "holder" is observed state.

- **Holder** — a node that actually holds verified bytes for a segment or
  chunk. Residency truth, recorded in the vault-ctl FSM (`holders` on the
  vault segment manifest entry and on the vault chunk manifest entry). Placement says who *should*
  hold; the holder set says who *does*. `holders ⊇ homes` is a release fast
  path, never the sole gate (a dead home must not pin a segment forever).
  A holder outside the vault's current placement runs no vault instance, so
  nothing of its own maintains its claim: every gate that destroys another
  copy counts only **placed holders** (`vaultctlfsm.PlacedHolderCount`), and
  **node prune** (`CmdPruneNode`) removes the node's claims — proposed by the
  placement leader's outside-placement prune when the node leaves the
  placement, and by the vault-ctl leader when it leaves the cluster.
  For a cloud-backed chunk a holder's GLCB is a **cached copy**: every
  home's cache eviction may remove it, and the home's holder receipt is
  withdrawn with it. The uploaded object, not the placed-holder count, is
  what makes a cloud-backed chunk supersede its segments.

- **Holder receipt** — the vault-ctl commit a node makes after it has pulled
  and verified bytes (`AckSegmentHolderCommand` for segments,
  `AckChunkHolderCommand` / `RevokeChunkHolderCommand` for chunks). Receipts
  are what grow the holder set — durability is proven by receipt, never
  inferred from optimistic counters.

- **RequiredHolders** — chunking's callback returning the placement member
  node IDs that must hold data before release gates open, plus whether the
  placement lookup resolved (`chunking.VaultConfig.RequiredHolders`, wired
  from the supervisor's `ChunkRequiredHolders`). Mandatory at registration;
  an unresolved lookup fails the release/purge gates closed, and
  `chunking.NoRequiredHolders` is the explicit no-holder-gate opt-out for
  single-node vaults. The replication quorum — the placed-holder count the
  planner's segment eligibility, segment supersession, and head purge all
  require — is `min(2, placement size)` (`vaultctlfsm.HolderQuorum`).

### Staging areas

Per-vault on-disk directories (`backend/internal/pipeline/paths`) — roles
bound to storage. Rename-paired areas co-locate so promotion is an atomic
`rename(2)`: `working` ↔ `completed` on the origin, `pre-head` ↔ `head` on
the collector.

- **working/** — the segment currently being appended by Segmentation. Its
  header is provisional; the segment is not yet eligible for anything.
- **completed/** — completed segments, renamed from `working/`. Published to
  vault-ctl and served for pulls; retained until release.
- **pre-head/** — in-flight transfers on a collecting home; invisible to
  queries and Chunking. A failed or corrupt transfer is discarded here and
  re-pulled from the next holder in the same collect pass.
- **head/** — whole, checksum-verified, immutable segments awaiting chunking.
  Where the recent, queryable records live: an Active chunk's records are
  read here, from the segments its open chunk manifest names.

- **Pipeline backlog** — a vault's published, unreleased completed segments:
  the bytes in the vault segment manifest that chunking has not yet consumed. It
  grows while chunking is behind; the inspector's pipeline view shows it.

- **Backlog budget** — the cluster-wide, per-vault bound on the pipeline
  backlog (`ClusterConfig.PipelineBacklogMax`; Cluster › **Backlog Budget**).
  At the budget, ingest admission for the vault refuses with a retryable
  backpressure error until chunking drains below it — the operating bound
  ahead of the disk-guard backstop. Empty means unbounded.

### Verbs

- **Mint** — Ingestion assigns the EventID
  (`IngesterID + NodeID + IngestTS + IngestSeq`) to an incoming message
  (`ingestion.Minter`). Identity, dedup, and order are carried by EventID
  from that point on.

- **Publish** — Distribution commits a completed segment's *metadata* to the
  vault-ctl log (`PublishCompletedSegment`), making it cluster-visible so
  Collection on each home can pull it. Metadata only — bytes move by pull.

- **Promote** — move a segment into the next staging area: an atomic
  rename `working/` → `completed/` at segment completion (Segmentation,
  `CompletePolicy`) and `pre-head/` → `head/` after verification
  (Collection, `PromoteVerified`), and a hard link (copy fallback) of
  `completed/` into `head/` when the origin is itself a holder
  (Distribution, `PromoteToHead`; never a stream to self). The `completed/`
  copy stays until release so peers can still pull it.

Three verbs cover segment end-of-life, one per layer — they are not synonyms:

- **Release** (vault-ctl / chunking) — drop a segment's vault segment
  manifest entry from the vault-ctl FSM (`ReleaseSegmentsCommand`), proposed by the
  chunking leader once the segment's records are superseded by replicated
  chunks (holder-gated). The FSM-level end of a segment's life in the vault
  segment manifest;
  bytes may still exist on disk.

- **Released-segment tombstone** (vault-ctl) — the FSM's record that a
  segment was released, kept after its vault segment manifest entry is gone and tagged
  with the **release sequence** it was released at (`releaseSeq`, counting
  the `ReleaseSegments` applies that tombstoned a segment). A
  `PublishCompletedSegment` naming one is ignored — the restart republish of
  a node that missed the release — and the release consumer's catch-up and
  the released-segment sweep purge its bytes on sight
  (`FSM.SegmentReleased`).

- **Purge confirmation** (vault-ctl) — a node's `ConfirmSegmentPurgeCommand`:
  it holds no staging copy of any segment released at or before the named
  release sequence. Every node sends one after its released-segment sweep
  (`Orchestrator.sweepReleasedSegments`); the vault-ctl leader's
  `PruneReleasedSegmentsCommand` drops the tombstones every member of its
  Raft configuration has confirmed, and the confirmations of nodes no longer
  in it.

- **Retire** (distribution) — drop a node's in-memory tracking of a segment
  and mark it so the stranded rescan does not republish still-on-disk
  `completed/` bytes (`Manager.RetireSegments`, `vaultDist.retireSegment`).
  Node-local bookkeeping, downstream of release.

- **Purge** (paths / disk) — delete the segment's bytes from staging areas
  (`paths.PurgeSegmentStaging` for all three areas once the segment is
  released, on every node whether or not it is a home;
  `paths.PurgeHeadStaging` after a home materializes the sealed GLCB). The
  physical end of a segment's life, ordered strictly
  after its records survive in a replicated chunk — a returning node gets
  those records via chunk replication, never a segment re-pull.

- **Retention give-up bound** — the delete-disposition retention window after
  which a segment still sitting in the vault segment manifest is **released unchunked**: its
  records are discarded without ever reaching a chunk
  (`VaultConfig.RetentionGiveUpTTL`, `scanGiveUpExpired`). The one path that
  breaks release's normal ordering, and the only one that destroys records the
  system accepted — so it is counted and alarmed
  (`chunking-retention-giveup`), never silent. Say "released unchunked"; do
  not coin "shed", "never-chunked", or "gave up" as nouns for it.

### Chunk build

- **GLCB** — GastroLog Chunk Blob: the current **chunk format**
  (`backend/internal/chunk/glcb`, `data.glcb`) — a single-file sealed object
  with its timestamp indexes as sections. Every home builds one per sealed
  chunk, and the same bytes, re-framed per section, are the cloud object of a
  cloud-backed chunk. See [Storage](#1-storage) for the layer it sits in.

- **Timestamp index** — the per-chunk ingest-TS and source-TS indexes. They
  are sections inside the GLCB, located from its TOC tail, never separate
  files: they cannot go missing on their own, a cloud-backed chunk's Cloud
  Storage rows in the inspector list them as `ts-index embedded`, and
  `reindex` rebuilds only the other indexes. Both
  hold one entry per record; the source-TS index (STSI) keys each record by
  its EffectiveSourceTS. An index with no entries (a chunk with no records)
  is present and empty, never read as missing.

- **Assignment log** — the vault-ctl log view Collection rolls
  (`collection.LogReader.Roll` → `AssignedSegment`) to learn which published
  segments this home should hold; it then pulls the ones it lacks and
  commits holder receipts.

- **Segment span** — a slice of one segment in EventID order
  (`chunking.Span{SegmentID, Start, Count}`): positions in the segment's
  EventID index, never file offsets. An open chunk manifest ref
  (`OpenChunkSegmentRef`) names one by its first and last record number. The
  chunking planner chooses spans as it extends the open chunk manifest; the
  build k-way-merges the sealed chunk manifest's spans.

- **Chunk record budget** — the rotation policy's `MaxRecords`: the planner
  stops adding records to the open chunk manifest at this count and seals it.

- **Chunk byte budget** — the rotation policy's `MaxBytes`: the same cut,
  measured in the segment frame bytes the open chunk manifest's slices hold.

- **Chunk time cut** — the planner's time-based rotation triggers on the open
  chunk manifest: `Cron` (a scheduled cut) and `MaxAge`, measured from the
  open chunk manifest's `OpenedAt` wall clock rather than from record timestamps, so a
  backlog of old records does not seal into slivers.

- **Seal queue** — the sealed chunk manifests whose chunk the leader has not
  yet sealed, FIFO (`sealed_manifests` in the FSM snapshot); the queue head is
  the one chunk the leader builds and seals next. After `CmdSealChunk` the
  sealed chunk manifest leaves the queue and is kept (`sealed_chunk_manifests`)
  until a segment it references is released or the chunk is deleted, so every
  home that has not built the chunk yet can still build it from its refs. A
  home's build list is the kept sealed chunk manifests whose GLCB is missing
  from its chunk root, then the seal-queue head. (Sealed chunk manifest: see
  **Manifest**.)

- **Eligible segment** — a vault segment manifest entry the chunking planner can still
  take: prior chunks have not consumed all its records
  (`segmentExhaustedForPlanning` is false) and at least
  `HolderQuorum(placement)` (`min(2, placement size)`) placement members hold
  it, so no open chunk manifest depends on a segment with a single copy. Segments held
  below the quorum for long raise `chunking-underreplicated`. The inspector's **N segments
  eligible to chunk** counts them; a growing count means chunking is behind.

- **GLCB quarantine** — an existing-but-unreadable sealed GLCB is renamed
  aside to a `data.glcb.corrupt` sibling (never silently deleted or silently
  rebuilt over) so the canonical path reads as **missing** and the ordinary
  missing-GLCB machinery heals the chunk: rebuild from source segments when
  they are still available, otherwise a peer re-pull via the GLCB catch-up
  sweep. The per-vault `chunking-glcb-corrupt` alarm stands until every
  quarantined chunk heals; healing removes the quarantine file
  (`chunking/glcb_corrupt.go`).

- **Sealed digest** — the whole-blob digest (GLCB TOC footer `blobDigest`) of
  the copy the sealing leader built, recorded by `CmdSealChunk` as
  `ManifestEntry.Hash`. Write-once: a later seal or a cloud upload carrying a
  different digest is refused, and a cloud re-fetch is verified against it.
  A home's copy that carries it is the **agreed copy** — the only copy a home
  indexes, serves, uploads to the cloud or claims a holder receipt for, and
  the only copy a peer pull promotes (its bytes hashed, not its footer read).

- **Divergent copy** — a home's copy of a sealed chunk whose digest differs
  from the sealed digest. Renamed aside to a `data.glcb.divergent` sibling so
  the canonical path reads as missing, forgotten by the chunk manager (its
  registration and cached mapping dropped), never uploaded, never rebuilt (the
  same sealed chunk manifest over the same local segments builds the same bytes), and
  replaced by the agreed
  copy through GLCB catch-up. Raises the latched `chunking-build-diverged`
  software-fault alarm, one per vault (`chunking/glcb_divergent.go`).

- **Mismatched GLCB** — a GLCB on a chunk's canonical path that is not a build
  of the chunk's sealed chunk manifest: it names another chunk, holds another record count,
  spans other record timestamps, or, for a sealed chunk, carries another digest
  than the sealed digest. The build pass and restart recovery never adopt it:
  it is renamed aside to a `data.glcb.mismatched` sibling and the chunk is built
  from its segments (`chunking/glcb_mismatched.go`).

### Stage counters

Per-vault, per-node **monotonic** counters for the discrete pipeline-stage
milestones, cross-node (`VaultStats` fields, `NodeStats` broadcast; the
inspector's *Pipeline stages* panel and `gastrolog cluster throughput` render
them). Each milestone is counted **exactly once by its
owner** (origin / home / leader) on the node where it happened, so cluster
totals are the plain sum across nodes — never double-counted, never inferred
from optimistic route/ingest counters. Rates (segments completed/published,
chunks built/sealed) come from the stats collector's rolling windows over the
totals, the same server-side mechanism as append/collected/sealed throughput —
never accumulated client-side.

Canonical milestone verbs (reuse these names; do not coin synonyms):

- **Completed** — a working segment promoted `working/` → `completed/` (origin).
- **Published** — a completed segment's metadata committed to vault-ctl (origin).
- **Released** — a segment dropped from the vault segment manifest (leader),
  superseded by replicated chunks. Distinct from **Retire** and **Purge**
  (see §9 Verbs).
- **Planned** — an open chunk manifest opened by the chunking leader.
- **Built** — a sealed GLCB materialized on a home.
- **Sealed** — a `CmdSealChunk` commit landed by the leader.
- **Head purge** — a segment copy deleted from `head/` after materialization.
- **GLCB pull (attempted / failed)** — a replica catch-up pull a home started to
  recover a missing chunk blob, and the ones no peer could satisfy.
- **Retention delete** — a chunk expired by the retention engine (leader).

---

## Consistency rules

### Canonical terms (and the variants to phase out)

| Canonical        | Do not use        | Rationale                                                          |
|------------------|-------------------|--------------------------------------------------------------------|
| leader           | primary           | Raft terminology is consistent across the industry.                |
| follower         | secondary, replica| "replica" is ambiguous with the separate concept of chunk replica. |
| active chunk     | open chunk        | "Active" matches `ChunkMeta.Sealed = false`.                       |
| sealed chunk     | closed chunk, finalized chunk | "Sealed" is what the chunk manager actually calls it.  |
| cloud-backed     | cloud chunk       | Cloud-backed describes storage; "cloud chunk" conflates with archival state. |
| alarm            | alert             | An alarm requires an operator action; "alert" blurs alarms, events and metrics together, which lets non-actionable diagnostics into the alarm list. The alarm identifiers are `AlarmType`, `Raise`, `Priority`, `alert.Alarm`, `alert.Collector`; the `alert`-named identifiers that remain (package `alert`, proto `SystemAlert`, `NodeStats.alerts`, `cluster.AlertProvider`, `orchestrator.RateAlerter`) are names to phase out, not a second concept. |
| archived         | cold              | "Archived" is the canonical flag; cloud storage class is orthogonal. |
| cached copy, uncached | warm cache, warm copy, cold chunk, cold read, lukewarm | Temperature words suggest storage layering, which GastroLog does not have. A cloud-backed chunk's local copy is a cached copy; a chunk with no local copy is uncached. |
| chunk format (GLCB is one) | universal chunk artifact, "the chunk" for the file | A chunk is the set of records; GLCB is how a sealed chunk is encoded today. |
| vault chunk manifest, vault segment manifest, open chunk manifest, sealed chunk manifest | manifest (bare) | "Manifest" marks a list as authoritative; it never names a kind on its own. Name the kind. |
| vault segment manifest | completed-segment registry, segment registry, registry | The authoritative list of a vault's published segments is a manifest, like the vault chunk manifest. The code's `CompletedSegment…` names are to be renamed. |
| deletion record (pending delete, chunk tombstone, released-segment tombstone) | manifest | A deletion record exists only while a deletion is made safe; a manifest lists what exists. |
| released unchunked | shed, shedding, never-chunked, gave up | The retention give-up path is a **release** — the canonical verb for dropping a segment's vault segment manifest entry — that happens before the records are safe in a chunk. "Shedding" reads like load-shedding (a deliberate throughput tradeoff) rather than what it is: records the system accepted being destroyed. |
| — (banned)       | tier, tiered, tiering | **GastroLog has no tiers.** Hot/cold storage layering is a rejected design, and the word is what brings it back: it reads as a standing concept, so the next writer reaches for it and then designs to it. Do not use it in our voice for ANYTHING — not storage, not a step in a fallback chain, not a severity band. Canonical replacements: a step in an ordered lookup is a **fallback**; a severity band is a **threshold**; a cloud archival class is a **cloud storage class**; the mechanism for moving data by age is **late-bound retention routing**. The ONLY permitted use is quoting a provider's own API surface where the identifier is theirs and cannot be renamed: Azure `AccessTier` / `SetTier` / `SetBlobTier`, AWS `types.Tier`, the S3 class literal `"INTELLIGENT_TIERING"`. Describing their behaviour in our prose still uses our words. Enforced by `scripts/hooks/pre-commit`. |
| storage class    |                   | The **uint32** local class on `FileStorage`, `CloudService` and `VaultConfig` that selects WHICH DISK a vault may live on. Numeric, and it owns the unqualified name. |
| cloud storage class | storage class (for cloud archival), archival tier | The **string** cloud archival storage class — `"GLACIER"`, `"DEEP_ARCHIVE"`, `"cold"`; empty means standard storage. Always carries the `cloud` qualifier in identifiers (`CloudStorageClass`, `cloud_storage_class`), because `CloudService` holds both meanings one field apart and an unqualified `StorageClass` there reads as the numeric one. The exception is the `blobstore` adapters, where `BlobInfo.StorageClass` deliberately mirrors the provider's own field name (S3 StorageClass, GCS StorageClass, Azure AccessTier). |
| vault-ctl Raft   |                   | One Raft group per vault, authoritative for that vault's chunk metadata. Follows the `{scope}-ctl` naming pattern for control-plane Raft groups. |
| cluster-ctl Raft | system Raft, config Raft, cluster Raft | One Raft group per cluster, authoritative for cluster-wide configuration. Pairs with `vault-ctl Raft` to form the `{scope}-ctl` pattern. Its Raft group ID is `cluster-ctl` (`raftgroup.ClusterControlPlaneGroupID`). |
| Raft contact     | peer heartbeat, liveness ping | Peer liveness comes from Raft's own per-group traffic. There is no peer heartbeat broadcast; "heartbeat" means only hashicorp/raft's replication heartbeat. |
| chunk replication |                  | Every home builds its own GLCB from its segments; a home that missed the build pulls it from a peer home. |
| reclamation / scavenge | WAL compaction | The shared Raft WAL's dead space returns via oldest-first segment reclamation: a drained segment (zero live payload bytes) unlinks outright; a nearly-drained one scavenges — its live records re-append through the write path onto the active segment before the original unlinks. There is no whole-log rewrite; no work unit in reclamation exceeds one segment's live remainder. |
| ingester         | source, collector | "Ingester" is the proto name; "source" leaks from UI copy.          |
| route            | pipeline (at ingest) | Ingestion "route" ≠ query "pipeline"; use "route pipeline" or "ingestion pipeline" to bridge. |
| record           | event, message    | "Event" conflates with `EventID`; "message" conflates with ingester internals. |
| applied index    | committed-and-applied | Precision: commit = quorum-persisted; applied = FSM-processed.  |
| node             | server, host      | "Node" is the cluster-member canonical. Reserve "server" for `cluster.Server` (the gRPC server component). |
| peer             | remote node       | "Peer" is relative; there is no absolute "remote".                 |
| retention event  | retention action, expire/eject/transition | A fired retention event destroys the chunk and, per the vault's retention disposition, first streams the records through the routing engine (`route`) or transfers the sealed chunk to a target vault (`transfer`). The "what" lives on routes; the "whether to invoke routes at all" lives on the disposition. |
| match expression | filter, FilterConfig | Match expressions are inlined on `RouteConfig.Stages`; there is no named `Filter` entity. UI label: "Match expression" on the route editor. |
| route table      | filter set        | The runtime structure is a priority-ordered `routing.Table`, not a per-vault `FilterSet`. First-match-wins, no catch-the-rest. Identifiers: proto `route_table_active` / NodeStats `route_stats_route_table_active`, `Orchestrator.IsRouteTableActive`, UI banner "Route table is inactive". |
| synthetic attribute | source predicate, RouteSource | Source/content predicates unify via `_source`/`_ingester`/`_vault`/`_reason` overlays at routing-eval time. |
| retire (segment, distribution) | forget | Distribution's node-local drop of segment tracking is `RetireSegments` / `retireSegment`; one verb per meaning. See [Pipeline](#9-pipeline) for the release / retire / purge distinction. |
| glcb (container-format package) | chunk/cloud | GLCB is universal — local-only vaults seal into it too — so its package is `chunk/glcb`; "cloud" names only genuine object-storage interaction (blobstore, cloud-backed cache, cloud upload). |
| storage disk protect | vault disk protect | The disk-guard free-space thresholds live on `system.FileStorage`: a below-floor storage protects every vault placed on it, not one vault's own threshold. Identifiers: proto enum `VAULT_ADMISSION_CAUSE_STORAGE_DISK_PROTECT`, `orchestrator.ErrStorageDiskProtect`, `VaultAdmissionCauseStorageDiskProtect`, NodeStats `storage_protected_vault_ids`, `PeerState.VaultStorageProtected(Nodes)`. |
| complete (segment lifecycle) | close | "Close" is overloaded: writer shutdown vs the segment lifecycle event (working/ → completed/). The rotation trigger is `segmentation.CompletePolicy` and a segment COMPLETES; reserve Close for genuine resource shutdown (`Close()` methods, closed writers). |
| routed (routing counter) | ingested (at routing) | The routing counter counts records ENTERING ROUTING, not ingestion — counter provenance matters when proving loss. `Routed` = entered routing, `Matched` = matched a route and fanned out (proto `total_routed`/`total_matched`, NodeStats `route_stats_*`, UI labels). "Ingested" stays only on genuine ingester counters (`MessagesIngested`, `BytesIngested`). |
| max size (policy bound) | max bytes | The proto field is `max_size` on both the rotation and the retention policy and the CLI flag is `--max-size`; the UI label is **Max Size** on both forms, never "Max Bytes" for one of them. |
| pasted certificate / file-path certificate | monitored files certificate, file-based certificate, PEM certificate | Two certificate kinds, by where the key material lives; one name each through the buttons, the kind badges and the help. |
| lookup table name | registry name | "Registry" is the server's internal structure; what the operator types after `\| lookup` is the table name, and the **Name** field says so. |
| verify server certificate | disable server verification | One checkbox with one polarity for the shared `tls_verify` ingester param: checked verifies, and only the literal `false` turns verification off. |
| ingester type label | raw type key in UI copy | The add menu and type badges show the type's help-topic title (`Fluent Forward`, `OTLP`); the key (`fluentfwd`, `otlp`) stays in config and the API. |
| unmatched (routing counter) | dropped (at routing) | "Dropped" at routing would name two different quantities. `Unmatched` = matched no route, an intentional counted drop (proto `total_unmatched`, NodeStats `route_stats_unmatched`, UI label "Unmatched"); the invariant is `Routed = Matched + Unmatched`. "Dropped" is reserved for delivery drops: `StatsSnapshot.PerVaultDropped` counts already-matched records whose fan-out delivery to a vault failed — a per-vault sub-account of `Matched`, never part of the routed sum. |

### Timestamp conventions

Every record carries three timestamps, and they mean different things:

- **SourceTS** — when the upstream system says the event happened. External,
  potentially untrustworthy, but usually what the user cares about when
  querying ("show me errors from 10:00 to 10:15 yesterday").

- **IngestTS** — when THIS node's ingester received the message. Always
  monotonic per ingester (thanks to `IngestSeq`); used for disambiguation
  in `EventID`.

- **WriteTS** — when the origin's segmentation writer appended the record to
  a segment; the chunk build copies it unchanged. Unique per record
  across the cluster (see `ChunkID + Pos`).

For user-facing features, **default to SourceTS**; fall back to IngestTS
if SourceTS is absent or obviously bogus. For internal ordering
(GetContext, catch-up, indexes), use **WriteTS**.

- **EffectiveSourceTS** (`chunk.EffectiveSourceTS`) — the fallback above,
  applied at read time: a record's SourceTS, or its IngestTS when SourceTS
  is zero. It is the record's place on the **source axis**, which
  `order=source_ts`, `source_start`/`source_end`, `bin(…, source_ts)` and
  the STSI all use. It never becomes the record's SourceTS: ingest leaves
  SourceTS zero when the source gave none, and results show it zero.

### ID conventions

- Every entity ID is a `GLID`. No exceptions.
- String form is 26-character base32hex. Always lowercase in logs; UI may
  prettify with middle-ellipsis if too long (`middleTruncate`).
- `glid.Nil` is the zero value; treat it as "not set" consistently. Never
  as "wildcard" — use an explicit parameter for wildcards.

### Error conventions

Error values that cross bounded contexts:

- `ErrVaultNotFound` — the vault doesn't exist on this node.
- `ErrVaultNotReady` — vault exists but the vault FSM hasn't applied enough
  log entries (or hasn't restored). Defined in
  [`cluster/vault_not_ready.go`](../backend/internal/cluster/vault_not_ready.go);
  the orchestrator re-exports it beside the readiness gate in
  [`vault_readiness.go`](../backend/internal/orchestrator/vault_readiness.go).
- `ErrChunkNotFound` / `ErrActiveChunk` / `ErrChunkTombstoned` — chunk
  manager errors with specific meanings. Never conflate.
- `ErrNoChunkManagers` — this node hosts no vaults.
- `ErrAlreadyRunning` — Run/Start was called on a component that is already
  running (every pipeline manager, the pipeline supervisor, the orchestrator).
- `ErrNotRunning` — the operation requires a running component (Stop before
  Start, submit after stop). Strictly means not-running; the Run-called-twice
  case is `ErrAlreadyRunning`, never this.

### What "replication" means in which context

- **Segment distribution** (pipeline) — record bytes reaching every home:
  the origin publishes segment metadata to vault-ctl and each home pulls the
  bytes. Acked by a segment holder receipt committed to vault-ctl.
- **Chunk replication** (pipeline) — every home building its own GLCB from a
  sealed chunk manifest, or pulling a missing one from a peer home (GLCB
  catch-up). Acked by a chunk holder receipt committed to vault-ctl.
- **Metadata replication** (vault-ctl Raft) — propagating chunk-create /
  seal / delete / upload events. Done by hraft via multiraft transport.
  Acked by Raft majority; bounded by `RaftApplyTimeout`.
- **Apply forwarding** — follower → leader forwarding of a write command.
  Done by `VaultApplyForwarder`, `VaultCtlChunkApplyForwarder`, or (for
  cluster-ctl Raft) `Forwarder`. This is not replication; it's routing to the
  node that CAN do the replication.

When you see "replication" in a log line or a comment, check whether the
subject is bytes or metadata — the operational consequences are different.

---

## Keeping this document honest

This document lives in `docs/` and is considered part of the review surface.
When you introduce a new domain term, add it here in the same PR. When you
rename or retire a term, update the Consistency rules table.

Start new issues and commit messages by grounding in this vocabulary.
If the vocabulary doesn't have a word for what you're doing, that is
evidence you are either (a) working in a new bounded context that needs
one, or (b) conflating existing concepts and should pick one.
