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

GastroLog is split into **eight bounded contexts**. The boundaries are not arbitrary;
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

At the end, a [Consistency rules](#consistency-rules) section names every known
synonym pair and picks the canonical side, plus conventions for timestamps, IDs,
and cross-context identifiers.

---

## 1. Storage

The physics of the system: bytes on disk (or in memory, or in cloud), organized
for append-heavy write patterns and time-ordered reads.

### Aggregates

- **Vault** — a named, versioned container for log records. A vault owns one or
  more **tiers** arranged in an ordered chain (position 0 is hottest). Records
  enter at tier 0; retention moves them down the chain. Operators create and
  delete vaults; the system manages tier instances. Defined:
  [`backend/internal/orchestrator/vault.go`](../backend/internal/orchestrator/vault.go),
  declarative config in
  [`system.VaultConfig`](../backend/internal/system/vault.go).

- **Tier** — a storage layer within a vault. Each tier has a **type** (memory,
  file, cloud, jsonl), a **position** in the vault chain, a **rotation policy**
  for when to seal its active chunk, and **retention rules** for when to expire
  sealed chunks. Declarative:
  [`system.TierConfig`](../backend/internal/system/tier.go). Runtime:
  [`TierInstance`](../backend/internal/orchestrator/tier_instance.go) — the
  per-node, per-tier bundle of chunk manager, index manager, query engine,
  and Raft callbacks.

- **Chunk** — an immutable, self-contained segment of records. One active chunk
  per tier per node accepts new records; it is **sealed** when a rotation
  policy fires (or manually). Sealed chunks can be compressed, indexed, and
  eventually expired. Each chunk has a `ChunkID` (sortable GLID) and metadata
  (`ChunkMeta`). Defined: [`chunk/types.go`](../backend/internal/chunk/types.go).

### Value objects

- **Record** — a single log entry: `SourceTS`, `IngestTS`, `WriteTS`, `EventID`,
  `Attrs`, `Raw`, optional `Ref` and `VaultID`, and an ack-gate flag
  `WaitForReplica`. Records are mostly immutable once ingested; `VaultID` gets
  stamped during routing.

- **EventID** — compound identity for a record: `IngesterID + NodeID + IngestTS +
  IngestSeq`. Makes the same record distinguishable from near-duplicates across
  parallel ingesters.

- **Attributes** (a.k.a. `Attrs`) — key-value pairs on a record. Persisted in
  each chunk's `attr.log`. Queryable as first-class fields (plus a handful of
  synthetic built-ins like `severity`, `source`).

- **RecordRef** — pointer to a record inside a chunk: `ChunkID + Pos`. Used
  wherever one record needs to refer to another (GetContext anchors, indexes).

- **ChunkMeta** — the stats bag for a chunk: sealed/compressed/cloud-backed
  flags, record count, byte counts, timestamps (`WriteStart/End`, `IngestStart/End`,
  `SourceStart/End`), retention-pending + transition-streamed flags, frame count
  for cloud chunks.

### States a chunk passes through

- **Active** — open for writes; lives only on the tier leader.
- **Sealed** — immutable; eligible for compression/indexing/replication.
- **Compressed** — `raw.log`/`attr.log` encoded zstd; `DiskBytes ≠ Bytes`.
- **Cloud-backed** — record bytes live in S3/Azure/GCS, not local disk; marked
  with `CloudBacked = true` in `ChunkMeta`.
- **Archived** — in an offline cloud storage class (e.g. Glacier). Unreadable
  until `Restore` completes. Tracked via `Archived = true`.
- **Retention-pending** — marked in the tier FSM for deletion on the next sweep.
- **Transition-streamed** — records have been streamed to the next tier; local
  copy is kept until the destination commits its receipt, then deleted.

### Physical storage

- **FileStorage** — a directory on a node's disk, identified by a GLID, tagged
  with a **StorageClass**. A node can have many file storages (different disks,
  different performance tiers). [`system.FileStorage`](../backend/internal/system/tier.go).

- **NodeStorageConfig** — the list of file storages on one node. Runtime state
  (not operator-authored). [`system.NodeStorageConfig`](../backend/internal/system/tier.go).

- **StorageClass** (`uint32`) — non-zero integer grouping storages by performance
  or role. A tier's `StorageClass` selects which FileStorage on each node hosts
  its chunks.

- **CloudService** — a cluster-wide cloud endpoint (S3, Azure, GCS) with
  optional archival lifecycle. [`system.CloudService`](../backend/internal/system/tier.go).

- **Frame** — a seekable zstd block within a cloud chunk. `NumFrames` records
  how many frames a cloud chunk was uploaded in; range queries seek to a
  specific frame to avoid pulling the whole blob.

### Placement

- **TierPlacement** — a mapping of a tier to a specific `FileStorage.ID` on a
  specific node, with a `Leader` flag. A tier normally has N placements (N =
  replication factor). [`system.TierPlacement`](../backend/internal/system/tier.go).

- **SyntheticStorageID** — test-only placeholder ID used when placement doesn't
  reference a real FileStorage (memory-tier tests). Production uses real IDs.

---

## 2. Ingestion

Everything between "bytes arrive from the outside" and "record is appended to
a tier's active chunk".

### Aggregates

- **Ingester** — a protocol adapter that receives external input and produces
  `IngestMessage` envelopes. Types include `chatterbox` (synthetic), `syslog-udp`,
  `http`, `kafka`, `file`, `jsonl`, and `self` (self-ingesting GastroLog logs).
  Declarative config: [`system.IngesterConfig`](../backend/internal/system/vault.go).

- **Singleton ingester** — an ingester that must run on exactly one node at a
  time (e.g. a Kafka consumer with a fixed partition). The placement manager
  assigns it; if the assigned node dies, another takes over. Has a
  `Singleton = true` flag.

- **Parallel ingester** — runs on every node in `NodeIDs` simultaneously. Each
  instance maintains a per-ingester sequence counter so records don't
  collide.

### Domain events

- **IngestMessage** — the internal envelope for a raw message entering the
  pipeline. Holds the raw bytes, ingester-specific metadata (syslog priority,
  HTTP headers), ingest timestamp, and ingester identity. The digester turns
  this into a `Record`.

- **Digester** — the parser for one ingester type. Knows how to decode its
  input format into structured fields and attributes. Examples:
  syslog-rfc5424 digester, apache-access-log digester, JSON digester.
  Digestion happens on the node that received the message, before routing.

### Routing

- **Filter** — a named query-language expression (`FilterConfig`) that matches
  a set of records. Special forms: `"*"` = match everything, `"+"` = match
  everything not already matched by a named filter ("catch the rest"),
  empty string = match nothing.

- **Route** — a named binding of a filter to one or more destination vaults,
  with a **distribution mode**. [`system.RouteConfig`](../backend/internal/system/vault.go).

- **Distribution mode** — how multi-vault routes pick a destination:
  - `fanout` — write to all vaults.
  - `round-robin` — rotate through vaults (load-balance).
  - `failover` — try first; on failure, try next.

- **EjectOnly route** — a route that is never used for live ingestion. Exists
  purely as a retention destination when a tier's retention rule says "eject
  to vault X" instead of deleting.

- **FilterSet** — the compiled, optimized set of all active filters on a node,
  plus precomputed node routing ("which nodes hold which vaults"). Reloaded
  when routes, vaults, or placements change.

- **MatchResult** — the output of filter evaluation: `VaultID`, optional
  `NodeID` (for forwarding), `RouteID`. One match per (vault, route) pair.

### Ack semantics

- **Ack-gated** — an ingester that waits for replica confirmation before
  acknowledging the upstream sender. Records from ack-gated sources carry
  `WaitForReplica = true`; the orchestrator does synchronous cross-node
  forwarding for them, skipping the async "fire and forget" fast-path.
  Examples: RELP syslog (with working acks), HTTP endpoints that accept an
  `X-Wait-Ack` header.

- **Fire-and-forget** — the default path: the orchestrator writes locally and
  dispatches remote replication in background goroutines; the ingester gets
  its ack immediately.

---

## 3. Query

Reading records back, filtering, aggregating, and rendering them.

### Aggregates

- **Query** — a search specification. Either a structured form
  (`StartTime`, `EndTime`, `Expression`, `Limit`, `Reverse`, `ContextBefore/After`)
  or a raw string parsed via the query language. The structured and string
  forms compile to the same internal representation.

- **Pipeline** — a sequence of query-language **operators** applied after the
  initial filter. Operators include `stats`, `where`, `eval`, `sort`, `tail`,
  `inline_stats` (in progress), `let` (in progress). The pipeline turns a
  stream of records into a `TableResult` or a filtered stream.

- **TableResult** — the output of an aggregating pipeline: columns + rows,
  plus a `result_type` distinguishing plain tables from time-series outputs
  (which get charted).

### Values

- **HistogramBucket** — counts for one time window: `timestamp_ms`, `count`,
  optional `group_counts` (for per-severity breakdowns), flags indicating
  whether any records came from cloud storage.

- **ChunkPlan** — per-chunk execution metadata inside `Explain`: time bounds,
  which indexes were usable, whether the chunk was skipped and why.

- **PipelineStep** — one operator in a compiled plan with metadata about
  whether it is streaming or materializing.

- **ResumeToken** — opaque pagination handle. Continue a search from a prior
  position without re-reading earlier chunks.

### Operations

- **Search** — return matching records, streamed. The canonical read RPC.

- **Histogram** — return record counts bucketed by time. Used for the
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

- **collectRemote** — the orchestrator entry point that fans a search out to
  remote nodes and merges results. Used by Search, Histogram, Explain,
  GetContext, GetFields.

---

## 4. Cluster Coordination

Nodes agreeing on what the cluster believes, via Raft.

### Identity

- **Node** — one running GastroLog process. Identified by a `NodeID` (GLID).
  A node hosts tier instances, runs ingesters, serves queries, and
  participates in Raft groups.

- **NodeConfig** — the declarative (`ID`, `Name`) record for a node. Lives
  in the system Raft's config. The `Name` is for humans; `ID` is canonical.

- **Peer** — another node, from this node's perspective. "Peer" is always
  relative; the same node is "local" to itself and "peer" to everyone else.

### Raft layers

GastroLog runs **multiple Raft groups** per node, multiplexed over a single
gRPC transport:

- **System Raft** (a.k.a. "config Raft", "cluster Raft") — one group per
  cluster. Replicates `system.Config` (operator-authored) and `system.Runtime`
  (cluster-managed). Every node is a voter. Leader changes propagate config
  via FSM apply; dispatcher drives downstream effects.

- **Vault Control-Plane Raft** (a.k.a. "vault-ctl Raft", "vault-ctl group") —
  one group *per vault*. Replicates that vault's chunk metadata across all
  nodes that host any of its tiers. Uses the `vaultraft.FSM` whose state is a
  map of **tier FSMs** — one sub-FSM per tier, namespaced by `OpTierFSM`
  commands. See
  [`vault-control-plane-architecture.md`](./vault-control-plane-architecture.md)
  for the design rationale.

- **Tier FSM** (`tierfsm.FSM`) — the per-tier sub-state-machine inside a
  vault-ctl FSM. Holds the **manifest** of chunks for one tier: each chunk's
  metadata (sealed? compressed? retention-pending?), transition receipts,
  tombstones.

- **Manifest** — the tier FSM's set of chunk entries. The authoritative
  answer to "which chunks should exist on this tier?" Compared against
  local disk in the reconcile sweep; disagreement means orphan cleanup.

### Raft primitives (hashicorp/raft vocabulary)

- **Term** — logical clock; increments on every election.
- **Log** — ordered sequence of entries. Entry types: `LogCommand` (goes
  through FSM.Apply), `LogConfiguration` (membership change), `LogNoop`
  (post-election commit), `LogBarrier`.
- **Commit index** — highest log index a quorum has ack'd; entries up to
  this are durable.
- **Applied index** — highest index this node's FSM has processed. Always
  `≤ commit index`. **This is the signal for vault readiness** — see
  `isFSMReady` in [`reconfig_vaults.go`](../backend/internal/orchestrator/reconfig_vaults.go).
- **Leader / Follower / Candidate** — Raft roles. Use "leader" and "follower"
  consistently; never "primary/secondary".
- **Voter / Non-voter** — membership state. Voters participate in elections;
  non-voters just catch up.
- **Snapshot** — FSM state serialized to disk; lets log be truncated.
- **InstallSnapshot** — RPC for streaming a snapshot to a slow follower.

### Placement & membership

- **TierPlacement** — covered under [Storage](#1-storage); also a cluster
  concept because the placement manager (in system Raft) decides which
  nodes host each tier.

- **Ingester placement** — the singleton-ingester assignment map in
  `system.Runtime`. The placement manager picks an alive node per singleton
  ingester; failover is automatic.

- **Placement manager** — the subsystem that owns placement decisions. Runs
  on the system Raft leader, reacts to node join/leave, vault create/delete,
  and ingester changes.

- **Dispatcher** — the subsystem that reacts to *applied* config changes
  (from system Raft FSM) and drives their side effects into the local
  orchestrator (register vault, build tier, start ingester, etc.).

### Transport

- **Multiraft transport** — gRPC service that multiplexes AppendEntries,
  RequestVote, InstallSnapshot, etc. across all groups. One connection pool,
  many logical Raft groups. [`backend/internal/multiraft/transport.go`](../backend/internal/multiraft/transport.go).

- **PeerConns** — the gRPC connection pool for cross-node RPCs (not Raft
  transport; for forward-apply, record-forward, chunk-transfer, etc.).

---

## 5. Orchestration

Each node has one **Orchestrator**. It is the top-level glue: holds vault
registry, dispatches jobs, manages lifecycle, coordinates retention and
rotation, and serves as the in-process API that RPC handlers delegate to.

### Aggregates

- **Orchestrator** — `*orchestrator.Orchestrator`. Owns:
  - `o.vaults map[glid.GLID]*Vault` — local vault registry (protected by
    `o.mu`, a `sync.RWMutex`).
  - `o.scheduler` — the job queue + cron runner.
  - `o.groupMgr` — handle to the multiraft `GroupManager`.
  - `o.forwarder`, `o.tierReplicator`, `o.peerConns` — cross-node I/O.
  - `o.filterSet` — compiled routing filters.
  - `o.replicaCircuit` — per-node circuit breaker for failed replication.

- **Factories** — the bundle passed to `Orchestrator.ApplyConfig` that
  contains component constructors (chunk manager factory, index manager
  factory, ingester registrations) plus cluster wiring (GroupManager,
  NodeAddressResolver, PeerConns, Logger).

### Lifecycle

- **`orch.Start(ctx)`** — start the scheduler, ingesters, and background
  loops (writeLoop, digestLoop, retention sweep, rotation sweep). Returns
  when startup completes.

- **`orch.Stop()`** — cancel all background goroutines, wait for in-flight
  writes, close chunk managers. Called via `t.Cleanup` in tests.

- **Shutting down (`o.phase`)** — a `tierfsm.Phase` atomic flag. When set,
  `fireAndForgetRemote` skips remote dispatches; drain and replication
  short-circuit. Used to suppress benign errors during shutdown.

- **Vault readiness** — a vault on this node is "ready" iff it has at least
  one local tier AND every local tier's `IsFSMReady()` callback returns
  `true` (i.e. the vault-ctl Raft has applied at least one log entry on
  this node, or restored from a snapshot). Canonical definition in
  [`vault_readiness.go`](../backend/internal/orchestrator/vault_readiness.go).
  Checked by every read and write path before touching tier state.

### Scheduler & Jobs

- **Scheduler** — the cron/queue subsystem. Runs scheduled jobs
  (retention sweep, rotation sweep, archival sweep) and one-shot tasks
  (post-seal compression/indexing, catchup replication).

- **Job** — a scheduled or one-shot unit of work. Proto
  [`Job`](../backend/api/proto/gastrolog/v1/job.proto) tracks
  status (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`) and kind
  (`TASK`, `SCHEDULED`).

- **`WaitIdle(timeout)`** — scheduler method used in tests to drain async
  post-seal work before asserting chunk state.

### Policies

- **Rotation policy** (`RotationPolicyConfig`) — when to seal the active
  chunk. Shapes: `MaxBytes`, `MaxAge`, `MaxRecords`, `Cron`, or a
  composite. Per-tier: `tier.RotationPolicyID` points at a policy.

- **Retention rule** (`RetentionRule`) — per-tier, per-policy: "when to
  expire sealed chunks". An expire can be a **delete** (drop the chunk)
  or an **eject** (forward records to another route before deleting).

- **Retention policy** (`RetentionPolicyConfig`) — named, reusable
  policy referenced by `RetentionRule`.

### Core state transitions (verbs)

- **Seal** — finalize an active chunk; it becomes immutable and enters the
  post-seal pipeline (compression → indexing → replication catchup).

- **Rotate** — open a new active chunk after sealing the old one.

- **Transition** — move a sealed chunk's records from one tier to the
  next in the vault chain. Driven by retention rules with the `eject`
  action. Uses a receipt protocol so the destination durably replicates
  the records before the source deletes its copy. See
  [`gastrolog-4913n`](https://... — closed).

- **Expire** — delete a chunk that has aged out according to retention.

- **Reconcile** — compare the tier FSM manifest against local disk;
  delete sealed chunks on disk that aren't in the manifest (orphan
  cleanup) and replicate manifest chunks that are missing locally.

- **Drain** — move all of a tier's (or a vault's) chunks to another
  node, then remove the local instance. Used for decommission.

- **Catchup** — replicate sealed chunks from a leader to a follower that
  just joined or restarted. Distinct from live replication (which happens
  per-record).

---

## 6. Replication & Forwarding

Cross-node data movement. Three distinct mechanisms; do not confuse them.

- **Vault-ctl Raft replication** — chunk metadata (create/seal/delete/upload
  events). Flows through hraft via the multiraft transport. Committed only
  when a majority acks. This is the **authoritative** metadata replication.

- **Tier replication** — actual chunk content (records) from a tier leader
  to its tier followers. Uses ordered streams per `(tierID, followerNodeID)`
  via the **TierReplicator**. Does NOT use Raft; uses gRPC streams with
  application-level acks.

- **Cross-vault record forwarding** — at ingestion time, a record that
  matches a vault owned by another node is forwarded via the
  **RecordForwarder** (batched, fire-and-forget) or **Forwarder**
  (synchronous, ack-gated).

### The actors

- **TierReplicator** — per-node manager of replication streams to follower
  tiers. Methods: `AppendRecords`, `SealTier`, `ImportSealedChunk`,
  `DeleteChunk`. Always invoked on the **tier leader**.
  [`cluster/tier_replicator.go`](../backend/internal/cluster/tier_replicator.go).

- **RecordForwarder** — per-node ingestion forwarder. Batches records by
  destination node; uses long-lived client-streaming RPCs with backpressure.
  [`cluster/record_forwarder.go`](../backend/internal/cluster/record_forwarder.go).

- **Forwarder** — simpler, synchronous per-command forwarder used by
  raftstore (config-Raft apply forwarding) and ack-gated paths.
  [`cluster/forwarder.go`](../backend/internal/cluster/forwarder.go).

- **VaultApplyForwarder** — forwards vault-ctl Raft applies from a
  follower node to the current vault-ctl leader. Used when `PeerConns` is
  wired. [`cluster/vault_apply_forwarder.go`](../backend/internal/cluster/vault_apply_forwarder.go).

- **TierApplyForwarder** — forwards a tier-FSM command (wrapped in
  `OpTierFSM`) to the vault-ctl leader. Same shape as vault forwarder,
  different wrapping. [`cluster/tier_apply_forwarder.go`](../backend/internal/cluster/tier_apply_forwarder.go).

### The verbs

- **`fireAndForgetRemote`** — called from `ingest()` and `AppendToTier`:
  dispatches per-follower replication goroutines. MUST be called
  *outside* `o.mu`; holding the lock across this call cascades into
  cluster-wide deadlock on a paused peer
  ([`gastrolog-5oofa`](../backend/internal/orchestrator/reliability_orch_test.go)).

- **Replica backoff circuit breaker** — `o.replicaCircuit`: per-node
  `failures`/`skipUntil` state. After consecutive failures to a node,
  subsequent replication attempts skip that node for an exponentially
  growing window (2s → 4s → 16s → ...).

- **Replica count** — how many nodes are known to have this chunk
  (leader + caught-up followers). Surfaced on `ChunkMeta.ReplicaCount`.

- **Transition receipt** — the durable ack from the destination tier
  that it has received and replicated a transitioned chunk's records.
  Source tier keeps `TransitionStreamed = true` until the receipt lands,
  then deletes the source copy.

### Connection management

- **PeerConns** — shared gRPC connection pool. One connection per peer
  node; reused by all callers (Broadcaster, RecordForwarder, SearchForwarder,
  TierReplicator). `Invalidate(nodeID)` drops a stuck connection so the
  next call re-dials.

- **MultiRaftTransport** — per-node multiplexing transport for Raft RPCs.
  Distinct from PeerConns: PeerConns is for application RPCs,
  MultiRaftTransport is only for `AppendEntries`, `RequestVote`, etc.

---

## 7. Observability

How the cluster reports what it's doing to itself, to operators, and to the UI.

- **Broadcaster** — per-node push mechanism for peer-to-peer state: stats,
  jobs, alerts. **Fire-and-forget**: `Send()` returns immediately; per-peer
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
  peer. Has a TTL (20s by default = 4× broadcast interval); entries older
  than TTL are treated as offline.
  [`cluster/peerstate.go`](../backend/internal/cluster/peerstate.go).

- **PeerJobState** — parallel cache for `NodeJobs` broadcasts; aggregates
  active jobs from all peers.
  [`cluster/peerjobstate.go`](../backend/internal/cluster/peerjobstate.go).

- **PeerByteMetrics** — cumulative wire bytes sent/received to each peer,
  with rate calculation and sparklines. Used by the inspector's network
  section and by replication-throughput diagnostics.

- **VaultRouteStats** / **PerRouteStats** — per-vault and per-route
  counters: `Matched`, `Forwarded`, `Ingested`, `Dropped`. Surfaced in
  `NodeStats` and aggregated cluster-wide.

- **AlertCollector** — per-node bounded store of alerts (`AlertSeverity`:
  `WARNING`, `ERROR`). Alerts have a stable key for dedup and auto-clear;
  included in each NodeStats broadcast.

- **SystemAlert** — one alert: `ID`, `Severity`, `Source`, `Message`,
  `FirstSeen`, `LastSeen`. Designed to be keyed ("alert X for reason Y on
  node Z") so repeated identical alerts don't accumulate.

---

## 8. Identity & Config

Operator-controlled state, user authentication, and the Raft-replicated
config store.

### State model

- **System** — `system.System`: the top-level cluster state. Two halves:
  - **Config** — operator-controlled (vaults, tiers, filters, routes,
    ingesters, policies, cloud services, server settings).
  - **Runtime** — cluster-managed (node membership, tier placements,
    ingester assignments, setup wizard dismissal).

  Both are replicated via the system Raft group.

- **Store** (`system.Store`) — the read/write interface over `System`.
  Two implementations: in-memory (`sysmem`, for tests) and Raft-backed
  (`raftstore`, for production).

- **StoreProxy** — a wrapper that intercepts config writes to emit
  change notifications via the dispatcher.

### Server settings

Live on `Config` directly (not as entities):

- **AuthConfig** — JWT secret, token duration, password policy.
- **QueryConfig** — query timeout, max follow duration.
- **SchedulerConfig** — scheduler cadence and concurrency.
- **TLSConfig** — ACME settings for external API TLS.
- **LookupConfig** — external lookup table configuration (HTTP, SQLite).
- **ClusterConfig** — broadcast interval override; other cluster tunables.
- **MaxMindConfig** — GeoIP database location.

### Authentication

- **User** — identified by username; has `Role`, password hash,
  zero-or-more refresh tokens. Managed via `SystemCommand_CreateUser`,
  `UpdatePassword`, etc.

- **Role** — coarse permission set. Today: `admin`, `operator`,
  `viewer` (exact set is in
  [`auth/roles.go`](../backend/internal/auth/roles.go)).

- **JWT** (access token) — short-lived bearer token. Carries claims:
  `sub` (username), `role`, `exp`, `iat`.

- **RefreshToken** — long-lived credential, stored in the config Raft.
  Used to mint a new JWT without re-entering password. Expires on
  password change or logout via `DeleteUserRefreshTokens`.

- **Cluster TLS** — mTLS material (`CA`, `Node cert`, `Node key`)
  generated at cluster-init. Used exclusively for intra-cluster gRPC.
  Separate from external API TLS.

### Commands & FSM

- **SystemCommand** — the Raft log entry for config/runtime mutations.
  A oneof of Put/Delete variants per entity type. Applied by
  [`system/raftfsm/fsm.go`](../backend/internal/system/raftfsm/fsm.go).

- **Notification** — the structured event emitted when a config apply
  completes. Consumed by the dispatcher to drive orchestrator changes
  and to push WatchConfig events to subscribers.

### IDs

- **GLID** — GastroLog ID: 16 bytes, UUIDv7-shaped, lexicographically
  sortable by creation time. Every entity (vault, tier, user, chunk, node,
  storage) has a GLID. 26-character base32hex string form is canonical in
  URLs, logs, and user-facing surfaces.
  [`backend/internal/glid/glid.go`](../backend/internal/glid/glid.go).

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
| archived         | cold, glacier-tier| "Archived" is the canonical flag; cloud storage-class is orthogonal.|
| vault-ctl Raft   | tier Raft         | Post-`gastrolog-5xxbd`, tier FSMs are sub-FSMs; there is no per-tier Raft group. |
| ingester         | source, collector | "Ingester" is the proto name; "source" leaks from UI copy.          |
| route            | pipeline (at ingest) | Ingestion "route" ≠ query "pipeline"; use "route pipeline" or "ingestion pipeline" to bridge. |
| record           | event, message    | "Event" conflates with `EventID`; "message" conflates with ingester internals. |
| applied index    | committed-and-applied | Precision: commit = quorum-persisted; applied = FSM-processed.  |
| node             | server, host      | "Node" is the cluster-member canonical. Reserve "server" for `cluster.Server` (the gRPC server component). |
| peer             | remote node       | "Peer" is relative; there is no absolute "remote".                 |

### Timestamp conventions

Every record carries three timestamps, and they mean different things:

- **SourceTS** — when the upstream system says the event happened. External,
  potentially untrustworthy, but usually what the user cares about when
  querying ("show me errors from 10:00 to 10:15 yesterday").

- **IngestTS** — when THIS node's ingester received the message. Always
  monotonic per ingester (thanks to `IngestSeq`); used for disambiguation
  in `EventID`.

- **WriteTS** — when the record was appended to its chunk. Unique per record
  across the cluster (see `ChunkID + Pos`).

For user-facing features, **default to SourceTS**; fall back to IngestTS
if SourceTS is absent or obviously bogus. For internal ordering
(GetContext, catch-up, indexes), use **WriteTS**.

### ID conventions

- Every entity ID is a `GLID`. No exceptions.
- String form is 26-character base32hex. Always lowercase in logs; UI may
  prettify with middle-ellipsis if too long (`GLID Shortener`).
- `glid.Nil` is the zero value; treat it as "not set" consistently. Never
  as "wildcard" — use an explicit parameter for wildcards.

### Error conventions

Error values that cross bounded contexts:

- `ErrVaultNotFound` — the vault doesn't exist on this node.
- `ErrVaultNotReady` — vault exists but tier FSM hasn't applied enough
  log entries (or hasn't restored). Canonical definition in
  [`vault_readiness.go`](../backend/internal/orchestrator/vault_readiness.go).
- `ErrChunkNotFound` / `ErrActiveChunk` / `ErrChunkTombstoned` — chunk
  manager errors with specific meanings. Never conflate.
- `ErrNoChunkManagers` — this node hosts no tiers for any vault.
- `ErrTierDraining` — tier is mid-drain; writes are rejected.

### What "replication" means in which context

- **Record replication** (tier layer) — copying record bytes from the
  tier leader to tier followers. Done by `TierReplicator`. Acked by
  a per-tier application-level ack; bounded by `ForwardingTimeout`.
- **Metadata replication** (vault-ctl Raft) — propagating chunk-create /
  seal / delete / upload events. Done by hraft via multiraft transport.
  Acked by Raft majority; bounded by `ReplicationTimeout`.
- **Apply forwarding** — follower → leader forwarding of a write command.
  Done by `VaultApplyForwarder`, `TierApplyForwarder`, or (for config
  Raft) `Forwarder`. This is not replication; it's routing to the node
  that CAN do the replication.

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
