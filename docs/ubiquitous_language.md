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
9. [Pipeline](#9-pipeline) — the fan-out write path from ingest to sealed chunk.

At the end, a [Consistency rules](#consistency-rules) section names every known
synonym pair and picks the canonical side, plus conventions for timestamps, IDs,
and cross-context identifiers.

---

## 1. Storage

The physics of the system: bytes on disk (or in memory, or in cloud), organized
for append-heavy write patterns and time-ordered reads.

### Aggregates

- **Vault** — a named, versioned container for log records. The unit of independent
  storage and the only abstraction over the chunk layer. A vault carries its full
  storage shape directly: a **type** (memory, file, jsonl), a **storage class**, an
  optional **cloud service** binding, a **rotation policy**, **retention rules**, a
  **replication factor**, and cache tuning. Operators create and delete vaults; the
  system manages placements. Declarative config in
  [`system.VaultConfig`](../backend/internal/system/vault.go); per-node runtime in
  [`VaultInstance`](../backend/internal/orchestrator/vault_instance.go) — the
  bundle of chunk manager, index manager, query engine, and Raft callbacks.

- **Chunk** — an immutable, self-contained segment of records. One active chunk
  per vault per node accepts new records; it is **sealed** when a rotation
  policy fires (or manually). Sealed chunks can be indexed and
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

- **ChunkMeta** — the stats bag for a chunk: sealed/cloud-backed
  flags, record count, byte counts, timestamps (`WriteStart/End`, `IngestStart/End`,
  `SourceStart/End`), retention-pending flag, frame count for cloud-backed chunks.

### States a chunk passes through

- **Active** — open for writes; lives only on the vault leader.
- **Sealed** — immutable; eligible for indexing/replication/cloud upload.
- **Chunk seal** — the cluster-wide fact: `CmdSealChunk` applied, record
  membership frozen. Happens exactly once per chunk.
- **Copy seal** — one home's completed local copy (GLCB build or replica
  pull) of a sealed chunk. There are up to RF copy seals per chunk, and a
  LATE copy seal — a rejoining node catching up — is normal operation, not
  a re-seal. The inspector's per-node seal pips render copy seals.
- **Cloud-backed** — record bytes live in S3/Azure/GCS, not local disk; marked
  with `CloudBacked = true` in `ChunkMeta`. A cloud-backed vault is a file vault
  with `CloudServiceID` set; there is no separate "cloud" vault type.
- **Archived** — in an offline cloud storage class (e.g. Glacier). Unreadable
  until `Restore` completes. Tracked via `Archived = true`.
- **Retention-pending** — marked in the vault FSM for deletion on the next sweep.

### Physical storage

- **FileStorage** — a directory on a node's disk, identified by a GLID, tagged
  with a **StorageClass**. A node can have many file storages (different disks,
  different performance classes). Carries the disk-guard free-space thresholds
  (`DiskFreeWarn`/`DiskFreeFloor`, size-or-percent expressions; empty inherits
  the node defaults) — moved here from `VaultConfig` (gastrolog-9akebz): the
  thresholds guard the volume, not the vaults sharing it. A storage below its
  floor puts every vault placed on it into admission refuse (cause
  `STORAGE_DISK_PROTECT`). [`system.FileStorage`](../backend/internal/system/storage.go).

- **NodeStorageConfig** — the list of file storages on one node. Runtime state
  (not operator-authored). [`system.NodeStorageConfig`](../backend/internal/system/storage.go).

- **StorageClass** (`uint32`) — non-zero integer grouping storages by performance
  or role. A vault's `StorageClass` selects which FileStorage on each node hosts
  its chunks.

- **CloudService** — a cluster-wide cloud endpoint (S3, Azure, GCS) with
  optional archival lifecycle. [`system.CloudService`](../backend/internal/system/storage.go).

- **Frame** — a seekable zstd block within a cloud-backed chunk. `NumFrames` records
  how many frames a cloud-backed chunk was uploaded in; range queries seek to a
  specific frame to avoid pulling the whole blob.

### Placement

- **VaultPlacement** — a mapping of a vault to a specific `FileStorage.ID` on a
  specific node, with a `Leader` flag. A vault normally has N placements (N =
  replication factor). [`system.VaultPlacement`](../backend/internal/system/storage.go).

- **SyntheticStorageID** — placeholder ID used when placement doesn't reference
  a real FileStorage (memory-vault on a node without file storages). Format:
  `node:<nodeID>`.

---

## 2. Ingestion

Everything between "bytes arrive from the outside" and "record is appended to
a vault's active chunk".

### Aggregates

- **Ingester** — a protocol adapter that receives external input and produces
  `IngesterMessage` envelopes. Types include `chatterbox` (synthetic), `syslog-udp`,
  `http`, `kafka`, `file`, `jsonl`, and `self` (self-ingesting GastroLog logs).
  The `Ingester` contract and its message/factory types live in
  [`pipeline/ingestion`](../backend/internal/pipeline/ingestion) — ingesters are
  "dumb" and depend only on those contracts, never on the orchestrator.
  Declarative config: [`system.IngesterConfig`](../backend/internal/system/vault.go).

- **Singleton ingester** — an ingester that must run on exactly one node at a
  time (e.g. a Kafka consumer with a fixed partition). The placement manager
  assigns it; if the assigned node dies, another takes over. Has a
  `Singleton = true` flag.

- **Parallel ingester** — runs on every node in `NodeIDs` simultaneously. Each
  instance maintains a per-ingester sequence counter so records don't
  collide.

### Domain events

- **IngesterMessage** — the envelope an ingester emits for a raw message
  entering the pipeline. Holds the raw bytes, ingester-specific metadata (syslog
  priority, HTTP headers), ingest timestamp, and ingester identity. The ingestion
  manager mints an EventID and forwards an **IngestMessage** (the digestion-queue
  element) downstream; the digester turns that into a `Record`.

- **Digester** — the parser for one ingester type. Knows how to decode its
  input format into structured fields and attributes. Examples:
  syslog-rfc5424 digester, apache-access-log digester, JSON digester.
  Digestion happens on the node that received the message, before routing.

### Routing

- **Route** — a row in the cluster-wide routing table:
  `{priority, name, stages, destinations, distribution, enabled}`.
  Routes are evaluated in priority order (lower fires first; name as
  deterministic tiebreaker) with **first-match-wins** semantics. No
  match → drop silently. [`system.RouteConfig`](../backend/internal/system/vault.go).

- **Stage** — one step in a route's pipeline (`RouteConfig.Stages`).
  Today's only variant is `MatchStage{expression}`, which gates the route
  on a boolean match expression. Future stage kinds (enrich, redact,
  sample, fork, route_by_field — gastrolog-5e85x) plug into the same
  oneof without re-shaping the proto.

- **Match expression** — a query-language predicate evaluated against
  each record's attributes. Special forms: `"*"` = match everything,
  empty string = match nothing (route enrolled but never fires —
  useful for muting). An explicit catch-all at the lowest priority replaces
  any "rest" semantics.

- **Synthetic attributes** — reserved-prefix attributes (`_source`,
  `_ingester`, `_vault`, `_reason`) overlaid on a record's real
  attrs at routing-evaluation time only. The overlay is computed
  per call and never mutates the record's persisted attrs. They
  unify source-predicate and content-filter into a single expression
  language. Routes match on `_source = "ingest"`,
  `_source = "retention" AND _vault = "<id>"`, etc. User records
  carrying `_`-prefixed attrs collide with this namespace and aren't
  supported.

- **Source kinds** — the canonical wire values of the `_source`
  synthetic: `"ingest"` (records arriving from an ingester) and
  `"retention"` (records the retention engine is feeding back through
  the routing table for a vault that's draining). Other kinds may be
  added by future stages.

- **Distribution mode** — how a single route fans matched records
  across its destinations:
  - `fanout` — write to all destinations (default).
  - `round-robin` — rotate through destinations (load-balance).
  - `failover` — try first; on failure, try next.

- **RouteSet** — the compiled, priority-sorted routing table on a
  node. Reloaded when routes, vaults, or placements change.

- **MatchResult** — the output of route evaluation: `VaultID`,
  optional `NodeID` (for cross-node forwarding), `RouteID`,
  `Distribution`. A single matched route returns one MatchResult
  per destination.

- **ValidateExpression RPC** — read-only, node-local check that an
  expression parses and uses only supported predicates. Drives the
  route filter editor's live feedback; uses the same compile path
  as `PutRoute` so editor verdict and save verdict cannot disagree.

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
  peer nodes and merges results. Used by Search, Histogram, Explain,
  GetContext, GetFields.

---

## 4. Cluster Coordination

Nodes agreeing on what the cluster believes, via Raft.

### Identity

- **Node** — one running GastroLog process. Identified by a `NodeID` (GLID).
  A node hosts vault instances, runs ingesters, serves queries, and
  participates in Raft groups.

- **NodeConfig** — the declarative (`ID`, `Name`) record for a node. Lives
  in the cluster-ctl Raft's config. The `Name` is for humans; `ID` is canonical.

- **Peer** — another node, from this node's perspective. "Peer" is always
  relative; the same node is "local" to itself and "peer" to everyone else.

### Raft layers

GastroLog runs **multiple Raft groups** per node, multiplexed over a single
gRPC transport:

- **Cluster-ctl Raft** (formerly "system Raft", "config Raft", "cluster
  Raft" — all retired) — one group per cluster. Replicates `system.Config`
  (operator-authored) and `system.Runtime` (cluster-managed). Every node is
  a voter. Leader changes propagate config via FSM apply; dispatcher drives
  downstream effects. Wire surface: `cluster_ctl_raft_index` on
  GetSystem/WatchSystem/SettingsMutationEcho.

- **Vault Control-Plane Raft** (a.k.a. "vault-ctl Raft", "vault-ctl group") —
  one group *per vault*. Replicates that vault's chunk metadata across all
  nodes participating in the vault. Uses the `vaultraft.FSM` whose state is
  a map of **instance FSMs** — one sub-FSM per vault instance, namespaced by
  `OpVaultChunkFSM` commands. See
  [`vault-control-plane-architecture.md`](./vault-control-plane-architecture.md)
  for the design rationale.

- **Vault chunk FSM** (`vaultctlfsm.FSM`) — the per-vault sub-state-machine
  inside a vault-ctl FSM. Holds the **manifest** of chunks for one vault: each
  chunk's metadata (sealed? retention-pending?), tombstones, and
  pending-delete receipt state.

- **Manifest** — the vault FSM's set of chunk entries. The authoritative
  answer to "which chunks should exist in this vault?" Compared against
  local disk in the reconcile sweep; disagreement means orphan cleanup.

### Raft primitives (hashicorp/raft vocabulary)

- **Term** — logical clock; increments on every election.
- **Log** — ordered sequence of entries. ManifestEntry types: `LogCommand` (goes
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

- **VaultPlacement** — covered under [Storage](#1-storage); also a cluster
  concept because the placement manager (in cluster-ctl Raft) decides which
  nodes host each vault.

- **Ingester placement** — the singleton-ingester assignment map in
  `system.Runtime`. The placement manager picks an alive node per singleton
  ingester; failover is automatic.

- **Placement manager** — the subsystem that owns placement decisions. Runs
  on the cluster-ctl Raft leader, reacts to node join/leave, vault create/delete,
  and ingester changes.

- **Dispatcher** — the subsystem that reacts to *applied* config changes
  (from cluster-ctl Raft FSM) and drives their side effects into the local
  orchestrator (register vault, build vault instance, start ingester, etc.).

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
  - `o.forwarder`, `o.chunkReplicator`, `o.peerConns` — cross-node I/O.
  - `o.routeSet` — compiled routing table (priority-ordered, first-match-wins).
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

- **Shutting down (`o.phase`)** — a `vaultctlfsm.Phase` atomic flag. When set,
  `fireAndForgetRemote` skips remote dispatches; drain and replication
  short-circuit. Used to suppress benign errors during shutdown.

- **Vault readiness** — a vault on this node is "ready" iff it has a local
  instance AND that instance's `IsFSMReady()` callback returns `true` (i.e.
  the vault-ctl Raft has applied at least one log entry on this node, or
  restored from a snapshot). Canonical definition in
  [`vault_readiness.go`](../backend/internal/orchestrator/vault_readiness.go).
  Checked by every read and write path before touching vault state.

### Scheduler & Jobs

- **Scheduler** — the cron/queue subsystem. Runs scheduled jobs
  (retention sweep, rotation sweep, archival sweep) and one-shot tasks
  (post-seal indexing, cloud upload, catchup replication).

- **Job** — a scheduled or one-shot unit of work. Proto
  [`Job`](../backend/api/proto/gastrolog/v1/job.proto) tracks
  status (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`) and kind
  (`TASK`, `SCHEDULED`).

- **`WaitIdle(timeout)`** — scheduler method used in tests to drain async
  post-seal work before asserting chunk state.

### Policies

- **Rotation policy** (`RotationPolicyConfig`) — when to seal the active
  chunk. Shapes: `MaxBytes`, `MaxAge`, `MaxRecords`, `Cron`, or a
  composite. Per-vault: `vault.RotationPolicyID` points at a policy.

- **Retention rule** (`RetentionRule`) — per-vault, per-policy: "when do
  sealed chunks fire retention events". A fired event destroys the chunk,
  optionally streaming its records through the routing engine first
  depending on the vault's retention disposition (see below).

- **Retention policy** (`RetentionPolicyConfig`) — named, reusable
  policy referenced by `RetentionRule`.

- **Refuse** (`RetentionPolicyConfig.Refuse`, gastrolog-5yfaqj) — ONE
  boolean generalizing `MaxSize`'s refuse behavior to every bound a
  retention policy states (`MaxAge`, `MaxSize`, `MaxChunks`). Default OFF
  (operator decision: bounds are drain-first, refusal is the explicit hard
  mode — unset reads as false, a policy must opt IN explicitly); the
  consequence is that an unset-flag policy is always drain-only, never
  refusing, even one that sets `MaxSize`. Every SET parameter on a
  `refuse=true` policy is a **hard bound**: drain restores it, and refusal
  guards it while violated. `refuse=false` (unset or explicit) makes every
  set parameter a **soft bound**: drain still restores it, but refusal is
  off — the operator explicitly accepts that only the node-level disk
  guard's own floor/warn bands backstop the vault while violated. Not
  per-parameter flags (three knobs of ceremony) and not paired
  refuse-values per dimension (the two-fields-per-concept split the
  `MaxSize` field combine killed, see below) — one flag per policy.
  - **Min-per-kind resolution**: a vault's effective bound, per KIND
    (age/size/count), is the min over every attached policy that states
    that kind. Refuse-eligibility follows the STATING policy's own flag —
    a vault mixing a hard and a soft policy refuses only on the hard
    one's bounds, even if the soft one's bound is the tighter (and
    therefore drain-triggering) one.
  - **Violation predicate for age/chunks** (the subtle part): normal
    operation transiently violates both between a chunk's seal and the
    next retention sweep — refusing on that transient would be pure
    flapping. A violation counts as refusal-worthy only once the
    retention runner has SWEPT AND FAILED TO CLEAR it
    (`retentionRunner.checkBoundViolations`, called at every sweep exit
    against a fresh post-sweep chunk listing) — clock-free, no streak, no
    slack duration: the sweep's own outcome, re-observed once per sweep,
    IS the predicate. `MaxSize`'s refuse check stays instantaneous
    (measured every disk-guard tick, unchanged) — it is resource-backed
    (disk fills regardless of sweep cadence), unlike age/count which are
    purely retention-policy-derived.
  - Surfaced as `VaultAdmissionCause` `AGE_BOUND` / `CHUNK_COUNT_BOUND`
    (alongside the existing `MAX_SIZE_BOUND`) and the `vault-bound-capped`
    alarm — see docs/alarm-management-design.md.

- **Max size (retention policy)** (`RetentionPolicyConfig.MaxSize`,
  gastrolog-33ul6h) — the vault's disk-claim bound, carried on a retention
  policy rather than `VaultConfig` (that field is removed; no reserved tag,
  per house rule). It means BOTH things at once — this is a corrected
  design (operator, 2026-07-19, comment c2): an earlier shape that split
  this into two fields was superseded before implementation, see the
  retention design doc's superseded-section note (docs/) for the
  measurement decisions, which still stand.
  - **Drain** (evaluated by `SizeRetentionPolicy`): drains the oldest sealed
    chunks, per the vault's disposition, once the vault's disk claim exceeds
    the bound. Scope: the chunk store retention can act on.
  - **Refuse** (evaluated by the disk guard's Admission gate; see the
    generalized `Refuse` entry above — this is its size-specific,
    instantaneous instantiation): refuses ingest admission for the vault
    once its whole local footprint (chunk store + pipeline segment
    backlog) exceeds the same bound, while the stating policy's `Refuse`
    is explicitly true (default off, so a bare `MaxSize` alone drains
    only). Scope: everything the vault holds on this node. The backstop
    while drain catches up or is deferred. The creation-default floor
    (below) stays refuse-only and unchanged, independent of any policy's
    `Refuse` flag — a default must never destroy data, so the floor never
    drains, only refuses.

  Effective per-vault REFUSE bound = min over the refuse-eligible
  (`Refuse` on) attached policies' `MaxSize`; falls back to the creation
  default (`system.DefaultVaultMaxSize`) only when NO attached policy
  states a size — that default floor is REFUSE-ONLY (it never drains),
  because a default must never destroy data. A vault whose only size
  policies are soft (`Refuse` off) has no refuse bound and no floor: the
  operator explicitly accepted drain-only, backstopped by the node-level
  guard alone. The DRAIN trigger mins over ALL stating policies regardless
  of the flag. Feeds
  `refreshVaultDiskGuards` → `orchestrator.resolveVaultSizeBound` → the disk
  guard's Admission gate; the gate mechanism itself is unchanged, only its
  config source. "Bound-only" (a size-only field with no drain) is not a
  concept: a policy that sets only `MaxSize` is simply a drain policy that
  also happens to bound the vault.

  Measures the **disk claim** (`chunk.DiskClaim`): `DiskBytes` when
  recorded (also what a cached cloud-backed chunk's cache file reports), 0
  for a cloud-backed chunk with no local copy (an evicted chunk's
  destruction frees nothing locally, so drain never selects one), otherwise
  logical `Bytes` plus index sizes. One field, one measurement, one bound —
  drain and refuse can no longer drift apart under compression the way two
  independently-set fields could (they diverged 3-4× before gastrolog-33ul6h,
  since the vault-level cap used to measure logical `Bytes` alone).

  **`DiskBytes` vs `CloudBytes`** (`ChunkMeta`, gastrolog-33ul6h) — two
  distinct currencies that must never substitute for each other. `DiskBytes`
  is always the LOCAL on-disk footprint on the responding node: for a
  cloud-backed chunk this is the warm-cache state (the cached GLCB's size
  while cached, 0 once evicted), live and per-node — it changes on upload,
  eviction, and re-warm. `CloudBytes` is the compressed cloud object's
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

- **Retention disposition** (`VaultConfig.RetentionDisposition`,
  gastrolog-18du3, extended by gastrolog-2l918) — per-vault flag
  controlling what happens to records when a retention event fires.
  Three canonical values:
  - **`delete`** (default): records drop, storage frees, the routing
    engine is never invoked. The safe default — no risk of accidental
    cascades. `transfer` cannot become the zero-config default: it
    requires a target vault, and a default must be a value the operator
    could type into the field.
  - **`route`**: records flow through the routing engine with synthetic
    `_source = "retention"` and `_vault = "<id>"`, so operator-configured
    routes can forward them to archive vaults, cold storage, etc.
  - **`transfer`** (`VaultConfig.RetentionTransferTargetVaultID`,
    gastrolog-2l918): the sealed chunk is re-homed to the target vault
    UNCHANGED — no record decode, no re-route, no re-ingest. Where
    `route` filters/re-tags/fans out and `delete` drops, `transfer`
    moves; it is the recommended primary pattern for archive/cold-
    storage vaults (route stays the tool for filtering/re-tagging).
    Mechanism: the destination's homes pull the sealed GLCB via the
    same verify-before-promote replica catch-up machinery same-vault
    replication uses (`glcb_catchup.go`), addressed at the source vault
    via `ManifestEntry.TransferSourceVaultID`; destination holder
    receipts must reach the destination's replication factor before the
    source expires its local copy (`AckChunkHolder`/receipt-protocol
    reuse — no loss window, nothing marked on the source until the
    destination confirms). The destination-side retention clock starts
    FRESH at arrival (`SealedAt` stamped on landing via the reused
    `CmdRepatriateChunk` announce-import), so a shorter destination TTL
    does not re-fire retention the moment the chunk lands; the chunk's
    own record timestamps and identity are untouched. File vaults only
    (source and target — cloud-backed and memory vaults have different
    at-rest forms); self-transfer is rejected at `PutVault`. A stalled
    or deferred transfer retains the chunk with the one-shot
    unconsumed, same as a stalled route fan-out. See
    docs/retention-transfer-disposition-design.md.

  Empty/unrecognized values resolve to `delete` via
  `VaultConfig.ResolveRetentionDisposition()`.

  Note: do not confuse the drain half of max size with the unrelated
  **Drain gate** / **Drain** verbs below (disk-guard backpressure, node
  decommission — different concepts that happen to share the word "drain").

### Core state transitions (verbs)

- **Seal** — finalize an active chunk; it becomes immutable and enters the
  post-seal pipeline (indexing → replication catchup).

- **Rotate** — open a new active chunk after sealing the old one.

- **Expire** — destroy a chunk that has aged out according to retention.
  With `disposition = delete` (default), records drop and the chunk is
  destroyed. With `disposition = route`, the records are first streamed
  through the routing engine (synthetic `_source = "retention"`), then
  the chunk is destroyed. A retention-trigger route directing records to
  another vault is how retention routing chains are expressed.

- **Disk guard** — the per-node free-space guard job (`disk_guard.go`):
  samples the node's data volumes every 15s and drives two staged gates
  plus the per-vault caps (max-size budget, backlog budget) and, since
  gastrolog-9akebz, a per-storage evaluation (`evaluateStorages`) — one
  statfs/warn-floor verdict per locally-hosted `FileStorage`, regardless
  of how many vaults share it; the floor is no longer a per-vault cap.

- **Admission gate** (`protect`) — the disk guard's outer gate: suspends
  ingest admission and catch-up pulls. Engages below the free-space
  floor, releases only above the warn band (asymmetric deadband so the
  release burst cannot re-cross the floor).

- **Drain gate** (`deferWrites`) — the disk guard's inner gate: pauses
  chunking builds, collection pulls, and retention route fan-out.
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
  lands. File vaults only (source and target); self-transfer is
  rejected at `PutVault`. See
  docs/retention-transfer-disposition-design.md (gastrolog-2l918).

- **Reconcile** — compare the vault FSM manifest against local disk;
  delete sealed chunks on disk that aren't in the manifest (orphan
  cleanup) and replicate manifest chunks that are missing locally.

- **Drain** — move all of a vault's chunks off this node, then remove the
  local instance. Used for decommission.

- **Catchup** — replicate sealed chunks from a leader to a follower that
  just joined or restarted. Distinct from live replication (which happens
  per-record).

---

## 6. Replication & Forwarding

Cross-node data movement. Three distinct mechanisms; do not confuse them.

- **Vault-ctl Raft replication** — chunk metadata (create/seal/delete/upload
  events). Flows through hraft via the multiraft transport. Committed only
  when a majority acks. This is the **authoritative** metadata replication.

- **Vault replication** — actual chunk content (records) from a vault leader
  to its followers. Uses ordered streams per `(vaultID, followerNodeID)`
  via the **ChunkReplicator**. Does NOT use Raft; uses gRPC streams with
  application-level acks.

- **Cross-vault record forwarding** — at ingestion time, a record that
  matches a vault owned by another node is forwarded via the
  **RecordForwarder** (batched, fire-and-forget) or **Forwarder**
  (synchronous, ack-gated).

### The actors

- **ChunkReplicator** — per-node manager of replication streams to follower
  vaults. Methods: `AppendRecords`, `SealVault`, `ImportSealedChunk`,
  `DeleteChunk`. Always invoked on the **vault leader**.
  [`cluster/chunk_replicator.go`](../backend/internal/cluster/chunk_replicator.go).

- **RecordForwarder** — per-node ingestion forwarder. Batches records by
  destination node; uses long-lived client-streaming RPCs with backpressure.
  [`cluster/record_forwarder.go`](../backend/internal/cluster/record_forwarder.go).

- **Forwarder** — simpler, synchronous per-command forwarder used by
  raftstore (config-Raft apply forwarding) and ack-gated paths.
  [`cluster/forwarder.go`](../backend/internal/cluster/forwarder.go).

- **VaultApplyForwarder** — forwards vault-ctl Raft applies from a
  follower node to the current vault-ctl leader. Used when `PeerConns` is
  wired. [`cluster/vault_apply_forwarder.go`](../backend/internal/cluster/vault_apply_forwarder.go).

- **VaultCtlChunkApplyForwarder** — forwards a chunk-FSM command (wrapped in
  `OpVaultChunkFSM`) to the vault-ctl leader. Same shape as the vault
  forwarder, different wrapping.
  [`cluster/vault_ctl_chunk_apply_forwarder.go`](../backend/internal/cluster/vault_ctl_chunk_apply_forwarder.go).

- **Apply-wait barrier** — the read-after-write guarantee on every
  forwarded Raft apply (config-Raft and vault-ctl alike): the leader
  returns the log index it applied, and the forwarding node blocks until
  its own local FSM has applied that index before returning. Event-driven
  — the FSM feeds an `applywait.Tracker` from its Apply path (and from
  snapshot restore), waking waiters the moment the mutation is locally
  visible; never a poll. [`applywait/applywait.go`](../backend/internal/applywait/applywait.go).

### The verbs

- **`fireAndForgetRemote`** — called from the ingest and append paths:
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

### Connection management

- **PeerConns** — shared gRPC connection pool. One connection per peer
  node; reused by all callers (Broadcaster, RecordForwarder, SearchForwarder,
  ChunkReplicator). `Invalidate(nodeID)` drops a stuck connection so the
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
  as a single "partial" badge. See gastrolog-66zrj / gastrolog-1ic07.

- **Contributing vaults** (`SearchResponse.contributing_vault_ids`) — the
  per-vault stream-health signal on a merged **search**: the remote vaults
  the search fanned out to and merged. Distinct from a contribution report
  because search is **fail-on-remote-failure** — a remote stream error
  aborts the whole search (there is no partial search), so every vault in
  this set contributed to any response the client actually receives. It is
  the positive record of the merge's cross-vault span, not a degraded
  signal; empty for a purely-local search. The pipeline aggregation path
  (`| stats`, `| timechart`) shares the same fail-hard policy — a partial
  aggregate is a wrong scalar presented as authoritative, so a failed
  remote vault fails the whole query rather than silently undercounting.
  See gastrolog-20lrg.

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
  demoted diagnostics are events. (A dedicated per-node "event journal" ring
  with its own RPC, inspector page and CLI command was built and removed on
  operator verdict in gastrolog-1m3e0d — the log pipeline already records,
  stores, and searches events; a parallel store duplicated it.)

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
  after boot is simply standing again. (Two operator-state layers were
  built and removed on operator verdict: an acknowledgment layer —
  "acknowledge/ack", acked and retained-after-clear states, an on-disk
  lifecycle journal — because awareness bookkeeping is ceremony and loud
  is safe; then operator shelving — "shelve/unshelve", bounded
  suppression with mandatory expiry — with the epic verdict "strip
  management, keep prevention": management machinery presumes the alarm
  volume the razor exists to eliminate. All those terms are retired; do
  not reintroduce them.) See `docs/alarm-management-design.md`.

- **Occurrence** — one continuous condition episode of an alarm ID in the
  suppression sense: a clear-and-return inside the delay-off window is
  the same occurrence (the alarm stays active, `FirstSeen` preserved); a
  raise after the alarm released is a fresh alarm.

- **Priority** — `alert.Priority`, the cataloged verdict per alarm type:
  `Critical` (data loss in progress or scheduled), `High` (durability or
  availability degraded, will compound), `Low` (needs attention on a human
  timescale). Replaced the old call-site-chosen `Severity`
  (Warning/Error). Priority always comes from the catalog, with zero
  exceptions — the short-lived "operator-defined alarm" category (priority
  from an operator-configured rule, retired in gastrolog-1cruar)
  modeled a feature that never existed; `retention-rate` is an ordinary
  catalog row. **Software faults** (e.g. `orchestrator-lock-leak`) are
  a class apart — defect tripwires whose response is to report, so they
  carry no priority.

- **AlertCollector** — per-node in-memory store of standing alarms with
  their suppression state (no file I/O; nothing survives restart). Alarms
  have a stable key (`typeID` or `typeID:instanceKey`) for dedup; the
  standing list (`Standing()`) is included in each NodeStats broadcast so
  the full attributed cluster list is readable from any node. (An
  "alarm flood" rate self-monitor — a per-node activation-rate meter
  raising a meta-alarm over an operator threshold — was built and removed
  on the same epic verdict as shelving; the term is retired.)

- **SystemAlert** — one alarm on the wire: `ID`, `Priority`, `Source`,
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
    separate `Filter` entity (gastrolog-4kkoo Phase 5).
  - **Runtime** — cluster-managed (node membership, vault placements,
    ingester assignments, setup wizard dismissal).

  Both are replicated via the cluster-ctl Raft group.

- **Store** (`system.Store`) — the read/write interface over `System`.
  Two implementations: in-memory (`sysmem`, for tests) and Raft-backed
  (`raftstore`, for production).

- **StoreProxy** — a wrapper that intercepts config writes to emit
  change notifications via the dispatcher.

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

- **RefreshToken** — long-lived credential, stored in the cluster-ctl Raft.
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
  sortable by creation time. Every entity (vault, user, chunk, node,
  storage) has a GLID. 26-character base32hex string form is canonical in
  URLs, logs, and user-facing surfaces.
  [`backend/internal/glid/glid.go`](../backend/internal/glid/glid.go).

---

## 9. Pipeline

The fan-out write path (`backend/internal/pipeline`, supervised by
`backend/internal/orchestrator/pipeline`): ingest → digest → route → segment →
distribute → collect → chunk. One manager per phase, each separated from the
next by a queue. Design rationale in
[the fan-out design notes](./fan-out/v3/design-notes.md); everything below
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
  segment registry entry and on chunk state). Placement says who *should*
  hold; the holder set says who *does*. `holders ⊇ homes` is a release fast
  path, never the sole gate (a dead home must not pin a segment forever).

- **Holder receipt** — the vault-ctl commit a node makes after it has pulled
  and verified bytes (`AckSegmentHolderCommand` for segments,
  `AckChunkHolderCommand` / `RevokeChunkHolderCommand` for chunks). Receipts
  are what grow the holder set — durability is proven by receipt, never
  inferred from optimistic counters.

- **RequiredHolders** — chunking's callback returning the placement member
  node IDs that must hold data before release gates open
  (`chunking.VaultConfig.RequiredHolders`, wired from the supervisor's
  `ChunkRequiredHolders`). The planner floor for chunk-holder eligibility is
  `min(2, placement size)` (`plannerMinHolders`).

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
  re-pulled from another holder.
- **head/** — whole, checksum-verified, immutable segments awaiting chunking.
  Where the recent, queryable records live (the role active chunks fill in
  the pre-pipeline architecture).

### Verbs

- **Mint** — Ingestion assigns the EventID
  (`IngesterID + NodeID + IngestTS + IngestSeq`) to an incoming message
  (`ingestion.Minter`). Identity, dedup, and order are carried by EventID
  from that point on.

- **Publish** — Distribution commits a completed segment's *metadata* to the
  vault-ctl log (`PublishCompletedSegment`), making it cluster-visible so
  Collection on each home can pull it. Metadata only — bytes move by pull.

- **Promote** — atomic rename moving a segment into the next staging area:
  `working/` → `completed/` at segment completion (Segmentation, `CompletePolicy`),
  `pre-head/` → `head/` after verification (Collection, `PromoteVerified`),
  and `completed/` → `head/` directly when the origin is itself a holder
  (Distribution; a local move, never a stream to self).

Three verbs cover segment end-of-life, one per layer — they are not synonyms:

- **Release** (vault-ctl / chunking) — drop a segment's completed-registry
  entry from the vault-ctl FSM (`ReleaseSegmentsCommand`), proposed by the
  chunking leader once the segment's records are superseded by replicated
  chunks (holder-gated). The FSM-level end of a segment's registry life;
  bytes may still exist on disk.

- **Retire** (distribution) — drop a node's in-memory tracking of a segment
  and mark it so the stranded rescan does not republish still-on-disk
  `completed/` bytes (`Manager.RetireSegments`, `vaultDist.retireSegment`).
  Node-local bookkeeping, downstream of release.

- **Purge** (paths / disk) — delete the segment's bytes from staging areas
  (`paths.PurgeCompleted` on the origin after release; `paths.PurgeHeadStaging`
  after a home materializes the sealed GLCB; `paths.PurgeSegmentStaging` for
  all three areas). The physical end of a segment's life, ordered strictly
  after its records survive in a replicated chunk — a returning node gets
  those records via chunk replication, never a segment re-pull.

### Chunk build

- **GLCB** — GastroLog Chunk Blob: the sealed-chunk container format
  (`backend/internal/chunk/glcb`, `data.glcb`). The universal sealed-chunk
  artifact — local-only file vaults seal into GLCB, and the same blob is the
  cloud upload unit for cloud-backed vaults.

- **Assignment log** — the vault-ctl log view Collection rolls
  (`collection.LogReader.Roll` → `AssignedSegment`) to learn which published
  segments this home should hold; it then pulls the ones it lacks and
  commits holder receipts.

- **Chunk build cursor** — per-segment progress in EventID order: how far
  prior chunks consumed that segment's index. Vault-ctl holds cursors + chunk
  budget; the next build k-way-merges from those positions until the budget
  is reached, then commits updated cursors.

- **Segment span** — `(segmentID, startRecord, count)` naming a slice in
  EventID order. Equivalent to cursor deltas for one build; **discovered during
  merge**, not precomputed from segment metadata alone.

- **Chunk record budget** — stop the merge after N records (rotation-policy
  `MaxRecords` shape). Deterministic cut axis over the merge walk.

- **Chunk byte budget** — stop when accumulated bytes (pinned unit) reach the
  limit (`MaxBytes` shape). Same category as record count over the merge walk.

- **Chunk time cut** — schedule-based (`Cron`) can work for chunking when
  committed on vault-ctl; age-since-chunk-open (`MaxAge` on active chunks) does
  not map to the segment→chunk build model.

- **Sealed manifest** — after `SealOpenChunkManifest`, the frozen segment-ref list
  on vault-ctl awaiting per-home GLCB build (`sealed_manifest` in FSM snapshot).
  Not the same word as V2 sequenced-write **materialization** (spool → chunk).

- **GLCB quarantine** — an existing-but-unreadable sealed GLCB is renamed
  aside to a `data.glcb.corrupt` sibling (never silently deleted or silently
  rebuilt over) so the canonical path reads as **missing** and the ordinary
  missing-GLCB machinery heals the chunk: rebuild from source segments when
  they are still available, otherwise a peer re-pull via the GLCB catch-up
  sweep. A per-vault operator alert stays up until every quarantined chunk
  heals; healing removes the quarantine file
  (`chunking/glcb_corrupt.go`, gastrolog-687m11).

### Stage counters

Per-vault, per-node **monotonic** counters for the discrete pipeline-stage
milestones — the events operators used to grep from `cluster.log`, now
first-class and cross-node (`VaultStats` fields, `NodeStats` broadcast; the
inspector's *Pipeline stages* panel and `gastrolog cluster throughput` render
them; gastrolog-4r784a). Each milestone is counted **exactly once by its
owner** (origin / home / leader) on the node where it happened, so cluster
totals are the plain sum across nodes — never double-counted, never inferred
from optimistic route/ingest counters. Rates (segments completed/published,
chunks built/sealed) come from the stats collector's rolling windows over the
totals, the same server-side mechanism as append/collected/sealed throughput —
never accumulated client-side.

Canonical milestone verbs (reuse these names; do not coin synonyms):

- **Completed** — a working segment promoted `working/` → `completed/` (origin).
- **Published** — a completed segment's metadata committed to vault-ctl (origin).
- **Released** — a segment dropped from the vault-ctl registry (leader),
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
| alarm            | alert             | An alarm requires an operator action; "alert" was applied indiscriminately to alarms, events, and metrics alike, which is what let non-actionable diagnostics into the alarm list. The phase 2 registry landed the alarm vocabulary in new identifiers (`AlarmType`, `Raise`, `Priority`); remaining `alert`-named identifiers (package `alert`, `SystemAlert`, `AlertCollector`) phase out with the lifecycle/UI phases. |
| archived         | cold              | "Archived" is the canonical flag; cloud storage-class is orthogonal. |
| vault-ctl Raft   |                   | One Raft group per vault, authoritative for that vault's chunk metadata. Follows the `{scope}-ctl` naming pattern for control-plane Raft groups. |
| cluster-ctl Raft | system Raft, config Raft, cluster Raft | One Raft group per cluster, authoritative for cluster-wide configuration. Pairs with `vault-ctl Raft` to form the `{scope}-ctl` pattern. The on-disk Raft group ID and type names were renamed from `system` → `cluster-ctl` in gastrolog-5eu6v. |
| instance FSM     |                   | Per-vault chunk-metadata sub-FSM in `vaultctlfsm`. |
| vault replication |                  | Record streams from leader to follower, per vault. |
| ingester         | source, collector | "Ingester" is the proto name; "source" leaks from UI copy.          |
| route            | pipeline (at ingest) | Ingestion "route" ≠ query "pipeline"; use "route pipeline" or "ingestion pipeline" to bridge. |
| record           | event, message    | "Event" conflates with `EventID`; "message" conflates with ingester internals. |
| applied index    | committed-and-applied | Precision: commit = quorum-persisted; applied = FSM-processed.  |
| node             | server, host      | "Node" is the cluster-member canonical. Reserve "server" for `cluster.Server` (the gRPC server component). |
| peer             | remote node       | "Peer" is relative; there is no absolute "remote".                 |
| retention event  | retention action, expire/eject/transition | A fired retention event destroys the chunk and (per the vault's retention disposition, gastrolog-18du3) optionally streams the records through the routing engine first. The "what" lives on routes; the "whether to invoke routes at all" lives on the disposition. |
| match expression | filter, FilterConfig | Match expressions are inlined on `RouteConfig.Stages`; the named-`Filter` entity is gone (gastrolog-4kkoo Phase 5). UI label: "Match expression" on the route editor. |
| route table      | filter set        | The runtime structure is a priority-ordered `RouteSet`, not a per-vault `FilterSet`. First-match-wins, no catch-the-rest. Renamed through the whole stack in gastrolog-5sdzfv: proto `route_table_active` / NodeStats `route_stats_route_table_active`, `Orchestrator.IsRouteTableActive`, UI banner "Route table is inactive". |
| synthetic attribute | source predicate, RouteSource | Source/content predicates unify via `_source`/`_ingester`/`_vault`/`_reason` overlays at routing-eval time. |
| retire (segment, distribution) | forget | Distribution's node-local drop of segment tracking was called `forgetSegment` while the exported entry point was `RetireSegments`; one verb per meaning (gastrolog-34zx9y). See [Pipeline](#9-pipeline) for the release / retire / purge distinction. |
| glcb (container-format package) | chunk/cloud | The GLCB container package lived at `chunk/cloud`, but GLCB is universal — local-only vaults seal into it too. The package is `chunk/glcb`; "cloud" names only genuine object-storage interaction (blobstore, cloud-backed cache, cloud upload) (gastrolog-34zx9y). |
| storage disk protect | vault disk protect | The disk-guard free-space thresholds moved from `VaultConfig` to `system.FileStorage` (gastrolog-9akebz): a below-floor storage protects every vault placed on it, not one vault's own threshold. Renamed through the stack: proto enum `VAULT_ADMISSION_CAUSE_VAULT_DISK_PROTECT` → `..._STORAGE_DISK_PROTECT`, `orchestrator.ErrVaultDiskProtect` → `ErrStorageDiskProtect`, `VaultAdmissionCauseVaultDiskProtect` → `VaultAdmissionCauseStorageDiskProtect`, NodeStats `disk_protected_vault_ids` → `storage_protected_vault_ids`, `PeerState.VaultDiskProtected(Nodes)` → `VaultStorageProtected(Nodes)`. |
| complete (segment lifecycle) | close | "Close" is overloaded: writer shutdown vs the segment lifecycle event (working/ → completed/). The rotation trigger is `segmentation.CompletePolicy` and a segment COMPLETES; reserve Close for genuine resource shutdown (`Close()` methods, closed writers) (gastrolog-34zx9y). |
| routed (routing counter) | ingested (at routing) | The counter that was `Ingested` on routing stats counts records ENTERING ROUTING, not ingestion — counter provenance matters when proving loss. Whole chain renamed: `Routed` = entered routing, `Matched` = matched a route and fanned out (proto `total_routed`/`total_matched`, NodeStats `route_stats_*`, UI labels) (gastrolog-34zx9y). "Ingested" stays only on genuine ingester counters (`MessagesIngested`, `BytesIngested`). |
| unmatched (routing counter) | dropped (at routing) | "Dropped" named two different quantities. `Unmatched` = matched no route, an intentional counted drop (proto `total_unmatched`, NodeStats `route_stats_unmatched`, UI label "Unmatched"); the invariant is `Routed = Matched + Unmatched`. "Dropped" is reserved for delivery drops: `StatsSnapshot.PerVaultDropped` counts already-matched records whose fan-out delivery to a vault failed — a per-vault sub-account of `Matched`, never part of the routed sum (gastrolog-5sdzfv). |

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
- `ErrVaultNotReady` — vault exists but the vault FSM hasn't applied enough
  log entries (or hasn't restored). Canonical definition in
  [`vault_readiness.go`](../backend/internal/orchestrator/vault_readiness.go).
- `ErrChunkNotFound` / `ErrActiveChunk` / `ErrChunkTombstoned` — chunk
  manager errors with specific meanings. Never conflate.
- `ErrNoChunkManagers` — this node hosts no vaults.
- `ErrAlreadyRunning` — Run/Start was called on a component that is already
  running (every pipeline manager, the pipeline supervisor, the orchestrator).
- `ErrNotRunning` — the operation requires a running component (Stop before
  Start, submit after stop). Strictly means not-running; the Run-called-twice
  case is `ErrAlreadyRunning`, never this (gastrolog-34zx9y).

### What "replication" means in which context

- **Record replication** (vault layer) — copying record bytes from the
  vault leader to vault followers. Done by `ChunkReplicator`. Acked by
  a per-vault application-level ack; bounded by `ForwardingTimeout`.
- **Metadata replication** (vault-ctl Raft) — propagating chunk-create /
  seal / delete / upload events. Done by hraft via multiraft transport.
  Acked by Raft majority; bounded by `ReplicationTimeout`.
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
