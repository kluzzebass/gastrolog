# Vault redesign — tiers go away, vaults are the only storage unit

This document captures the architectural decisions that frame a refactor
of GastroLog's storage model: removing the tier abstraction entirely.
It is not the full plan (order of operations, audit, code-to-delete);
it is the foundation those things will be built on. Each decision is
stated, justified, and followed by the implications that fall out.

The refactor is feasible because GastroLog has zero production
deployments. Atomic, single-coordinated change against `main` rather
than a multi-release deprecation. State of `main` either has tiers or
it doesn't.

---

## 1. Vaults are the only storage unit. Tiers do not exist.

**Decision.** A vault is the unit of independent storage and the only
abstraction over the chunk layer. There are no tiers, no tier chains,
no tier types, no tier transitions. The cluster is organized as
vault-ctl Raft per vault, which already reflects this — that level of
the architecture is correct as-is.

**Rationale.** Tiers were a model fiction. They produced a startling
amount of compensation logic — cross-tier dedup, `TransitionStreamed`,
`ImportToTier`, leader-tier query engine, tier sub-FSM,
placement-per-tier, the tier-vs-vault ID space dance — much of it to
solve problems tiers themselves created. Most of the bugs that have
required attention in the storage layer are bugs *of* the tier model,
not bugs in any specific tier feature. A single-tier vault has none of
these issues; making single-tier the only kind of vault eliminates the
problem class wholesale.

**Implications.** `tier_id` deleted everywhere. `TierConfig`,
`TierInstance`, `TierPlacement` all collapse into vault types.
`LeaderTierQueryEngine` collapses into the vault's query engine. The
pagination work landed earlier this session simplifies dramatically:
the routing of resume tokens between tier-ID and vault-ID spaces (the
whole thing that the splitResumeToken / ProtoToLocalResumeToken work
exists to manage) goes away — there is one ID space.

---

## 2. A vault has one storage shape. Differentiated storage within a vault comes only from cloud-backing.

**Decision.** A vault has a single storage class. The only mechanism
by which a vault's records live on more than one kind of storage is
cloud-backing: a cloud-backed vault keeps its active chunk on local
storage, uploads sealed chunks to the cloud, and keeps the local
copy of the sealed-chunk file as a cache that the eviction policy
can reclaim. A local-only vault stores everything on its single
configured storage class until retention deletes it.

**Rationale.** Today's tier chain lets an operator compose multiple
storage shapes into one vault — tier 0 on one storage class, tier 1
on another, retention moving records between them. With tiers gone,
that composition has to come from somewhere or it doesn't exist. The
only place we are willing to reintroduce a multi-storage-shape
arrangement is the local/cloud split, because that split already
exists for orthogonal reasons (durability, cost) and because the
cache-eviction work has already built the lifecycle that manages it.
Reintroducing operator-configurable multi-storage local layouts
within a single vault would be reinventing tiers under a different
name.

For operators who want records on different physical storage shapes
without involving cloud, the answer is "use multiple vaults connected
by routing": route by source/content at ingest into vault A or vault
B, where each vault is configured independently. This is not the
same as tier chains — there is no system-managed flow of records
between A and B; routes place records once, and they stay there.

**Implications.** Vault config has a single `storage_class`. The
`active_chunk_class` and `cache_class` fields (already simplified in
gastrolog-4k5mg) collapse to nothing. The `cloud_service_id` field
is what differentiates cloud-backed vaults from local-only vaults.
The cache eviction work (gastrolog-2idw8) remains the lifecycle
manager for the local copy of cloud-uploaded chunks.

---

## 3. Routing is the central mechanism. Retention is an event that re-feeds records into routing.

**Decision.** The routing engine is the only mechanism that decides
where records live. Every flow event in the system — ingest,
retention trigger, anything else that produces records that need to
go somewhere — passes records through the same routing table. The
table evaluates the records and produces a destination decision: a
vault (or vaults), or drop.

A retention trigger is a *chunk-age event* fired by a vault. When it
fires, the records of the affected chunk are fed back through the
routing engine. The routing table then decides:
- Records re-routed to a different vault → moved.
- Records re-routed to the same vault → effectively kept.
- No route matches (or routes evaluate to drop) → deleted.

There is no separate "retention action" enum. There is no "eject"
mode distinct from routing. Retention is an event source; routing is
the decision layer. This is the IP-routing analogy applied
literally: a router doesn't have ingress and egress rule sets, it
has a routing table that evaluates packets at each hop.

A second mechanism, separate from routing, is **investigation
extraction**: an operator-driven, query-time primitive that copies a
subset of records (selected by query) into a new vault. The source
vault is unaffected. This is not a flow event — it's a curation
operation, initiated by a human, that bypasses the routing engine
because the operator is explicitly choosing the destination.

**Rationale.** Today retention has its own action enum (delete vs
eject) that duplicates what routing already does. Tier transitions
are a third path that's neither routing nor retention. The
duplication produces bugs (different code paths handling the "send
records elsewhere" case) and confusion (operators have to learn two
mental models for "where do my records go"). Collapsing both flows
into one routing engine is consistent with the vaults-only goal:
fewer abstractions, fewer mental models.

The IP-routing framing is load-bearing for the design: it implies
that the routing table needs to be expressive enough to handle both
*new records* (with source = ingester) and *re-fed records* (with
source = retention trigger from a specific vault). The source
predicate becomes the disambiguator. An operator who wants
"`level=error` records re-route to long-retention; everything else
drops on retention" expresses that as a routing rule keyed on the
retention-trigger source.

Investigation extraction is intentionally not part of the routing
flow. It's operator-driven, query-time, and copy-not-move. Folding
it into routing would require a "manual trigger" event source, which
is the kind of overloading we're trying to avoid. It's a separate
operation because it has different semantics (copy not move,
operator-initiated not event-driven, query-defined not source-defined).

**Implications.** The routing table needs to express source
predicates rich enough to distinguish ingester sources from
retention-trigger sources. The retention configuration on a vault is
reduced to "when do chunks fire a retention event" — typically a
chunk-age policy. What happens after the event is entirely a
routing-table concern.

The "delete" outcome becomes a routing decision (no matching route,
or an explicit drop destination), not a separate retention action.
This unifies how operators reason about record lifecycle: they look
at the routing table to understand what happens to records at every
flow event, including the retention-triggered re-routing.

Investigation extraction is a first-class operator feature
(connecting to the investigation pillar in the vision doc) — but it
is its own primitive, not a hack on top of routing.

---

## 4. Routing is a table, like networking — source and content both decide the destination.

**Decision.** The routing layer is a static table. Each row is
`(source predicate, content predicate, destination)` evaluated in
priority order. Source = which ingester / connection / origin
produced the record. Content = record attributes, extracted fields,
tokens. Destination = the vault(s) that receive the record.

**Rationale.** With tiers gone, the routing layer is the only
mechanism for organizing records into vaults. Today's routing model
(filter → destination, where the filter is content-only) isn't
expressive enough — operators use tier transitions to compensate
for what routing can't say. Networking-style routing tables are a
well-understood pattern: they give you compositional rules,
priority-ordering for tie-breaking, and the ability to express
"records from ingester X go here, records from ingester Y go there,
records with `level=error` from anywhere go to the long-retention
vault" — without coupling those rules to anything downstream.

**Rationale, addendum.** "Static" here means the routing table
itself does not mutate based on data flowing through it (no
machine-learned routes, no auto-shifting destinations based on
load). The table is operator-authored config. But records are
evaluated against the table at every flow event — ingest, retention
trigger, anything else — and the table's decision can be different
depending on the source predicate. A record's vault membership is
determined by the routing table at the moment of each event it
participates in. Past placement is not revisited; future events are
re-evaluated.

**Implications.** Filter expressivity becomes load-bearing. Today
filters are mostly token-matching; under this model they need richer
predicates — attribute equality, source matching (including
retention-trigger sources), and conditional fall-through. Route
changes affect future events only — they do not retroactively
reorganize records placed under the old table. Operators who want
to rearrange existing data use investigation extraction, not route
reconfiguration.

---

## 5. Duplication is what the routes literally said.

**Decision.** If the routing table delivers the same record to two
vaults, it lives in both. The system does not try to reconcile that.
Not at ingest, not at search, not at aggregation. If a user wants
deduplication, they express it as a pipeline operator in their
query, like any other transformation.

**Rationale.** "Hidden" cross-vault dedup is a system invariant that
costs more than it pays. The cross-tier dedup machinery has been
responsible for a substantial fraction of the bugs that have hit
this codebase — duplicate counts, ghost records, histogram
mis-attribution. Most of those bugs come from the system trying to
make multi-source data look single-source. The simpler stance is:
the routes are the source of truth. If the operator routed a record
to two vaults, that's literally what they asked for; the system
honors it.

**Rationale, addendum.** "Not now" rather than "never." If at some
point inline dedup proves cheap and robust against the post-tier
architecture, we'll revisit. Keeping it deferred means the rest of
the system stabilizes without dedup-aware paths poisoning the
design.

**Implications.** The cross-tier dedup machinery, the histogram
dedup logic, and any other code paths that exist to make multi-source
data look single-source can be deleted. The chunk-redesign-era
machinery that exists to support tier transition correctness can also
go (since there are no transitions to be correct about).

`EventID` itself stays. The dedup machinery uses EventID as input,
but the field is not a dedup field — it's the record's stable
cluster-wide identity (ingester + node + ingest timestamp + per-
ingester sequence number, with NodeID required because
singleton/parallel HA can run the same ingester on multiple nodes).
That identity has uses unrelated to dedup: record pinning,
annotation, permalinks, investigation extraction (so re-running an
extraction doesn't produce duplicates in the destination vault),
investigation handoff, and replication idempotency. Removing the
dedup paths does not remove the need for stable record identity.

---

## 6. Chunks are atomic, self-contained, uncompressed units. Compression is a separate file-level wrapper.

**Decision.** A sealed chunk is one self-contained file — the GLCB
— with a self-describing TOC, the records section (in WriteTS
append order), and the structural and content indexes. The format
itself is **always uncompressed**: every section is directly
readable without a decompression step, and tools that read GLCBs
don't need to know about compression.

Compression, when applicable, is applied as a **generic file-level
wrapper** on top of a finished GLCB. The application uses
compression in exactly one place: cloud uploads. A cloud blob is a
GLCB run through a system-chosen compressor (zstd by default) and
uploaded as `chunk.glcb.zst`. On download, the wrapper is
decompressed and the result is a regular GLCB. The format
definition is silent on compression — it's a transport concern,
not a format concern.

For *local* storage, GastroLog does not compress files. If an
operator wants compressed local storage, they use a compressing
filesystem (ZFS, btrfs, APFS with compression, etc.) — the
filesystem handles it transparently. This is covered separately in
decision 9.

**The blob is atomic.** To read any part of it, you fetch the
whole file. There is no segmented compression, no
seekable-anything-inside-the-blob, no partial range fetching
against the cloud blob. The chunk-size knob (rotation policy) is
the primary lever for fetch granularity in cloud-backed vaults:
smaller chunks = more S3 objects, cheaper individual fetches;
bigger chunks = fewer objects, better compression ratio on cloud
upload, heavier individual fetches. The operator picks their
tradeoff at vault creation; the system doesn't try to be clever.

**The chunk's natural ordering is WriteTS** (the order records were
appended to the chunk). That ordering is implicit in the layout —
records are physically stored in append order, and a record's
position in the chunk *is* its WriteTS-relative ordinal. There is
no separate WriteTS index because there's nothing to index; the
order is the storage.

**IngestTS and SourceTS are secondary indexes**, layered on top of
WriteTS ordering, that let queries find records by TS without
scanning. WriteTS, IngestTS, and SourceTS are usually close to
each other (often equal, modulo internal queueing) but not
identical: WriteTS is when the chunk manager appended; IngestTS
is when GastroLog received the record; SourceTS is when the
record was originally generated at its source. Records aren't
guaranteed to be in IngestTS or SourceTS order even though they
are guaranteed to be in WriteTS order, so the secondary indexes
are how time-bounded queries on those fields work efficiently.

**Severity is a secondary column index** rather than a primary one
in the same sense. It maps record positions to severity values
(or, equivalently, severity values to position sets). Every
record has a severity (with "other" as the explicit fallback),
which is what makes it required.

**Structural indexes are not optional in the optimization sense.**
IngestTS and severity are required because every record has them
and every realistic query filters on them — dashboards, alerts,
investigations all routinely select by time and by level. That
dual criterion (every record has it; every query potentially
filters on it) is what makes a field warrant secondary-index
treatment in the chunk format — separated from the free-form
record body, stored as a compact dedicated structure. Without
these indexes, finding records by IngestTS or severity would mean
reading each record's body to extract the field, which is the
access pattern the format exists to avoid.

SourceTS gets the same treatment when present: when records carry
a meaningful source timestamp, time-bounded queries on SourceTS
need the secondary index to be efficient. The sparseness (some
records have it, some don't) is encoded per-record with a
sentinel for absence.

The line between "structural" and "content" is the query-pattern
universality combined with reliable population. A field that every
record has and every query potentially filters on is structural
and required. A field that's data-dependent but, when present,
meets the same query-pattern criterion is structural and sparse.
A field that's just an attribute in some records that operators
may want to filter is content — lives in the flexible attribute
body, indexed only if the operator opts in.

Heavy content indexes (token, KV postings, per-field-value
structures — anything that scales with vocabulary or value
cardinality and can outgrow the data) are part of the GLCB when
built. Whether to build them is a separate vault-level choice —
they take storage space on disk and bandwidth on cloud upload but
make queries fast. Time-shape and severity structures qualify
unconditionally; the rest are operator-configurable per vault.

The TOC sits at a known offset so a reader can identify the
format and section layout immediately.

This is a successor format to today's GLCB. The new name is **GLCB —
GastroLog Chunk Blob**. The acronym is preserved (file extensions,
package paths, and command names that already use it survive the
rename); the expansion changes from "Cloud Block" to "Chunk Blob,"
which captures what the format actually is — the on-disk
representation of a chunk, used universally rather than only in
cloud storage.

**Rationale.** Internal seekability inside a chunk is expensive
complexity (custom seekable zstd format, segment boundaries,
partial-fetch logic on the cloud read path). It exists because we
wanted to avoid downloading whole chunks for queries against
cloud-backed vaults whose chunks aren't locally cached. But the
right way to avoid that download isn't to seek inside the blob —
it's to *not need the blob in the first place*, via a vault-wide
candidate index (decision 8). With a vault-wide narrowing layer,
blob fetches happen only for chunks already known to be relevant;
once relevant, the whole blob is what you want anyway.

The chunk-size knob (rotation policy) becomes the primary lever for
fetch granularity in cloud-backed vaults. Smaller chunks = more S3
objects, cheaper individual fetches. Bigger chunks = fewer objects,
better compression ratio (when compression is in play), heavier
individual fetches. The operator picks their tradeoff at vault
creation; the system doesn't try to be clever.

**Implications.** The seekable-zstd machinery, the per-frame
boundary tracking, the partial-blob caching code path, and the
range-request orchestration all go. A chunk read path becomes
"obtain the file (cache-hit or download), decompress if compressed,
search." When compression *is* applied (the cloud-blob case, or a
vault that opts into local compression), it sees the entire chunk
in memory, which means better compression ratios (the dictionary
covers more), simpler decoding, and uniform treatment of data +
indexes.

---

## 7. Chunks have three lifecycle states: Active, Sealing, Sealed.

**Decision.** A chunk's lifecycle has three states. The state is a
cluster-wide fact maintained on the vault-ctl FSM; transitions are
observable Raft-committed events visible to every node.

- **Active** — accepting appends. Lives on the vault leader. The
  vault has at most one Active chunk at a time. The leader's copy
  is authoritative; followers maintain an active-form mirror via
  record streaming as a leadership-transfer safety net (best-effort,
  may have minor drift). Queries on Active chunks go through the
  leader. Queryable.
- **Sealing** — appends are rejected; on the leader, the sealed-form
  GLCB is being assembled (indexes built, sealed layout finalized,
  local vault-index entry populated). The leader's active-form
  layout is still intact and remains the authoritative read path
  on the leader during assembly. Followers see this state on the
  FSM; they stop expecting more record streaming for this chunk
  and route queries that need the canonical state to the leader
  until the sealed GLCB is replicated to them. Their active-form
  mirror sits idle — not used for query serving — kept around as
  a leadership-transfer safety net. Queryable (via the leader).
- **Sealed** — cluster-wide fact: the chunk's bytes are frozen,
  eligible for cloud upload (in cloud-backed vaults) and selection
  by investigation extraction. The chunk's lifecycle state on the
  FSM has reached its terminal value. Queryable.

**The leader is the single authoritative source for chunk
content.** During Active and Sealing, only the leader's copy is
authoritative — its records, its sealed-form bytes. Followers
maintain an active-form mirror via record streaming, but that
mirror exists primarily as a *leadership-transfer safety net*,
not as a co-equal data source. Record streaming is best-effort
(fire-and-forget today), so the follower's mirror may have
minor drift from the leader's canonical content (missed appends
in flight, reorderings, delivery delays). This drift is accepted:
ensuring perfect byte-equivalence across replicas in the active
phase would require expensive synchronous replication on every
append, which would defeat the performance point of active-form
storage. If we ever find a live-replication scheme that's strong
enough to trust without sacrificing throughput, follower mirrors
could become a query-parallelism bonus on top of their primary
safety-net role — but that's not part of this design.

**Sealed-form replication is one-way: leader → followers, byte
copy.** Once the leader's local sealing completes (sealed GLCB
on disk, vault-index entry written), the leader proposes the
Sealing→Sealed event on the FSM and begins shipping the sealed
GLCB to followers. Replication is binary copy — followers don't
build their own sealed-form from the active-form mirror, because
the mirror isn't guaranteed to match. The leader's sealed GLCB
is what the cluster agrees the chunk is.

**Leadership transfer mid-Active or mid-Sealing.** If the leader
fails or transfers leadership before the sealed GLCB has been
produced and replicated, the new leader takes over with whatever
its local copy contains — its own active-form mirror, possibly
with minor drift from what the old leader had. That copy becomes
authoritative going forward: the new leader completes the chunk
(if it was Active) or the sealing (if it was Sealing) and
produces the canonical sealed GLCB from its own state. Some
records may be lost or duplicated relative to the old leader's
state. This tiny gap is the cost we live with for performance —
making the active-phase replication strong enough to eliminate
the gap would require synchronous quorum on every append, which
is too expensive for the throughput targets.

**Follower outage during Active.** A follower going down and
coming back up while the leader is still writing to the same
active chunk produces a *gap* in the follower's mirror — appends
the leader sent during the downtime are missing, and record
streaming alone won't recover them (the leader doesn't replay
historical appends to a reconnecting follower). The follower
generally can't even measure the gap precisely; it just knows
its mirror is no longer trustworthy as a promotion source for
this chunk.

The policy: a follower that detects (or can't rule out) a gap in
its active mirror discards that mirror. From that point until
the chunk is sealed, the follower has nothing locally for the
chunk — it's not a viable promotion candidate for it. Other
follower replicas (those that stayed up) still have valid
mirrors and can promote if needed. When the leader eventually
seals the chunk and ships the sealed GLCB, the follower receives
it via the normal replication path; from that moment on, the
follower has a canonical copy.

This trades reduced promotion redundancy during the gap window
for simplicity. Backfilling the gap (e.g., the follower asking
the leader for the missing append range) is possible but adds
complexity to the active-phase replication path; not in this
design.

**Local form on followers serves reads when sealed-form is
available.** A follower with a fully-replicated sealed GLCB
serves queries from it locally. A follower without the sealed
GLCB yet (replication still in flight) routes queries that
require the canonical state to the leader; the active-form
mirror is not used for query serving in normal operation. The
mirror's job is purely to be the safety net for promotion, not
to provide query-time read parallelism for unsealed data.

A record committed to a vault is queryable from the moment of
commit, through every state transition, until the routing engine
decides to drop it on a retention re-route (or it lands in a vault
the user can't reach). State transitions on a chunk change *which
on-disk form* serves the read; they never remove records from
query visibility. When retention destroys a chunk, the records
inside are re-routed and remain queryable from wherever the routing
engine sent them.

The Sealed state carries an absolute invariant: **nothing about a
sealed chunk ever changes**. Not the records, not the indexes, not
the metadata. If something needs to "modify" a sealed chunk (e.g.,
rebuild an index after a code update changes the format), the only
mechanism is to produce a new chunk via an explicit operation —
investigation extraction, recovery rebuild, etc. — that creates a
fresh chunk with its own ID. The original is untouched.

The invariant scope is **the chunk as bytes**, not the records as
permanent residents. A sealed chunk's bytes don't change while it
exists. When retention destroys the chunk, the records inside are
re-fed through the routing engine (decision 3) and become part of
*other* chunks — possibly in the same vault, possibly in a
different one, possibly nowhere at all if the routing table drops
them. The original chunk, having had its records extracted, is
deleted. The records do not "move" in the sense of mutating the
sealed chunk; the sealed chunk is destroyed and the records flow
out of it as routing input. From the perspective of the chunk
itself, it was never modified — it existed, it was queryable, and
then it was gone.

**Rationale.** Today the active→sealed transition is treated as
instantaneous, but it's actually a multi-step assembly process:
finalizing the in-place format, building the token/KV indexes,
computing content hashes, posting `CmdSealChunk` and
`CmdAttachOffsets` to the FSM. During this assembly window the
chunk is neither writable (writes are rejected) nor cleanly sealed
(indexes still building, FSM commands still in flight). The system
has bugs at this boundary: queries against a transitioning chunk
sometimes see partial state, the post-seal indexer can race with
upload, crash recovery has to guess what state a chunk was in.

Naming the state explicitly fixes all of those by making the
boundaries observable. A query against a Sealing chunk knows the
chunk isn't fully ready and either waits or skips. The post-seal
indexer is the operation that drives the Sealing→Sealed transition;
nothing else does. Crash recovery sees a chunk in Sealing state and
either resumes the assembly or marks the chunk corrupt.

The Sealed-is-immutable invariant is what makes the rest of the
system simple. Cloud upload can happen because the chunk won't
change underneath the upload. The vault-wide index can be populated
once and trusted. Investigation extraction can read a sealed chunk
without coordinating with anything because nothing else can modify
it. Replication is byte-identical because the bytes are frozen.
Every downstream subsystem benefits from this invariant being
absolute rather than approximate.

**Implications.** The Active→Sealing transition is triggered by
rotation policy (size, age, record count). The Sealing→Sealed
transition is triggered by the leader completing all required
assembly steps (indexes built, sealed-form bytes persisted
locally, FSM commands committed). Both transitions are observable
on the FSM — `CmdBeginSeal` and `CmdSealChunk`, or some
equivalent. Followers project the chunk's lifecycle state from
the FSM rather than inferring it from local file presence.

Operations that today implicitly assume "if it's not active, it's
sealed" need to be audited and updated to handle Sealing
explicitly: the query path reads Sealing chunks via the
active-form layout (which is still intact during this state); the
upload path waits for cluster-Sealed; retention triggers only
fire on cluster-Sealed chunks; the vault-wide index is populated
locally as each node receives the sealed GLCB (leader at seal
time, followers as replication delivers).

A node's read path consults whatever local form it has — sealed
if available, active-form otherwise — independent of the FSM
lifecycle state. Cluster-state Sealed plus locally-only-active
is a normal, expected condition for followers between FSM commit
and replication delivery; the read path handles it without
distinguishing.

The CHUNKMETA on the FSM gains a state field. The current
implicit state ("sealed = WriteEnd is set") becomes explicit
("state = Active | Sealing | Sealed"). Per-node local form is
not on the FSM — it's a property of which files are present on
the node's local disk.

---

## 8. Each node has a per-vault candidate index on its filesystem. Throwaway, rebuildable.

**Decision.** Each node maintains a per-vault candidate index on
its local filesystem — a compact collection of chunk summaries
(Bloom filters, MinHash signatures, attribute-value sets, level
histograms) keyed by chunk ID. The index covers the chunks that
node knows about for the vault; nodes that store different subsets
of chunks have correspondingly different indexes.

The index is **not in the FSM, not Raft-replicated, not part of
cluster state**. It's a node-local performance artifact, written
to disk alongside the vault's chunk storage. If a node loses its
index (disk failure, format change, deliberate deletion), it
rebuilds by scanning the chunks it has — same way it would rebuild
any other derived artifact. There is no cluster-wide consistency
guarantee on the index; each node's copy is independent.

A node's query flow becomes:
1. Local vault index → candidate chunk IDs *for chunks this node has*
2. Candidate chunks → fetch (cache hit or download for cloud-backed)
3. Authoritative search inside fetched blobs
4. Cross-node fan-out for chunks held by other nodes (existing
   mechanism, each node consulting its own index)

The index does not return matches; it returns "chunks that *may*
contain matches." The blob's in-file index is what authoritatively
answers what's in the chunk.

**Rationale.** Putting this index in the FSM would replicate it
via Raft, which is wrong on multiple axes: it'd grow the FSM
state proportionally to chunk count (snapshots and log compaction
get expensive); it'd force every node to carry indexes for chunks
it doesn't have local access to (waste); and it'd add a
correctness obligation (the index has to stay consistent with
what the FSM thinks the chunk content is) that has zero
operational benefit since the index is just a query optimization,
not an authoritative source.

A filesystem-local index is throwaway by construction: lose it,
rebuild it. That matches its actual role — it speeds up queries,
it doesn't define correctness. The chunk's in-file index remains
authoritative for "what's in this chunk"; the vault-wide
filesystem index is just a fast-path filter.

Without this filesystem index, every search has a baseline cost
proportional to the number of chunks whose time bounds overlap the
query, even just to figure out which chunks are relevant. For
local-only vaults, that's a per-chunk index open per query (cheap
individually, expensive at scale). For cloud-backed vaults whose
chunks have been evicted from the cache, that's an S3 round-trip
per chunk, which is fatal. The filesystem index collapses both
cases.

**Rationale, addendum.** This is not a "future direction." It is
load-bearing for the cloud-backed case and strongly desirable
everywhere. The plan should treat it as required infrastructure for
the post-tier model. Specific data structures (Bloom vs MinHash vs
attribute-value sets) are open implementation choices; the
architectural commitment is to a per-node, filesystem-resident
narrowing layer.

**Implications.** Per-chunk in-file indexes do not go away — they're
still authoritative for "what is actually in this chunk." The
filesystem candidate index sits in front of them as a fast path.

**The index covers sealed chunks only.** Active chunks are
intentionally not in the vault index. They're a small,
well-known set (one Active chunk per vault) that the query path
knows to consult directly via the active-form layout — fast
enough that index entry maintenance on every append would be
pure churn. The index is for the stable, immutable chunks: the
summary is computed once at the Sealing→Sealed transition (when
the chunk's contents are frozen), written to the local
filesystem, and never mutated.

**Replication obligates index updates.** When a node receives a
replicated sealed chunk, it must update its local vault index to
include that chunk's summary entry. The chunk and its index entry
are coupled: a node that has the chunk locally must have the
index entry locally, otherwise queries on that node would miss
candidates that should match. This is part of the chunk's
delivery completion — the chunk isn't "received" until its index
entry is also written. (For the leader, the same coupling holds:
the Sealing→Sealed transition includes writing the index entry
locally; the chunk isn't fully Sealed from the local node's
perspective until that's done.)

The "FSM proportional distribution" hack that the histogram path
uses for uncached chunks gets replaced by exact answers from the
local index (per-chunk record counts, group counts) — no more
approximation.

Nodes that hold different chunk subsets have different index
contents; that's expected. Cross-node search uses each node's
local index for its own narrowing pass, then merges results at
the coordinator (same fan-out mechanism that exists today; the
index just makes each node's pass faster).

---

## 9. Local compression is the filesystem's job, not GastroLog's.

**Decision.** GastroLog does not compress files on local disk.
Sealed GLCBs live as uncompressed files. If an operator wants
compressed local storage, they use a compressing filesystem —
ZFS with compression, btrfs, APFS with compression enabled, etc.
The filesystem handles compression transparently; GastroLog
neither knows nor cares.

GastroLog uses compression in exactly one place: cloud uploads.
The wrapper compressor (zstd, by default) is a system choice, not
a per-vault knob.

**Rationale.** Local compression is a solved problem at the
filesystem layer. Btrfs supports transparent compression with
zstd/lzo/zlib at multiple levels; ZFS supports lz4/zstd/zlib
with selectable algorithms per dataset; APFS does compression on
macOS. These implementations are mature, integrate with the OS
page cache, and let operators tune the algorithm and level
independently of the application. Reimplementing "compress GLCB
files on disk" in the application would duplicate that work,
complicate every read path with a decompression layer, and add a
configuration knob for a concern that's not actually GastroLog's.

Cloud uploads are different — the application owns the bytes on
the wire and at rest in the bucket, so the application is
responsible for transport-level compression.

The default ethos is "fast queries by default." Local storage
unconditionally uncompressed at the application level means every
query reads bytes directly without a decompression step.
Operators who care about disk efficiency get it from the
filesystem they chose; they don't pay for it in query latency.

**Implications.** Vault config has no compression knob. The chunk
manager's seal pipeline never compresses anything. The read path
is uniformly uncompressed across all locally-stored GLCBs (cache
hits, local-only vaults, all of it). Cloud upload and download
are the only places compression code runs.

This walks back chunk-redesign step 7's "seal directly to
data.glcb" choice in spirit — sealing still produces the unified
single-file sealed format (which is the win), but compression is
not applied at seal time at all. The format is unconditionally
uncompressed; cloud transport adds the wrapper.

---

## 10. Investigation extraction is a first-class operator primitive.

**Decision.** An operator can run a query against one or more
vaults, select a subset of records (by query, time window,
filtering, etc.), and load those records into a new vault as a
copy. The source vault is untouched. This becomes the way operators
turn a transient investigation into a permanent dataset.

**Rationale.** "I want this specific slice as its own dataset" is a
fundamentally different operation from "records flowing through the
routing engine." It's operator-initiated, query-defined, and copies
rather than moves. Folding it into the routing engine would require
a "manual trigger" event source that doesn't fit the
event-driven-flow framing. Keeping it separate preserves the routing
engine's single responsibility (evaluate flow events) and gives
investigation extraction its own clear semantics (selection + copy).

It connects naturally to the investigation pillar of the vision
(saveable investigations, shareable workspace state) — extracting an
investigation's records into a vault is the durable version of "save
this investigation."

**Implications.** Investigation extraction is the only mechanism in
the system that *copies* records (rather than moving them via the
routing engine). The source vault is unmodified; the destination
vault is new (or existing, but the records arriving via extraction
are independent of any routes that target it). No "move records
between vaults" mechanism exists outside the routing engine and
this extraction primitive.

---

## 11. The refactor is atomic.

**Decision.** Single coordinated change against `main`. No
back-compat shims, no migration tooling, no period during which
"old tier path" and "new vault path" coexist. Existing dev clusters
get wiped (`cluster-init`); zero production deployments means there
is no migration story to design.

**Rationale.** A two-phase or N-phase deprecation would force every
intermediate phase to support both models, which is enormously more
expensive than supporting one. Given there is no external constraint
forcing a multi-release window, the cheapest move is also the
cleanest: the state of `main` either has tiers or it doesn't.

**Implications.** Code-to-delete and code-to-rewrite happen in the
same change. Tests for the tier model get deleted in the same
change. The plan document (forthcoming) sequences the work, but the
sequence is internal to the refactor — not a release-by-release
gradient.

---

## Sequencing relative to the indexing redesign

[`docs/advanced_indexing.md`](./advanced_indexing.md) is a separate
research document covering the indexing rewrite — FSTs, roaring
bitmaps, columnar layout, and related ideas for chunk-local
search. The two efforts are related (the vault redesign establishes
where the new indexes live; the indexing redesign decides what they
are) but distinct, and they should not be landed together.

**The intended order of work:**

1. **Vault redesign.** Land the architectural changes in this
   document. Keep current indexing in place — token/KV postings as
   they exist today, no vault-wide index yet. The resulting system
   is correct under the new architecture but doesn't yet have the
   cold-fetch optimization that decision 8 promises.

2. **Indexing redesign.** Once the vault refactor is stable,
   implement the new indexing on top of it: vault-wide candidate
   indexes, the FST/bitmap-based per-chunk content indexes, the
   structural columns for severity (and any other fields that
   meet the dual criterion). The choices come from the
   advanced_indexing research; the place they land is the
   architecture this doc establishes.

**Why split them.** The vault refactor is already large — collapse
the tier model, rewrite the orchestrator, retrofit the routing
engine, change the chunk lifecycle to three states. Bundling an
indexing rewrite multiplies the surface area of breakage and makes
debugging harder. Each subsystem in flux makes the others harder to
reason about. Splitting the work means: when the vault refactor
lands, we have a stable platform to evaluate the indexing redesign
against; when the indexing redesign lands, we know that any
behavior change is from indexing, not from the storage substrate
moving underneath it.

**What gets worse in the interim.** Cold-chunk search performance
on cloud-backed vaults won't improve until the vault-wide index is
implemented — every search past the cache will still pay
per-chunk S3 round-trips. This is exactly the same cost we have
today; the vault refactor doesn't make it worse, just doesn't make
it better. Operators with cloud-backed vaults during the interim
should size cache budgets generously and use rotation to keep
chunks small enough that cache misses are tolerable.

**What stays committed in this doc.** The architectural
commitments — vault-wide index as a concept (decision 8),
structural columns including severity (decision 7's framing in the
GLCB format discussion), the GLCB format itself with its
required/sparse/optional structural set — are the destination.
They're stated here so that the indexing redesign work is built
*toward* a known endpoint, not negotiated freshly. The sequencing
note is just about *when* the implementation lands, not whether
the architecture is contingent.

---

## Open questions deferred until the plan stage

- **Vault-wide index data structures.** Bloom filters, MinHash,
  attribute-value sets — which combination, what budget, what
  fallback when a structure can't represent a query (e.g., a
  predicate the index doesn't cover)?
- **Index growth bounds.** Vault-wide index has to fit on every
  node's disk (and ideally in memory for low-latency lookups). What's
  the per-chunk size budget, and how do we degrade gracefully when a
  vault's chunk count grows past it?
- **Routing table semantics.** Priority order, fall-through,
  default destination, how matches against `(source AND content)`
  compose — these need a complete spec before implementation.
- **Investigation extraction implementation.** Is it a CLI command,
  a server-side RPC, a UI button, all three? What's the consistency
  guarantee — point-in-time snapshot, eventually-consistent copy,
  something else?
- **Default rotation policy.** With tiers gone and chunk size
  becoming the primary lever for blob-fetch granularity in
  cloud-backed vaults, what's a sensible default? Today's defaults
  were tuned against the tier chain assumption.
- **Migration of existing test fixtures.** A lot of test setup is
  multi-tier; what's the minimal rewrite vs delete-and-replace?
