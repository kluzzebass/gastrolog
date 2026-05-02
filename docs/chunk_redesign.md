# Chunk redesign — single source of truth, one on-disk format

Status: **draft, in progress under gastrolog-66b7x.**

The investigation that opened this thread looked like "fix the cloud-tier
histogram." It became a chunk redesign as soon as we asked the right
questions. There is no clean fix to the cloud path that doesn't simplify
the file path too — the spaghetti runs through both because the two
classes share a lifecycle and a metadata path while pretending to be
different things.

This document describes how a chunk's metadata, on-disk shape, and
lifecycle should flow through the system. The scope is **all sealed
chunks** (file-tier, cloud-tier, cached or not) plus the metadata
boundary between active and sealed chunks. The previous "cloud-only"
framing has been retired; the cloud-vs-file distinction collapses to a
single bit on the FSM ManifestEntry under this redesign.

It exists because the existing implementation grew several layers of
"compensate for bad data" code that violated our most important
invariant — **the FSM is the source of truth** — and produced cascading
histogram bugs at the tier 2/3 boundary. Those bugs were the symptom;
the divergent file/cloud handling and the read paths that reach around
the FSM are the disease.

The goal of the redesign is not to fix the symptoms one more time. It is
to remove every code path that exists *only* to paper over a mistake
somewhere else in this codebase, and to collapse the parallel
file/cloud machinery into one shape parameterised by a single property.

## Contents

**Part I — Foundation**

- [The rule: FSM as source of truth](#the-rule-fsm-as-source-of-truth)
  - [Exception: the active chunk](#exception-the-active-chunk)
  - [Implementation note: this matches today's code](#implementation-note-this-matches-todays-code)

**Part II — Construction**

- [The on-disk format: GLCB everywhere](#the-on-disk-format-glcb-everywhere)
  - [Why GLCB: compressed local and GLCB are the same content](#why-glcb-compressed-local-and-glcb-are-the-same-content)
  - [Active multi-file vs sealed GLCB](#active-multi-file-vs-sealed-glcb)
  - [What the unification collapses](#what-the-unification-collapses)
  - [Sealing semantics](#sealing-semantics)
  - [Indexer / seal / upload coordination](#indexer--seal--upload-coordination)
  - [Read performance](#read-performance)
- [The unified Manager type](#the-unified-manager-type)
  - [The whole interface in one shape](#the-whole-interface-in-one-shape)
  - [Cache vs permanent: just `CloudBacked` on the FSM](#cache-vs-permanent-just-cloudbacked-on-the-fsm)
  - [Cache-eviction signals are node-local, not FSM state](#cache-eviction-signals-are-node-local-not-fsm-state)
  - [Tier `type` field can go away](#tier-type-field-can-go-away)
  - [Read flow](#read-flow)
  - [Partial-blob caching](#partial-blob-caching)
  - [Write flow](#write-flow)
- [What the FSM ManifestEntry needs](#what-the-fsm-manifestentry-needs)
  - [Already on the ManifestEntry today](#already-on-the-manifestentry-today)
  - [Resolved one Raft hop sideways via the tier config](#resolved-one-raft-hop-sideways-via-the-tier-config)
  - [Should be on the ManifestEntry but isn't](#should-be-on-the-manifestentry-but-isnt)
  - [Not needed on the ManifestEntry](#not-needed-on-the-manifestentry)
  - [Minimum viable change to the FSM](#minimum-viable-change-to-the-fsm)
- [The collapse: file vs cloud is one bit](#the-collapse-file-vs-cloud-is-one-bit)

**Part III — Action**

- [Audit against existing code](#audit-against-existing-code)
  - [What is already correct](#what-is-already-correct)
  - [What is wrong (and why)](#what-is-wrong-and-why)
  - [Code to delete](#code-to-delete)
  - [Code to add](#code-to-add)
- [Compatibility with the per-chunk indexing redesign](#compatibility-with-the-per-chunk-indexing-redesign)
- [Order of operations](#order-of-operations)
- [Open questions](#open-questions)

---

# Part I — Foundation

## The rule: FSM as source of truth

Every persistent fact about a *sealed* chunk lives in exactly one place:
its `ManifestEntry` in the tier sub-FSM
([backend/internal/tier/raftfsm/fsm.go](../backend/internal/tier/raftfsm/fsm.go)).

That `ManifestEntry` is mutated only by the sanctioned `Cmd*` applies. Outside
those write points, **nothing computes, recomputes, validates, repairs, or
second-guesses any field of the ManifestEntry.** All read paths trust the FSM
unconditionally.

If two stored values disagree, the FSM wins by definition. Other stores
(`cloudIdx` B+tree, in-memory `chunkMeta`, GLCB TOC, on-disk `*.idx`
files) are caches or derivations of the FSM. When they go stale, they get
rebuilt or discarded — never merged with the FSM via "validation."

When code reaches around the FSM to ask the chunk file directly, that's the
bug. There is no exception for "but maybe the FSM is wrong" — if the FSM is
wrong, we fix the write point that put it there.

### Exception: the active chunk

The active chunk is not part of this rule. All six of its bound fields
(`WriteStart`, `WriteEnd`, `IngestStart`, `IngestEnd`, `SourceStart`,
`SourceEnd`) plus `RecordCount` and `Bytes` change as records arrive;
broadcasting every increment through Raft would crater throughput. For
non-monotonic active chunks, even the *Start* bounds aren't stable —
`expandBounds` extends them *backward* whenever a record with an earlier
TS arrives.

For an active chunk, the in-memory `chunkMeta` (and the live B+ trees
the manager maintains as records are appended) *is* the source of
truth. The FSM only knows the chunk exists (via `CmdCreateChunk` at open
time, with bounds set to chunk-creation wall-clock as a placeholder)
and the immutable per-ManifestEntry flags. The act of sealing — `AnnounceSeal`
→ `CmdSealChunk` — is the handover: the manager's running maxima get
committed to the ManifestEntry as final values, the FSM's placeholder Start
fields get overwritten with the real ones it didn't know about until
now, and from that instant onward the chunk obeys the rule like any
sealed chunk.

This exception is narrow but it does cover all eight live-extending
fields:

- It applies *only* to the chunk currently identified by
  `m.active.meta.id`.
- It applies *only* to the eight mutating fields above. `Sealed`,
  `Compressed`, `CloudBacked`, `RetentionPending`, `TransitionStreamed`,
  `DiskBytes`, `IngestIdxOffset/Size`, `SourceIdxOffset/Size`,
  `NumFrames` are FSM-owned even on an active chunk (they don't change
  until sealing or later, and they're set by their dedicated Cmd).
- Read paths that need a sealed chunk's bounds always go through the
  FSM. Read paths that touch the live active chunk's running bounds
  (e.g. histogram computing right-edge buckets while a record is
  arriving) ask the manager and accept that they are seeing a moving
  target — that's inherent to "this chunk is still being written."
- The FSM's create-time `WriteStart`/`IngestStart`/`SourceStart` for
  the active chunk are *placeholders* — useful as "the chunk exists"
  signal and for ordering against tombstones, but not authoritative for
  any computation that cares about real record timestamps. Read paths
  that need the real Start bounds of an active chunk ask the manager.

Everything below in this document — read paths, deletions, reordering —
is about *sealed* chunks, where the rule holds without exception.

### Implementation note: this matches today's code

The exception isn't a redesign decision. It is how the system already
works:

- `applyCreate` ([fsm.go:615](../backend/internal/tier/raftfsm/fsm.go#L615))
  writes only `ID` + the three `*Start` placeholders.
- `applySeal` ([fsm.go:645](../backend/internal/tier/raftfsm/fsm.go#L645))
  is the only command that overwrites Start/End/Count/Bytes with real
  values.
- There is no `CmdAppend` or `CmdGrowChunk`. Per-record updates never
  touch Raft. By construction, the FSM cannot be the source of truth
  for any of the eight running fields on an unsealed ManifestEntry — only the
  manager can be.

The redesign therefore doesn't need to introduce or carve out anything
for the active chunk. It needs to make the *read* paths consistent with
this fact: ask the manager for active-chunk numbers, ask the FSM for
everything else, never blend the two.

---

# Part II — Construction

## The on-disk format: GLCB everywhere

GLCB stops being "the cloud format." It becomes the canonical format
for *every* sealed chunk on disk and on the wire.

### Why GLCB: compressed local and GLCB are the same content

The compressed file-backed chunk and the GLCB blob already store the
same content under the same compression strategy:

| Content | Compressed local (today) | GLCB (today) |
|---|---|---|
| Header / metadata | `idx.log` header + chunk-dir layout | 96-byte fixed header |
| Dictionary | `attr_dict.log` (uncompressed) | dictionary section (uncompressed) |
| Record index | `idx.log` (uncompressed, 12 bytes/entry) | record-index section (uncompressed, 12 bytes/entry) |
| Record data | `raw.log` (seekable-zstd 256KB frames) + `attr.log` (zstd) | seekable-zstd 256KB record-frame stream |
| IngestTS index | IM-rooted `*.tsidx` | TS index section + TOC pointer |
| SourceTS index | IM-rooted `*.tsidx` | TS index section + TOC pointer |

Same compression frame size, same record-index entry shape, same
TS-index entry shape. The only difference is *packaging*: compressed
local fans across four-plus files in a chunk dir; GLCB concatenates
into one file with a footer TOC. There is no on-the-wire requirement
for GLCB to stay GLCB only "in transit." It's just bytes.

So drop the dual format. **Every sealed chunk on disk is a GLCB**,
regardless of tier or whether it's a local seal or a downloaded cloud
copy.

### Active multi-file vs sealed GLCB

```
Active chunk (per tier):
  <tier>/<chunkID>/raw.log          ← appended per record
  <tier>/<chunkID>/idx.log
  <tier>/<chunkID>/attr.log
  <tier>/<chunkID>/attr_dict.log
  <tier>/<chunkID>/ingest.bt        ← B+ tree for live FindGE
  <tier>/<chunkID>/source.bt
  (multi-file, mutating)

Sealed chunk (any tier, file-backed or cloud-cached):
  <tier>/<chunkID>/data.glcb        ← one file, replaces all of the above
  (immutable)

Cloud-backed chunk on S3:
  vault-<id>/<chunkID>.glcb         ← byte-identical to the local data.glcb
```

The active multi-file layout stays exactly as today's file manager
keeps it: incremental writes against `raw.log`/`idx.log`/`attr.log`
plus B+ trees for live `FindGE`. Sealing converts to GLCB; the
multi-file artifacts are removed atomically.

### What the unification collapses

- **`CompressChunk` and the "compressed local" path disappear.**
  Sealing converts multi-file → GLCB directly. There is no
  uncompressed-sealed intermediate state. The `Compressed` flag on the
  FSM ManifestEntry stops being meaningful (sealed = GLCB = compressed by
  definition) — drop it, or treat it as a synonym for `Sealed`.
- **`uploadToCloud` becomes "PUT the local `data.glcb` to S3" plus
  `AnnounceUpload`.** No format conversion. No `chunkcloud.NewWriter`
  walking the cursor record-by-record to produce GLCB bytes — they're
  already on disk.
- **`adoptCloudBlob` (when another node uploaded first) becomes
  "GET the blob into `<tier>/<chunkID>/data.glcb`" plus
  `AnnounceUpload`.** Same shape.
- **Cache fill is just download.** No demux step. No `.ts-cache`
  directory. No separate `CacheDir` concept. Local-disk lifecycle and
  cloud-cache lifecycle are the same lifecycle.
- **Read paths collapse to one** GLCB-aware implementation.
  `openCloudCursor`, `openCachedCursor`, `downloadAndCacheCursor`,
  `scanAttrsCloud`, `scanAttrsSealed`, `findCloudTSPosition`,
  `findCloudTSRank`, `loadIngestEntries`, `searchTSCacheFile`,
  `tsCachePath`, the entire `.ts-cache` machinery — all gone, replaced
  by GLCB section reads via TOC offsets.
- **`removeLocalDataFiles` becomes `os.Remove(data.glcb)`** for cloud
  retention/eviction. One file, no multi-file orchestration.
- **`dataFileNames` and the "preserve some, delete others" rules** that
  exist today to keep IM indexes alive across cloud upload — gone. Only
  the IM root (token/attr/kv/json indexes) is "preserve across cloud
  retention," and it's already separately rooted.

What stays:

- The GLCB writer and reader in
  [chunk/cloud/](../backend/internal/chunk/cloud/), but they're no
  longer "the cloud format." They become the seal-time writer and the
  read-time reader for *every* sealed chunk.
- `RegisterCloudChunk` (follower nodes that learn about a cloud-backed
  chunk via FSM propagation but haven't downloaded `data.glcb` yet) —
  same lifecycle, just no separate cache layout.
- The FSM `ManifestEntry` shape — `IngestIdxOffset/Size`, `SourceIdxOffset/Size`,
  `NumFrames` are still recorded for the GLCB structure, regardless of
  whether the local copy is present.
- **The GLCB TOC is explicitly extensible.** This redesign uses only
  the existing TS-index TOC slots, but the on-disk format must permit
  the per-chunk indexing redesign
  ([docs/advanced_indexing.md](advanced_indexing.md)) to add additional
  sections — FST term dictionaries, roaring posting lists, per-block
  bloom filters, columnar fast-field metadata — without breaking
  compatibility. TOC entries are
  `(magic, version, offset, size, hash)` rows; new section kinds add
  new TOC entries, old readers ignore unknown kinds. The 96-byte
  header's `version` byte selects the codec. Per-section `hash` is
  load-bearing for byte-range cache verification — see the integrity
  story under [Partial-blob caching](#partial-blob-caching) and the
  whole-blob digest under
  [Should be on the ManifestEntry but isn't](#should-be-on-the-manifestentry-but-isnt).
- The IM-built indexes (token/attr/kv/json) stay IM-rooted in this
  pass, but their final destination is *inside the GLCB footer* per the
  indexing redesign. The redesign therefore must not introduce any
  IM-rooted-only assumption (e.g. "indexes always live at
  `<IM-root>/<chunkID>.*`") that would make the eventual move
  expensive.

### Sealing semantics

Sealing today: flip the `Sealed` flag in `chunkMeta` and on disk, then
later run `CompressChunk` to compress raw/attr files in place.

Sealing under this proposal:

1. Quiesce active appends (existing per-chunk lock).
2. Use the GLCB writer to stream the active chunk's records into
   `<tier>/<chunkID>/data.glcb.tmp`.
3. `fsync` + `rename` to `data.glcb`.
4. `os.RemoveAll` the multi-file artifacts (`raw.log`, `idx.log`,
   `attr.log`, `attr_dict.log`, `ingest.bt`, `source.bt`).
5. `AnnounceSeal` (FSM gets the canonical bounds) and, if the tier is
   cloud-tiered and the local seal is the leader, `AnnounceCompress` is
   merged into seal — there is no separate compress step.

Crash-safe: until step 3 lands, the active multi-file is still the
authority and any restart re-runs sealing. After step 3 the GLCB is the
authority; step 4 is best-effort cleanup that any restart finishes.

### Indexer / seal / upload coordination

The per-chunk indexing redesign
([docs/advanced_indexing.md](advanced_indexing.md)) wants per-chunk FST
+ posting lists + blooms + columnar metadata to live in the GLCB
footer so a remote node can open the chunk via a single range GET.
That puts the indexer on the seal/upload critical path in a way today's
async indexer is not. Three plausible orderings; this redesign
commits to **option B**:

- **A. Seal → upload → reindex-in-place.** Footer-less GLCB lands on
  S3, indexer rewrites local + S3 GLCB later (atomic tmp+rename
  locally; S3 PUT-overwrite). Reduces seal/upload latency, but every
  chunk has a transient unindexed window after upload, S3 PUT
  amplification, and a more complex reconcile path. **Rejected.**
- **B. Seal → index-locally → upload.** Sealing produces the multi-file
  → GLCB conversion as above; before `AnnounceUpload`, the indexer
  finalises the in-footer indexes (rewrites `data.glcb` once,
  atomically). Upload happens against the indexed GLCB. Adds
  index-build time before the cloud copy is durable, but the GLCB
  lifecycle is "draft, then complete" with one rewrite point and no
  S3 amplification. **Adopted.**
- **C. Sidecar index file.** Footer-less GLCB plus separate
  `<chunkID>.idx` written and uploaded independently. Loses the
  open-with-one-range-GET property, doubles cache/eviction surface.
  **Rejected.**

Under option B, `CmdSealChunk` fires when the multi-file → GLCB
conversion lands; the chunk is now FSM-sealed but not yet indexed.
`CmdUploadChunk` does not fire until indexing completes. A new
intermediate state — *sealed, indexed* — exists implicitly between the
two commands. Today's `CmdCompressChunk` essentially fills this slot;
under the redesign it becomes "indexing done." Whether to keep
`CmdCompressChunk` as the indexer-completion signal or rename it is a
detail; the FSM transitions are unchanged.

Crash safety: if the node dies after seal but before indexing
completes, restart finds a sealed-but-unindexed `data.glcb` and the
indexer reruns. If it dies after indexing but before upload, restart
finds an indexed `data.glcb` and upload reruns.

### Read performance

GLCB random-access reads use `pread`/`mmap` against fixed offsets in
`data.glcb` (header, dict, record-index, and TS-index sections are all
uncompressed; record-data section is seekable-zstd). No worse than the
multi-file layout — the multi-file layout has the same offsets, just
spread across more file handles.

## The unified Manager type

The `chunk.file.Manager` / `chunk.cloud.Manager` split also goes away.
After GLCB unification there is no functional difference between them
worth keeping as a type distinction:

- Both manage local GLCBs.
- Both use the same active-chunk multi-file layout.
- Both honor retention rules.

The remaining differences — does sealing trigger a cloud upload, can
sealed chunks be evicted, can a missing local file be re-fetched — are
all driven by a single tier-level property: whether the tier has a
cloud store configured.

### The whole interface in one shape

```go
type Manager struct {
    cfg        Config           // includes optional CloudStore + eviction policy
    metas      map[ChunkID]*chunkMeta
    active     *chunkState      // current writeable chunk, multi-file
    cloudStore CloudStore       // nil for local-only tiers
    fsmReader  MetadataReader   // read-only view of the tier FSM
    // ...
}
```

`cloudStore == nil` ⇒ local-only tier:

- Sealing produces `<tier>/<chunkID>/data.glcb` and is done. No upload
  step, no cache eviction, retention rules are the only deletion path.
- A missing `data.glcb` for a non-tombstoned ManifestEntry is a hard error
  (permanent data loss).

`cloudStore != nil` ⇒ cloud-capable tier:

- Sealing produces `data.glcb` locally; an asynchronous upload pass
  promotes it to cloud, lands `CmdUploadChunk` (which sets
  `ManifestEntry.CloudBacked=true` and records hash + key scheme + cloud-service
  snapshot), then leaves the local copy in place as warm cache.
- Eviction policies (LRU under disk budget, TTL, etc.) may remove the
  local `data.glcb` for `ManifestEntry.CloudBacked=true` chunks. The ManifestEntry
  stays. Next reader re-fetches.
- Retention deletion still applies: `AnnounceDelete` removes the ManifestEntry,
  removes the cloud blob (if `CloudBacked`), removes the local file.

### Cache vs permanent: just `CloudBacked` on the FSM

That single bit on the FSM `ManifestEntry` is the entire cluster-wide
distinction:

| ManifestEntry.CloudBacked | Meaning | Local file missing means | Eviction policies that apply |
|---|---|---|---|
| `false` | local `data.glcb` is the only copy | permanent data loss | retention only |
| `true` | local `data.glcb` is a cache; durable copy on S3 | re-fetch on next read using `blobKey(ManifestEntry)` + integrity hash | retention *and* cache-eviction |

No other per-chunk metadata is needed *on the FSM* to drive cache-vs-
permanent behavior. All the other ManifestEntry fields the reader uses
(`DiskBytes`, `IngestIdxOffset/Size`, `SourceIdxOffset/Size`,
`NumFrames`, hash, key scheme, cloud-service-ID snapshot) are about
*reading the GLCB and verifying its integrity* — they're populated
exactly the same way in both classes (a `false`-class chunk just never
gets the upload-time fields filled, because no upload happened).

### Cache-eviction signals are node-local, not FSM state

Eviction policies need additional information that **does not belong
on the FSM ManifestEntry**: each node decides independently when to drop its
local cache copy, and the cluster doesn't care that node A evicted
chunk X at 02:13 while node B keeps it warm. Putting "last accessed
at" on the FSM would force a Raft apply per read — even worse than the
per-record `RecordCount` updates we already excluded.

The signals each eviction policy needs, and where they actually come
from:

| Policy | Signal it needs | Where it comes from | Extra state |
|---|---|---|---|
| TTL (`cache_ttl`) | `now − (when local copy was populated) > ttl` | `os.Stat(data.glcb).ModTime()` — sealing's `tmp → rename` and S3 download's `tmp → rename` both stamp this naturally | none |
| LRU (`cache_eviction: lru` + `cache_budget`) | last-read time per chunk | in-memory `map[ChunkID]time.Time` maintained by the Manager on each read | one map per cloud-capable Manager, lives in process memory only |
| Disk budget pressure (paired with LRU) | total bytes of cached `data.glcb` files vs `cache_budget` | `os.Stat` on each `data.glcb` — same scan that picks LRU candidates | none |

The LRU map is **lost on restart by design**. LRU is a cache policy
and approximate; resetting the access clock means all chunks get equal
weight until first access. That's preferable to writing a per-read
journal entry. If empirical eviction patterns ever become visible to
users post-restart, persistence can be added later as a per-tier
snapshot file — but it's not part of the minimum redesign.

Equivalently: anything the FSM stores about a chunk persists across
node restart and replicates across the cluster. Anything the Manager
maintains in memory or via filesystem stats is local-only and
ephemeral. Cache-eviction signals belong squarely in the second
category. The ManifestEntry stays minimal; cache state stays node-local.

### Tier `type` field can go away

Today tier config has `type: 2` (file) vs `type: 3` (cloud). After the
unification, that tag is redundant with whether `cloud_service_id` is
set. A tier that has a cloud service is cloud-capable; one that doesn't
is local-only. Behavior follows configuration; no enum needed. This is
a tier-config simplification that's separate from the manager-type
collapse but lands cleanly alongside it.

### Read flow

For any sealed chunk:

1. Ask the FSM (via `MetadataReader`) for the ManifestEntry.
2. Check `<tier>/<chunkID>/data.glcb` on disk:
   - present and complete → open it, read. Done.
   - missing or partial → branch on `ManifestEntry.CloudBacked`:
     - `true`: fetch the bytes the read needs from `cloudStore` using
       `blobKey(ManifestEntry)`, validate against `ManifestEntry.DiskBytes` /
       `ManifestEntry.Hash` (full-blob case) or per-section integrity
       (range-GET case), populate the local cache. The simplest
       implementation is a full-blob GET into `data.glcb.tmp` →
       `fsync` → `rename`; the format also permits range-GETs that
       populate only the bytes the query needs (see "Partial-blob
       caching" below).
     - `false`: hard error. The chunk is gone.
3. Reads use the single GLCB reader. No `cloudBacked` branch in the
   reader itself.

For the active chunk: no FSM round-trip; manager memory is truth, per
the active-chunk exception above.

### Partial-blob caching

The per-chunk indexing redesign wants cross-node search to issue
footer-only range GETs followed by targeted body GETs, rather than
full-blob downloads on every cold-data query. A "the local cache is
either the full GLCB or nothing" model would force a full download for
every query that touches a non-local chunk and defeat that property.

The eviction model under this redesign is therefore **byte-range
based, not whole-file based**:

- The local file at `<tier>/<chunkID>/data.glcb` is a sparse cache of
  the canonical S3 GLCB. Possible states include:
  - **Absent** — no local bytes; reads fully fetch from S3.
  - **Footer-only** — local file holds at least the GLCB header + TOC
    + footer-resident sections (TS indexes, FST roots, blooms,
    column metadata). Sufficient to *plan* a query without touching
    the body. ~KB to single-digit MB per chunk.
  - **Footer + targeted body ranges** — additional body-frame ranges
    populated on demand. Sparse-file friendly: holes report as
    zeroes from the filesystem, and the cache tracks the populated
    range set in a small per-chunk metadata file or in-memory map.
  - **Complete** — the full blob is local; behaves identically to a
    file-backed sealed chunk.
- Eviction operates at the byte-range level: LRU/TTL/disk-budget
  policies remove ranges (or sections) rather than whole files.
  Simplest first implementation may treat the file as atomic
  (full-blob fetch, full-blob evict); a later iteration can add
  partial-range tracking without changing the FSM ManifestEntry shape.
- The integrity guarantees stay coherent through three layers of
  hash:
  - **Per-section hash** in every TOC row. A range GET that fetched
    section X recomputes section X's hash and compares to the TOC
    entry. The section is verified without touching any other bytes.
    Trusted because the TOC itself is included in the whole-blob
    digest below.
  - **Whole-blob digest** on the FSM ManifestEntry (`ManifestEntry.Hash`). Derived
    from per-section hashes: `sha256(header ‖ section_hashes_in_TOC_order ‖ TOC_bytes)`.
    On any cache populate (full or partial), recompute from the
    footer alone and compare to `ManifestEntry.Hash`. Detects substitution
    (wrong blob in S3 with self-consistent contents) and any
    tampering with header, TOC, or section hashes. O(1) work
    regardless of blob size.
  - **Section-internal checks** as a third layer where the section
    has them (the seekable-zstd body has per-frame xxhash via
    klauspost/compress; the GLCB header has chunkID/vaultID
    self-identification). Defence-in-depth, not load-bearing.

  Per-section hash sizing: 8-byte truncated sha256 for sections that
  the consumer parses structurally before use (TS indexes, future
  FST/posting-list/bloom/column sections); 4-byte CRC32C for sections
  whose content has its own internal verification (seekable-zstd
  body). Whole-blob digest is full-length sha256.

This redesign does not need to *implement* partial caching to be
correct. It needs to **not preclude** it — the on-disk shape, the read
flow, and the eviction policy hooks must all admit a later sparse-cache
implementation without an FSM-shape change or a manager-API rewrite.

### Write flow

| Step | What happens | FSM command | Local effect |
|---|---|---|---|
| Append | record written to active multi-file | none | running maxima updated in `m.active.meta` |
| Seal | active multi-file → `data.glcb.tmp` → fsync → rename → cleanup | `CmdSealChunk` | `data.glcb` exists; `ManifestEntry.Sealed=true` |
| (cloud tier only) Upload | PUT `data.glcb` to S3, computing hash during PUT | `CmdUploadChunk` | local file untouched; `ManifestEntry.CloudBacked=true`; hash, key scheme, cloud-service-ID snapshot land on ManifestEntry |
| Cache eviction (cloud tier only) | `os.Remove(data.glcb)` | none | local file gone; ManifestEntry unchanged |
| Retention | `ManifestEntry.RetentionPending=true` → action runs → `AnnounceDelete` | `CmdRetentionPending` then `CmdDeleteChunk` | local file gone, cloud blob gone (if CloudBacked), ManifestEntry tombstoned |

The only structural code change relative to today's `file.Manager` is
the upload pass, which moves from "convert to GLCB and upload" to
"upload the existing GLCB." Everything else simplifies or disappears.

## What the FSM ManifestEntry needs

If `data.glcb` is locally absent (cold cache or evicted) and the FSM
says `CloudBacked=true`, every reader must be able to fetch the blob
back from S3 *using only what's on the ManifestEntry plus the tier config*. No
GLCB inspection, no `cloudIdx` lookup, no compensating "find the blob"
heuristics.

### Already on the ManifestEntry today

| Field | Use in retrieval |
|---|---|
| `ID` | identifies the chunk; combined with the tier's vault ID gives the deterministic object key `vault-<vaultID>/<chunkID>.glcb` via `blobKey()` |
| `DiskBytes` | expected blob size — validates the `HEAD`, sizes range requests, sanity-checks that GET returned the right bytes |
| `IngestIdxOffset` / `IngestIdxSize` | offset+size of the IngestTS index section — for cheap partial reads (range request of just the TS index without pulling the whole blob) |
| `SourceIdxOffset` / `SourceIdxSize` | same for SourceTS |
| `NumFrames` | locates the seekable-zstd seek table at the tail of the seekable section |
| `CloudBacked` | "the authoritative copy is on S3" |

### Resolved one Raft hop sideways via the tier config

| Field | Why not on the ManifestEntry |
|---|---|
| `cloud_service_id` (which bucket / endpoint / credentials) | All chunks in a tier share the same store; per-chunk duplication is wasteful — *unless* a tier ever changes its cloud service after chunks are uploaded (see below). |

### Should be on the ManifestEntry but isn't

| Field | Why it matters | Where it would land |
|---|---|---|
| **Content hash** (whole-blob digest, *derived* from per-section hashes) | Integrity — once the local copy is a cache, every re-fetch implicitly trusts whatever S3 returns. Without a hash on the ManifestEntry, a corrupt download, a partial PUT, or another node racing an overwrite is silently fed through every reader. The GLCB header self-identifies chunkID/vaultID but that catches almost no corruption mode. The ManifestEntry hash is **derived from the section hashes**, not computed over the body bytes: `sha256(header ‖ section_hashes_in_TOC_order ‖ TOC_bytes)`. Computing it costs hundreds of bytes of input regardless of blob size, validates substitution and tampering at the whole-blob level in O(1) work, and stays consistent with the per-section hashes that range GETs verify against. | `CmdUploadChunk` payload, computed at upload time. |
| **Cloud-service-ID snapshot at upload time** | Pins the chunk to the store it actually went to. Today this is read from the *current* tier config; if the tier is ever reconfigured to a different cloud service, blobs uploaded to the old store become unreachable. | `CmdUploadChunk` payload — snapshot of the tier's `cloud_service_id` as of the upload. |
| **Key scheme / version** (a one-byte enum, or the resolved key as a string) | Future-proofs `blobKey()`. Today the derivation is hard-coded `vault-<id>/<chunkID>.glcb`. If we ever want date-prefixed / hash-sharded / multi-bucket keys, every existing FSM ManifestEntry that just stores the chunk ID becomes ambiguous. | `CmdUploadChunk` payload — at minimum a `KeyScheme uint8` that selects from a table of derivation functions. |

### Not needed on the ManifestEntry

- Region, endpoint, credentials — tier-level config; chunk-level isn't where these belong.
- Compression level, frame size — GLCB format-level; the reader infers from the bytes.
- TOC magic / version — GLCB format-level; in the file's footer.
- `Compressed` flag — drop entirely. Sealed = GLCB = compressed by definition.
- `IngestTSMonotonic` — verify on the redesign sketch whether anything actually needs this. If yes, it stays; if it's only a histogram-implementation hint, it's a property derivable from the TS index when needed.

### Minimum viable change to the FSM

Concretely, what's blocking and what's optional:

- **Blocking (must land with the redesign):** content hash. Without it,
  cache eviction becomes a correctness risk; with it, a re-fetched
  GLCB is verifiable.
- **Defensible (land in the same pass since `CmdUploadChunk` is being
  rewritten anyway):** cloud-service-ID snapshot, key scheme byte.
  Cheap to add now, painful to retrofit later.
- **Defer:** any further packing of derived data (e.g. levels histogram
  for the chunk, IM-built index offsets) into the ManifestEntry. Decide that
  separately once the histogram redesign settles on what it actually
  needs.

## The collapse: file vs cloud is one bit

Once GLCB is the only sealed-chunk format, the meaningful distinction
between a cloud-backed chunk and a file-backed chunk reduces to a
single property: **durability authority**.

- **File-backed sealed chunk**: the local `data.glcb` is the only copy.
  Loss = data loss. Cannot be evicted from disk; only retention rules
  may delete it, and deletion is permanent.
- **Cloud-backed sealed chunk**: the local `data.glcb` is a cache; the
  durable copy lives on S3. Eviction is safe — the next reader does a
  GET to repopulate. Retention rules acting on the FSM ManifestEntry still
  delete from S3 + locally; cache-eviction policies act on the local
  copy only.

Same file. Same format. Same byte layout. Same on-disk path. Same
readers. Same FSM `ManifestEntry` shape. The `CloudBacked` flag is the entire
difference, and it picks between two eviction policies operating on
the same byte sequence in the same place.

That collapse is the point of this redesign. The forking access paths,
caches, validators, and read-side branches that produced the
gastrolog-66b7x cascade existed only to bridge two formats and three
storage layouts that never had to be different.

---

# Part III — Action

## Audit against existing code

### What is already correct

These parts of the existing code are consistent with the rule and stay:

- The set of FSM commands and their `apply*` handlers in
  [tier/raftfsm/fsm.go](../backend/internal/tier/raftfsm/fsm.go).
- The `Announcer`
  ([tier/raftfsm/announcer.go](../backend/internal/tier/raftfsm/announcer.go)),
  which is the only allowed proposer of `Create/Seal/Compress/Upload/Delete`.
- `expandBounds` (running min/max applied per record at Append-time and
  inside `ImportRecords.writeRecord`) — produces correct
  IngestStart/IngestEnd/SourceStart/SourceEnd regardless of monotonicity,
  so by the time `AnnounceSeal` fires the bounds it sends are the truth.
- `scanTSBounds` as a startup recovery path inside `loadChunkMeta` —
  recomputing in-memory cache from the persisted idx file is fine; the
  FSM still owns the canonical bounds, the manager just rebuilds its
  local cache after a restart.
- The receipt-protocol commands
  (`TransitionStreamed/RequestDelete/AckDelete/FinalizeDelete`) — they
  mutate state flags only, never bounds.

### What is wrong (and why)

#### 1. `repairCloudBounds`, `validateTSExtents`, `cloudTSExtents`

Located in
[backend/internal/chunk/file/manager.go](../backend/internal/chunk/file/manager.go)
(`repairCloudBounds` ~3597, `validateTSExtents` ~2678, `cloudTSExtents`
~2699).

These functions exist to compensate for a historical bug where
`computeIngestBounds` derived `IngestStart/IngestEnd` from the first and
last *physical* records — which is correct only for monotonic chunks, and
gives garbage for ImportRecords-built (tier 2+) chunks. Once the bug was
fixed by replacing `computeIngestBounds` with `scanTSBounds`, the
repair pass was kept around to "heal" cloud chunks uploaded before the
fix.

Two violations of the rule:

- The repair pass treats the GLCB blob's embedded TS index as authoritative
  and overwrites `cloudIdx` to match it. That's a read path computing
  truth from outside the FSM.
- It only operates on the in-memory `cloudIdx` cache. Even if there were
  a legitimate reason to re-derive bounds, doing so without going through
  a `Cmd*` apply means the FSM and the cache diverge until the next
  reconcile.

This codebase is not in production. Any chunk on disk that was written
with the old (broken) `computeIngestBounds` does not need to be salvaged —
the user wipes `data/` and the cluster repopulates with correctly-bounded
chunks. The repair pass is dead weight.

#### 2. `computeIngestBounds`, `computeSourceBounds`

Located at
[backend/internal/chunk/file/manager.go:1529](../backend/internal/chunk/file/manager.go#L1529).

No callers. Pure dead code, leftover from the bug they introduced.

#### 3. The histogram cloud path reaches around the FSM

`findIngestPos` and `findIngestRank` in
[query/histogram.go](../backend/internal/query/histogram.go) chain three
fallbacks: chunk-manager-active-BTree, chunk-manager-cloud-TS-index, and
index-manager-on-disk-TS-mmap. None of those is the FSM. The bounds the
histogram needs (`IngestStart`, `IngestEnd`, `RecordCount`,
`IngestTSMonotonic`) all live in the FSM ManifestEntry. The histogram should
read them from there and stop dispatching across three different stores
that can disagree with each other and with the FSM.

The cascade of bugs we just walked through (L>>count spikes, gaps, dips,
"missing other") all share a root cause: the histogram trusts whichever of
those three stores answers first, and they answer differently during the
upload race.

#### 4. `uploadToCloud`'s ordering creates a transient state-of-the-world hole

The sequence today is:

1. Upload GLCB to S3.
2. `removeLocalDataFiles` (raw.log, idx.log, attr.log, dict, B+ trees gone).
3. Mutate in-memory meta: `meta.cloudBacked = true`, `delete(m.metas, id)`.
4. `cloudIdx.Insert(id, meta)`.
5. `Announcer.AnnounceUpload(...)` → `CmdUploadChunk` → FSM marks
   `CloudBacked=true`.

Between step 2 and step 5, the chunk briefly exists in a state where the
local manager has lost its local-file answer for "where are this chunk's
records by IngestTS" but neither `cloudIdx` nor the FSM has yet been told
the cloud TS index location. Histogram code that races into this window
sees `findIngestPos` fail and produces undefined-shape artifacts. We've
been patching the symptoms in the histogram. The root fix is to close the
window at the upload site.

#### 5. Cached cloud chunks have their own scattered on-disk layout

A cached cloud chunk's bytes today live across three different roots:

```
<CacheDir>/<chunkID>.glcb                     full GLCB blob, packaged
<tier>/.ts-cache/<chunkID>.<offset>           per-section TS index cache
                                                (one for ingest, one for source)
<IM-root>/<chunkID>.tsidx + ...               IM-derived indexes
                                                (token/attr/kv/json), preserved
                                                across upload because
                                                removeLocalDataFiles only
                                                touches the chunk dir
```

A file-backed sealed chunk lives entirely under `<tier>/<chunkID>/`
plus the IM-rooted indexes. Same data, different home, no design
reason for the divergence. Every read path that touches a chunk's
bytes therefore has two implementations or a `cloudBacked` branch:
`OpenCursor` → `openCloudCursor` vs sealed-local cursor; `ScanAttrs` →
`scanAttrsCloud` vs `scanAttrsSealed`; the histogram's `findIngestPos`
/ `findIngestRank` dispatch over three stores; cache eviction is its
own subsystem (`CacheDir` LRU) instead of regular chunk-dir lifecycle.

The GLCB packaging exists for *transport* — one S3 PUT/GET, one set of
HTTP overhead, one zstd dictionary frame. Once the bytes are local,
keeping them packaged buys nothing. Unifying onto GLCB-everywhere
([the on-disk format section](#the-on-disk-format-glcb-everywhere))
collapses every one of those forks.

### Code to delete

- `repairCloudBounds`
  ([file/manager.go:3597](../backend/internal/chunk/file/manager.go#L3597))
- `validateTSExtents`
  ([file/manager.go:2678](../backend/internal/chunk/file/manager.go#L2678))
- `cloudTSExtents`
  ([file/manager.go:2699](../backend/internal/chunk/file/manager.go#L2699))
- `computeIngestBounds`, `computeSourceBounds`
  ([file/manager.go:1529 / 1539](../backend/internal/chunk/file/manager.go#L1529))
- The `cloudIdx` re-derivation call in `RegisterCloudChunk` that pulls
  bounds from `cloudTSExtents` instead of trusting the FSM's
  `CmdSealChunk` payload
  ([file/manager.go:4044](../backend/internal/chunk/file/manager.go#L4044)).
- The histogram's `findIngestPos` / `findIngestRank` "no-index
  interpolation" fallbacks and per-bucket race guards in
  `timechartChunkByIndex` and `chunkBucketTotals`. With the upload-race
  fix in place, these fallbacks fire only on genuinely corrupt or absent
  TS-index files — which is a hard error, not a histogram artifact.
- `defensive AnnounceCreate + AnnounceSeal` in `uploadToCloud` and
  `adoptCloudBlob`
  ([file/manager.go:3763](../backend/internal/chunk/file/manager.go#L3763),
  [file/manager.go:3991](../backend/internal/chunk/file/manager.go#L3991)).
  These belt-and-braces re-announces exist because the writer didn't
  trust that the original `CmdCreateChunk`/`CmdSealChunk` had landed.
  After the upload reordering they're redundant and they're a violation
  of the rule (they re-write FSM state from a non-FSM source).
- The cloud-only read path: `openCloudCursor`, `openCachedCursor`,
  `downloadAndCacheCursor`, `scanAttrsCloud`, `findCloudTSPosition`,
  `findCloudTSRank`, `loadIngestEntries`, `searchTSCacheFile`,
  `tsCachePath`, the `.ts-cache` directory and every helper, separate
  `CacheDir` config. Replaced by a single GLCB-aware reader against
  `<tier>/<chunkID>/data.glcb`.
- `CompressChunk` and the compressed-multi-file format. Sealing
  produces a GLCB directly.
- The `Compressed` flag on the FSM ManifestEntry (or treat it as a synonym for
  `Sealed`).
- The `chunk.cloud.Manager` type. Folded into the unified
  `chunk.Manager`.

### Code to add

Most of the redesign is removal. The substantive additions:

- A single `MetadataReader` (or extension of the existing tier-instance
  query interface) that exposes everything the read paths need, sourced
  exclusively from the FSM. Read paths take a `MetadataReader` and never
  reach into `chunkMeta` or `cloudIdx` directly.
- A unified `chunk.Manager` with optional `CloudStore` (collapses
  `chunk.file.Manager` and `chunk.cloud.Manager`).
- The reordered upload sequence in `uploadToCloud` /
  `adoptCloudBlob` (and the GLCB-as-canonical seal path).
- Three new fields on `CmdUploadChunk` (and the corresponding FSM
  ManifestEntry): content hash, cloud-service-ID snapshot, key-scheme byte.
- A per-Manager in-memory `lastAccess map[ChunkID]time.Time` for LRU
  cache eviction. Ephemeral, no persistence.
- A wipe-and-restart note in the gastrolog-66b7x close-out: any existing
  cloud blobs in the user's bucket from before this change carry stale
  bounds; the user wipes their dev cluster (`data/` + the bucket) and
  starts fresh.

## Compatibility with the per-chunk indexing redesign

The repo carries a separate research artifact,
[docs/advanced_indexing.md](advanced_indexing.md), describing a future
rewrite of per-chunk indexing onto a Lucene-style codec
(Vellum FST + roaring posting lists + per-block bloom + Quickwit-style
hotcache footer). That work is deferred until this chunk redesign
lands, but the two efforts directly intersect on the GLCB format, the
seal/upload pipeline, and the cross-node read shape. To avoid painting
the indexing rewrite into a corner, this redesign commits to the
following invariants up front.

### What this redesign locks in (and why it's compatible)

| Decision in this redesign | What the indexing redesign needs | Compatible? |
|---|---|---|
| GLCB is the canonical sealed-chunk format on disk and on the wire | Same | Yes |
| Single GLCB-aware reader, mmap-based for local files | Same | Yes |
| TOC at end-of-blob with `(magic, version, offset, size)` slots | Generalizes the TOC for additional sections (FST roots, posting-list directory, blooms, column metadata) | Yes — TOC format is extensible by design; old readers ignore unknown section kinds |
| 96-byte GLCB header with `version` byte | Codec dispatch on `version` (versioned codec interface) | Yes — same dispatch point |
| Active-chunk B+ trees stay as the live-write index | Indexer builds sealed-chunk indexes; active-chunk path unchanged | Yes — different lifecycle stages |
| FSM ManifestEntry stays minimal (no per-chunk index metadata fields beyond what's needed for retrieval/integrity) | Index metadata lives in the GLCB footer, not the FSM | Yes — FSM doesn't grow |

### Constraints the indexing redesign imposes on this work

These have been folded into the redesign above and are restated here
so the order-of-operations list doesn't accidentally violate them:

1. **GLCB TOC must be extensible.** No new fixed-format TOC slots; new
   section kinds added by appending TOC entries with their own magic.
   Old readers MUST ignore unknown kinds.
2. **Seal/upload coordination is option B (seal → index → upload).**
   `CmdUploadChunk` does not fire until the indexer has finalised the
   in-footer indexes. A sealed-but-unindexed window is allowed locally
   between `CmdSealChunk` and indexer completion; cloud upload happens
   after.
3. **The local cache is byte-range cacheable, not whole-file-only.**
   Implementations are free to start with full-blob GETs; the
   on-disk shape, FSM, and eviction-policy hooks must permit a
   later sparse-cache implementation.
4. **Cross-node fetch shape is co-owned.** This redesign owns "how a
   cloud chunk's bytes are accessed." The indexing redesign owns "how
   the planner decides which bytes to fetch." Neither doc takes a
   position that would prevent the other from landing.
5. **No IM-rooted-only assumptions.** The IM token/attr/kv/json
   indexes stay in their current location during this redesign, but
   their final destination is inside the GLCB footer. Code that
   assumes "this index always lives at `<IM-root>/<chunkID>.*`" is a
   future-blocker; prefer abstractions that take the section's location
   from the GLCB TOC, even if today's implementation hard-codes the
   IM-rooted path.

### What we still don't know

The indexing redesign was written before this chunk redesign. It
assumes some details of the current GLCB format and chunk lifecycle
that this redesign changes (notably the seal/compress/upload sequence).
When the indexing rewrite lands as real code, expect at least one
round of "actually, the seal pipeline has to look like X for the
indexer to fit." That round will be cheap if the invariants above hold
and expensive if they don't.

The right posture: don't over-design either redesign for the other.
Land this one, get the chunk path clean, and let the indexing rewrite
amend the seal/upload pipeline in its own commit set on top of a
known-good baseline.

## Order of operations

When we start landing this:

1. Delete the dead code (`computeIngestBounds`, `computeSourceBounds`).
   Pure cleanup, no behavior change. Run tests, commit.
2. Reorder `uploadToCloud` and `adoptCloudBlob` so the upload-race
   window closes. Smallest behavioral change that lets us delete
   compensating code. Test, commit.
3. Delete `repairCloudBounds`, `validateTSExtents`, `cloudTSExtents`,
   and the bounds re-derivation in `RegisterCloudChunk`. Test
   (especially: restart with a fresh `data/`, make sure nothing relies
   on the repair pass). Commit.
4. Delete the histogram interpolation/per-bucket-guard fallbacks
   introduced in this thread. With step 2 in place they're unreachable.
   Test, commit.
5. Delete the defensive `AnnounceCreate + AnnounceSeal` in
   `uploadToCloud` / `adoptCloudBlob`. Test, commit.
6. Introduce `MetadataReader` and route the histogram read paths
   through it. Test, commit.
7. Land GLCB-as-sealed-format: replace `CompressChunk` with
   seal-time GLCB conversion, update `uploadToCloud` /
   `adoptCloudBlob` to PUT/GET `data.glcb` directly, delete the
   `.ts-cache` and separate-CacheDir machinery. Test, commit.
8. Collapse `chunk.cloud.Manager` into `chunk.Manager` with optional
   `CloudStore`. Drop the tier `type` field; behavior follows
   `cloud_service_id` set/unset. Test, commit.
9. Add the new FSM fields on `CmdUploadChunk` (content hash,
   cloud-service-ID snapshot, key-scheme byte) and the verification
   path on cache re-fetch. Test, commit.

Each step is a clean revert if it goes wrong. None of them depend on
keeping any compensating code "for safety" — we either commit to the
rule or we don't.

## Open questions

These are the things to nail down before writing code:

- Where does the histogram get the per-chunk *bucket distribution* for
  uncached cloud chunks? Two options: (a) embed a coarse histogram in
  the GLCB TOC at upload time (so it's a single small range request via
  offsets the FSM gave us), or (b) accept that uncached cloud chunks
  contribute one bucket-uniform-distribution entry to the histogram,
  rendered as ghost. Option (a) costs a bit at write, option (b) loses
  intra-chunk resolution for cold data. Both are consistent with the
  rule.
- Do we want `ManifestEntry` to gain an explicit `Tier` field? Today tier is
  implicit (which `ManifestEntry` you are looking at depends on which tier's FSM
  you queried). For the histogram that's fine — it iterates per tier
  anyway. Other callers might want it; deferring.
- Do we collapse `chunkMeta` and the FSM `ManifestEntry` shape? They duplicate a
  lot. Probably not in this pass — keep `chunkMeta` as a private
  Manager cache and let the FSM ManifestEntry be the wire/persistence shape.
- Receipt-protocol cleanup is out of scope for this document; the
  receipts already obey the rule.
