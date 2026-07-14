# V3 Chunking — Design Exploration

**Status:** exploratory — nothing here is locked. Work on chunking separately from
[`design-notes.md`](./design-notes.md); that file still reflects an earlier lean
(merge-at-seal, k-way merge, searchable head). This document captures the design
conversation as of June 2026 and will supersede the chunking-related bullets in
design-notes once something is settled.

**Parent context:** Phase 7 ChunkingManager (`gastrolog-3mndj`). Merge + GLCB encode
are implemented on branch history (`chunking.MergeSpanRefs`, `chunking.BuildGLCB`);
orchestration, vault-ctl feed/manifest, and build-at-rotate are not.

---

## Problem

Turn completed **segments** (ephemeral, deterministic build inputs) into sealed
**chunks** (durable GLCB artifacts) on every vault home, such that:

- All replicas that **build** the same chunk produce **byte-identical** output.
- **Binary completeness** — never seal a short chunk because a segment was missing.
- **Rotation** respects operator policy (record count, bytes, schedule, …).
- **Query** can see recent data before it is sealed (the role active chunks fill today).
- Segment **purge** at the origin only after data survives in replicated chunks.

Segments solved the old fan-out problem: each segment has a **single origin** and
identical bytes on every holder. The remaining hard part is **how homes coordinate**
which segment data belongs in which chunk, and **when** heavy I/O runs.

---

## Manifest segment reference (invariant)

Every entry in the open-chunk manifest names a **partial or whole slice** of one
segment. Each ref must carry three fields (plus optional byte totals for size policy):

| Field | Meaning |
|-------|---------|
| **segmentID** | Which segment file to read. |
| **firstRecordNumber** | EventID-sorted **record number** of the first included record (0-based). |
| **lastRecordNumber** | EventID-sorted **record number** of the last included record (inclusive). |

**Record number, not file position.** Use **record number** in manifest/API names —
not “position,” which reads like on-disk placement. `firstRecordNumber` and
`lastRecordNumber` are ordinals in the segment’s **sorted** EventID sequence: record
number `n` is the `n`th record when ordered by `EventID` (0-based; same order used for
k-way merge and chunk build). They are **not** append-order frame numbers,
byte offsets, or `FilePos` values. A segment’s on-disk layout follows collection append
order; EventID order can differ. Resolve refs only through the segment EventID index
(e.g. `RecordAtEventOrder(n)`), never by scanning the file front-to-back or by raw
offset.

Same coordinate system as `chunking.Span` (`Start` / `Count`) and
`segment.IndexEntry` lookups.

**Record count** for one ref: `lastRecordNumber - firstRecordNumber + 1`. A ref with
`firstRecordNumber > lastRecordNumber` is invalid.

**Partial inclusion:** a budget cut mid-segment produces a ref whose
`lastRecordNumber` is before the segment’s final record. The **next** open chunk (or the
next ref after rotation) continues the same `segmentID` at
`firstRecordNumber = previousLast + 1`. Leader pick order: **continue the partial
segment before** pulling a new segment.

**Whole segment:** `firstRecordNumber = 0`, `lastRecordNumber = segmentRecordCount - 1`.

**Code mapping today:** `Span{Start, Count}` ↔ `firstRecordNumber = Start`,
`lastRecordNumber = Start + Count - 1`. Manifest FSM and vault-ctl should use the
explicit first/last form; builders convert at the build boundary if needed.

Multiple refs may reference the same `segmentID` in one manifest only if they are
non-overlapping contiguous slices (normally one ref per segment per manifest).

---

## What the earlier V3 lean assumed

From design-notes §23–27, 36–37 (paraphrased):

| Idea | Summary |
|------|---------|
| Planner vs builder | vault-ctl plans; per-home ChunkingManager executes. |
| Chunk = span list | Manifest refs: `(segmentID, firstRecordNumber, lastRecordNumber)` in merge order. |
| Merge-at-seal | k-way merge over segment EventID indexes → GLCB; one pass. |
| Searchable head | Per-segment EventID index for query until chunked. |
| Every home builds | Same inputs → same GLCB; no designated builder. |
| Completeness | Builder holds every needed segment or waits + nudges Collection. |

**Implemented toward that lean:** EventID-ordered segment index, key-only k-way merge,
streaming GLCB build (staging file, front-fixed layout). **Not implemented:** vault-ctl
chunk plan, manifest ref commit, ChunkingManager loop, eligibility snapshot.

---

## Insight: spans are discovered during merge, not planned from metadata

Segment metadata (record count, byte size, first/last IngestTS) names what is
**eligible**. It does **not** predict how records from multiple segments interleave
under a record or byte budget.

A chunk cut is the result of a **budget-limited k-way merge** from committed
**per-segment resume record number** (next `firstRecordNumber` after a partial cut).
Equivalently, manifest refs are the **output** of the build/planner, not derivable
from segment metadata totals before interleaving.

**vault-ctl durable state (original lean):**

- Record/size **budget** (rotation-policy shape).
- Per-segment **resume record number** (`firstRecordNumber` for the next ref after a partial cut).
- Segment **eligibility snapshot** (which completed segments participate — still
  needs a precise rule so lagging replicas do not merge different segment sets).

---

## Determinism

The budget stop is **deterministic** given shared inputs:

1. Committed per-segment resume record numbers (`firstRecordNumber` at chunk open).
2. Same segment eligibility snapshot (same segment IDs, same bytes).
3. Same budget spec and evaluation rule (see rotation mapping below).
4. Same merge/compare (`EventID` total order).

**Not derivable from metadata alone:** record count and byte totals on segments before
merge interleaves — but both are **monotonic accumulators over the merge walk** once
the walk runs.

**Replica agreement** fails if nodes run the merge with **different segment sets**
(e.g. one home has S3, another does not). Completeness gating must define the
snapshot before anyone builds.

**Rejected (straggler model):** time-window closure with per-origin watermarks — a
“late” record cannot belong inside an already-sealed chunk because the chunk is
defined by named segments/spans, not by a time predicate.

---

## Rotation policy mapping

Legacy rotation (`RotationPolicyConfig`) was built for **incrementally appended active
chunks** (`ActiveChunkState`, `ShouldRotate` before each append).

| Policy | Deterministic across replicas? | Fits segment→chunk flow? |
|--------|-------------------------------|---------------------------|
| **MaxRecords** | Yes (counter over merged or manifest totals). | Yes. |
| **MaxBytes** | Yes (pinned byte unit, same counter). | Yes (byte sum per ref when adding to manifest, or merge-walk accumulator). |
| **Cron** | Yes (vault-ctl schedule all replicas observe). | Yes — “switch at noon.” |
| **MaxAge** | Yes — see below. | Yes, in the **manifest FSM** model (D). |

**MaxAge in the manifest model**

Legacy MaxAge used `ActiveChunkState.CreatedAt` on an incrementally growing chunk
file. That object does not exist while the open chunk is only a manifest. The same
policy intent maps cleanly if age is **FSM state**, not local file metadata:

- On the **first** `AddSegmentRef`, commit `manifest_opened_at` (timestamp carried in
  the command or derived from deterministic segment fields).
- On each later add, the command carries `ref_added_at` (same source rule).
- Evaluate rotation when `ref_added_at - manifest_opened_at >= MaxAge` as part of
  that command’s state transition (rotate-then-add, or add-then-rotate — pin one rule).

Every replica replays the same commands and stores the same timestamps, so the
predicate is **deterministic across replicas** — the same class of argument as
record/byte totals on the manifest.

**Pin the timestamp source:** either an explicit field in each Raft command (leader
chooses once at commit; followers never call `time.Now()` at apply), or a
deterministic function of segment metadata (e.g. segment close / header time). Mixing
sources across commands would break replay.

**Not the same as legacy “poll ShouldRotate on a timer”:** if no new segments arrive,
nothing evaluates MaxAge until the next add (unless a separate scheduled
`EvaluateRotation` command exists — same Cron/orchestration question as today).

Still valid for **segment closure on the origin** (§24 in design-notes) — orthogonal
clock.

**IngestTS time windows** can be deterministic with identical segments and a pinned
cut rule; the issue is suitability, not replayability. Design-notes §27 “no
time-window closure” refers to **straggler/watermark** semantics, not “never use time.”

---

## Approaches considered

### A. Merge-at-seal (original lean)

- Head holds indexed segments; on rotation, k-way merge + GLCB in one pass.
- **Pros:** No per-replica growing state during collection; heavy work once.
- **Cons:** Complex cursor/span discovery; searchable head; merge + encode at seal.

### B. Segments → incremental active chunk

Treat segments like record sources: drain into a **real active chunk** (append +
incremental B+ tree, immediately searchable). Drop head indexing and k-way merge at
seal; pick segments oldest-event-first (continue partial segment first).

- **Pros:** Reuses rotation policy and active-chunk query path; scan segment on drain
  (no segment search index if staging is dump-on-arrival).
- **Cons:** **Incremental feed vs identical replicas** — if homes collect at different
  rates, active chunks diverge unless tightly synchronized.

**Sync options discussed:**

| Option | Issue |
|--------|--------|
| Shared cursor / batch commits (1, 3) | One dead or slow replica **blocks** global progress. |
| Single builder (2) | No chunk replication during build — breaks contract. |

### C. Leader-directed feed on vault-ctl Raft

Vault leader commits **segment-into-chunk decisions**; followers apply the same
commands. Segment bodies stay in the work area until the chunk is built enough
cluster-wide; then origin may purge.

- Commands are metadata-only (e.g. `AddSegmentRef` with first/last record numbers) — not record bodies.
- Dead node: another replica **replays** the log and continues; no salvage of a
  half-written divergent active chunk.
- **Pros:** Coordinated progress without independent merge plans.
- **Cons:** Orchestration surface on vault-ctl; leader rules for lagging replicas;
  Raft volume if commands are too fine-grained.

**Open:** pace on slowest home vs any holder; seal/purge when RF vs all homes ready;
query on lagging `AppliedIndex`.

### D. Manifest of segment refs — build at rotate (current leading direction)

Do **not** build a physical active chunk during the open phase. The “open chunk” is a
**replicated manifest**: ordered list of segment refs `(segmentID, firstRecordNumber,
lastRecordNumber)` (+ byte totals per ref for size policy), extended by the vault leader
on Raft until rotation.

**While open:**

- Segments remain in the work area.
- **Per-segment EventID index** serves query over listed refs (virtual open chunk).
- Followers apply manifest updates; heavy I/O deferred.

**On rotation:**

- **Build once** at seal: walk manifest, read segments (index helps for partial spans
  and byte accounting), k-way merge / encode GLCB / seal.
- One **k-way merge at seal** over listed spans only, using segment indexes as
  cursors — merge moves from collection phase to build phase.

**Pros:**

- Cheap open phase (manifest only on apply).
- Rotation predicate from **summed ref metadata** (record count; bytes if each ref
  carries slice byte total).
- Follower lag: manifest applies without local build until segments present.
- Same family of leader + Raft coordination as C, without incremental chunk files.

**Cons / scrutiny:**

- Query layer must treat open chunk as **manifest + segment index fan-out**.
- Byte totals per ref must be computed **deterministically** when the leader adds a ref.
- Build must be **idempotent** (temp GLCB + rename).
- Seal/release gating: build when local holder has all manifest segments; cluster
  purge when chunk sealed/replicated per holder-set rules.

---

## Comparison

| | A merge-at-seal | B incremental active | C Raft feed → active | D manifest → build at seal |
|--|-----------------|----------------------|----------------------|---------------------------|
| Open-phase storage | Indexed head segments | Growing active chunk | Growing active chunk | Segment files + manifest |
| Open-phase query | Segment indexes | Active chunk B+ tree | Active chunk B+ tree | Segment indexes (listed refs) |
| Heavy I/O | At seal | Continuous | Continuous | **At seal only** |
| Coordination | Shared snapshot + build | Hard (divergence) | Leader Raft feed | Leader Raft manifest |
| Segment index | Yes (head + merge) | Optional (scan on drain) | Optional | **Yes (query + build)** |
| k-way merge | At seal | Dropped | Dropped | **At seal (over manifest only)** or sort at seal |

---

## Orchestration sketch (D + leader)

Loose — not an API spec.

```
Open chunk (manifest on vault-ctl FSM):
  Leader: AddSegmentRef(segmentID, firstRecordNumber, lastRecordNumber, bytes?)
          — pick order: continue partial segment (next first = prev last + 1), else
            oldest-event-first (header firstIngestTS or min EventID + segmentID tie-break)
  Followers: apply ref to local manifest when command applied

  Rotation when committed totals satisfy policy (records / bytes / cron)

Build at seal:
  Leader: SealOpenChunkManifest(chunkID)
  Each replica: if all manifest segments local → merge → GLCB → SealChunk
  Verify: blob digest equality across builders

Release:
  When chunk replicated per holder-set / RF rules
  Leader: ReleaseSegments(segmentIDs…) — origin and holders purge work-area copies
```

**Failure:**

- Leader dies: new leader continues manifest from FSM.
- Builder dies mid-build: replay or re-run idempotent build.
- Replica missing segment: cannot build locally; does not block manifest apply;
  seal/purge predicates must define behavior for stragglers.

---

## Package layout

Where code lives — aligned with design-notes (“one manager per phase; orchestrator
wires, does not own phase logic”).

| Concern | Package | What goes here |
|---------|---------|----------------|
| **Decisions (planner)** | `backend/internal/pipeline/chunking` | Pure functions: open manifest + resume record numbers + eligible segments + rotation policy → next `AddSegmentRef` (segmentID, first/last record number, byte total) or rotate. No Raft, no segment I/O — unit-testable. Lives beside existing `merge.go` / `glcb.go` (same span/budget semantics, different phase of the pipeline). |
| **Leader loop** | `backend/internal/pipeline/chunking` (`ChunkingManager`) | Vault-leader-only goroutine: read FSM + segment registry, call planner, **propose** commands through a vault-ctl `Applier`. Gated on vault leadership (same class of check as other leader-only work, not on every home). |
| **Execution (followers + leader)** | `backend/internal/pipeline/chunking` (`ChunkingManager`) | Every home: watch sealed manifest, build at seal (`BuildSealedChunk` / `MergeSpanRefs` + `BuildGLCB`), nudge Collection for missing segments, report chunk identity/digest back to vault-ctl. Does **not** invent membership. |
| **Durable state** | `backend/internal/vaultraft/vaultctlfsm` | Command types and `Apply` handlers only — open manifest refs, running totals, `manifest_opened_at`, resume map, seal/release. **No** segment scans, merge simulation, or pick-order policy. |
| **Harness integration** | `backend/internal/pipeline/flow_test.go` | Wire `ChunkingManager` into the in-process pipeline harness with test doubles (`gastrolog-5x29i`). Proves leader loop + build-at-seal + collection nudge **before** production wiring. |
| **Wiring (Rubicon)** | `backend/internal/orchestrator` | Start/stop `ChunkingManager` per vault home; inject real vault-ctl `Applier`, FSM callbacks, Collection nudge (`gastrolog-214bz` **only** — not before). |

Suggested files under `pipeline/chunking` (names provisional):

- `planner.go` — budget walk / next-ref / should-rotate
- `build.go` — `BuildSealedChunk` from sealed manifest
- `manager.go` — `ChunkingManager` Run loop (leader planner on vault-ctl events + follower build at seal)
- existing `span.go`, `merge.go`, `glcb.go`, `index.go`

Proto / generated commands for `AddSegmentRef` and open-chunk manifest fields extend
`vaultctlfsm` (and `vaultctlfsm.proto`); the planner never imports segment file paths
for decision-making — only segment metadata and indexes the leader can read when
simulating a cut.

---

## Relation to existing code

| Component | Status | Notes if direction D wins |
|-----------|--------|---------------------------|
| `pipeline/segment` index | Built | Keep — query + build reads. |
| `chunking.MergeSpanRefs` | Built | Reuse at **build** over manifest spans. |
| `chunking.BuildGLCB` | Built | Reuse as build output path. |
| `chunking.Span` / `SpanRef` | Built | `Start`/`Count` ↔ first/last; manifest uses explicit last. |
| `chunking` planner | Not built | Pure next-ref / rotate; see [Package layout](#package-layout). |
| vault-ctl chunk FSM | Not built | Manifest, rotation totals, seal, release (`vaultctlfsm` Apply only). |
| `chunking` ChunkingManager | Partial | Leader planner on vault-ctl events; build at seal + nudge Collection. |

**Warning — no FSM polling:** Collection and Chunking managers must not poll vault-ctl
state on a timer. Use FSM apply callbacks (`SetOnPublishCompletedSegment`,
`SetOnSealedManifest`), explicit nudges, and Run-start catch-up. See design-notes
§ Phases & managers (“Do not poll vault-ctl FSM state on a timer”).
| Query “virtual open chunk” | Not built | Fan-out to segment indexes for listed refs. |

---

## Open questions ( chew on these )

1. **Eligibility snapshot** — ~~when adding refs, must every required home already hold
   the segment, or only “some holder has it” with catch-up before seal?~~ **SETTLED
   (2026-07):** "some holder has it" + catch-up. A missing home must never block the
   plan; segments are transport (design-notes 29), so eligibility is holder-count ≥
   build-need, not all-homes. See design-notes "Revised by lived experience" R4.

2. **Seal / purge predicate** — ~~all homes built vs replication RF vs best-effort
   with available nodes; when can origin wipe segments?~~ **SETTLED (2026-07):**
   chunk-replication supersession, not all-homes. Purge once the segment's records
   survive in a replicated chunk (design-notes 39), with retention-elapsed as the
   give-up bound; `holders ⊇ homes` is a fast path only. The shipped code gated on
   all-holders and pinned segments on a dead node (the completed/ leak). See
   design-notes R3.

3. **Ref granularity on Raft** — whole segment vs partial span per command; batch size.

4. **Build ordering** — manifest pick order × within-segment scan vs one k-way
   merge at seal; GLCB / ingest index requirements.

5. **Byte accounting** — pinned unit; who computes slice bytes when adding a partial ref.

6. **Query** — manifest indirection, staleness when `AppliedIndex` or segment collection
   lags; cluster-first expectations.

7. **Chunk identity** — planner-assigned ID vs content hash as identity at seal.

8. **Revert design-notes chunking bullets?** — once this doc is settled, update
   design-notes and ubiquitous language in one deliberate pass (not piecemeal).

---

## Rejected or deprioritized

- Precomputed span plans from segment metadata alone (without merge/manifest walk).
- Time-window / watermark straggler closure for chunk membership.
- MaxAge tied to **local active-chunk file `CreatedAt`** without manifest FSM timestamps (legacy object model only).
- Searchable staging for **all** segments on disk (direction D: index only **listed**
  refs; staging otherwise dumb queue).
- Uncoordinated incremental active-chunk feed on each home without Raft (divergence).
- Blocking global progress on one dead replica (options 1/3 without takeover/replay).

---

## References

- [`design-notes.md`](./design-notes.md) — overall V3 pipeline (chunking bullets may be stale).
- [`../../ubiquitous_language.md`](../../ubiquitous_language.md) — canonical terms (pending chunking update).
- Implementation: `backend/internal/pipeline/chunking/`, `backend/internal/chunk/glcb/`.
- Tracker: `gastrolog-3mndj` (parent). Children (open):
  - `gastrolog-53ron` — vault-ctl open-chunk manifest FSM
  - `gastrolog-uffcg` — chunking planner (next ref / rotate)
  - `gastrolog-5u73c` — ChunkingManager build-at-seal
  - `gastrolog-5i9e6` — ChunkingManager leader planner loop
  - `gastrolog-5x29i` — ChunkingManager pipeline harness integration (`flow_test`)
  - `gastrolog-6chex` — virtual open-chunk query (isolated / harness)
  - `gastrolog-214bz` — **Rubicon:** orchestrator wiring for the full V3 stack (includes ChunkingManager lifecycle)
  - Prerequisite under Distribution: `gastrolog-5pyl3` — vault-ctl segment metadata Publisher
  - Closed: `gastrolog-4mry7`, `gastrolog-3a53d`, `gastrolog-25c02`
