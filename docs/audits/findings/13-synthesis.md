# Phase 13 — Synthesis

**Epic:** gastrolog-2p313  
**Date:** 2026-06-17

## Executive summary

~120 formal findings across 11 BC memos. **5 P0 root causes** drive most user-visible pain; **~25 P1** items are perf, cluster-first gaps, or compensator lattices. Remediation should be **5 epics**, not 120 tickets.

---

## Dedupe groups (one fix, many finding IDs)

| Group | Root cause | Finding IDs | Existing dcat |
|-------|------------|-------------|---------------|
| **G1: Search TS path** | Search heap-decodes ITSI + reorder buffer instead of mmap rank/position | audit-storage-001/002/009, audit-query-001/002/003/004/011, audit-storage-014 | **gastrolog-2o9e9** |
| **G2: FSM ↔ local cm** | Follower chunk meta diverges; overlay at read time | audit-orch-001/002/014/015, audit-storage-007/008, audit-query-008/014, audit-server-012 | gastrolog-3ukgz (closed; residual overlay debt) |
| **G3: GLCB at seal** | Build/seal ordering; triple registration compensator | audit-orch-003, audit-ingestion-009 | gastrolog-4trvb (closed) |
| **G4: Vault-ctl forward barrier** | System Raft has read-after-write; vault-ctl does not | audit-cluster-001 | **gastrolog-4l24u** |
| **G5: Pipeline event durability** | Non-blocking sends → 2s rescan/recollect/replan chain | audit-ingestion-002–004, sweep-001–004 | gastrolog-2i62e, 5vwav, 12gue |
| **G6: Orchestrator sweep lattice** | 20s vaultCatchup runs 6 reconciler sweeps | audit-orch-005, sweep-010 | **gastrolog-3fu9t** |
| **G7: Histogram/search divergence** | Rank miss → FSM proportional estimate vs exact search | audit-query-005/006/007, audit-storage-006/007 | extends G1 + G2 |
| **G8: Ingester BC coupling** | Types in orchestrator; adapter + non-cluster RPC ops | audit-ingestion-005, audit-orch-020, audit-server-001/002 | new cluster-first task |
| **G9: Compensator placement/cloud** | 15s placement, 5s cloud backfill, 30s vault-ctl membership | audit-orch-006–008, sweep-005/006/013, audit-server-006/007 | partial new |
| **G10: Config/store integrity** | Storage class fallback, dual placement mirror, export bundles runtime | audit-system-001/002/003, audit-cmd-002/005, audit-api-006 | gastrolog-4dag5 family |

---

## P0 stack rank (fix first)

| Rank | Group | Why P0 | Remediation epic |
|------|-------|--------|------------------|
| 1 | **G1** | Wrong results + RAM at scale on every search | Query index unification (**2o9e9**) |
| 2 | **G4** | Follower can read pre-apply vault-ctl state after forward | Vault-ctl apply barrier (**gastrolog-4l24u**) |
| 3 | **G2** | Cluster node matters; skip-with-warn; ghost histogram | FSM-grounded chunk reader (retire OverlayFromFSM) |
| 4 | **G3** | Query miss immediately after seal on pipeline chunks | Seal+GLCB atomicity (verify 4trvb fix holds) |
| 5 | **G5** | Silent segment loss → 2s latency floor on ingest | Pipeline event graph (remove rescan chain) |

---

## P1 stack rank (next)

| Rank | Item | dcat action |
|------|------|-------------|
| 1 | G6 — vaultCatchup 20s lattice | **gastrolog-3fu9t** |
| 2 | G9 — cloud backfill 5s (sweep-013) | **gastrolog-576bm** |
| 3 | audit-cmd-002 — add-storage NodeId decode | **gastrolog-4gp8h** |
| 4 | audit-system-001/002 — StorageIDForNode fallback | **gastrolog-2bv1x** |
| 5 | audit-server-001/002 — ingester trigger/validate not cluster-first | **gastrolog-5kdzj** |
| 6 | audit-server-003 — peer fanout silent partial | Link gastrolog-csspr / 66zrj |
| 7 | audit-cluster-002 — raftwal per-log submit + heap retention | New perf task |
| 8 | audit-orch-011 — dual delete paths | Link gastrolog-51gme |
| 9 | G8 — move Ingester interfaces out of orchestrator | Refactor epic |
| 10 | sweep-005/006 — placement 15s + vault-ctl 30s | Event graph tasks |

---

## Remediation epics (proposed)

### Epic A — Query index unification (G1, G7)
**Owner issue:** gastrolog-2o9e9  
Unify `buildTSOrderedScanner` on `FindIngestRank` + position scanner; remove `LoadIngestEntries` from search hot path; align histogram rank-miss behavior.

### Epic B — Vault-ctl read-after-write (G4)
Mirror `raftstore.forwardAndWait` / `ForwardApplyResponse.applied_index` for vault-ctl `ForwardVaultApply`; remove reliance on "usually catches up."

### Epic C — FSM-grounded chunk metadata (G2)
Single reader API; followers don't maintain divergent `CloudBacked`; retire `OverlayFromFSM` call sites; fix `manifest_reader` local-only rank.

### Epic D — Pipeline event durability (G5)
Blocking or durable completed-segment handoff; vault-ctl publish events for segment pull; remove sweep-001–004.

### Epic E — Compensator retirement program (G6, G9)
Inventory in [`04-sweep-compensators.md`](./04-sweep-compensators.md); per sweep: fix upstream → delete tick. Start with vaultCatchup (sweep-010) and cloud backfill (sweep-013).

### Epic F — Layering & cluster-first transport (G8, server findings)
Move ingester contracts; thin server handlers; ingester RPC forward; degraded-peer wire signal.

---

## Findings × criterion (rollup)

| Criterion | P0 count | P1 count | Top violators |
|-----------|----------|----------|---------------|
| 1 Indexes | 3 | 8 | query, chunk/file, index/file |
| 2 Heap | 2 | 10 | query, chunk/file, raftwal |
| 3 Cluster-first | 1 | 6 | server ingesters, manifest_reader, forward |
| 4 Correctness | 4 | 12 | orch FSM projection, histogram/search |
| 5 Performance | 0 | 8 | primeHeap, raftwal, lookup |
| 6 Compensators | 0 | 15 | pipeline 2s, orch 20s, app 15s/30s |
| 7 Layering | 0 | 8 | VaultInstance, server handlers, ingester→orch |
| 8 Patterns | 1 | 14 | dual delete, dual placement, proto synonyms |

---

## dcat children filed in Phase 13

| Finding | Issue | Priority |
|---------|-------|----------|
| audit-cluster-001 | gastrolog-4l24u (vault-ctl apply barrier) | 1 |
| audit-cmd-002 | gastrolog-4gp8h (add-storage NodeId) | 1 |
| audit-system-001/002 | gastrolog-2bv1x (storage class fallback) | 1 |
| audit-server-001/002 | gastrolog-5kdzj (ingester RPC cluster-first) | 2 |
| sweep-010 / audit-orch-005 | gastrolog-3fu9t (vaultCatchup compensator lattice) | 2 |
| sweep-013 / audit-orch-008 | gastrolog-576bm (cloud backfill 5s compensator) | 2 |

**Not duplicated (reference only):** gastrolog-2o9e9, 2i62e, 5vwav, 12gue, 3ukgz, 9ohip.

---

## Audit complete

Phases 0–13 documentation complete. Remediation is **separate epics** (A–F above). Epic gastrolog-2p313 can move to `in_review` after user validates docs.
