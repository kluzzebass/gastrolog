# Backend architecture audit

**Epic:** **gastrolog-2p313** — *Full backend architecture audit (8 criteria)*  
**Branch:** `chore/gastrolog-2p313-full-backend-audit`  
**Status:** Documentation complete. **Tracker:** P0/P1 filed under remediation sub-epics (see [issue-map.md](./issue-map.md)). Epic gastrolog-2p313 → `in_review` when you approve.

---

## What this is

A **read-only, whole-backend review** of every production package under [`backend/`](../../backend/). The goal is not lint scores or test coverage percentages — it is to document where the codebase violates architectural invariants that GastroLog depends on for correctness, scale, and cluster behavior.

This audit was triggered by a recurring pattern: features shipped quickly by bypassing existing machinery (`FindIngestRank`, mmap views, position scanners, FSM-grounded metadata) and compensating with heap buffers, periodic sweeps, or silent degradation. Those workarounds create regressions that are hard to see in single-node tests but show up under cluster load, snapshot restore, or large vaults.

**This pass documents problems and files remediation issues.** It does **not** change production code. Fixing findings is separate work tracked in remediation epics (see [13-synthesis.md](./findings/13-synthesis.md)).

---

## Scope

### In scope

| Path | Reviewed |
|------|----------|
| [`backend/internal/*`](../../backend/internal/) | All 39 top-level packages (+ `orchestrator/pipeline`) |
| [`backend/cmd/*`](../../backend/cmd/) | `gastrolog`, `walinspect`, `multirun`, `compress-assets` |
| [`backend/api/proto`](../../backend/api/proto/), [`api/gen`](../../backend/api/gen/) | Wire contracts and generated drift |
| [`backend/justfile`](../../backend/justfile), [`go.mod`](../../backend/go.mod), [`ROADMAP.md`](../../backend/ROADMAP.md) | Tooling and stale docs (phase 12) |
| Co-located `*_test.go` | Reviewed with each package (gaps noted as findings, not fixed here) |

### Out of scope

Repo-root [`frontend/`](../../frontend/), [`deploy/`](../../deploy/), [`scripts/`](../../scripts/), [`.github/`](../../.github/), [`test/e2e/`](../../test/e2e/). CI implementation and test rewrites are separate efforts.

### Organizing principle

Phases follow the eight **bounded contexts** in [`docs/ubiquitous_language.md`](../ubiquitous_language.md), plus dedicated passes for CLI/API, shared libraries, cross-cutting import graph, and synthesis — so no package is “assumed covered” by a neighbor.

Canonical references used during review: [`CLAUDE.md`](../../CLAUDE.md) (cluster-first, `raftwal` priority), [`docs/query_execution.md`](../query_execution.md), [`docs/vault-control-plane-architecture.md`](../vault-control-plane-architecture.md), [`backend/internal/raftwal/README.md`](../../backend/internal/raftwal/README.md).

---

## The eight criteria

Every finding is tagged against one or more of these lenses:

| # | Criterion | Question we ask |
|---|-----------|-----------------|
| **1** | **Indexes used where possible** | Does hot-path code use mmap rank, inverted lookup, `ScanAttrs`, position scanners — or fall back to full scans / heap decode? |
| **2** | **No unnecessary heap loading** | Are full chunks, full ITSI tables, or reorder buffers allocated when a bounded read or iterator would suffice? |
| **3** | **Cluster-first** | Does behavior depend on which node the user is connected to? Do RPCs forward correctly? Are partial cluster views surfaced? |
| **4** | **Correctness** | Is vault-ctl FSM the authority for chunk metadata? Do histogram and search agree? Are errors fail-closed vs skip-with-warn? |
| **5** | **Performance** | Is complexity proportional to `limit` and result size — not O(chunks) or O(vault records) for small queries? |
| **6** | **No compensating workarounds** | Does a periodic sweep or retry loop paper over a missed event? Trace the upstream gap; fix that, then remove the tick. |
| **7** | **No leaky abstractions** | Do handlers reach through interfaces into concrete packages? Do BC boundaries invert (e.g. ingester → orchestrator for types)? |
| **8** | **Separation / coherent patterns** | Dual delete paths, dual placement sources, proto synonym drift, duplicate APIs for the same concept. |

**Criterion 6 is not grep-driven.** For each `time.Ticker` or gocron job we ask: *what invariant should an event have enforced already?* Documented in [`findings/04-sweep-compensators.md`](./findings/04-sweep-compensators.md) and [`invariant-ledger.md`](./invariant-ledger.md).

---

## How the review was run

1. **Phase per bounded context** — read production code and tests; record violations with location, correct pattern, and whether a workaround exists.
2. **Invariant ledger** — map broken invariants to compensating code across phases (not isolated per file).
3. **Sweep rollup** — index all periodic compensators (001–018); tie each to upstream gap.
4. **Cross-cut** — import graph, ubiquitous-language drift, legacy vs pipeline v3, tooling notes.
5. **Synthesis** — dedupe ~120 rows into ~10 root-cause groups; stack-rank P0/P1; propose remediation epics.

No package was marked reviewed without a memo row or explicit cross-ref. See [coverage.md](./coverage.md) (**48/48** scoped packages + phase 12 tooling).

---

## Artifacts (how to use this folder)

| File | Purpose |
|------|---------|
| [coverage.md](./coverage.md) | Package × phase checklist with memo links — start here to see what was reviewed |
| [invariant-ledger.md](./invariant-ledger.md) | **Cross-phase truth map** — invariant → authoritative source → violator / compensator |
| [findings/](./findings/) | Per-phase finding tables and rollups |
| [findings/13-synthesis.md](./findings/13-synthesis.md) | **Executive summary** — dedupe groups, P0/P1 rank, remediation epics A–F |
| [findings/04-sweep-compensators.md](./findings/04-sweep-compensators.md) | All periodic sweep findings in one place |
| [issue-map.md](./issue-map.md) | Finding ID → dcat issue under remediation sub-epics |

### Issue tracker (`dcat list gastrolog-2p313`)

```
gastrolog-2p313  Full backend architecture audit
├── gastrolog-18k9l  Remediation A: Query index unification
├── gastrolog-2e3mt  Remediation B: Vault-ctl read-after-write
├── gastrolog-64ipe  Remediation C: FSM-grounded chunk metadata
├── gastrolog-36wys  Remediation D: Pipeline event durability
├── gastrolog-8gmd0  Remediation E: Compensator retirement
└── gastrolog-3471q  Remediation F: Layering & cluster-first transport
```

Every **P0/P1** finding has a task (title prefix `audit-*` or `sweep-*`). **gastrolog-2o9e9** reparented under Epic A from gastrolog-q9tek.

### Phase memos

| Phase | Bounded context | Memo |
|-------|-----------------|------|
| 1 | Storage | [01-storage.md](./findings/01-storage.md) |
| 2 | Ingestion | [02-ingestion.md](./findings/02-ingestion.md) |
| 3 | Query | [03-query.md](./findings/03-query.md) |
| 4 | Cluster / Raft | [06-cluster.md](./findings/06-cluster.md) |
| 5 | Orchestration | [05-orchestration.md](./findings/05-orchestration.md) |
| 6 | Replication | [06-replication.md](./findings/06-replication.md) (cross-ref index) |
| 7 | Transport | [07-transport.md](./findings/07-transport.md) |
| 8 | Observability | [08-observability.md](./findings/08-observability.md) |
| 9 | Shared libs | [09-shared.md](./findings/09-shared.md) |
| 10 | API + cmd | [10-api-cmd.md](./findings/10-api-cmd.md) |
| 11 | system / config | [11-system.md](./findings/11-system.md) |
| — | Sweeps (rollup) | [04-sweep-compensators.md](./findings/04-sweep-compensators.md) |
| 12 | Cross-cut | [12-crosscut.md](./findings/12-crosscut.md) |
| 13 | Synthesis | [13-synthesis.md](./findings/13-synthesis.md) |

---

## Finding format and tracking

| Field | Meaning |
|-------|---------|
| **ID** | `audit-<area>-<nnn>` (e.g. `audit-query-001`) or `sweep-<nnn>` for compensators |
| **Crit** | Criteria numbers 1–8 |
| **Sev** | P0 / P1 / P2 (below) |
| **Workaround?** | Y if compensating code exists upstream of the symptom |
| **dcat** | Child issue under **gastrolog-2p313**, or link to existing bug (e.g. **gastrolog-2o9e9**) |

### Severity

- **P0** — Wrong results, cluster node matters, data loss risk, silent FSM/disk divergence
- **P1** — Order-of-magnitude performance or scale; cluster-first gaps; compensator lattices
- **P2** — Layering, duplication, dead code, comment/proto drift

### Filing rule

Each P0/P1 finding → dcat task under the remediation sub-epic (see [issue-map.md](./issue-map.md)). P2 remain memo-only unless promoted.

---

## What we found (headline themes)

~**120** formal findings; **5 P0 root causes** account for most user-visible pain.

1. **Search vs histogram path split (G1)** — Search heap-decodes ITSI via `LoadIngestEntries` and may buffer entire chunks in `reorderByTSWithBounds`; histogram uses mmap `FindIngestRank`. Same vault, different machinery → RAM and wrong-order risk. **Primary issue: gastrolog-2o9e9.**

2. **FSM vs local chunk manager (G2)** — Followers keep divergent `CloudBacked` and related flags; `OverlayFromFSM` patches at read time. Histogram/search skip or estimate when local cm lags FSM. Residual debt after gastrolog-3ukgz.

3. **Compensator lattice (G5, G6, G9)** — Pipeline: non-blocking `completed` send → 2s rescan/recollect/replan chain (sweep-001–004). Orchestrator: 20s `vaultCatchupSweepAll` runs six reconciler sweeps (sweep-010); 5s cloud backfill (sweep-013); 15s placement (sweep-005); 30s vault-ctl membership (sweep-006). Steady-state load and latency floors that should not exist if events were reliable.

4. **Vault-ctl forward without apply barrier (G4)** — System config Raft fixed read-after-write (gastrolog-2nxij); vault-ctl `ForwardVaultApply` still returns before local FSM catch-up. **gastrolog-4l24u.**

5. **Layering inversion** — All `ingester/*` packages import `orchestrator` for core types; server handlers carry substantial domain logic; `VaultInstance` is a wide callback bag.

6. **Cluster-first gaps** — Ingester trigger/test RPCs local-only; peer fan-out merges partial results without wire degradation signal.

Full dedupe groups, stack rank, and remediation epics **A–F** are in [13-synthesis.md](./findings/13-synthesis.md).

---

## Remediation model (after audit)

Remediation is **not** “fix all 120 tickets.” Synthesis proposes **six epics**:

| Epic | Focus | Anchor issue |
|------|-------|--------------|
| **A** | Query index unification | gastrolog-2o9e9 |
| **B** | Vault-ctl read-after-write | gastrolog-4l24u |
| **C** | FSM-grounded chunk metadata | gastrolog-3ukgz family |
| **D** | Pipeline event durability | gastrolog-2i62e, 5vwav, 12gue |
| **E** | Compensator retirement | gastrolog-3fu9t, 576bm, sweep rollup |
| **F** | Layering & cluster-first transport | gastrolog-5kdzj, 4gp8h, 2bv1x |

Each epic should **remove compensators** as upstream events become reliable — not tune sweep intervals.

---

## Finding counts by phase (approx.)

| Phase | P0 | P1 | P2 |
|-------|----|----|-----|
| Storage | 2 | 6 | 6 |
| Ingestion | 1 | 8 | 9 |
| Query | 2 | 6 | 10 |
| Cluster | 1 | 4 | 9 |
| Orchestration | 3 | 10 | 9 |
| Transport | 0 | 6 | 8 |
| Observability | 0 | 1 | 4 |
| Shared | 0 | 0 | 5 |
| API+cmd | 0 | 2 | 14 |
| system | 0 | 2 | 6 |

---

## dcat children (audit epic)

Run `dcat list gastrolog-2p313` for the current list. Key children filed during synthesis:

| Issue | Topic |
|-------|-------|
| gastrolog-2i62e, 5vwav, 12gue | Pipeline 2s compensators |
| gastrolog-4l24u | Vault-ctl apply barrier |
| gastrolog-4gp8h | CLI add-storage NodeId |
| gastrolog-2bv1x | Storage class fallback |
| gastrolog-5kdzj | Ingester RPC cluster-first |
| gastrolog-3fu9t | vaultCatchup 20s lattice |
| gastrolog-576bm | Cloud backfill 5s |

---

## Next steps

1. **Review** this folder and synthesis stack-rank.
2. Set epic **gastrolog-2p313** → `in_review` when satisfied.
3. Pick remediation epic **A** or **B** first (highest P0 impact).
4. When closing audit epic: merge branch per project workflow; do **not** merge audit docs to `main` without review.

---

## What this audit does not do

- Change production code
- Replace CI, e2e, or reliability-matrix tests (those validate; this audit explains *why* gaps matter)
- Audit frontend UI (backend-only scope)
- Guarantee every P2 proto rename — those are documented for coordinated full-stack refactors
