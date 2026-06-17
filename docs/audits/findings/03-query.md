# Phase 3 — Query BC audit

**Packages:** `query/`, `querylang/`, `lookup/`  
**Epic:** gastrolog-2p313

## Clean

`querylang/` (pure parse/eval), `lookup/` static/file/mmdb paths, `merge.go`, `vault_filter.go`, `plan.go`, rank-based histogram fast path when index serves.

## Findings

| ID | Crit | Sev | Location | Violation | Correct pattern | Workaround? | dcat |
|----|------|-----|----------|-----------|-----------------|-------------|------|
| audit-query-001 | 1,2,5,8 | P0 | `query.go` `loadTSEntries` → `buildTSOrderedScanner` | Search heap-decodes full ITSI via `LoadIngestEntries` | `FindIngestRank` / mmap + position scanner (histogram path) | N | **gastrolog-2o9e9** |
| audit-query-002 | 1,2,5,6 | P0 | `query.go` + `scanner.go` `reorderByTSWithBounds` | Index miss → buffer entire chunk + sort | ITSI range fetch + position walk | Y | **gastrolog-2o9e9** |
| audit-query-003 | 2,5 | P1 | `search.go` `primeHeap` | O(chunks) iterators before merge for `limit=100` | Lazy chunk activation on heap demand | N | **gastrolog-2o9e9** |
| audit-query-004 | 4,8 | P1 | histogram vs `query.go` | Exact search vs rank/FSM estimates when index missing | Same `IndexReader` path both sides | Y | **gastrolog-2o9e9** |
| audit-query-005 | 4,6 | P1 | `histogram.go` `distributeChunkRecordsByOverlap` | FSM `RecordCount` × overlap when rank probe fails | FSM TOC + range index read | Y | **gastrolog-2o9e9** |
| audit-query-006 | 4,6 | P1 | `histogram.go` `applyCloudSelectivity` | Scales cloud by local filter ratio | Filtered cloud scan / remote rank | Y | TBD |
| audit-query-007 | 4,6 | P2 | `search_histogram.go` `buildHistogramBuckets` | Global level-ratio for unsampled remainder | Full scan or skip breakdown | Y | TBD |
| audit-query-008 | 3,6 | P2 | `query.go` `searchChunkWithRef` | gastrolog-3ukgz skip-with-warn on `ErrChunkNotFound` | FSM/cm convergence | Y — peer fan-out | **gastrolog-3ukgz** |
| audit-query-009 | 3,6 | P2 | `search.go` cloud prime skip | Unreadable cloud chunk omitted | Repair/range fetch | Y | gastrolog-1dg3i |
| audit-query-010 | 6 | P2 | `search.go` `pruneStaleResumePositions` | Resume + highwater when chunks vanish | Stable chunk identity | Y | TBD |
| audit-query-011 | 2,5 | P1 | `scanner.go` `reorderByTS` | Active chunk full buffer + sort | `ScanActiveByIngestTS` walk | N | **gastrolog-2o9e9** |
| audit-query-012 | 2,5 | P1 | `pipeline_ops.go` `applyRecordOps` | Materializes full search for pipe ops | Streaming pipeline | Partial | TBD |
| audit-query-013 | 2,5 | P2 | memory vault + query | Perpetual reorder path for sealed memory chunks | Memory index equivalent | Y | TBD |
| audit-query-014 | 7,8 | P2 | `manifest_reader` via `indexReader` | Rank only on locally hosted vault | FSM-visible rank all voters | Y | audit-storage-007 |
| audit-query-015 | 8 | P2 | `histogram.go` dead `timechartFastPath` | Uncalled + stale comments | Delete / fix docs | N | TBD |
| audit-query-016 | 7,8 | P2 | `querylang` vs `server/query` directives | Manual sync of directive strip list | Shared directive API | Y | TBD |
| audit-query-017 | 5 | P2 | `lookup` in pipeline | Per-record network I/O | Batch prefetch | N | TBD |
| audit-query-018 | 4,5 | P2 | `timechartScanPath` 1M cap | Filtered histogram truncated | Streaming binning | Y | TBD |

**Primary remediation:** **gastrolog-2o9e9** — unify search on mmap rank + position scanner.
