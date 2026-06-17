# Phase 1 — Storage BC audit

**Packages:** `chunk/`, `index/`, `record/`, `manifest/`, `blobstore/`, `format/`  
**Epic:** gastrolog-2p313  
**Date:** 2026-06-17

## Clean (no P0/P1 in-package)

`record/`, `format/`, `blobstore/` (interface), `manifest/` (contracts), `index/idxmmap/`, `index/inverted/`, `index/build.go`

## Findings

| ID | Crit | Sev | Location | Violation | Correct pattern | Workaround? | dcat |
|----|------|-----|----------|-----------|-----------------|-------------|------|
| audit-storage-001 | 1,2,5,8 | P0 | `query/query.go` `buildTSOrderedScanner` → `loadTSEntries` | Full ITSI decode to heap via `LoadIngestEntries` | `FindIngestRank` / `MmapView.SearchTS` (histogram path) | N | (links query phase; fix with 2o9e9) |
| audit-storage-002 | 1,5,6 | P0 | `query/query.go` + `scanner.go` `reorderByTSWithBounds` | Index miss → buffer entire chunk + sort | Position scanner on mmap ITSI; fetch ITSI section only if needed | Y — gastrolog-1dg3i | **gastrolog-2o9e9** |
| audit-storage-003 | 1,5,6 | P1 | `chunk/file/manager.go` `openCloudCursor` | Full zstd blob download per cold read | `blobstore.DownloadRange` + FSM `IngestIdxOffset` | Y — warm cache path | TBD |
| audit-storage-004 | 1,5 | P1 | `chunk/file/manager.go` `scanAttrsViaGLCB` | Full record decode for attr sampling | `scanAttrsSealed` idx+attr only | Y | TBD |
| audit-storage-005 | 1,5 | P1 | `chunk/file/manager.go` `ReadWriteTimestamps` | Full cursor scan on cloud chunks | idx.log offset reads (local path) | N | TBD |
| audit-storage-006 | 4,7,8 | P1 | `manifest.IndexReader` + `manifest_reader.go` | FSM offset fields unused; local file paths only | Range read from GLCB/blob using FSM TOC | Y → histogram FSM estimate | TBD |
| audit-storage-007 | 3,6 | P1 | `manifest_reader.go` `lookupVaultManagers` | Rank lookup nil without local vault instance | FSM-visible rank on all voters | Y → skip vault / distribute | TBD |
| audit-storage-008 | 4,8 | P1 | `chunk/file/manager.go` `List`/`lookupMeta` | Local meta vs FSM projection | Single FSM-grounded reader | Y — overlay + reconciler | gastrolog-3ukgz family |
| audit-storage-009 | 2,8 | P1 | `index/file/manager.go` | Dual TS cache: mmap + heap `LoadIngestEntries` | Mmap-only API for search | N | TBD |
| audit-storage-010 | 2,5 | P2 | `index/file/manager.go` `evictCache` | Mmap not closed on evict | `MmapView.Close()` | N | TBD |
| audit-storage-011 | 7 | P2 | `index/file/*/indexer.go` CloudBacked skip | Sidecars not in GLCB; skip misdocumented | PostSeal sidecars before cloud flip | Y → runtime scan | TBD |
| audit-storage-012 | 4 | P2 | `chunk/file` `FindIngestEntryIndex` active | Position as rank when non-monotonic | Gate on `IngestTSMonotonic` | Y — histogram gates | TBD |
| audit-storage-013 | 1 | P2 | `chunk/file` `FindStartPosition` cloud | Always false for cloud | N/A (use IngestTS index) | Y | TBD |
| audit-storage-014 | 8 | P2 | `query/histogram.go` comment L267 | Says `LoadIngestEntries`; code uses `FindIngestRank` | Fix comment | N | TBD |

## Cross-cutting summary

**Coherent:** Histogram fast path (mmap rank arithmetic), `idxmmap` for sidecars, ITSI embedded in GLCB.

**Incoherent:** Query TS-ordering uses heap `LoadIngestEntries`; cold cloud uses RAM reorder buffer (**gastrolog-2o9e9**); `IndexReader` contract ahead of implementation; `DownloadRange` unused.

## Next

Phase 2 — Ingestion BC (`ingester/*`, `digester`, `tokenizer`, `pipeline/*`).
