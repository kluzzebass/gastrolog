# Phase 2 — Ingestion BC audit

**Packages:** `ingester/*`, `digester/`, `tokenizer/`, `pipeline/`  
**Epic:** gastrolog-2p313  
**Date:** 2026-06-17

## Clean (no P0/P1 in-package)

`digester/level`, `digester/timestamp`, `pipeline/paths`, `pipeline/routing` (table/ackjoin/sink), `pipeline/segment` (format), `pipeline/chunking/planner`, `release`, `merge`, `pipeline/ingestion` (message/minter), `tokenizer/` (query-only path)

## Findings

| ID | Crit | Sev | Location | Violation | Correct pattern | Workaround? | dcat |
|----|------|-----|----------|-----------|-----------------|-------------|------|
| audit-ingestion-001 | 6,8 | P1 | `segmentation/writer.go` `closeSegmentLocked` | Non-blocking `completed` send can drop notification | Blocking handoff or durable outbox | Y → rescan | **gastrolog-2i62e** |
| audit-ingestion-002 | 6,8 | P1 | `distribution/manager.go` `rescanInterval` | 2s disk scan republishes missed segments | Reliable segmentation→distribution event | Y — compensates 001 | **gastrolog-2i62e** |
| audit-ingestion-003 | 6,8 | P2 | `collection/manager.go` `recollectInterval` | 2s periodic recollect | FSM publish wakes cover all cases | Y | **gastrolog-5vwav** |
| audit-ingestion-004 | 6,8 | P1 | `chunking/manager.go` replan/pendingSeal/sealRetry | Timing patches for stall recovery | Complete FSM/event chain | Y | **gastrolog-12gue** |
| audit-ingestion-005 | 6,7,8 | P1 | `ingester_adapter.go` + `ingester/*` | Ingester BC imports orchestrator types | `pipeline/ingestion` contracts only | Y — V3 adapter bridge | TBD |
| audit-ingestion-006 | 7,8 | P2 | `ingester/metrics` `StatsSource` | Depends on `VaultSnapshot` | Narrow stats interface | Y | TBD |
| audit-ingestion-007 | 3,4 | P1 | `routing/manager.go` `route()` n==0 | Ack without durable write if no local sink | Fail-closed or forward | Y — origin registration timing | TBD |
| audit-ingestion-008 | 3,4 | P1 | `orchestrator/pipeline.go` `noopPublisher` | Silent drop when no vault-ctl handle | Fail-closed or explicit single-node FSM | Y | TBD |
| audit-ingestion-009 | 6,4 | P1 | `OnBuilt` / `registerBuiltPipelineChunk` | GLCB built after seal → cm miss | Unified seal/build ordering | Y — gastrolog-4trvb | closed |
| audit-ingestion-010 | 3,4 | P1 | `chunking/release.go` nil `RequiredHolders` | Holder gate off when nil | Always wire placement in cluster | Y — test shortcut | TBD |
| audit-ingestion-011 | 2,5 | P1 | `collection/manager.go` `collectOne` | Full segment in `bytes.Buffer` | Stream to pre-head | N | TBD |
| audit-ingestion-012 | 2 | P2 | `digestion/manager.go` `buildRecord` | Heap copy raw+attrs | Acceptable immutable handoff | N | — |
| audit-ingestion-013 | 5,8 | P2 | digestion/routing unbounded `work` chans | Unbounded worker queues | Bounded per stage | N | — |
| audit-ingestion-014 | 4,6 | P2 | `distribution` `onPull` | Swallows pull errors | Surface to caller | Y | TBD |
| audit-ingestion-015 | 4,6 | P2 | `chunking/leader.go` `loadSegmentViews` | Silent skip on locate failure | Nudge collect / error | Y | TBD |
| audit-ingestion-016 | 6,8 | P2 | `releaseWake` vs `wake` | Separate holder-ack signal | Single event graph | Y — avoid vault-ctl flood | TBD |
| audit-ingestion-017 | 8 | P2 | `ingestion/manager.go` checkpoint | Extra goroutine + 5s ticker | Inline checkpoint | N | — |
| audit-ingestion-018 | 1 | P2 | `digester/timestamp` | Full-line scan | OK at ingest | N | — |

## Compensator chain (core)

See also [`04-sweep-compensators.md`](./04-sweep-compensators.md) rollup.

```
segment close → completed/ on disk
  → non-blocking completed chan (may drop)     [001]
  → distribution rescan every 2s               [002]
  → vault-ctl publish → FSM → collection → chunking
  → replan/pendingSeal/seal retry            [004]
  → OnBuilt if build after seal              [009]
```

## Next

Phase 3 — Query BC (deep pass; overlaps **gastrolog-2o9e9**).
