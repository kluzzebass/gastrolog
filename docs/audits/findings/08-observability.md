# Phase 8 — Observability audit

**Packages:** `logging/`, `alert/`, `notify/`, `sysmetrics/`, `lifecycle/`  
**Epic:** gastrolog-2p313

## Clean

`alert/` (dumb registry), `lifecycle/` (shutdown gate), `sysmetrics/` (node-local CPU/RSS).

## Findings

| ID | Crit | Sev | Location | Violation | Correct pattern | Workaround? | dcat |
|----|------|-----|----------|-----------|-----------------|-------------|------|
| audit-obs-001 | 3,6 | P1 | `logging/capture.go` `CaptureHandler` | Full capture channel → silent drop | Alert on `dropped` advance | Y | gastrolog-5d5a3 |
| audit-obs-002 | 3,2 | P2 | `notify/bus.go` `Emit` | Subscriber channel full → silent drop | Version-gap resync (documented) | N | gastrolog-3pf9w |
| audit-obs-003 | 7,8 | P2 | `server/system_log_levels.go` glob helpers | Duplicates `logging.LevelRule` match logic | Shared helpers from `logging/` | N | TBD |
| audit-obs-004 | 8 | P2 | `app/log_level_watcher.go` | Wakes on any `configSignal` | Dedicated LogLevels signal | N | — |
| audit-obs-005 | 8 | P2 | `logging/comp/` + `ListLogComponents` | Effective level is per-node; RPC looks cluster-wide | Label local vs FSM rules | N | — |
