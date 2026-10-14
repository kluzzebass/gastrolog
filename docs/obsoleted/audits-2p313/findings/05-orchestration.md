# Phase 5 — Orchestration BC audit

**Packages:** `orchestrator/`, `orchestrator/pipeline/` (supervisor slice)  
**Epic:** gastrolog-2p313

## Clean

`pipeline/supervisor.go` (thin wiring), `scheduler.go`, receipt-protocol delete design (when used), `orphan_repatriation.go` (operator-driven).

## Findings (summary — 22 total)

| ID | Crit | Sev | Location | Violation | dcat |
|----|------|-----|----------|-----------|------|
| audit-orch-001 | 4,6,8 | P0 | `OverlayFromFSM` everywhere | Follower local `CloudBacked` wrong without overlay | gastrolog-3ukgz |
| audit-orch-002 | 4,6,8 | P0 | `projectAllCloudBackedFromFSM` | Snapshot skips upload effects → empty cloudIdx | gastrolog-3ukgz |
| audit-orch-003 | 4,6,8 | P0 | `registerPipelineGLCB` triple path | Seal before GLCB on disk | gastrolog-4trvb closed |
| audit-orch-004 | 3,4,6 | P1 | `noopPublisher` | Segments dropped without vault-ctl | audit-ingestion-008 |
| audit-orch-005 | 6,8 | P1 | `vaultCatchupSweepAll` @ 20s | Six reconciler sweeps compensate missed events | **sweep-010** |
| audit-orch-006 | 6,8 | P1 | `placementSweep` @ 15s | Placement/routing safety net | **sweep-005** |
| audit-orch-007 | 6,8 | P1 | vault-ctl membership @ 30s | Missed `desiredChanged` | **sweep-006** |
| audit-orch-008 | 6 | P1 | `backfillCloudUploads` @ 5s | Missed upload announce | **sweep-013** |
| audit-orch-009 | 6,8 | P1 | archival hourly + daily reconcile | Policy vs compensator mix | sweep-017/018 |
| audit-orch-010 | 6,2 | P2 | duplicate `EvictCache` | Retention + cache-eviction job | **sweep-012** |
| audit-orch-011 | 8 | P1 | `wireVaultFSMOnDelete` vs reconciler | Dual delete paths | gastrolog-51gme |
| audit-orch-012 | 7,8 | P1 | `VaultInstance` god object | 15+ callbacks, last-writer-wins Wire | TBD |
| audit-orch-013 | 7 | P1 | reconciler holds `*Orchestrator` | Deep reach | TBD |
| audit-orch-014 | 4,7 | P1 | `manifest_reader` local-only rank | Same as audit-storage-007 | TBD |
| audit-orch-015 | 4,8 | P1 | dual FSM read APIs | `manifest_reader` vs `VaultManifestEntriesFromCtlFSM` | TBD |
| audit-orch-016 | 1,4 | P1 | unknown orphan preserve | Intentional data safety (gastrolog-3y8py) | — |
| audit-orch-017 | 6 | P2 | progress notifier 1s | UI throttle for chunk storms | TBD |
| audit-orch-018 | 6 | P2 | readiness 500ms cache | Infra poll | TBD |
| audit-orch-019–022 | various | P2 | pipeline SubmitToVault, ingester adapter, resumeSealing, placement reload | See full agent memo | TBD |

See [`04-sweep-compensators.md`](./04-sweep-compensators.md) for sweep-010, -012, -013, -017, -018.
