# Phase 7 — Transport & composition audit

**Packages:** `server/`, `app/`, `auth/`, `cert/`, `internal/frontend/` (embed)  
**Epic:** gastrolog-2p313

## Clean

`auth/` (JWT/interceptor), `cert/` (PEM manager), `frontend/` (static embed handler).

## Findings

| ID | Crit | Sev | Location | Violation | Correct pattern | Workaround? | dcat |
|----|------|-----|----------|-----------|-----------------|-------------|------|
| audit-server-001 | 1,3 | P1 | `system_ingesters.go` `TriggerIngester` | Triggers local orchestrator only | Forward to assignment node | N | TBD |
| audit-server-002 | 1,3 | P1 | `system_ingesters.go` `TestIngester` / `validateIngester` | Port checks on receiving node; weak cross-node validate | Forward validate or require all replicas | N | TBD |
| audit-server-003 | 2,3 | P1 | `peer_fanout.go` + vault/query callers | Failed peers logged only; partial merge, no wire flag | `degraded_peers[]` on inspector RPCs | N | gastrolog-csspr |
| audit-server-004 | 2,3 | P1 | `query_remote.go` `collectRemote` | Incomplete remote streams merged blindly | Per-vault stream health metadata | Y | TBD |
| audit-server-005 | 3,7 | P1 | `app/dispatch.go` `configDispatcher` | Sync orchestrator side effects; errors logged not returned | Async queue + reconcile | Y | TBD |
| audit-server-006 | 6,8 | P1 | `app/placement.go` 15s reconcile | Non-blocking trigger drop + cron safety net | Reliable event graph | Y | **sweep-005** |
| audit-server-007 | 6,8 | P2 | `unreachable_sweep` / `peer_cache_reconcile` / `learner_promoter` @ 30s | PeerState / Raft observer backstops | Observer-only correctness | Y | sweep-009, -016, -017 |
| audit-server-008 | 6,8 | P2 | `app/raft.go` FSM catch-up / leader polls | 50ms–500ms startup polls | Apply-complete event | Y | sweep-008 |
| audit-server-009 | 6,3 | P2 | `ingester_alive_reconciler.go` | Compensates dropped `SetIngesterAlive` | Durable outbox | Y | gastrolog-1ox8z |
| audit-server-010 | 7,8 | P2 | `server/system*.go`, `vault_chunks.go`, `query.go` | Fat handlers (~3.3k LOC domain in RPC layer) | Thin handlers → services | N | TBD |
| audit-server-011 | 8 | P2 | `routing/routes.go` vs handlers | Route taxonomy drift (fan-out inside handler) | Single strategy per shape | N | TBD |
| audit-server-012 | 4,8 | P2 | `server.go` `ResolveVaultOwner` | Uses `VaultConfig.Placements` mirror | `GetVaultPlacements` only | Y | TBD |
| audit-server-013 | 1,2 | P2 | `vault_chunks.go` `active_only` | Skips remote fan-out | Document cluster-incomplete contract | N | — |
| audit-server-014 | 7 | P2 | `routing/interceptor.go` | RouteLeader relies on store Apply | Document / verify all paths | N | — |

**Cluster-first:** Routing + interceptor solid for vault ops; gaps in ingester ops and degraded fan-out merges.
