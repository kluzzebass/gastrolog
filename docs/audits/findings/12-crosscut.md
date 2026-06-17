# Phase 12 — Cross-cutting audit

**Epic:** gastrolog-2p313  
**Scope:** import graph, compensator inventory, synonym drift, tooling, invariant ledger completion

## Tooling

| Path | Finding | Severity |
|------|---------|----------|
| `backend/justfile` L47 | `query` recipe defaults `GLOG_DATA_DIR` to `/tmp/gastrolog` — conflicts with repo `data/node*` layout in CLAUDE.md | P2 |
| `backend/justfile` L136 | `cluster-kill` defaults `/tmp/gastrolog` while `cluster-run` uses `../scripts/cluster.sh` (repo `data/`) | P2 |
| `backend/justfile` | `audit` recipe is golangci/vet/race — **not** architecture audit (name collision with gastrolog-2p313) | P2 doc |
| `backend/ROADMAP.md` L5 | Says "Go 1.25+"; `go.mod` is **1.26.0** | P2 |
| `backend/ROADMAP.md` L9–17 | "store filters", "rotation policies" — pre-v3 vocabulary; omits vault-ctl FSM, pipeline v3, cloud-backed chunks | P2 stale |
| `backend/ROADMAP.md` L34–36 | OTLP, Fluent Forward, Kafka listed `[ ]` — **implemented** in `ingester/` | P2 stale |
| `backend/go.mod` | Query hot path: no direct heavy deps; **zstd** enters via `chunk/file` + `chunk/cloud` on every sealed read | note |
| `backend/go.mod` | **gojq**, **maxminddb**, **fsnotify** on lookup pipe path only (audit-query-017) | note |

## Import layering (top violations)

| # | Edge | Violation | Remediation direction |
|---|------|-----------|----------------------|
| L1 | `ingester/*` → `orchestrator` | All ~25 ingester subpackages import orchestrator for `Ingester` / `IngestMessage` types | Move contracts to `ingester` or `pipeline/ingestion` |
| L2 | `orchestrator` → `query` | Orchestrator builds query engines | Inject `query.Engine` factory port |
| L3 | `orchestrator` → `chunk/file` | `vault_transfers.go`, `reconfig_vaults.go` use concrete file manager | Stay on `chunk.ChunkManager` |
| L4 | `server` fat handlers | ~3.3k LOC domain in RPC layer (`vault_chunks.go`, `query.go`) | Extract services |
| L5 | `app` → `server` | Composition reaches transport | Wire-only in `app` |
| L6 | `pipeline/routing` → `querylang` | Ingestion routing parses query language | Shared filter AST or ingest-only subset |
| L7 | `digester` → `pipeline/ingestion` | Cross-BC digestion | Consolidate in ingestion BC |

**Clean:** `query/` does not import `orchestrator` or `chunk/file`. `pipeline/` does not import `orchestrator`.

## Pattern matrix (wake / retry / purge)

| Pattern | Legitimate use | Compensator abuse |
|---------|----------------|-------------------|
| FSM apply callback | Seal, upload, delete, config | — |
| `notify.Signal` / channel | Wake one goroutine | Drop + periodic rescan |
| gocron scheduled job | Retention policy (hourly/daily rules) | vaultCatchup 20s, placement 15s, peer-cache 30s |
| `time.After` poll | — | WaitVaultReady 100ms, waitForLocalApply 5ms |
| Receipt protocol + ack | Delete durability | + six sweeps every 20s anyway |

See [`04-sweep-compensators.md`](./04-sweep-compensators.md) for full sweep index (001–018).

## Synonym drift (non-test `internal/`)

| Phrase | Hits | Canonical | Hot spots |
|--------|------|-----------|-----------|
| open chunk | 14 | active chunk | `vaultctlfsm/open_chunk.go`, `chunk_progress.go` |
| cloud chunk | ~48 | cloud-backed | `chunk/file/manager.go`, `query/histogram.go` |
| primary (Raft-adjacent) | ~6–8 | leader | `vault_ctl_leader_manager.go` |
| replica_count | 4 | residency / holder (TBD) | `server/vault_chunks.go` |
| system_raft | 0 in Go | cluster_ctl_raft | proto/gen only (audit-api-001) |

## Legacy vs pipeline v3

| Legacy / pre-v3 | v3 canonical | Status |
|-----------------|--------------|--------|
| Store + rotation policy | Vault + retention rules | Coexist; ROADMAP stale |
| Filter sets | Route table / RouteSet | Proto drift (audit-api-004) |
| Direct cm meta authority | vault-ctl FSM + reconciler | Dual path (audit-orch-001) |
| `CmdDeleteChunk` onDelete | Receipt-protocol delete | Dual path (audit-orch-011) |
| Segmentation without vault-ctl receipt | Pipeline v3 holder-gated pull | Partial (gastrolog-hl6cp) |

## Phase 12 artifacts updated

- [`invariant-ledger.md`](../invariant-ledger.md) — extended through cluster/orch/query
- [`coverage.md`](../coverage.md) — phase 12 rows checked
