# Package coverage matrix

Check off when phase audit is complete. Parent epic: **gastrolog-2p313**.

**Status:** 48 / 48 scoped packages reviewed (phases 1–11). Phase 12–13 complete.

| Package | Phase | Memo | Reviewed |
|---------|-------|------|----------|
| `chunk` | 1 | [01-storage.md](./findings/01-storage.md) | [x] |
| `index` | 1 | [01-storage.md](./findings/01-storage.md) | [x] |
| `record` | 1 | [01-storage.md](./findings/01-storage.md) | [x] |
| `manifest` | 1 | [01-storage.md](./findings/01-storage.md) | [x] |
| `blobstore` | 1 | [01-storage.md](./findings/01-storage.md) | [x] |
| `format` | 1 | [01-storage.md](./findings/01-storage.md) | [x] |
| `ingester` | 2 | [02-ingestion.md](./findings/02-ingestion.md) | [x] |
| `digester` | 2 | [02-ingestion.md](./findings/02-ingestion.md) | [x] |
| `tokenizer` | 2 | [02-ingestion.md](./findings/02-ingestion.md) | [x] |
| `pipeline` | 2 | [02-ingestion.md](./findings/02-ingestion.md) | [x] |
| `query` | 3 | [03-query.md](./findings/03-query.md) | [x] |
| `querylang` | 3 | [03-query.md](./findings/03-query.md) | [x] |
| `lookup` | 3 | [03-query.md](./findings/03-query.md) | [x] |
| `cluster` | 4, 6 | [06-cluster.md](./findings/06-cluster.md) | [x] |
| `multiraft` | 4 | [06-cluster.md](./findings/06-cluster.md) | [x] |
| `raftgroup` | 4 | [06-cluster.md](./findings/06-cluster.md) | [x] |
| `raftwal` | 4 | [06-cluster.md](./findings/06-cluster.md) | [x] |
| `vaultraft` | 4 | [06-cluster.md](./findings/06-cluster.md) | [x] |
| `system/raftfsm` | 4 | [06-cluster.md](./findings/06-cluster.md) | [x] |
| `system/raftstore` | 4 | [06-cluster.md](./findings/06-cluster.md) | [x] |
| `system/command` (Raft) | 4 | [06-cluster.md](./findings/06-cluster.md) | [x] |
| `orchestrator` | 5, 6 | [05-orchestration.md](./findings/05-orchestration.md) | [x] |
| `orchestrator/pipeline` | 5 | [05-orchestration.md](./findings/05-orchestration.md) | [x] |
| `server` | 7 | [07-transport.md](./findings/07-transport.md) | [x] |
| `app` | 7 | [07-transport.md](./findings/07-transport.md) | [x] |
| `auth` | 7 | [07-transport.md](./findings/07-transport.md) | [x] |
| `cert` | 7 | [07-transport.md](./findings/07-transport.md) | [x] |
| `frontend` (embed) | 7 | [07-transport.md](./findings/07-transport.md) | [x] |
| `logging` | 8 | [08-observability.md](./findings/08-observability.md) | [x] |
| `alert` | 8 | [08-observability.md](./findings/08-observability.md) | [x] |
| `notify` | 8, 9 | [08-observability.md](./findings/08-observability.md) | [x] |
| `sysmetrics` | 8 | [08-observability.md](./findings/08-observability.md) | [x] |
| `lifecycle` | 8 | [08-observability.md](./findings/08-observability.md) | [x] |
| `glid` | 9 | [09-shared.md](./findings/09-shared.md) | [x] |
| `home` | 9 | [09-shared.md](./findings/09-shared.md) | [x] |
| `convert` | 9 | [09-shared.md](./findings/09-shared.md) | [x] |
| `btree` | 9 | [09-shared.md](./findings/09-shared.md) | [x] |
| `callgroup` | 9 | [09-shared.md](./findings/09-shared.md) | [x] |
| `chanwatch` | 9 | [09-shared.md](./findings/09-shared.md) | [x] |
| `safeutf8` | 9 | [09-shared.md](./findings/09-shared.md) | [x] |
| `units` | 9 | [09-shared.md](./findings/09-shared.md) | [x] |
| `system` (non-Raft) | 11 | [11-system.md](./findings/11-system.md) | [x] |
| `memtest` | 12 | — (test helpers only) | [x] |
| `cmd/gastrolog` | 10 | [10-api-cmd.md](./findings/10-api-cmd.md) | [x] |
| `cmd/compress-assets` | 10 | [10-api-cmd.md](./findings/10-api-cmd.md) | [x] |
| `cmd/multirun` | 10 | [10-api-cmd.md](./findings/10-api-cmd.md) | [x] |
| `cmd/walinspect` | 10 | [10-api-cmd.md](./findings/10-api-cmd.md) | [x] |
| `api/proto` | 10 | [10-api-cmd.md](./findings/10-api-cmd.md) | [x] |
| `api/gen` | 10 | [10-api-cmd.md](./findings/10-api-cmd.md) | [x] |

## Phase 12 — tooling (not yet in matrix above)

| Path | Phase | Reviewed |
|------|-------|----------|
| `backend/justfile` | 12 | [x] |
| `backend/go.mod` | 12 | [x] |
| `backend/ROADMAP.md` | 12 | [x] |
| Cross-cut import graph / invariant ledger | 12 | [x] |
| Phase 13 synthesis (dedupe, stack-rank) | 13 | [x] |

Replication BC (phase 6) is indexed in [06-replication.md](./findings/06-replication.md) — no separate packages.
